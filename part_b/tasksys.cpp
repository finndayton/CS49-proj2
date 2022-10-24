#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation for Async
 * ================================================================
 */

// forward decl for workerThreadFunc
void workerThreadFunc(TaskSystemParallelThreadPoolSleeping* task_system, int thread_id);

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    max_threads = num_threads;
    
    ready_btl_map_mutex = new std::mutex();
    ready_task_queue_mutex = new std::mutex();
    waiting_btl_vec_mutex = new std::mutex();

    // initialize condition variables
    ready_btl_map_cv = new std::condition_variable();
    ready_task_queue_cv = new std::condition_variable();
    waiting_btl_vec_cv = new std::condition_variable();

    busy_threads = 0;
    done = false;

    makeThreadPool();
    
}

void TaskSystemParallelThreadPoolSleeping::makeThreadPool() {
    for (int i = 0; i < max_threads; i++) {
        workers.push_back(std::thread(&workerThreadFunc, this, i));
    }
}

void TaskSystemParallelThreadPoolSleeping::killThreadPool() {
    // I don't think we need a lock here, because we are the only thread
    // modifying the done variable
    done = true;
    ready_task_queue_cv->notify_all();
    for (int i = 0; i < max_threads; i++) {
        workers[i].join();   
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    killThreadPool();
    delete ready_btl_map_cv;
    delete ready_task_queue_cv;
    delete waiting_btl_vec_cv;
    delete ready_btl_map_mutex;
    delete ready_task_queue_mutex;
    delete waiting_btl_vec_mutex;
}


void workerThreadFunc(
    TaskSystemParallelThreadPoolSleeping* instance, 
    int thread_id
) {
    while (true) {
        std::unique_lock<std::mutex> lk(*(instance->ready_task_queue_mutex)); //Declare condition variable and aquire lock on ready_task_queue
        // did we need a lock for the done variable?
        while(!instance->done && instance->ready_task_queue.size() == 0) {
            instance->ready_task_queue_cv->wait(lk); //
        }
        if (instance->done) {
            return;
        }

        // do the work in the critical section
        instance->busy_threads++;
        SubTask subtask = instance->ready_task_queue.front();
        instance->ready_task_queue.pop();
        lk.unlock();
        
        instance->ready_task_queue_cv->notify_all();
        
        // do actual run
        auto runnable = subtask.runnable;
        auto num_total_sub_tasks = subtask.num_total_sub_tasks;
        runnable->runTask(subtask.sub_task_id, num_total_sub_tasks);
        instance->finishedSubTask(subtask); // does the postprocessing 
        instance->busy_threads--; 

    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelThreadPoolSleeping::readyBtl(Task btl) {
    // pushes a BTL onto the ready_btl_map and all its subtasks onto the ready_task_queue
    // also pops it from the waiting_btl_queue
    // definitely lock for this shared resource access
    ready_btl_map[btl.task_id] = btl; //book keeping map
    for (int i = 0; i < btl.num_total_sub_tasks; i++) {
        // create a subtask object for each
        ready_task_queue.push({btl.runnable, i, btl.num_total_sub_tasks, btl.task_id});
    }
    // waiting_btl_set.erase(btl);
    // find index to delete and remove the btl
    for (int i = 0; i < waiting_btl_vec.size(); i++) {
        if (i == btl.task_id){
            waiting_btl_vec.erase(waiting_btl_vec.begin() + i);
            break;
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::finishedSubTask(SubTask subtask) {
    // when you finish a sub task, you need to increment the number of finished sub tasks
    Task papa_task = ready_btl_map[subtask.btl_task_id];
    papa_task.num_finished_sub_tasks++;
    if (papa_task.num_finished_sub_tasks == papa_task.num_total_sub_tasks) {
        TaskID task_id = papa_task.task_id;
        // loop through every BTL in the waiting_btl_queue
        // remove the papa_task TaskID from the dependencies set of every BTL 
        // in the waiting_btl_queue (if it exists)
        // for each BTL in the waiting_btl_queue, if the dependencies set is empty, push it onto the ready_btl_queue
        // and push its subtasks onto the ready_task_queue
        for (auto btl : waiting_btl_vec) {
            if (btl.dependencies.count(task_id) > 0) {
                btl.dependencies.erase(task_id);
            }
            if (btl.dependencies.size() == 0) {
                readyBtl(btl);
            }
        }
        // remove the papa task from the ready_btl_map
        ready_btl_map.erase(task_id);
    }
}

// TODO: 
// Fix compiler errors. Don't hashing for TaskID / Write hash function for sets
// One mutex per resource. 
// Implement sync() to block until all threadFunctions are done runnong. 

/**
 * runAsyncWithDeps is never going to be called from multiple threads at the same time.
 * We append a btl to the waiting_btl_queue and return the task id.
*/
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    // add BTL to the waiting queue
    std::unordered_set<TaskID> deps_as_set;
    for (auto dep : deps) {
        deps_as_set.insert(dep);
    }

    Task task {runnable, 0, num_total_tasks, cur_task_id, deps_as_set};
    // lock here - shared resource
    // waiting_btl_set.insert(task);
    waiting_btl_vec.push_back(task);
    // unlock

    cur_task_id++;
    return cur_task_id - 1;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    // wait for all tasks to finish - this is a blocking call
    // so no need to put the thread to sleep? Actually that's still a good idea becasue you don't want this guy to be 
    // taking up resources while it's waiting for the other threads to finish
    //  means we check
    // 1. if there are any tasks in the ready queue
    // 2. if there are any BTLs in the waiting queue
    // 3. busy threads is 0
    // Put sync to sleep until these conditions are satisfied using the 3 condition vars declared int he header. 
    while (true) {
        // do nothing
    }
}
