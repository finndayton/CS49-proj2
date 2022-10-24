#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation for Async
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    max_threads = num_threads;
    
    ready_btl_map_mutex = new std::mutex();
    ready_task_queue_mutex = new std::mutex();
    waiting_btl_set_mutex = new std::mutex();

    // initialize condition variables
    ready_btl_map_cv = new std::condition_variable();
    ready_task_queue_cv = new std::condition_variable();
    waiting_btl_set_cv = new std::condition_variable();

    // initialize data structures
    waiting_btl_set = new std::unordered_set<Task>;

    busy_threads = 0;
    done = false;

    makeThreadPool();
    
}

void TaskSystemParallelThreadPoolSleeping::makeThreadPool() {
    for (int i = 0; i < max_threads; i++) {
        workers.push_back(std::thread(&workerThreadFuncSleeping, this, i));
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
    delete waiting_btl_set_cv;
    delete ready_btl_map_mutex;
    delete ready_task_queue_mutex;
    delete waiting_btl_set_mutex;
}


void workerThreadFunc(
    TaskSystemParallelThreadPoolSleeping* instance, 
    int thread_id
) {
    while (true) {
        std::unique_lock<std::mutex> lk(*(instance->ready_task_queue_mutex));
        // did we need a lock for the done variable?
        while(!instance->done && instance->ready_task_queue.size() == 0) {
            instance->ready_task_queue_cv->wait(lk);
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
        finishedSubTask(subtask); // does the postprocessing 
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
    ready_btl_map[btl.task_id] = btl;
    int btl_index = ready_btl_list.size() - 1;
    for (int i = 0; i < btl.num_subtasks; i++) {
        // create a subtask object for each
        SubTask subtask = {btl.runnable, i, btl.num_total_subtasks, btl.task_id}
        ready_task_queue.push(subtask);
    }
    waiting_btl_set.remove(btl);
}

void TaskSystemParallelThreadPoolSleeping::finishedSubTask(SubTask subtask) {
    // when you finish a sub task, you need to increment the number of finished sub tasks
    Task* papa_task = ready_btl_map[subtask.btl_task_id];
    papa_task->num_finished_subtasks++;
    if (papa_task->num_finished_subtasks == papa_task->num_total_subtasks) {
        TaskID task_id = papa_task->task_id;
        // loop through every BTL in the waiting_btl_queue
        // remove the papa_task TaskID from the waiting_for set of every BTL 
        // in the waiting_btl_queue (if it exists)
        // for each BTL in the waiting_btl_queue, if the waiting_for set is empty, push it onto the ready_btl_queue
        // and push its subtasks onto the ready_task_queue
        for (auto task : waiting_btl_set) {
            if (task->waiting.count(task_id) > 0) {
                task->waiting.erase(task_id);
            }
            if (task->waiting.size() == 0) {
                readyBtl(task);
            }
        }
        // remove the papa task from the ready_btl_map
        ready_btl_map.erase(papa_task);
    }
}

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

    Task task = {runnable, 0, num_total_tasks, cur_task_id, deps_as_set};
    // lock here - shared resource
    waiting_btl_set.insert(task);
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
    while (true) {
        // do nothing
    }
}
