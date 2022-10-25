#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}


/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation for Async
 * ================================================================
 */

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

// forward decl for workerThreadFunc
void workerThreadFunc(TaskSystemParallelThreadPoolSleeping* task_system, int thread_id);

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    max_threads = num_threads;
    
    // ready_btl_map_mutex = new std::mutex();
    // ready_task_queue_mutex = new std::mutex();
    // waiting_btl_vec_mutex = new std::mutex();
    // num_finished_sub_tasks_mutex = new std::mutex();
    sync_cv = new std::condition_variable;
    threads_cv = new std::condition_variable;

    general_mutex = new std::mutex();
    sync_mutex = new std::mutex();

    // initialize condition variables
    // ready_btl_map_cv = new std::condition_variable();
    // ready_task_queue_cv = new std::condition_variable();
    // waiting_btl_vec_cv = new std::condition_variable();
    // condition_variable_ = new std::condition_variable();

    cur_task_id = 0;
    // busy_threads = 0;

    total_sub_tasks = 0;
    completed_sub_tasks = 0;

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
    threads_cv->notify_all();
    // condition_variable->notify_all();
    for (int i = 0; i < max_threads; i++) {
        workers[i].join();   
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    killThreadPool();
    // delete ready_btl_map_cv;
    // delete ready_task_queue_cv;
    // delete waiting_btl_vec_cv;
    // delete condition_variable_;
    // delete ready_btl_map_mutex;
    // delete ready_task_queue_mutex;
    // delete waiting_btl_vec_mutex;
    // delete num_finished_sub_tasks_mutex;

    delete sync_cv;
    delete threads_cv;
    delete general_mutex;
    delete sync_mutex;
}


void workerThreadFunc(
    TaskSystemParallelThreadPoolSleeping* instance, 
    int thread_id
) {
    while (true) {
        std::unique_lock<std::mutex> lk(*(instance->general_mutex)); //Declare condition variable and aquire lock on ready_task_queue
        // did we need a lock for the done variable?
        while(!instance->done && instance->ready_task_queue.size() == 0) { //while there's no work yet
            instance->threads_cv->wait(lk); //awoken by notify all. lock held when awoken.
            printf("thread %d just woke up. condition is %d\n", thread_id, instance->ready_task_queue.size() == 0);
        }
        printf("thread %d exited the loop because condition is %d\n", thread_id, instance->ready_task_queue.size() == 0);

        if (instance->done) {
            return;
        }

        // do the work in the critical section
        // instance->busy_threads++; //guarded by lock (lk) held on ready_task_queue_mutex
        SubTask subtask = instance->ready_task_queue.front();
        instance->ready_task_queue.pop();
        lk.unlock();
        
        // instance->threads_cv->notify_all();    //wake up other threads sleeping to wait(). 
        printf("thread %d just called notify all\n");

        // do actual run
        auto runnable = subtask.runnable;
        auto num_total_sub_tasks = subtask.num_total_sub_tasks;
        runnable->runTask(subtask.sub_task_id, num_total_sub_tasks);

        instance->completed_sub_tasks++; //atomically increment the completed sub tasks

        instance->finishedSubTask(subtask); // does the postprocessing 

    }
}

// Remove BTL from waiting_btl_vec, add to ready_btl_map and add sub_tasks to ready_task_queue
void TaskSystemParallelThreadPoolSleeping::readyBtl(Task btl) {
    // printf("readyBtl\n");
    general_mutex->lock();
    // printf("readyBtl after lock has been acquired\n");
    ready_btl_map[btl.task_id] = btl; //book keeping map

    // add subtasks to ready_task_queue
    for (int i = 0; i < btl.num_total_sub_tasks; i++) {
        // printf("i: %d\n", i);
        ready_task_queue.push({btl.runnable, i, btl.num_total_sub_tasks, btl.task_id});
    }

    // remove btl from waiting_btl_vec
    // printf("waiting_btl_vec.size() before: %d\n", waiting_btl_vec.size());
    for (size_t i = 0; i < waiting_btl_vec.size(); i++) {
        if (waiting_btl_vec[i].task_id == btl.task_id){
            waiting_btl_vec.erase(waiting_btl_vec.begin() + i);
            break;
        }
    }
    // printf("waiting_btl_vec.size() after: %d\n", waiting_btl_vec.size());
    general_mutex->unlock();
    // printf("calling notify_all in readyBtl()\n");
    threads_cv->notify_all();  //wake up all the threads waiting on ready_task_queue_cv.size() because we just changed it!
}

void TaskSystemParallelThreadPoolSleeping::finishedSubTask(SubTask subtask) {
    // when you finish a sub task, you need to increment the number of finished sub tasks
    TaskID papa_task_id = subtask.btl_task_id;

    general_mutex->lock();
    ready_btl_map[papa_task_id].num_finished_sub_tasks++; // bookkeeping map of BTLs 

    // printf("BTL finished_sub_tasks: %d. (total = %d)\n", ready_btl_map[papa_task_id].num_finished_sub_tasks, ready_btl_map[papa_task_id].num_total_sub_tasks);

    // was this the last subtask in BTL?
    if (ready_btl_map[papa_task_id].num_finished_sub_tasks == ready_btl_map[papa_task_id].num_total_sub_tasks) { 
        // maybe this was not only last subtask in papa, but the VERY last one in the entire runAsync().
        // printf("if %d == %d\n", ready_btl_map[papa_task_id].num_finished_sub_tasks, ready_btl_map[papa_task_id].num_total_sub_tasks);
        //loop through all other waiting btls. 
        for (auto btl : waiting_btl_vec) {
            if (btl.dependencies.count(papa_task_id) > 0) { //if this BTL listed this completed task_id as a dependency
                btl.dependencies.erase(papa_task_id);       //remove this task_id as dependency
            }
            if (btl.dependencies.size() == 0) {        //if that made this other random BTL have 0 dependencies
                // printf("btl.dependencies.size() = %d. about to call readyBtl\n", btl.dependencies.size());
                general_mutex->unlock();
                readyBtl(btl);                         //add it to readyBtl map and add its subtasks to the ready task queue 
                general_mutex->lock();
            }
        }
        // printf("finished inner for loop");
        // remove the papa task from the ready_btl_map
        ready_btl_map.erase(papa_task_id);
        // printf("about to call sync_cv->notify_all()\n");
        sync_cv->notify_all();  
        general_mutex->unlock();

    } else {
        general_mutex->unlock();
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
    std::unordered_set<TaskID> deps_as_set;
    for (TaskID dep : deps) {
        deps_as_set.insert(dep);
    }

    Task task = Task{runnable, 0, num_total_tasks, cur_task_id, deps_as_set};
    // lock here - shared resource
    // waiting_btl_set.insert(task);
    
    total_sub_tasks += num_total_tasks;

    if (deps_as_set.size() == 0) { //if this task happens to have no dependencies, add it straight to ready_btl_map and ready_task_queue
        readyBtl(task);
    } else {
        general_mutex->lock();
        waiting_btl_vec.push_back(task); // add BTL to the waiting queue
        general_mutex->unlock();
    }

    cur_task_id++;
    return cur_task_id - 1;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lk(*sync_mutex); //deadlocking here. 

    while (total_sub_tasks != completed_sub_tasks) {
        printf("right about to sleep. total_sub_tasks: %u, completed_sub_tasks: %u\n", total_sub_tasks.load(), completed_sub_tasks.load());
        sync_cv->wait(lk);
    }
        //reset these!
        total_sub_tasks = 0;
        completed_sub_tasks = 0;
        cur_task_id = 0;
}


//condition variable1 to wake up / put to sleep the thread worker functions. Same as in part_a sleeping. 
//mutex 1. general mutex to guard accesses to shared data in the class 

//condition variable2 to wake sync up. called by worker threads. 
//mutex2 to use in conjunction with cond. var. 2. 