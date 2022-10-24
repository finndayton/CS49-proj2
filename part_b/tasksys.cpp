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

    threads_mutex = new std::mutex();
    sync_mutex = new std::mutex();

    // initialize condition variables
    // ready_btl_map_cv = new std::condition_variable();
    // ready_task_queue_cv = new std::condition_variable();
    // waiting_btl_vec_cv = new std::condition_variable();
    // condition_variable_ = new std::condition_variable();

    cur_task_id = 0;

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
    delete threads_mutex;
    delete sync_mutex;
}


void workerThreadFunc(
    TaskSystemParallelThreadPoolSleeping* instance, 
    int thread_id
) {
    while (true) {
        std::unique_lock<std::mutex> lk(*(instance->threads_mutex)); //Declare condition variable and aquire lock on ready_task_queue
        // did we need a lock for the done variable?
        while(!instance->done && instance->ready_task_queue.size() == 0) {
            instance->threads_cv->wait(lk); //awoken by notify all. lock held when awoken.
            printf("thread %d just woke up\n", thread_id);
        }

        if (instance->done) {
            return;
        }

        // do the work in the critical section
        instance->busy_threads++; //guarded by lock (lk) held on ready_task_queue_mutex
        SubTask subtask = instance->ready_task_queue.front();
        instance->ready_task_queue.pop();
        lk.unlock();
        
        instance->threads_cv->notify_all();    //wake up other threads sleeping to wait(). 
        // instance->condition_variable_->notify_all(); //wake up other threads sleeping to wait(). 
        
        // do actual run
        auto runnable = subtask.runnable;
        auto num_total_sub_tasks = subtask.num_total_sub_tasks;
        runnable->runTask(subtask.sub_task_id, num_total_sub_tasks);
        instance->finishedSubTask(subtask); // does the postprocessing 
        instance->busy_threads--; //need a lock on this, no?

    }
}

// Remove BTL from waiting_btl_vec, add to ready_btl_map and add sub_tasks to ready_task_queue
void TaskSystemParallelThreadPoolSleeping::readyBtl(Task btl) {
    // pushes a BTL onto the ready_btl_map and all its subtasks onto the ready_task_queue
    // also removes it from  waiting_btl_vec
    // definitely lock for this shared resource access
    printf("readyBtl\n");
    threads_mutex->lock();
    printf("244\n");
    ready_btl_map[btl.task_id] = btl; //book keeping map
    printf("btl task_id: %d, btl.num_total_sub_tasks: %d\n", ready_btl_map[btl.task_id].task_id, btl.num_total_sub_tasks);

    for (int i = 0; i < btl.num_total_sub_tasks; i++) {
        // create a subtask object for each
        printf("i: %d\n", i);
        ready_task_queue.push({btl.runnable, i, btl.num_total_sub_tasks, btl.task_id});
    }

    // remove btl from waiting_btl_vec
    printf("waiting_btl_vec.size() before: %d\n", waiting_btl_vec.size());
    for (size_t i = 0; i < waiting_btl_vec.size(); i++) {
        if (waiting_btl_vec[i].task_id == btl.task_id){
            waiting_btl_vec.erase(waiting_btl_vec.begin() + i);
            break;
        }
    }
    printf("waiting_btl_vec.size() after: %d\n", waiting_btl_vec.size());
    threads_mutex->unlock();

    threads_cv->notify_all();  //wake up all the threads waiting on ready_task_queue_cv.size() because we just changed it!
}

void TaskSystemParallelThreadPoolSleeping::finishedSubTask(SubTask subtask) {
    // when you finish a sub task, you need to increment the number of finished sub tasks
    TaskID papa_task_id = subtask.btl_task_id;

    threads_mutex->lock();
    ready_btl_map[papa_task_id].num_finished_sub_tasks++; // bookkeeping map of BTLs 

    printf("BTL finished_sub_tasks: %d. (total = %d)\n", ready_btl_map[papa_task_id].num_finished_sub_tasks, ready_btl_map[papa_task_id].num_total_sub_tasks);

    if (ready_btl_map[papa_task_id].num_finished_sub_tasks == ready_btl_map[papa_task_id].num_total_sub_tasks) { 
        // if this was the last subtask in the btl (papa task). Papa is done. 
        // loop through every BTL in the waiting_btl_queue and 
        // remove the papa_task TaskID from the dependencies set of every BTL 
        // in the waiting_btl_queue (if it exists)
        // If this zeros our the dependency set of a BTL in the waiting_btl_queue, remove it and push it to the ready_btl_queue
        // AND push its subtasks onto the ready_task_queue
        for (auto btl : waiting_btl_vec) {
            if (btl.dependencies.count(papa_task_id) > 0) { //if this BTL listed this completed task_id as a dependency
                btl.dependencies.erase(papa_task_id);       //remove this task_id as dependency
            }
            if (btl.dependencies.size() == 0) {        //if that made this BTL have 0 dependencies
                printf("btl.dependencies.size() = %d. about to call readyBtl\n", btl.dependencies.size());
                readyBtl(btl);                         //add it to readyBtl map and add its subtasks to the ready task queue 
            }
        }
        // remove the papa task from the ready_btl_map
        printf("290\n");
        ready_btl_map.erase(papa_task_id); //papa_task is DONE running
        threads_mutex->unlock();

        sync_cv->notify_all(); //it could be the case that this was the last subtask in the entire runAsync(). 

    } else {
        threads_mutex->unlock();
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

    Task task = Task{runnable, 0, num_total_tasks, cur_task_id, deps_as_set};
    // lock here - shared resource
    // waiting_btl_set.insert(task);

    if (deps_as_set.size() == 0) { //if this task happens to have no dependencies, add it straight to ready_btl_map and ready_task_queue
        readyBtl(task);
    } else {
        threads_mutex->lock();
        waiting_btl_vec.push_back(task);
        threads_mutex->unlock();
    }
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
    // Put sync to sleep until these conditions are satisfied using the 3 condition vars declared in the header. 


    // A lock must be held in order to wait on a condition variable.
    // This lock is atomically released before the thread goes to sleep
    // when `wait()` is called. The lock is atomically re-acquired when
    // the thread is woken up using `notify_all()`.
    while (true) {
        std::unique_lock<std::mutex> lk(*threads_mutex); //deadlocking here. 

        while (ready_task_queue.size() > 0) {
            sync_cv->wait(lk);
        }

        if (busy_threads == 0 && waiting_btl_vec.size() == 0) {
            printf("got here\n");
            lk.unlock();
            return;
        } else {
            lk.unlock();
            // threads_cv->notify_all();
        }
    }
}


//condition variable1 to wake up / put to sleep the thread worker functions. Same as in part_a sleeping. 
//mutex 1. general mutex to guard accesses to shared data in the class 

//condition variable2 to wake sync up. called by worker threads. 
//mutex2 to use in conjunction with cond. var. 2. 