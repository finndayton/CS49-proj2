#include "tasksys.h"
#include <cstdio>

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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    max_threads_ = num_threads;
}

static inline void thread_worker_function(IRunnable* runnable, int thread_id, int num_threads, int num_total_tasks) {
    int tasks_per_thread = num_total_tasks / num_threads;

    // task_id should be from 0 to num_total_tasks - 1
    int task_id_start = thread_id * tasks_per_thread;
    int task_id_end = task_id_start + tasks_per_thread;
    
    /* 
    Case where num_total_tasks % num_threads != 0
    Example: num_total_tasks = 15
             num_threads     = 8
    In this case, we launch 8 threads. The 8 thread worker functions receive thread_ids {0, 1, 2...7}
    But, tasks_per_thread for each thread worker is only 1 (15 / 8 = 1). This means the last thread, thread_id = 7,
    is going to have to pick up the slack and, instead of just completing the 8th task, do task 8, 9, 10...15. That's right. 
    This means that if (thread_id == num_threads - 1) AND num_total_tasks % num_threads != 0, we must update 
    task_id_end to include tasks 9, 10, 11, 12...15. (7 more tasks). This is num_total_tasks % num_threads MORE tasks 
    */ 

    if (thread_id == num_threads - 1 && num_total_tasks % num_threads != 0) {
        task_id_end += num_total_tasks % num_threads;
    }
    for (int i = task_id_start; i < task_id_end; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // printf("max_threads_: %d\n", max_threads_);


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A. The implementation provided below runs all
    // tasks sequentially on the calling thread.

    // Make up to std::min(num_threads,  num_total_tasks) number of threads
    // each thread gets a certain contiguous chunk of the total work.

    // runnable is, for example, a PingPongTask class instance, which extends IRunnable. 
    // PingPongTask's implementation of runTask() is to fill an output array, from an input array, etc. 
    // For this super_light test, our run() function is called 400 times (400 bulk task launches)
    // with each launch containing 64 tasks to be done. num_elements = 32 * 1024;
    // and n_iters = 32. We and to parallelize these 64 tasks using num_thread threads. 
    // 

    int num_threads = std::min(max_threads_, num_total_tasks);
    std::thread workers[num_threads];
    for (int i = 0; i < num_threads; i++) {
        workers[i] = std::thread(thread_worker_function, runnable, i, num_threads, num_total_tasks);
    }

    // handle case where (num_total_tasks / num_threads) is something like 258 / 8 = 32 tasks for
    // the 8 threads + 2 tasks left over. This means the 8th thread (i == 7) 

    for (int i = 0; i < num_threads; i++) {
        workers[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
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

void workerThreadFunc(
    TaskSystemParallelThreadPoolSpinning* instance, 
    int thread_id
) {    
    while (!instance->done) {

        instance->task_queue_mutex->lock(); // common mutex for the class

        if (instance->task_queue.size() > 0) {
            instance->busy_threads++;
            Task task = instance->task_queue.front();
            instance->task_queue.pop();

            instance->task_queue_mutex->unlock();

            auto runnable = task.runnable;
            auto num_total_tasks = task.num_total_tasks;
            // printf("runTask(task.task_id = %d, num_total_tasks = %d);\n", task.task_id, num_total_tasks);
            runnable->runTask(task.task_id, num_total_tasks);


            instance->busy_threads--;

        } else {
            instance->task_queue_mutex->unlock();
        }
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    max_threads = num_threads;
    task_queue_mutex = new std::mutex();
    done = false;
    busy_threads = 0;
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // printf("entering destructor\n");
    done = true;
    killThreadPool();
    delete task_queue_mutex;
}

void TaskSystemParallelThreadPoolSpinning::makeThreadPool() {
    for (int i = 0; i < max_threads; i++) {
        workers.push_back(std::thread(&workerThreadFunc, this, i));
    }
}

void TaskSystemParallelThreadPoolSpinning::killThreadPool() {
    for (int i = 0; i < max_threads; i++) {
        // make threads, and make them free to start off with
        workers[i].join();   
    }
}


void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        Task task = {runnable, i, num_total_tasks}; //task_id is set to i {0, 1, 2, ... , num_total_tasks - 1}
        task_queue.push(task);
    }
    if (workers.size() == 0) {
        makeThreadPool();
    } 

    while (true) {
        task_queue_mutex->lock();
        if (task_queue.size() == 0 && busy_threads == 0) {
            task_queue_mutex->unlock();
            // printf("leaving run\n");
            return;
        } else {
            task_queue_mutex->unlock();
        }
    }
}


TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    condition_variable_ = new std::condition_variable();
    mutex_ = new std::mutex();
    busy_threads = 0;
    done = false;
    max_threads = num_threads;
}


void workerThreadFuncSleeping(
    TaskSystemParallelThreadPoolSleeping* instance, 
    int thread_id
) {
    while (!instance->done) {
        // how do we know its time to kill the thread?
        // a lock must be held in order to wait on a condition variable
        // always awoken because of notify_all from main thread, which is fine
        std::unique_lock<std::mutex> lk(*(instance->mutex_));
        instance->condition_variable_->wait(lk);
        // lock is now re-acquired
        // do the work in the critical section
        instance->busy_threads++;
        Task task = instance->task_queue.front();
        instance->task_queue.pop();
        lk.unlock();
        // do actual run
        auto runnable = task.runnable;
        auto num_total_tasks = task.num_total_tasks;
        runnable->runTask(task.task_id, num_total_tasks);
        instance->busy_threads--;
    }
}

void TaskSystemParallelThreadPoolSleeping::makeThreadPool() {
    for (int i = 0; i < max_threads; i++) {
        workers.push_back(std::thread(&workerThreadFuncSleeping, this, i));
    }
}

void TaskSystemParallelThreadPoolSleeping::killThreadPool() {
    for (int i = 0; i < max_threads; i++) {
        // make threads, and make them free to start off with
        workers[i].join();   
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    done = true;
    killThreadPool();
    delete condition_variable_;
    delete mutex_;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        Task task = {runnable, i, num_total_tasks}; //task_id is set to i {0, 1, 2, ... , num_total_tasks - 1}
        task_queue.push(task);
    }
    if (workers.size() == 0) {
        makeThreadPool();
    } 


    // signalling thread must spin until all tasks are done
    mutex_->lock();
    while (task_queue.size() > 0 || busy_threads > 0) {
        mutex_->unlock();
        printf("task queue now has %ul tasks\n", task_queue.size());
        // release the mutex before calling notify_all to make sure waiting threads have a chance 
        // to make progress
        condition_variable_->notify_all();
        // reacquire lock
        mutex_->lock();
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
