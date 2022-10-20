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
    printf("running serial now\n");
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
    // TODO: SHASHANK assuming that max_threads refers to worker threads only not main thread
    max_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::workerThreadStart(IRunnable* runnable, int start, int end, int num_total_tasks) {
    // function called by worker thread
    // loop from start to end inclusive
    for (int i = start; i < end; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    printf("Running always spawn\n");
    // static assignment is easiest
    std::thread workers[max_threads];
    int num_tasks_per_thread = num_total_tasks/max_threads;
    int i = 0;
    int prev = 0;
    int next = num_tasks_per_thread;
    while (i < max_threads) {
        workers[i] = std::thread(workerThreadStart, runnable, prev, next, num_total_tasks);
        prev = next;
        next = next + num_tasks_per_thread; 
        i += 1;
    }

    // join worker threads 
    for (int i = 0; i < max_threads; i++) {
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    
    max_threads = num_threads;
    queue_mutex = new std::mutex();
    // printf("mkaing thread pool");
    // makeThreadPool();
}

void TaskSystemParallelThreadPoolSpinning::makeThreadPool() {
    for (int i = 0; i < max_threads; i++) {
        // make threads, and make them free to start off with
        printf("spawinging thread");
        workers.push_back(std::thread(workerThreadStart, queue, queue_mutex, free_threads));
        // workers.push_back(new_thread);
        free_threads++;
    }
}

void TaskSystemParallelThreadPoolSpinning::killThreadPool() {
    for (unsigned int i = 0; i < workers.size(); i++) {
        // make threads, and make them free to start off with
        workers[i].join();   
        free_threads++;
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    delete queue_mutex;
}

void TaskSystemParallelThreadPoolSpinning::workerThreadStart(
    std::vector<ThreadPoolQueueMember*> queue,
    std::mutex* queue_mutex, 
    std::atomic<int>* free_threads
) {
    // need to pass in the lock as well? 
    // why can't a thread just 
    // queue is not defined here
    while (true) {
        if (queue.empty()) {
            // do nothing
        } else {
            // acquire mutex and then pop_back
            queue_mutex->lock(); // common mutex for the class
            auto item = queue.back();
            queue.pop_back();
            free_threads--;
            queue_mutex->unlock();
            // does it release the mutex now? it should
            // do something with item
            auto runnable = item->runnable;
            auto i = item->thread_id;
            auto num_total_tasks = item->num_total_tasks;
            runnable->runTask(i, num_total_tasks);
            free_threads++;
        }
    }

}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // add tasks to queue, if queue is full, wait until there is space
    // return;
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
    // printf("starting spinning");
    // for (int i = 0; i < num_total_tasks; i++) {
    //     // while (true) {
    //         if ((int) queue.size() > max_threads) {
    //             // wait for the queue to get shorter
    //             continue;
    //         } else {
    //             // add to queue, we know there is a free thread that'll run this
    //             ThreadPoolQueueMember item = {runnable, i, num_total_tasks};
    //             queue.push_back(&item);
    //             break;
    //         }
    //     // }
    // }

    
    // we are done with looping through all of them
    // wait until the queue is empty
    // wait until all the threads are "free"
    // atomic int that stores number of free threads
    // then call sync()???? the instructions don't say to do that
    // check the number of free threads is max threads and that queue is empty 
    // while (free_threads != 0 || queue.size() != 0) {
    //     // wait 
    // }
    // return sync();
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
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
