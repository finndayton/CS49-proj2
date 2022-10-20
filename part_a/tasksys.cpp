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
    busy_threads = 0;
    // printf("mkaing thread pool");
    // makeThreadPool();
}

void TaskSystemParallelThreadPoolSpinning::makeThreadPool() {
    for (int i = 0; i < max_threads; i++) {
        workers.push_back(std::thread(workerThreadFunc, queue, queue_mutex, busy_threads));
    }
}

void TaskSystemParallelThreadPoolSpinning::killThreadPool() {
    for (unsigned int i = 0; i < workers.size(); i++) {
        // make threads, and make them free to start off with
        workers[i].join();   
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    delete queue_mutex;
}

void TaskSystemParallelThreadPoolSpinning::workerThreadFunc(
    std::vector<Task*> queue,
    std::mutex* queue_mutex, 
    std::atomic<int>* busy_threads
) {
    while (true) {
        if (queue.empty()) {
            // do nothing
            // printf("queue is empty, doing nothing!");
            // printf("queue empty");
            // if we are asked to terminate, end it 
        } else {
            // acquire mutex and then pop_back
            queue_mutex->lock(); // common mutex for the class
            printf("acquired lock");
            auto item = queue.back();
            printf("took up a task!");
            queue.pop_back();

            // parent thread tries to check queue.size() here

            busy_threads++;
            queue_mutex->unlock();
            // does it release the mutex now? it should
            // do something with item
            auto runnable = item->runnable;
            auto i = item->thread_id;
            auto num_total_tasks = item->num_total_tasks;
            runnable->runTask(i, num_total_tasks);
            busy_threads--;
        }
    }

}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // add tasks to queue, if queue is full, wait until there is space
    // return;
    if (workers.size() == 0) {
        printf("making thread pool!");
        makeThreadPool();
        printf("made thread pool with %d threads", workers.size());
    } else {
        printf("thread pool has %d threads", workers.size());
    }

    for (int i = 0; i < num_total_tasks; i++) {
        Task task = {runnable, i, num_total_tasks};
        queue.push_back(&task);
    }
    
    
    while (busy_threads != 0 || queue.size() != 0) {
        // wait 
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
