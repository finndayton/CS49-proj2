#include "tasksys.h"
#include <stdio.h>
#include <thread>


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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
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
