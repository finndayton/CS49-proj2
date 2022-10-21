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


//Shashank Implmentation
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
    max_threads_ = num_threads;
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
    std::thread workers[max_threads_];
    int num_tasks_per_thread = num_total_tasks/max_threads_;
    int i = 0;
    int prev = 0;
    int next = num_tasks_per_thread;
    while (i < max_threads_) {
        workers[i] = std::thread(workerThreadStart, runnable, prev, next, num_total_tasks);
        prev = next;
        next = next + num_tasks_per_thread; 
        i += 1;
    }

    // join worker threads 
    for (int i = 0; i < max_threads_; i++) {
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
    // printf("thread %d is starting\n", thread_id);
    while (!instance->done) {
        instance->task_queue_mutex->lock(); // common mutex for the class
        // printf("instance->task_queue.size(): %d\n", instance->task_queue.size());
        // printf("debug: %d\n", instance->debug);
        if (instance->task_queue.size() > 0) {
            
            // acquire mutex and then pop_back
            if (instance->task_queue.size() == 0) break;

            printf("thread %d successfully acquired the lock\n", thread_id);
            Task task = instance->task_queue.front();
            // printf("    to try to run task %d\n", task.task_id);
            // int task_id = task_queue->front();
            printf("    to take up task %d\n", task.task_id);
            instance->task_queue.pop();
            instance->debug--;
            printf("Now tasks_queue is %ld elements long\n", instance->task_queue.size());

            // parent thread tries to check queue.size() here

            // busy_threads++;
            instance->task_queue_mutex->unlock();
            // does it release the mutex now? it should
            // do something with task
            auto runnable = task.runnable;
            // printf("%d", task_id);
            auto num_total_tasks = task.num_total_tasks;
            runnable->runTask(task.task_id, num_total_tasks);
            // busy_threads--;
        } else {
            instance->task_queue_mutex->unlock();
        }
        // printf("thead %d is stuck here while task_queue size = %ld\n", thread_id, instance->task_queue.size());
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    max_threads = num_threads;
    task_queue_mutex = new std::mutex();
    done = false;
    debug = 0;

    // busy_threads = 0;
    // printf("mkaing thread pool");
    // makeThreadPool();
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

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    done = true;
    delete task_queue_mutex;
    killThreadPool();
}


// what we know: run() is called once and fucks up on the first call. spins. 
// task_queue in main thread is not zero?
void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // add tasks to queue, if queue is full, wait until there is space
    printf("run() called with num_total_tasks = %d", num_total_tasks);
    for (int i = 0; i < num_total_tasks; i++) {
        Task task = {runnable, i, num_total_tasks};
        task_queue.push(task);
        printf("run called! incrementing debug from %d to %d\n", debug, debug + 1);
        debug++;
    }
    printf("just created the task queue. It has %d tasks in it\n", task_queue.size());
    // return;
    if (workers.size() == 0) {
        makeThreadPool();
    } 

    printf("debug: %d\n", debug);
    printf("Hello\n");
    while (task_queue.size() > 0) {
        // printf("line 191 spinning. Task_queue size: %d\n", task_queue.size());
        printf("line 191 spinning\n");
    }
    printf("end of run reached\n");

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
