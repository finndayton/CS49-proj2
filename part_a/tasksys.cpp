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
    int task_id = thread_id;
    while (task_id < num_total_tasks) {
        runnable->runTask(task_id, num_total_tasks);
        task_id += num_threads;
    }
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    std::thread workers[max_threads_];
    // keep a global hashmap of tasks, each individual thread can pick up whatever it wants to
    // once done with its own work?
    for (int i = 0; i < max_threads_; i++) {
        workers[i] = std::thread(thread_worker_function, runnable, i, max_threads_, num_total_tasks);
    }
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
    while (!instance->done) {
        std::unique_lock<std::mutex> lk(*(instance->task_queue_mutex));
        if (instance->task_queue.size() > 0) {
            instance->busy_threads++;
            Task task = instance->task_queue.front();
            instance->task_queue.pop();
            lk.unlock();

            auto runnable = task.runnable;
            auto num_total_tasks = task.num_total_tasks;
            runnable->runTask(task.task_id, num_total_tasks);

            lk.lock();
            instance->busy_threads--;
            lk.unlock();
        } else {
            lk.unlock();
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
        // printf("joining thread %d\n", i);
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
        // printf("thread %d waiting\n", thread_id);
        // keep waiting while task queue is empty or we are done
        while(!instance->done && instance->task_queue.size() == 0) {
            instance->condition_variable_->wait(lk);
        }
        // printf("thread %d acquired lock\n", thread_id);
        if (instance->done) {
            // printf("thread %d exiting\n", thread_id);
            return;
        }
        // do the work in the critical section
        instance->busy_threads++;
        Task task = instance->task_queue.front();
        instance->task_queue.pop();
        lk.unlock();
        // printf("thread %d released lock\n", thread_id);
        // let someone else have the lock
        instance->condition_variable_->notify_all();
        // do actual run
        auto runnable = task.runnable;
        auto num_total_tasks = task.num_total_tasks;
        runnable->runTask(task.task_id, num_total_tasks);
        // its possible we still need the lock to do this atomic update
        // lk.lock();
        // printf("thread %d acquired lock for updating busy_threads \n", thread_id);
        // even if the shared variable is atomic, it must be modified 
        // under the mutex in order to correctly pusblish the modification
        // to the waiting thread
        instance->busy_threads--;
        // lk.unlock();
        // printf("thread %d released lock after updating busy_threads \n", thread_id);
        instance->condition_variable_->notify_all();

    }
}

void TaskSystemParallelThreadPoolSleeping::makeThreadPool() {
    for (int i = 0; i < max_threads; i++) {
        workers.push_back(std::thread(&workerThreadFuncSleeping, this, i));
    }
}

void TaskSystemParallelThreadPoolSleeping::killThreadPool() {
    // trick was to have the lock while some waiting thread that had it
    // released it, then notify all and let them take the lock back. 

    // I don't think we need a lock here, because we are the only thread
    // modifying the done variable
    done = true;
    condition_variable_->notify_all();
    for (int i = 0; i < max_threads; i++) {
        // make threads, and make them free to start off with
        workers[i].join();   
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    killThreadPool();
    delete condition_variable_;
    delete mutex_;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // its not possible to have a task_id > num_total_tasks
    // unless we're looking at garbage data
    for (int i = 0; i < num_total_tasks; i++) {
        Task task = {runnable, i, num_total_tasks}; //task_id is set to i {0, 1, 2, ... , num_total_tasks - 1}
        task_queue.push(task);
    }
    // printf("pushed %d tasks to queue\n", num_total_tasks);
    if (workers.size() == 0) {
        makeThreadPool();
    } 


    // signalling thread must spin until all tasks are done
    while (true) {
        // lock to check task_queue size
        // printf("busy threads: %d, task_queue: %d \n", busy_threads.load(), task_queue.size());
        // printf("main thread waiting\n");
        // we need to automagically release this lock when it goes out of scope
        std::unique_lock<std::mutex> lk(*mutex_);
        // printf("main thread got lock\n");
        // printf("task queue size is %ld\n", task_queue.size());
        // printf("busy threads is %d\n", busy_threads.load());
        if (task_queue.size() == 0 && busy_threads == 0) {
            // no more work to be done, return from run
            lk.unlock();
            // printf("run is returning\n");
            return;
        } else {
            // work remains, let someone else have the lock
            // std::unique_lock<std::mutex> lk(*mutex_);
            condition_variable_->notify_all();
            lk.unlock();
        }
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
