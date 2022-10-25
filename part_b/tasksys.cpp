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
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

//forward decl.
void workerThreadFunc(TaskSystemParallelThreadPoolSleeping* instance, int thread_id);
void postRun(SubTask subtask);

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}
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
    num_threads_ = num_threads;
    done_ = false;
    target_total_sub_tasks_ = 0;
    total_sub_tasks_completed_so_far_ = 0;
    curr_task_id_ = 0;

    sync_cv_ = new std::condition_variable;
    threads_cv_ = new std::condition_variable;

    sync_mutex_ = new std::mutex; 
    mutex_ = new std::mutex; 

    initializeThreadPool();
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    done_ = true;
    threads_cv_->notify_all(); // may need to synchonize on sleeping_threads first


}

void TaskSystemParallelThreadPoolSleeping::initializeThreadPool() {
    for (int i = 0; i < num_threads_; i++) {
        workers_.push_back(std::thread(&workerThreadFunc, this, i));
    }
}

void workerThreadFunc(
    TaskSystemParallelThreadPoolSleeping* instance, 
    int thread_id
) {
    while(true) {
        std::unique_lock<std::mutex> lk(*(instance->mutex_));
        while (!instance->done_ && instance->subtasks_queue_.size() == 0) {
            instance->threads_cv_->wait(lk);
        }
        if (instance->done_) return;

        SubTask subtask = instance->subtasks_queue_.front();
        instance->subtasks_queue_.pop();
        lk.unlock();
        
        //run!
        subtask.runnable->runTask(subtask.sub_task_id, subtask.num_total_subtasks);
        instance->postRun(subtask);
    }
}

void TaskSystemParallelThreadPoolSleeping::postRun(SubTask subtask) {
    mutex_->lock();
    TaskID papa_id = subtask.papa_id;
    btls_num_subtasks_left_[papa_id]--;

    //if done with this btl...
    if (btls_num_subtasks_left_[papa_id] == 0) {
        completed_tasks_[papa_id] = true;
    }

    //come back to this based on your implementation of run below
    for (auto task: tasks_) {
        for (TaskID dep_id: task.deps) {
            if (!completed_tasks_[dep_id]) return;
        }

    }
    mutex_->unlock(); //here?
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {

    Task task = Task{runnable, num_total_tasks, curr_task_id_, deps};
 
    //func that determines what to do with this task. maybe deps are handled already

    target_total_sub_tasks_ += num_total_tasks;
    
    
    return curr_task_id_++;
                                                        
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
