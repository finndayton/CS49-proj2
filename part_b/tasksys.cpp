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

    for (int i = 0; i < num_threads_; i++) {
        workers_[i].join();   
    }

    //deletes here

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

            instance->sleeping_threads_[thread_id] = true;
            instance->threads_cv_->wait(lk); //go to sleep. SEGFAULT
            instance->sleeping_threads_[thread_id] = false;

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

        //big debate: where to put this block?
        sync_mutex_->lock();
        total_sub_tasks_completed_so_far_++;
        sync_mutex_->unlock();
        sync_cv_->notify_one();

    }

    //there may be some different tasks we can taskify now
    for (auto task: tasks_) {
        bool covered = true;
        for (TaskID dep_id: task.deps) {
            if(!completed_tasks_[dep_id]) {
                covered = false;
            }
        }
        if (covered) {
            mutex_->unlock();
            taskify(task);
            mutex_->lock();
        }
    }
    mutex_->unlock(); //this here? for now. 

}

void TaskSystemParallelThreadPoolSleeping::taskify(Task task) {
    mutex_->lock();
    for (int i = 0; i < task.num_total_subtasks; i++) {
        subtasks_queue_.push({task.runnable, task.num_total_subtasks, task.task_id, i});
    }
    mutex_->unlock();
    threads_cv_->notify_all(); //could cause race condition. discuss solutions
}

const std::vector<TaskID> TaskSystemParallelThreadPoolSleeping::updateDeps(const std::vector<TaskID>& deps){
    std::vector<TaskID> new_deps;
    for (TaskID id: deps){
        if (!completed_tasks_[id]) {
            new_deps.push_back(id);
        }
    }
    return new_deps;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    mutex_->lock();
    Task task = Task{runnable, num_total_tasks, curr_task_id_, deps};

    // sync_mutex_->lock();
    target_total_sub_tasks_ += num_total_tasks; //guard???
    // sync_mutex_->unlock();
    
    tasks_.push_back(task);
    completed_tasks_.push_back(false);
    btls_num_subtasks_left_.push_back(num_total_tasks);

    if (updateDeps(deps).size() == 0) taskify(task);

    mutex_->unlock();
    return curr_task_id_++;
                                                        
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lk(*sync_mutex_); //deadlocking here. 

    while (total_sub_tasks_completed_so_far_ != target_total_sub_tasks_) {
         sync_cv_->wait(lk);
    }
    total_sub_tasks_completed_so_far_ = 0;
    target_total_sub_tasks_ = 0;
    curr_task_id_ = 0;
    

    return;
}
