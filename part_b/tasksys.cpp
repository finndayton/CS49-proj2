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

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    max_threads = num_threads;


}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}


void workerThreadFunc(
    TaskSystemParallelThreadPoolSleeping* instance, 
    int thread_id
) {
    while (true) {
        std::unique_lock<std::mutex> lk(*(instance->ready_task_queue_mutex));
        // did we need a lock for the done variable?
        while(!instance->done && instance->ready_task_queue.size() == 0) {
            instance->condition_variable_->wait(lk);
        }
        if (instance->done) {
            return;
        }

        // do the work in the critical section
        instance->busy_threads++;
        SubTask subtask = instance->ready_task_queue.front();
        instance->ready_task_queue.pop();
        lk.unlock();
        
        instance->ready_task_queue_cv->notify_all();
        
        // do actual run
        auto runnable = subtask.runnable;
        auto num_total_sub_tasks = subtask.num_total_sub_tasks;
        runnable->runTask(subtask.sub_task_id, num_total_sub_tasks);
        finishedSubTask(subtask); // does the postprocessing 
        instance->busy_threads--; 
    }
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

void TaskSystemParallelThreadPoolSleeping::readyBtl(Task btl) {
    // pushes a BTL onto the ready_btl_map and all its subtasks onto the ready_task_queue
    // also pops it from the waiting_btl_queue
    // definitely lock for this shared resource access
    ready_btl_map[btl.task_id] = btl;
    int btl_index = ready_btl_list.size() - 1;
    for (int i = 0; i < btl.num_subtasks; i++) {
        // create a subtask object for each
        SubTask subtask = {btl.runnable, i, btl.num_total_subtasks, btl.task_id}
        ready_task_queue.push(subtask);
    }
    waiting_btl_set.remove(btl);
}

void TaskSystemParallelThreadPoolSleeping::finishedSubTask(SubTask subtask) {
    // when you finish a sub task, you need to increment the number of finished sub tasks
    Task* papa_task = ready_btl_map[subtask.btl_task_id];
    papa_task->num_finished_subtasks++;
    if (papa_task->num_finished_subtasks == papa_task->num_total_subtasks) {
        TaskID task_id = papa_task->task_id;
        // loop through every BTL in the waiting_btl_queue
        // remove the papa_task TaskID from the waiting_for set of every BTL 
        // in the waiting_btl_queue (if it exists)
        // for each BTL in the waiting_btl_queue, if the waiting_for set is empty, push it onto the ready_btl_queue
        // and push its subtasks onto the ready_task_queue
        for (auto task : waiting_btl_set) {
            if (task->waiting.count(task_id) > 0) {
                task->waiting.erase(task_id);
            }
            if (task->waiting.size() == 0) {
                readyBtl(task);
            }
        }
        // remove the papa task from the ready_btl_map
        ready_btl_map.erase(papa_task);
    }
}

/**
 * runAsyncWithDeps is never going to be called from multiple threads at the same time.
 * We append a btl to the waiting_btl_queue and return the task id.
*/
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    // add BTL to the waiting queue
    
    Task task = {runnable, 0, num_total_tasks, cur_task_id, deps};
    // lock here - shared resource
    waiting_btl_set.insert(task);
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
    while (true) {
        std::unique_lock<std::mutex> lock(mtx);
        if (num_tasks == 0) {
            break;
        }
        cv.wait(lock);
    }
}
