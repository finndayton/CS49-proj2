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

// forward decl for workerThreadFunc
void workerThreadFunc(TaskSystemParallelThreadPoolSleeping* task_system, int thread_id);

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    max_threads = num_threads;
    
    ready_btl_map_mutex = new std::mutex();
    ready_task_queue_mutex = new std::mutex();
    waiting_btl_vec_mutex = new std::mutex();
    subtasks_mutex = new std::mutex();
    completed_btls_mutex = new std::mutex();

    // initialize condition variables
    ready_btl_map_cv = new std::condition_variable();
    ready_task_queue_cv = new std::condition_variable();
    waiting_btl_vec_cv = new std::condition_variable();
    subtasks_cv = new std::condition_variable();

    total_subtasks = 0;
    finished_subtasks = 0;
    cur_task_id = 0;
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
    ready_task_queue_cv->notify_all();
    for (int i = 0; i < max_threads; i++) {
        workers[i].join();   
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    killThreadPool();
    delete ready_btl_map_cv;
    delete ready_task_queue_cv;
    delete waiting_btl_vec_cv;
    delete subtasks_cv;
    delete ready_btl_map_mutex;
    delete ready_task_queue_mutex;
    delete waiting_btl_vec_mutex;
    delete subtasks_mutex;
    delete completed_btls_mutex;
}


void workerThreadFunc(
    TaskSystemParallelThreadPoolSleeping* instance, 
    int thread_id
) {
    while (true) {
        std::unique_lock<std::mutex> lk(*(instance->ready_task_queue_mutex)); //Declare condition variable and aquire lock on ready_task_queue
        // did we need a lock for the done variable?
        // // // printf("thread %d: ready task queue size is %lu\n", thread_id, instance->ready_task_queue.size());
        while(!instance->done && instance->ready_task_queue.size() == 0) {
            instance->ready_task_queue_cv->wait(lk); 
            // // // // printf("thread %d: woke up\n", thread_id);
        }
        if (instance->done) {
            instance->ready_task_queue_cv->notify_all();
            return;
        }

        // do the work in the critical section
        SubTask subtask = instance->ready_task_queue.front();
        instance->ready_task_queue.pop();
        lk.unlock();
        // // // printf("thread %d released ready_task_queue_mutex\n", thread_id);
        // // // // printf("Thread %d is running sub task %d of task %d \n", thread_id, subtask.sub_task_id, subtask.btl_task_id);
        
        // do actual run
        auto runnable = subtask.runnable;
        auto num_total_sub_tasks = subtask.num_total_sub_tasks;
        runnable->runTask(subtask.sub_task_id, num_total_sub_tasks);
        instance->finishedSubTask(subtask); // does the postprocessing 
        // // // printf("thread %d waiting on subtasks_mutex 215\n", thread_id);
        instance->subtasks_mutex->lock();
        // // // printf("thread %d acquired subtasks_mutex 217\n", thread_id);
        instance->finished_subtasks++; 
        instance->subtasks_mutex->unlock();
        // // // printf("thread %d released subtasks mutex 220\n", thread_id);
        instance->subtasks_cv->notify_all(); 
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, std::vector<TaskID>());
    sync();
}

void TaskSystemParallelThreadPoolSleeping::readyBtl(TaskID btl_task_id) {
    // pushes a BTL onto the ready_btl_map and all its subtasks onto the ready_task_queue
    // // // printf("task %d trying to acquire ready_btl_map\n", btl.task_id);
    ready_btl_map_mutex->lock();
    auto btl = task_info[btl_task_id];
    // // // printf("task %d acquires mutex\n", btl.task_id);
    ready_btl_map[btl_task_id] = btl; //book keeping map
    // // printf("added btl %d onto ready_btl_map, size is now %d\n", btl.task_id, ready_btl_map.size());
    for (int i = 0; i < btl.num_total_sub_tasks; i++) {
        // create a subtask object for each
        ready_task_queue.push({btl.runnable, i, btl.num_total_sub_tasks, btl.task_id});
    }
    ready_btl_map_mutex->unlock();
    // // // printf("task %d releases ready_btl_map_mutex\n", btl.task_id);
    // wake up because there's more ready tasks!
    ready_task_queue_cv->notify_all();
}

void TaskSystemParallelThreadPoolSleeping::removeDependenciesFromWaitingBtlVec(TaskID btl_task_id) {
    auto it = waiting_btl_vec.begin();
    while (it != waiting_btl_vec.end()) {
        TaskID task_id = (*it);
        // printf("task_id in waiting_btl_vec is %d\n", task_id);
        dependencies[task_id].erase(btl_task_id);
        if (dependencies[task_id].size() == 0) {
            // printf("adding %d to ready queue\n", task_id);
            waiting_btl_vec_mutex->unlock();
            // printf("task %d releases waiting_btl_vec_mutex\n", btl_task_id);
            // printf("task %d is still in the waiting_btl\n", task_id);
            // printf("calling readyBtl on %d from removeDeps function\n", task_id);
            // doesn't this need to happen before the call to readyBtl so we don't go back in the loop?

            it = waiting_btl_vec.erase(it);
            readyBtl(task_id);
            // printf("task %d re-acquires waiting_btl_vec_mutex\n", btl_task_id);
            waiting_btl_vec_mutex->lock();
            // it didn't properly take it out of the waiting_btl_vec??
            dependencies.erase(task_id);
            // printf("waiting_btl_vec is now size %d\n", waiting_btl_vec.size());
        } else {
            ++it;
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::finishedTask(TaskID papa_task_id) {

    // // // printf("task %d waiting to acquire ready_btl_map_mutex\n", papa_task_id);
    ready_btl_map_mutex->lock();
    // remove the papa task from the ready_btl_map
    ready_btl_map.erase(papa_task_id);
    ready_btl_map_mutex->unlock();

    // // // printf("task %d waiting to acquire waiting_btl_vec_mutex\n", papa_task_id);
    waiting_btl_vec_mutex->lock();
    task_info.erase(papa_task_id);
    removeDependenciesFromWaitingBtlVec(papa_task_id);
    completed_btls.insert(papa_task_id);
    waiting_btl_vec_mutex->unlock();
    // // // printf("task %d releases waiting_btl_vec_mutex\n", papa_task_id);

    // // // printf("task %d releases ready_btl_map_mutex\n", papa_task_id);
}

void TaskSystemParallelThreadPoolSleeping::finishedSubTask(SubTask subtask) {
    // when you finish a sub task, you need to increment the number of finished sub tasks
    TaskID papa_task_id = subtask.btl_task_id;
    // definitely lock for this shared resource access
    // // // printf("subtask %d, task %d waiting to acquire ready_btl_map_mutex", subtask.sub_task_id, subtask.btl_task_id);
    ready_btl_map_mutex->lock();
    ready_btl_map[papa_task_id].num_finished_sub_tasks++;
    
    printf(
        "num_finished/num_total at this point is %d/%d for task %d\n", 
        ready_btl_map[papa_task_id].num_finished_sub_tasks,
        ready_btl_map[papa_task_id].num_total_sub_tasks,
        papa_task_id
    );
    if (ready_btl_map[papa_task_id].num_finished_sub_tasks == ready_btl_map[papa_task_id].num_total_sub_tasks) {
        ready_btl_map_mutex->unlock();
        printf("subtask %d, task %d releases ready_btl_map_mutex\n", subtask.sub_task_id, subtask.btl_task_id);
        finishedTask(papa_task_id);
    } else {
        ready_btl_map_mutex->unlock();
        // // // printf("subtask %d, task %d releases ready_btl_map_mutex", subtask.sub_task_id, subtask.btl_task_id);
    }
}

/**
 * runAsyncWithDeps is never going to be called from multiple threads at the same time.
 * We append a btl to the waiting_btl_queue and return the task id.
*/
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    
    // is this the right place to do this? 
    subtasks_mutex->lock();
    total_subtasks += num_total_tasks;
    subtasks_mutex->unlock();

    // add BTL to the waiting queue, or the ready queue if no dependencies
    // // // printf("main thread waiting on waiting_btl_vec_mutex\n");
    waiting_btl_vec_mutex->lock();
    // // // printf("main thread acquires waiting_btl_vec_mutex\n");
    std::unordered_set<TaskID> depsClone;
    for (auto dep : deps) {
        if (completed_btls.count(dep) == 0) {
            depsClone.insert(dep);
        }
    }
    Task task = Task{runnable, 0, num_total_tasks, cur_task_id};
    task_info[cur_task_id] = task;
    
    if (depsClone.size() == 0) {
        // // // printf("main thread releases waiting_btl_vec_mutex\n");
        waiting_btl_vec_mutex->unlock();
        // // printf("calling readyBtl on %d\n", cur_task_id);
        readyBtl(cur_task_id);
    } else {
        // should only exist for waiting_btl_vec entries
        dependencies[cur_task_id] = depsClone;
        waiting_btl_vec.push_back(cur_task_id);
        // // // printf("main thread releases waiting_btl_vec_mutex\n");
        waiting_btl_vec_mutex->unlock();
    }

    cur_task_id++;
    return cur_task_id - 1;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lk(*subtasks_mutex);
    while (finished_subtasks != total_subtasks) {
        // // // // printf("waiting for lock in sync\n");
        // // // printf("finished subtasks at this point is %d/%d\n", finished_subtasks, total_subtasks);
        // // // printf("ready_task_queue.size() = %lu, waiting_btl_vec.size() = %lu \n", ready_task_queue.size(), waiting_btl_vec.size());
        // loop through all the completed tasks and call removeDeps on them
        // waiting_btl_vec_mutex->lock();
        // for (auto btl : completed_btls) {
        //     removeDependenciesFromWaitingBtlVec(btl);
        // }
        // waiting_btl_vec_mutex->unlock();        
        subtasks_cv->wait(lk);
        // // // printf("got lock in sync\n");
    }
    // // // printf("returned from sync\n");
}





// void TaskSystemParallelThreadPoolSleeping::removeBtlFromWaitingBtlVec(TaskID btl_task_id) {
//     // nobody else can be modifying waiting_btl_vec
//     // // // printf("WAITING TO ACQUIRE LOCK IN removeBtlFromWaitingBtlVec\n");
//     waiting_btl_vec_mutex->lock();
//     // // // printf("ACQUIRED LOCK IN removeBtlFromWaitingBtlVec\n");
//     // // // printf("trying to delete btl %d, acquired lock\n", btl_task_id);
//     for (size_t i = 0; i < waiting_btl_vec.size(); i++) {
//         auto cur_btl = waiting_btl_vec[i];
//         if (waiting_btl_vec[i].task_id == btl_task_id){
//             waiting_btl_vec.erase(waiting_btl_vec.begin() + i);
//             // // // printf("removed btl %d from waiting_btl_vec\n", btl_task_id);
//             // // // printf("waiting_btl_vec size is %lu\n", waiting_btl_vec.size());
//             break;
//         }
//     }
//     // // // printf("removed btl %d from waiting_btl_vec, releasing lock\n", btl_task_id);
//     // // // printf("waiting_btl_vec size is %lu\n", waiting_btl_vec.size());
//     waiting_btl_vec_mutex->unlock();
// }
