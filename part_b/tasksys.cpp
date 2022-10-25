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
        printf("thread %d: ready task queue size is %lu\n", thread_id, instance->ready_task_queue.size());
        while(!instance->done && instance->ready_task_queue.size() == 0) {
            instance->ready_task_queue_cv->wait(lk); 
            printf("thread %d: woke up\n", thread_id);
        }
        if (instance->done) {
            instance->ready_task_queue_cv->notify_all();
            return;
        }

        // do the work in the critical section
        SubTask subtask = instance->ready_task_queue.front();
        instance->ready_task_queue.pop();
        lk.unlock();
        printf("Thread %d is running sub task %d of task %d \n", thread_id, subtask.sub_task_id, subtask.btl_task_id);
        
        // do actual run
        auto runnable = subtask.runnable;
        auto num_total_sub_tasks = subtask.num_total_sub_tasks;
        runnable->runTask(subtask.sub_task_id, num_total_sub_tasks);
        instance->finishedSubTask(subtask); // does the postprocessing 
        printf("returned from finishedSubTask on subtask %d, task %d\n", subtask.sub_task_id, subtask.btl_task_id);
        printf("trying to acquire lock on subtasks_mutex in thread %d\n", thread_id);
        instance->subtasks_mutex->lock();
        instance->finished_subtasks++; 
        printf("made finished_subtasks %d in thread %d\n", instance->finished_subtasks, thread_id);
        instance->subtasks_mutex->unlock();
        // think about making busy threads mutex and condition variable
        instance->subtasks_cv->notify_all(); 
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, std::vector<TaskID>());
    sync();
}

void TaskSystemParallelThreadPoolSleeping::removeBtlFromWaitingBtlVec(TaskID btl_task_id) {
    // nobody else can be modifying waiting_btl_vec
    printf("WAITING TO ACQUIRE LOCK IN removeBtlFromWaitingBtlVec\n");
    waiting_btl_vec_mutex->lock();
    printf("ACQUIRED LOCK IN removeBtlFromWaitingBtlVec\n");
    printf("trying to delete btl %d, acquired lock\n", btl_task_id);
    for (size_t i = 0; i < waiting_btl_vec.size(); i++) {
        auto cur_btl = waiting_btl_vec[i];
        if (waiting_btl_vec[i].task_id == btl_task_id){
            waiting_btl_vec.erase(waiting_btl_vec.begin() + i);
            printf("removed btl %d from waiting_btl_vec\n", btl_task_id);
            printf("waiting_btl_vec size is %lu\n", waiting_btl_vec.size());
            break;
        }
    }
    printf("removed btl %d from waiting_btl_vec, releasing lock\n", btl_task_id);
    printf("waiting_btl_vec size is %lu\n", waiting_btl_vec.size());
    waiting_btl_vec_mutex->unlock();
}

void TaskSystemParallelThreadPoolSleeping::readyBtl(Task btl) {
    // pushes a BTL onto the ready_btl_map and all its subtasks onto the ready_task_queue
    // also pops it from the waiting_btl_queue
    // definitely lock for this shared resource access
    ready_btl_map_mutex->lock();
    ready_btl_map[btl.task_id] = btl; //book keeping map

    for (int i = 0; i < btl.num_total_sub_tasks; i++) {
        // create a subtask object for each
        ready_task_queue.push({btl.runnable, i, btl.num_total_sub_tasks, btl.task_id});
    }
    ready_btl_map_mutex->unlock();
    // wake up because there's more ready tasks!
    ready_task_queue_cv->notify_all();
}

void TaskSystemParallelThreadPoolSleeping::removeDependenciesFromWaitingBtlVec(TaskID btl_task_id) {
    // we currently hold the waiting_btl_vec_mutex, so we make sure to unlock before 
    // calling readyBtl
    for (auto it = waiting_btl_vec.begin(); it != waiting_btl_vec.end(); it++) {
        auto cur_btl = *it;
        for (size_t i = 0; i < cur_btl.dependencies.size(); i++) {
            if (cur_btl.dependencies[i] == btl_task_id) {
                cur_btl.dependencies.erase(cur_btl.dependencies.begin() + i);
                break;
            }
        }
        if (cur_btl.dependencies.size() == 0) {
            readyBtl(cur_btl);
            it = waiting_btl_vec.erase(it);
            it--;
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::finishedSubTask(SubTask subtask) {
    // when you finish a sub task, you need to increment the number of finished sub tasks
    TaskID papa_task_id = subtask.btl_task_id;
    // definitely lock for this shared resource access
    ready_btl_map_mutex->lock();
    ready_btl_map[papa_task_id].num_finished_sub_tasks++;
    printf("finished subtask %d of btl %d\n", subtask.sub_task_id, subtask.btl_task_id);
    // printf("num finished subtasks is %d\n", ready_btl_map[papa_task_id].num_finished_sub_tasks);
    // printf("num total subtasks is %d\n", ready_btl_map[papa_task_id].num_total_sub_tasks);
    if (ready_btl_map[papa_task_id].num_finished_sub_tasks == ready_btl_map[papa_task_id].num_total_sub_tasks) {
        ready_btl_map_mutex->unlock();
        printf("waiting for the waiting_btl_vec_mutex lock in sub task %d of task %d\n", subtask.sub_task_id, subtask.btl_task_id);
        waiting_btl_vec_mutex->lock();
        printf("got the lock\n");
        removeDependenciesFromWaitingBtlVec(papa_task_id);
        completed_btls.insert(papa_task_id);
        printf("[HERE] added btl %d to completed_btls\n", papa_task_id);
        waiting_btl_vec_mutex->unlock();

        ready_btl_map_mutex->lock();
        // remove the papa task from the ready_btl_map
        ready_btl_map.erase(papa_task_id);
        ready_btl_map_mutex->unlock();
        printf("should return to workerThreadFUnc now that btl %d is done\n", papa_task_id);
    } else {
        ready_btl_map_mutex->unlock();
    }
}

/**
 * runAsyncWithDeps is never going to be called from multiple threads at the same time.
 * We append a btl to the waiting_btl_queue and return the task id.
*/
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    // add BTL to the waiting queue, or the ready queue if no dependencies
    waiting_btl_vec_mutex->lock();
    std::unordered_set<TaskID> deps_as_set;
    printf("appending btl %d with asyncRun\n", cur_task_id);
    for (auto dep : deps) {
        if (completed_btls.count(dep) == 0) {
            deps_as_set.insert(dep);
        }
    }

    // printf("deps_as_set for btl %d is: ", cur_task_id);
    // for (auto dep : deps_as_set) {
    //     printf("%d ", dep);
    // }
    // printf("\n");

    // printf("completed_btls is: ");
    // for (auto it = completed_btls.begin(); it != completed_btls.end(); it++) {
    //     printf("%d ", *it);
    // }
    // printf("\n");
    

    Task task = Task{runnable, 0, num_total_tasks, cur_task_id, deps_as_set};
    
    if (deps_as_set.size() == 0) {
        waiting_btl_vec_mutex->unlock();
        readyBtl(task);
    } else {
        printf("adding btl %d to waiting_btl_vec\n", cur_task_id);
        waiting_btl_vec.push_back(task);
        waiting_btl_vec_mutex->unlock();
    }

    // is this the right place to do this? 
    subtasks_mutex->lock();
    total_subtasks += num_total_tasks;
    subtasks_mutex->unlock();

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
    // Put sync to sleep until these conditions are satisfied using the 3 condition vars declared int he header. 
    std::unique_lock<std::mutex> lk(*subtasks_mutex);
    while (finished_subtasks != total_subtasks) {
        printf("waiting for lock in sync\n");
        printf("finished subtasks at this point is %d/%d\n", finished_subtasks, total_subtasks);
        printf("ready_task_queue.size() = %lu, waiting_btl_vec.size() = %lu \n", ready_task_queue.size(), waiting_btl_vec.size());
        subtasks_cv->wait(lk);
        printf("got lock in sync\n");
    }
    printf("returned from sync\n");
}
