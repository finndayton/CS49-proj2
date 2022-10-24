#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <unordered_set>
#include <thread>
#include <functional>
#include <atomic>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <unordered_map>


/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

// might need default constructors here
struct SubTask {
    IRunnable* runnable;
    int sub_task_id;
    int num_total_sub_tasks;
    int btl_task_id;
};

struct Task {
    IRunnable* runnable;
    // std::atomic<int> num_finished_sub_tasks;
    int num_finished_sub_tasks; //guard with mutex
    int num_total_sub_tasks;
    TaskID task_id;
    std::unordered_set<TaskID> dependencies; // can this live on the stack or should it be elsewhere?
    
    // Task() = default; 
    // Task(IRunnable* runnable_, 
    //      std::atomic<int> num_finished_sub_tasks_, 
    //      int num_total_sub_tasks_,
    //      TaskID task_id_,
    //      std::unordered_set<TaskID> dependencies_ = std::unordered_set<TaskID>());

    // Task(IRunnable* runnable_, 
    //      std::atomic<int> num_finished_sub_tasks_, 
    //      int num_total_sub_tasks_,
    //      TaskID task_id_,
    //      std::unordered_set<TaskID> dependencies_):
    //         runnable(runnable_),
    //         num_finished_sub_tasks(num_finished_sub_tasks_),
    //         num_total_sub_tasks(num_total_sub_tasks_),
    //         task_id(task_id_),
    //         dependencies(dependencies_) {}        
        

    // bool operator==(const Task& otherTask) const {
    //     // not bulletproof, but good enough for this assignment
    //     return task_id == otherTask.task_id;
    // };

    // struct HashFunction {
        // size_t operator()(const Task& task) const {
        //     return std::hash<int>()(task.task_id);
        // }
    // };   
};

// class HashClass {
//     public: 
//         size_t operator()(const Task& task) const {
//             return std::hash<int>()(task.task_id);
//         }
// };



// size_t hashFunction(Task* task) {
//     return std::hash<int>()(task->task_id);
// }

// bool equals(Task* a, Task* b){
//     return a->task_id == b->task_id;
// }


/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        
        // all these can be private honestly
        int max_threads;
        
        void makeThreadPool();
        void killThreadPool();
        
        void finishedSubTask(SubTask subtask);
        void readyBtl(Task btl); 

        
        std::condition_variable* busy_threads_cv;
        std::condition_variable* ready_btl_map_cv;
        std::condition_variable* ready_task_queue_cv;
        std::condition_variable* waiting_btl_vec_cv;

        std::mutex* ready_btl_map_mutex;
        std::mutex* ready_task_queue_mutex;
        std::mutex* waiting_btl_vec_mutex;

        std::unordered_map<TaskID, Task> ready_btl_map;
        std::queue<SubTask> ready_task_queue;

        // std::unordered_set<Task, HashClass> waiting_btl_set;
        // Because I couldn't get the above set to work, a temporary fix is to mimic the set with a vector
        std::vector<Task> waiting_btl_vec;

        std::vector<std::thread> workers;
        std::atomic<int> busy_threads;
        bool done;
        int cur_task_id; // it is initially 0
};

#endif
