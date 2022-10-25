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

struct Task {
    IRunnable* runnable;
    int num_total_subtasks;
    TaskID task_id;
    std::unordered_set<TaskID> set;
};

struct SubTask {
    IRunnable* runnable;
    int num_total_subtasks;
    TaskID papa_id;
    int sub_task_id;
};


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

    //class variables 
    std::atomic<int> target_total_sub_tasks_;
    std::atomic<int> total_sub_tasks_completed_so_far_; 

    std::condition_variable* sync_cv_;
    std::condition_variable* threads_cv_;

    std::mutex* sync_mutex_; 
    std::mutex* mutex_; 

    int num_threads_;
    bool spinning_;

    std::vector<Task> Tasks_; // read-only. length = numb Btls added since last sync()
    std::vector<int> btls_num_subtasks_left_; //length = same as Tasks
    std::vector<bool>completed_tasks_; //length = same as Tasks
   
    std::vector<std::thread>workers_;    //length = num_threads_
    std::vector<bool> sleeping_threads_; //length = num_threads_

    std::queue<SubTask> subtasks_;

    //class functions
    void initializeThreadPool();
};

#endif
