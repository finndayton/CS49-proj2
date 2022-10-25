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
    int num_finished_sub_tasks; //guard with mutex
    int num_total_sub_tasks;
    TaskID task_id;
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
        
        // all these can be private honestly
        int max_threads;
        
        void makeThreadPool();
        void killThreadPool();
        
        void finishedSubTask(SubTask subtask);
        void readyBtl(TaskID btl_task_id);
        void finishedTask(TaskID papa_task_id);
        // void removeBtlFromWaitingBtlVec(TaskID btl_task_id);
        void removeDependenciesFromWaitingBtlVec(TaskID btl_task_id);

        std::condition_variable* ready_btl_map_cv;
        std::condition_variable* ready_task_queue_cv;
        std::condition_variable* waiting_btl_vec_cv;
        std::condition_variable* subtasks_cv;

        std::mutex* ready_btl_map_mutex;
        std::mutex* ready_task_queue_mutex;
        std::mutex* waiting_btl_vec_mutex;
        std::mutex* subtasks_mutex;
        std::mutex* completed_btls_mutex;

        std::unordered_map<TaskID, Task> ready_btl_map;
        std::queue<SubTask> ready_task_queue;

        std::vector<TaskID> waiting_btl_vec;
        std::unordered_map<TaskID, Task> task_info;
        std::unordered_set<TaskID> completed_btls;
        std::unordered_map<TaskID, std::unordered_set<TaskID>> dependencies;

        // keep track of total number of subtasks
        int total_subtasks;
        int finished_subtasks;


        std::vector<std::thread> workers;
        std::atomic<int> busy_threads;
        bool done;
        int cur_task_id; // it is initially 0
};

#endif
