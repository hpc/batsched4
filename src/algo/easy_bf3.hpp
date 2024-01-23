#pragma once

#include <list>
#include <algorithm>
#include "../isalgorithm.hpp"
#include "../json_workload.hpp"
#include "../locality.hpp"
#include "../schedule.hpp"
//added
#include "../machine.hpp"
#include "../batsched_tools.hpp"
#include <random>
#include <regex>

// @note LH: returns the minimum of between a and b
#define MIN(a,b) (((a)<(b)) ? (a) : (b))

//@note LH: added for optional failures logging
#define B_LOG_INSTANCE _logFailure

// @note LH: converts some numerical type to type double
#define C2DBL(x) (x.convert_to<double>()) 

class EasyBackfilling3 : public ISchedulingAlgorithm
{
public:
    EasyBackfilling3(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                    double rjms_delay, rapidjson::Document * variant_options);
    virtual ~EasyBackfilling3();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_event);
    virtual void on_simulation_end(double date);
    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);
    void sort_queue_while_handling_priority_job(Job * priority_job_before,
                                                Job *& priority_job_after,
                                                SortableJobOrder::UpdateInformation * update_info);

    // @note LH: simulated checkpointing addtions                             
    void on_machine_down_for_repair(double date);
    void on_machine_instant_down_up(double date);
    void on_requested_call(double date,int id,batsched_tools::call_me_later_types forWhat);
    void on_myKillJob_notify_event(double date);
    void on_no_more_static_job_to_submit_received(double date);
    virtual void on_start_from_checkpoint(double date,const rapidjson::Value & batsim_config);
    virtual void on_checkpoint_batsched(double date);
    virtual void on_ingest_variables(const rapidjson::Document & doc,double date);
    virtual void on_first_jobs_submitted(double date);
protected:
    bool _debug = false;
    std::string _output_folder;
    //added
    b_log *_logFailure;
    std::string _queue_policy;

    // @note LH: Additions for handling scheduling decisions
    void check_priority_job(const Job * priority_job, double date);
    void check_next_job(const Job * next_job, double date);
    void handle_scheduled_job(const Job * job, double date);
    void handle_finished_job(std::string job_id, double date);

    // @note LH: Additions for replacing the queue class
    struct CompareQueue{
        bool compare_original;
        CompareQueue(bool compare_original = false);
        inline bool operator()(Job* jobA, Job* jobB) const;
    };
    void max_heapify_queue(int root, int size, const CompareQueue& comp);
    void heap_sort_queue(int size, const CompareQueue& comp);
    std::vector<Job *> _waiting_jobs;
    std::vector<Job *>::iterator find_waiting_job(std::string job_id);
    std::vector<Job *>::iterator delete_waiting_job(std::vector<Job *>::iterator wj_iter);
    std::vector<Job *>::iterator delete_waiting_job(std::string waiting_job_id);
    Job * get_first_waiting_job();

    // @note LH: Additions for replacing the schedule class
    struct Scheduled_Job
    {
        std::string id;
        int requested_resources;
        double wall_time;
        double start_time;
        double est_finish_time;
        IntervalSet allocated_machines;
    };
    Scheduled_Job * _tmp_job = NULL;
    std::vector<Scheduled_Job *> _scheduled_jobs;
    void max_heapify_schedule(int root, int size);
    void heap_sort_schedule(int size);

    // @note LH: Struct to keep track of priority job
    struct Priority_Job
    {
        std::string id;
        int requested_resources;
        int extra_resources;
        double shadow_time;
        double est_finish_time;
    };
    Priority_Job * _p_job = NULL;

    // @note LH: Added variables
    IntervalSet _available_machines;
    int _nb_available_machines = -1;
    bool _can_run = false;
    bool _is_priority = false;

    // @note LH: Checkpointing variables
    IntervalSet _repair_machines;
    bool _need_to_send_finished_submitting_jobs = true;
    bool _checkpointing_on=false;
    std::vector<double> _call_me_laters;
    std::map<Job *,batsched_tools::Job_Message *> _my_kill_jobs;
    
};