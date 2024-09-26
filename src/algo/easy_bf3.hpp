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


// @note LH: converts some numerical type to type double
#define C2DBL(x) (x.convert_to<double>()) 

// @note LH: returns the schedules current machine utilization 
#define NOTIFY_MACHINE_UTIL ((_nb_machines) ? (static_cast<double>(_nb_machines-_nb_available_machines)/_nb_machines) : 0)

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

    // @note LH: simulated checkpointing additions                             
    virtual void on_machine_down_for_repair(batsched_tools::KILL_TYPES forWhat, double date);
    virtual void on_machine_instant_down_up(batsched_tools::KILL_TYPES forWhat, double date);
    void on_requested_call(double date,batsched_tools::CALL_ME_LATERS cml);
    void on_myKillJob_notify_event(double date);
    void on_no_more_static_job_to_submit_received(double date);

    // @note LH: real checkpointing additions (TBA)
    virtual void on_start_from_checkpoint(double date,const rapidjson::Value & batsim_config);
    virtual void on_checkpoint_batsched(double date);
    virtual void on_ingest_variables(const rapidjson::Document & doc,double date);
    virtual void on_first_jobs_submitted(double date);

protected:
      
    // @note LH: Additions for handling scheduling decisions
    void check_priority_job(const Job * priority_job, double date);
    void check_backfill_job(const Job * backfill_job, double date);
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
    
    std::vector<Job *>::iterator find_waiting_job(std::string job_id);
    std::vector<Job *>::iterator delete_waiting_job(std::vector<Job *>::iterator wj_iter);
    std::vector<Job *>::iterator delete_waiting_job(std::string waiting_job_id);
    Job * get_first_waiting_job();
    void max_heapify_schedule(int root, int size);
    void heap_sort_schedule(int size);
    std::string queue_to_string();

    // @note LH: Additions for replacing the schedule class
    
    batsched_tools::Scheduled_Job * _tmp_job = nullptr;
    std::vector<batsched_tools::Scheduled_Job *> _scheduled_jobs;


    // @note LH: Struct to keep track of priority job
    
    batsched_tools::Priority_Job * _p_job = nullptr;

    // @note LH: Added variables
    
    bool _can_run = false;

    // @note LH: Checkpointing variables
        
    
};
