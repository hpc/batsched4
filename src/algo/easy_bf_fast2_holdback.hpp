#pragma once

#include <unordered_map>

#include <unordered_set>
#include <list>
#include <random>

#include "../isalgorithm.hpp"
#include "../json_workload.hpp"
#include "../locality.hpp"
#include "../external/pointers.hpp"
#include <rapidjson/document.h>
#include "../batsched_tools.hpp"

class easy_bf_fast2_holdback : public ISchedulingAlgorithm
{
public:
    

    easy_bf_fast2_holdback(Workload * workload, SchedulingDecision * decision,
        Queue * queue, ResourceSelector * selector,
        double rjms_delay,
        rapidjson::Document * variant_options);
    virtual ~easy_bf_fast2_holdback();

    virtual void on_simulation_start(double date,
        const rapidjson::Value & batsim_event);
    virtual void on_start_from_checkpoint(double date,const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);
    //virtual void on_machine_unavailable_notify_event(double date, IntervalSet machines);
    virtual void on_machine_available_notify_event(double date, IntervalSet machines);
    //virtual void on_job_fault_notify_event(double date, std::string job);
    virtual void on_myKillJob_notify_event(double date);
    virtual void on_requested_call(double date,int id,  batsched_tools::call_me_later_types forWhat);
    //virtual void set_workloads(myBatsched::Workloads * w);
    virtual void make_decisions(double date,
        SortableJobOrder::UpdateInformation * update_info,
        SortableJobOrder::CompareInformation * compare_info);
    std::string to_json_desc(rapidjson::Document *doc);
    void on_machine_instant_down_up(double date);
    void on_machine_down_for_repair(double date);
    virtual void on_no_more_external_event_to_occur(double date);
    virtual void on_job_end(double date, std::vector<std::string> job_ids);
    virtual void on_machine_state_changed(double date, IntervalSet machines, int new_state);
    virtual void on_no_more_static_job_to_submit_received(double date);

private:
    //Normal maintenance functions
    bool handle_newly_finished_jobs();
        
    void handle_new_jobs_to_kill(double date);
    //************************************************************resubmission if killed
    //Handle jobs to queue back up (if killed)  , handled by _decision now
    //void handle_resubmission(double date);    
    //***********************************************************
    
    void handle_machines_coming_available(double date);
    void handle_ended_job_execution(bool job_ended,double date);
    void handle_newly_released_jobs(double date);

    //backfilling
    struct FinishedHorizonPoint
    {
        double date;
        int nb_released_machines;
        IntervalSet machines; //used if share-packing
    };

    struct Allocation
    {
        IntervalSet machines;
        std::list<FinishedHorizonPoint>::iterator horizon_it;
        bool has_horizon = true;
    };


private:
    // Machines currently available
    IntervalSet _available_machines;
    IntervalSet _unavailable_machines;
    IntervalSet _repair_machines;
    IntervalSet _available_core_machines;
   
    int _nb_available_machines = -1;

    // Pending jobs (queue)
    std::list<Job *> _pending_jobs;
    std::list<Job *> _pending_jobs_heldback;
    std::map<Job *,batsched_tools::Job_Message *> _my_kill_jobs;
    std::unordered_set<std::string> _running_jobs;
   // myBatsched::Workloads * _myWorkloads;
    double _oldDate=-1;
    int _killed=0;
    bool _wrap_it_up = false;
    bool _need_to_send_finished_submitting_jobs = true;
    bool _checkpointing_on=false;
    std::vector<double> _call_me_laters;
    std::string _output_folder;
        
    struct machine{
        int id;
        std::string name;
        int core_count = -1;
        int cores_available;
        double speed;
    };
    bool _share_packing = false;
    double _core_percent = 1.0;
    std::map<int,machine *> machines_by_int;
    std::map<std::string,machine *> machines_by_name;
    

    // Allocations of running jobs
    //std::unordered_map<std::string, IntervalSet> _current_allocations;

    //backfilling
    double compute_priority_job_expected_earliest_starting_time();

    std::list<FinishedHorizonPoint>::iterator insert_horizon_point(const FinishedHorizonPoint & point);

    std::unordered_map<std::string, Allocation> _current_allocations;
    std::list<FinishedHorizonPoint> _horizons;
    Job * _priority_job = nullptr;
    
    
    int _p_counter = 0; //pending jobs erased counter
    int _e_counter = 0; //execute job counter
    b_log *_myBLOG;
    int _share_packing_holdback = 0;
    IntervalSet _heldback_machines;

};
