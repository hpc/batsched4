#pragma once

#include <unordered_map>

#include <unordered_set>
#include <list>
#include <random>

#include "../isalgorithm.hpp"
#include "../external/pointers.hpp"
#include "../json_workload.hpp"
#include "../locality.hpp"
#include <rapidjson/document.h>
#include "../batsched_tools.hpp"

class easy_bf_fast2 : public ISchedulingAlgorithm
{
public:
    

    easy_bf_fast2(Workload * workload, SchedulingDecision * decision,
        Queue * queue, ResourceSelector * selector,
        double rjms_delay,
        rapidjson::Document * variant_options);
    virtual ~easy_bf_fast2();

    virtual void on_simulation_start(double date,
        const rapidjson::Value & batsim_event);
    virtual void on_start_from_checkpoint(double date,const rapidjson::Value & batsim_config);
    virtual void on_ingest_variables(const rapidjson::Document & doc,double date);
    virtual void on_first_jobs_submitted(double date){};

    virtual void on_simulation_end(double date);
    //virtual void on_machine_unavailable_notify_event(double date, IntervalSet machines);
    virtual void on_machine_available_notify_event(double date, IntervalSet machines);
    //virtual void on_job_fault_notify_event(double date, std::string job);
    virtual void on_myKillJob_notify_event(double date);
    virtual void on_requested_call(double date,batsched_tools::CALL_ME_LATERS cml);
    //virtual void set_workloads(myBatsched::Workloads * w);
    virtual void make_decisions(double date,
        SortableJobOrder::UpdateInformation * update_info,
        SortableJobOrder::CompareInformation * compare_info);
    std::string to_json_desc(rapidjson::Document *doc);
    virtual void on_machine_down_for_repair(batsched_tools::KILL_TYPES forWhat, double date);
    virtual void on_machine_instant_down_up(batsched_tools::KILL_TYPES forWhat, double date);
    virtual void on_no_more_external_event_to_occur(double date);
    virtual void on_job_end(double date, std::vector<std::string> job_ids);
    virtual void on_machine_state_changed(double date, IntervalSet machines, int new_state);
    virtual void on_no_more_static_job_to_submit_received(double date);
    virtual void on_checkpoint_batsched(double date){}

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
    void handle_null_priority_job(double date);



private:
  
    //backfilling
    double compute_priority_job_expected_earliest_starting_time();

    std::list<batsched_tools::FinishedHorizonPoint>::iterator insert_horizon_point(const batsched_tools::FinishedHorizonPoint & point);

};
