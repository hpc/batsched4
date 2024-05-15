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

class FCFSFast2 : public ISchedulingAlgorithm
{
public:
    

    FCFSFast2(Workload * workload, SchedulingDecision * decision,
        Queue * queue, ResourceSelector * selector,
        double rjms_delay,
        rapidjson::Document * variant_options);
    virtual ~FCFSFast2();

    virtual void on_simulation_start(double date,
        const rapidjson::Value & batsim_config);


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
    //void handle_resubmission(double date);  handled by _decision now
    void on_machine_instant_down_up(double date);
    void on_machine_down_for_repair(double date);
    virtual void on_no_more_external_event_to_occur(double date);
    virtual void on_job_end(double date, std::vector<std::string> job_ids);
    virtual void on_machine_state_changed(double date, IntervalSet machines, int new_state);
    virtual void on_no_more_static_job_to_submit_received(double date);

    virtual void on_start_from_checkpoint(double date,const rapidjson::Value & batsim_config);
    virtual void on_ingest_variables(const rapidjson::Document & doc,double date);
    virtual void on_checkpoint_batsched(double date);
    virtual void on_first_jobs_submitted(double date){};
  
};
