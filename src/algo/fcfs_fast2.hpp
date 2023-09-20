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
    virtual void on_requested_call(double date,int id,  batsched_tools::call_me_later_types forWhat);
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

private:
    // Machines currently available
    IntervalSet _available_machines;
    IntervalSet _unavailable_machines;
    IntervalSet _repair_machines;
    IntervalSet _available_core_machines;
   
    int _nb_available_machines = -1;

    // Pending jobs (queue)
    std::list<Job *> _pending_jobs;
    std::map<Job *,batsched_tools::Job_Message *> _my_kill_jobs;
    std::unordered_set<std::string> _running_jobs;
    //myBatsched::Workloads * _myWorkloads;
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
    std::unordered_map<std::string, IntervalSet> _current_allocations;
    b_log *_myBLOG;

};
