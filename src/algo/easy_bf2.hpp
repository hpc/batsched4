#pragma once

#include <list>

#include "../isalgorithm.hpp"
#include "../json_workload.hpp"
#include "../locality.hpp"
#include "../schedule.hpp"
//added
#include "../machine.hpp"
#include "../batsched_tools.hpp"
#include <random>
// @note Leslie added 
#define B_LOG_INSTANCE _myBLOG
class EasyBackfilling2 : public ISchedulingAlgorithm
{
public:
    EasyBackfilling2(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                    double rjms_delay, rapidjson::Document * variant_options);
    virtual ~EasyBackfilling2();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_event);
    virtual void on_simulation_end(double date);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

    void sort_queue_while_handling_priority_job(const Job * priority_job_before,
                                                const Job *& priority_job_after,
                                                SortableJobOrder::UpdateInformation * update_info,
                                                SortableJobOrder::CompareInformation * compare_info);

    virtual void on_start_from_checkpoint(double date,const rapidjson::Value & batsim_config);
    virtual void on_checkpoint_batsched(double date);
    virtual void on_ingest_variables(const rapidjson::Document & doc,double date);
    virtual void on_first_jobs_submitted(double date);
    virtual bool all_submitted_jobs_check_passed();

    //added
    virtual void on_machine_down_for_repair(batsched_tools::KILL_TYPES forWhat, double date);
    virtual void on_machine_instant_down_up(batsched_tools::KILL_TYPES forWhat, double date);
    //virtual void set_workloads(myBatsched::Workloads * w);
    virtual void set_machines(Machines *m);
    virtual void on_requested_call(double date,batsched_tools::CALL_ME_LATERS cml);
    
protected:
    
    bool _debug = false;

    //added
    // @note Leslie commented out 
    //Queue * _reservation_queue=nullptr;
    //std::string _output_folder;



    double _previous_date;
    // @note Leslie commented out 
    //std::vector<Schedule::ReservedTimeSlice> _saved_reservations;
    bool _killed_jobs = false;
    bool _need_to_send_finished_submitting_jobs = true;
    std::vector<std::string> _saved_recently_queued_jobs;
    std::vector<std::string> _saved_recently_ended_jobs;
    IntervalSet _recently_under_repair_machines;
    bool _checkpointing_on;
    // @note Leslie commented out
    //bool _start_a_reservation=false; 
    bool _need_to_compress = false;
    
    b_log *_myBLOG;
    std::map<std::string,batsched_tools::KILL_TYPES>_resubmitted_jobs;
    std::vector<std::pair<const Job *,batsched_tools::KILL_TYPES>>_resubmitted_jobs_released;
    
    std::vector<batsched_tools::KILL_TYPES> _on_machine_instant_down_ups;
    std::vector<batsched_tools::KILL_TYPES> _on_machine_down_for_repairs;

};
