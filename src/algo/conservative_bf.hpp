#pragma once

#include <list>

#include "../isalgorithm.hpp"
#include "../external/pointers.hpp"
#include "../json_workload.hpp"
#include "../locality.hpp"
#include "../schedule.hpp"
#include "../batsched_tools.hpp"
#include "../machine.hpp"
#include "../decision.hpp"
#include <random>


class ConservativeBackfilling : public ISchedulingAlgorithm
{
public:
    //ConservativeBackfilling(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
    //                      double rjms_delay, std::string svg_prefix,rapidjson::Document * variant_options);
    ConservativeBackfilling(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                            double rjms_delay, rapidjson::Document * variant_options);
    virtual ~ConservativeBackfilling();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_event);
    virtual void on_start_from_checkpoint(double date,const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);
    void on_machine_instant_down_up(batsched_tools::KILL_TYPES forWhat,double date);
    void on_machine_down_for_repair(batsched_tools::KILL_TYPES forWhat,double date);
    //virtual void set_workloads(myBatsched::Workloads * w);
    virtual void set_machines(Machines *m);
    virtual void on_requested_call(double date,int id,  batsched_tools::call_me_later_types forWhat);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);
    virtual void on_checkpoint_batsched(double date);
    virtual void on_ingest_variables(const rapidjson::Document & doc);
    virtual void on_first_jobs_submitted(double date);

private:
    void handle_killed_jobs(std::vector<std::string> & recently_queued_jobs,double date);
    void handle_reservations(std::vector<std::string> & recently_released_reservations,
                            std::vector<std::string>& recently_queued_jobs,
                            double date);
    void handle_schedule(std::vector<std::string>& recently_queued_jobs,double date);
    
    Schedule _schedule;
    Queue * _reservation_queue=nullptr;
    std::string _output_svg;
    long _svg_frame_start;
    long _svg_frame_end;
    long _svg_output_start;
    long _svg_output_end;
    Schedule::RESCHEDULE_POLICY _reschedule_policy;
    Schedule::IMPACT_POLICY _impact_policy;
    double _previous_date;
    std::vector<Schedule::ReservedTimeSlice> _saved_reservations;
    bool _killed_jobs = false;
    bool _need_to_send_finished_submitting_jobs = true;
    std::vector<std::string> _saved_recently_queued_jobs;
    std::vector<std::string> _saved_recently_ended_jobs;
    IntervalSet _recently_under_repair_machines;
    bool _need_to_compress = false;
    
    bool _dump_provisional_schedules = false;
    std::string _dump_prefix = "/tmp/dump";
    //myBatsched::Workloads * _myWorkloads;
    bool _checkpointing_on;
    bool _start_a_reservation=false;
    b_log *_myBLOG;
    std::map<std::string,batsched_tools::KILL_TYPES>_resubmitted_jobs;
    std::vector<std::pair<const Job *,batsched_tools::KILL_TYPES>>_resubmitted_jobs_released;
    


   
    std::vector<batsched_tools::KILL_TYPES> _on_machine_instant_down_ups;
    std::vector<batsched_tools::KILL_TYPES> _on_machine_down_for_repairs;

   
    

};
