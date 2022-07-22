#pragma once

#include <list>

#include "../isalgorithm.hpp"
#include "../external/pointers.hpp"
#include "../json_workload.hpp"
#include "../locality.hpp"
#include "../schedule.hpp"
#include "../batsched_tools.hpp"
#include <random>

class ConservativeBackfilling : public ISchedulingAlgorithm
{
public:
    //ConservativeBackfilling(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
    //                      double rjms_delay, std::string svg_prefix,rapidjson::Document * variant_options);
    ConservativeBackfilling(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                            double rjms_delay, rapidjson::Document * variant_options);
    virtual ~ConservativeBackfilling();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);
    void on_machine_instant_down_up(double date);
    void on_machine_down_for_repair(double date);
    virtual void set_workloads(myBatsched::Workloads * w);
    virtual void on_requested_call(double date,int id,  batsched_tools::call_me_later_types forWhat);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

private:
    Schedule _schedule;
    Queue * _reservation_queue=nullptr;
    std::string _output_folder;
    std::string _output_svg;
    Schedule::RESCHEDULE_POLICY _reschedule_policy;
    Schedule::IMPACT_POLICY _impact_policy;
    double _previous_date;
    std::vector<Schedule::ReservedTimeSlice> _saved_reservations;
    bool _killed_jobs = false;
    bool _need_to_send_finished_submitting_jobs = true;
    std::vector<std::string> _saved_recently_queued_jobs;
    std::vector<std::string> _saved_recently_ended_jobs;
    bool _need_to_compress = false;
    
    bool _dump_provisional_schedules = false;
    std::string _dump_prefix = "/tmp/dump";
    myBatsched::Workloads * _myWorkloads;
    bool _checkpointing_on;
    bool _start_a_reservation=false;
    b_log *_myBLOG;
    std::list<std::string>_resubmitted_jobs;
    std::vector<const Job *>_resubmitted_jobs_released;
    


    std::mt19937 generator;
    std::exponential_distribution<double> * distribution;
    std::mt19937 generator2;
    std::uniform_int_distribution<int> * unif_distribution;
    int _on_machine_instant_down_ups=0;
    int _on_machine_down_for_repairs = 0;
   
    

};
