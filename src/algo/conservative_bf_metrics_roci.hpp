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
//enum xyz{ JUST_QUEUE_METRICS,JUST_QUEUE_STRING,JUST_UTIL, QUEUE_AND_UTIL_METRICS,ALL};
struct metrics_roci
{
    std::string queue_string;
    std::string running_string;
    int hour=0;
};

class ConservativeBackfilling_metrics_roci : public ISchedulingAlgorithm
{
public:
    //ConservativeBackfilling_metrics_roci(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
    //                      double rjms_delay, std::string svg_prefix,rapidjson::Document * variant_options);
    ConservativeBackfilling_metrics_roci(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                            double rjms_delay, rapidjson::Document * variant_options);
    virtual ~ConservativeBackfilling_metrics_roci();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_event);
    virtual void on_start_from_checkpoint(double date,const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);
    virtual void on_machine_down_for_repair(batsched_tools::KILL_TYPES forWhat, double date);
    virtual void on_machine_instant_down_up(batsched_tools::KILL_TYPES forWhat, double date);
    //virtual void set_workloads(myBatsched::Workloads * w);
    virtual void set_machines(Machines *m);
    virtual void on_requested_call(double date,batsched_tools::CALL_ME_LATERS cml);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);
    virtual void on_checkpoint_batsched(double date);
    virtual void on_ingest_variables(const rapidjson::Document & doc,double date);
    virtual void on_first_jobs_submitted(double date);

private:
    void handle_killed_jobs(std::vector<std::string> & recently_queued_jobs,double date);
    void handle_reservations(std::vector<std::string> & recently_released_reservations,
                            std::vector<std::string>& recently_queued_jobs,
                            double date);
    void handle_schedule(std::vector<std::string>& recently_queued_jobs,double date);

    void get_queue_metrics(metrics_roci &metrics);
    
public:
 const std::string _metrics_type = "metrics_roci";
 

 std::vector<std::string> _executed_jobs;
 bool _need_to_write_metrics = false;
 metrics_roci _metrics;


    //double _previous_date;
           
    

    //myBatsched::Workloads * _myWorkloads;
   
       


   
    

};
