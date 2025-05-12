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
enum xyz{ JUST_QUEUE_METRICS,JUST_QUEUE_STRING,JUST_UTIL, QUEUE_AND_UTIL_METRICS,ALL};
struct metrics
{
    bool arrived = false;
    int _nb_jobs_in_queue =0;
    double _work_in_queue = 0.0;
    double _util = 0.0;
    std::string _queue_to_json_string;
};

class ConservativeBackfilling_metrics : public ISchedulingAlgorithm
{
public:
    //ConservativeBackfilling_metrics(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
    //                      double rjms_delay, std::string svg_prefix,rapidjson::Document * variant_options);
    ConservativeBackfilling_metrics(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                            double rjms_delay, rapidjson::Document * variant_options);
    virtual ~ConservativeBackfilling_metrics();

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

    void get_x_y_z_queue_metrics(xyz metric, metrics &metrics);
    
public:
 const std::string _metrics_type = "metrics";
 

 std::vector<std::string> _executed_jobs;
 metrics _arrive_metrics, _start_metrics, _end_metrics;


    //double _previous_date;
           
    

    //myBatsched::Workloads * _myWorkloads;
   
       


   
    

};
