#include "wt_estimator.hpp"

WaitingTimeEstimator::WaitingTimeEstimator(Workload *workload,
                   SchedulingDecision *decision,
                   Queue *queue,
                   ResourceSelector *selector,
                   double rjms_delay,
                   rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{

}

WaitingTimeEstimator::~WaitingTimeEstimator()
{

}

void WaitingTimeEstimator::on_simulation_start(double date, const rapidjson::Value &batsim_config)
{
    (void) date;
    (void) batsim_config;
}

void WaitingTimeEstimator::on_simulation_end(double date)
{
    (void) date;
}

void WaitingTimeEstimator::make_decisions(double date,
                                          SortableJobOrder::UpdateInformation *update_info,
                                          SortableJobOrder::CompareInformation *compare_info)
{
    (void) update_info;
    (void) compare_info;

    /* Another stupid algorithm.
     * This one rejects all jobs.
     * It also estimates a negative waiting time for all jobs, as they will never be launched.
     */

    for (const std::string & job_id : _jobs_whose_waiting_time_estimation_has_been_requested_recently)
        _decision->add_answer_estimate_waiting_time(job_id, -1, date);

    for (const std::string & job_id : _jobs_released_recently)
        _decision->add_reject_job(job_id, date);
}
