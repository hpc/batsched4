#include "rejecter.hpp"

Rejecter::Rejecter(Workload *workload,
                   SchedulingDecision *decision,
                   Queue *queue,
                   ResourceSelector *selector,
                   double rjms_delay,
                   rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{

}

Rejecter::~Rejecter()
{

}

void Rejecter::on_simulation_start(double date, const rapidjson::Value &batsim_config)
{
    (void) date;
    (void) batsim_config;
}

void Rejecter::on_simulation_end(double date)
{
    (void) date;
}

void Rejecter::make_decisions(double date,
                              SortableJobOrder::UpdateInformation *update_info,
                              SortableJobOrder::CompareInformation *compare_info)
{
    (void) update_info;
    (void) compare_info;

    for (const std::string & job_id : _jobs_released_recently)
        _decision->add_reject_job(job_id, date);
}
