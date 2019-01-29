#include "energy_bf_monitoring_period.hpp"

#include <loguru.hpp>

#include "../pempek_assert.hpp"

using namespace std;

EnergyBackfillingMonitoringPeriod::EnergyBackfillingMonitoringPeriod(Workload * workload,
                                                                 SchedulingDecision * decision,
                                                                 Queue * queue,
                                                                 ResourceSelector * selector,
                                                                 double rjms_delay,
                                                                 rapidjson::Document * variant_options) :
    EnergyBackfilling(workload, decision, queue, selector, rjms_delay, variant_options)
{
    PPK_ASSERT_ERROR(variant_options->HasMember("output_dir"), "Invalid options JSON object: Member 'output_dir' cannot be found");
    PPK_ASSERT_ERROR((*variant_options)["output_dir"].IsString(), "Invalid options JSON object: Member 'output_dir' must be a string");
    _output_dir = (*variant_options)["output_dir"].GetString();

    PPK_ASSERT_ERROR(variant_options->HasMember("monitoring_period"), "Invalid options JSON object: Member 'monitoring_period' cannot be found");
    PPK_ASSERT_ERROR((*variant_options)["monitoring_period"].IsNumber(), "Invalid options JSON object: Member 'monitoring_period' must be a number");
    _period_between_monitoring_stages = (*variant_options)["monitoring_period"].GetDouble();
}

EnergyBackfillingMonitoringPeriod::~EnergyBackfillingMonitoringPeriod()
{

}

void EnergyBackfillingMonitoringPeriod::on_simulation_end(double date)
{
    EnergyBackfilling::on_simulation_end(date);

    PPK_ASSERT_ERROR(_simulation_finished == false);
    _simulation_finished = true;

    LOG_F(INFO, "EnergyBackfillingMonitoringPeriod: 'End of simulation' message received from Batsim. date=%g", date);
}

void EnergyBackfillingMonitoringPeriod::on_job_release(double date, const std::vector<string> &job_ids)
{
    if (!_monitoring_period_launched)
    {
        _next_monitoring_period_expected_date = date + _period_between_monitoring_stages;

        LOG_F(INFO, "EnergyBackfillingMonitoringPeriod: First monitoring nop is expected to be at date=%g",
               (double) _next_monitoring_period_expected_date);

        _decision->add_call_me_later((double)(_next_monitoring_period_expected_date), date);
        _nb_call_me_later_running++;
        _monitoring_period_launched = true;
    }

    EnergyBackfilling::on_job_release(date, job_ids);
}

void EnergyBackfillingMonitoringPeriod::on_requested_call(double date)
{
    EnergyBackfilling::on_requested_call(date);
    LOG_F(INFO, "on_requested_call, date = %g", date);

    if (!_simulation_finished)
    {
        // Let's execute on_monitoring_stage
        on_monitoring_stage(date);

        if (!_stop_sending_call_me_later)
        {
            // Let's request a call for the next monitoring stage
            _next_monitoring_period_expected_date = date + _period_between_monitoring_stages;
            _decision->add_call_me_later((double)(_next_monitoring_period_expected_date), date);
            _nb_call_me_later_running++;

            LOG_F(INFO, "EnergyBackfillingMonitoringPeriod: 'Chose to launch a call_me_later at %g",
                   (double)_next_monitoring_period_expected_date);
        }
    }
}

void EnergyBackfillingMonitoringPeriod::on_monitoring_stage(double date)
{
    LOG_F(INFO, "EnergyBackfillingMonitoringPeriod: Monitoring stage at date=%g", date);
}

Rational EnergyBackfillingMonitoringPeriod::period() const
{
    return _period_between_monitoring_stages;
}

Rational EnergyBackfillingMonitoringPeriod::next_monitoring_stage_date() const
{
    return _next_monitoring_period_expected_date;
}

bool EnergyBackfillingMonitoringPeriod::is_simulation_finished() const
{
    return _simulation_finished;
}


