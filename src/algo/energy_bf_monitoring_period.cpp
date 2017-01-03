#include "energy_bf_monitoring_period.hpp"

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

    printf("EnergyBackfillingMonitoringPeriod: 'End of simulation' message received from Batsim. date=%g\n", date);
}

void EnergyBackfillingMonitoringPeriod::on_job_release(double date, const std::vector<string> &job_ids)
{
    if (!_monitoring_period_launched)
    {
        _next_monitoring_period_expected_date = date + _period_between_monitoring_stages;

        printf("EnergyBackfillingMonitoringPeriod: First monitoring nop is expected to be at date=%g\n",
               (double) _next_monitoring_period_expected_date);

        _decision->add_nop_me_later((double)(_next_monitoring_period_expected_date), date);
        _nb_nop_me_later_running++;
        _monitoring_period_launched = true;
    }

    EnergyBackfilling::on_job_release(date, job_ids);
}

void EnergyBackfillingMonitoringPeriod::on_nop(double date)
{
    EnergyBackfilling::on_nop(date);

    printf("on_nop, date = %g\n", date);

    // Let's detect whether this NOP corresponds to a monitoring stage
    bool is_monitoring_nop = false;

    if (date >= _next_monitoring_period_expected_date)
    {
        is_monitoring_nop = true;
    }
    else
    {
        Rational diff = abs(_next_monitoring_period_expected_date - date);
        if (diff < 1)
            is_monitoring_nop = true;
        else if (_nb_nop_me_later_running == 0)
            is_monitoring_nop = true;
        else
        {
            printf("EnergyBackfillingMonitoringPeriod: Not a monitoring nop... diff = %g\n",
                   (double) diff);
        }
    }

    if (is_monitoring_nop)
    {
        on_monitoring_stage(date);

        // Let's call the next monitoring stage if the simulation is not finished
        if (!_simulation_finished)
        {
            _next_monitoring_period_expected_date = date + _period_between_monitoring_stages;
            _decision->add_nop_me_later((double)(_next_monitoring_period_expected_date), date);
            _nb_nop_me_later_running++;

            printf("EnergyBackfillingMonitoringPeriod: 'Chose to launch a nop_me_later at %g\n",
                   (double)_next_monitoring_period_expected_date);
        }
    }
}

void EnergyBackfillingMonitoringPeriod::on_monitoring_stage(double date)
{
    cout << "EnergyBackfillingMonitoringPeriod: Monitoring stage at date=" << date << endl;
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


