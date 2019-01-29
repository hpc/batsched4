#pragma once

#include "energy_bf.hpp"

class EnergyBackfillingMonitoringPeriod : public EnergyBackfilling
{
public:
    EnergyBackfillingMonitoringPeriod(Workload * workload,
                                      SchedulingDecision * decision,
                                      Queue * queue,
                                      ResourceSelector * selector,
                                      double rjms_delay,
                                      rapidjson::Document * variant_options);

    virtual ~EnergyBackfillingMonitoringPeriod();

    virtual void on_simulation_end(double date);

    virtual void on_job_release(double date, const std::vector<std::string> & job_ids);

    virtual void on_requested_call(double date);

    virtual void on_monitoring_stage(double date);

public:
    Rational period() const;
    Rational next_monitoring_stage_date() const;
    bool is_simulation_finished() const;

protected:
    std::string _output_dir;
    bool _stop_sending_call_me_later = false;

private:
    bool _monitoring_period_launched = false;
    bool _simulation_finished = false;
    Rational _period_between_monitoring_stages;
    Rational _next_monitoring_period_expected_date;

};
