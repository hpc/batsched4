#pragma once

#include "energy_bf_monitoring_inertial_shutdown.hpp"

class EnergyBackfillingMachineSubpartSleeper : public EnergyBackfillingMonitoringInertialShutdown
{
public:
    EnergyBackfillingMachineSubpartSleeper(Workload * workload, SchedulingDecision * decision,
                                           Queue * queue, ResourceSelector * selector,
                                           double rjms_delay, rapidjson::Document * variant_options);
    virtual ~EnergyBackfillingMachineSubpartSleeper();

    virtual void on_monitoring_stage(double date);

private:
    Rational _fraction_of_machines_to_let_awake;
};
