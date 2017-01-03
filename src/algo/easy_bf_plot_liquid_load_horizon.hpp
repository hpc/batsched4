#pragma once

#include <fstream>

#include "easy_bf.hpp"
#include "../queueing_theory_waiting_time_estimator.hpp"

class EasyBackfillingPlotLiquidLoadHorizon : public EasyBackfilling
{
public:
    EasyBackfillingPlotLiquidLoadHorizon(Workload * workload,
                                         SchedulingDecision * decision,
                                         Queue * queue,
                                         ResourceSelector * selector,
                                         double rjms_delay,
                                         rapidjson::Document * variant_options);

    virtual ~EasyBackfillingPlotLiquidLoadHorizon();

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

public:
    void write_current_metrics_in_file(double date);

public:
    static Rational compute_liquid_load_horizon(const Schedule & schedule,
                                                const Queue * queue,
                                                Rational starting_time);

private:
    std::ofstream _output_file;
    //QueueingTheoryWaitingTimeEstimator estimator;
};

