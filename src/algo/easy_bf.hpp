#pragma once

#include <list>

#include "../isalgorithm.hpp"
#include "../json_workload.hpp"
#include "../locality.hpp"
#include "../schedule.hpp"

class EasyBackfilling : public ISchedulingAlgorithm
{
public:
    EasyBackfilling(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
                    double rjms_delay, rapidjson::Document * variant_options);
    virtual ~EasyBackfilling();

    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    virtual void on_simulation_end(double date);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

    void sort_queue_while_handling_priority_job(const Job * priority_job_before,
                                                const Job *& priority_job_after,
                                                SortableJobOrder::UpdateInformation * update_info,
                                                SortableJobOrder::CompareInformation * compare_info);

protected:
    Schedule _schedule;
    bool _debug = false;
};
