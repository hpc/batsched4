#pragma once

#include "sequencer.hpp"

#include "../locality.hpp"
#include <intervalset.hpp>

class Workload;
class SchedulingDecision;

/**
 * @brief The amazing and gorgeous Crasher scheduler
 * @details This scheduler is meant to crash!
 *          It allows various types of crash,
 *          and allows to define when the crash should occur.
 */

class Crasher : public Sequencer
{
public:
    enum class CrashType
    {
        SEGMENTATION_FAULT,
        INFINITE_LOOP,
        TERMINATE_PROCESSUS_SUCCESS,
        TERMINATE_PROCESSUS_FAILURE,
        ABORT,
        SUSPEND_PROCESS,
    };

    CrashType crash_type_from_string(const std::string & str);
    std::string crash_type_to_string(CrashType type);

public:
    Crasher(Workload * workload, SchedulingDecision * decision, Queue * queue, ResourceSelector * selector,
             double rjms_delay, rapidjson::Document * variant_options);

    virtual ~Crasher();

    void on_simulation_start(double date, const rapidjson::Value & batsim_config);

    void on_simulation_end(double date);

    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info);

private:
    static void crash(CrashType type);

private:
    bool _crash_on_start = false;
    bool _crash_on_end = true;

    bool _crash_on_decision_call = false;
    int _crash_on_decision_call_number = 0;
    int _decision_call_number = -1;
    CrashType _crash_type = CrashType::SEGMENTATION_FAULT;
};
