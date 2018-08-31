#include "crasher.hpp"

#include "../pempek_assert.hpp"

// Mouhahahaha
#include <stdlib.h>
#include <signal.h>

#include <stdexcept>

#include <loguru.hpp>

Crasher::CrashType Crasher::crash_type_from_string(const std::string &str)
{
    if (str == "segmentation_fault") return CrashType::SEGMENTATION_FAULT;
    else if (str == "infinite_loop") return CrashType::INFINITE_LOOP;
    else if (str == "terminate_processus_success") return CrashType::TERMINATE_PROCESSUS_SUCCESS;
    else if (str == "terminate_processus_failure") return CrashType::TERMINATE_PROCESSUS_FAILURE;
    else if (str == "abort") return CrashType::ABORT;
    else if (str == "suspend_process") return CrashType::SUSPEND_PROCESS;
    else
    {
        PPK_ASSERT_ERROR(false, "Invalid crash type string: %s", str.c_str());
        return CrashType::SEGMENTATION_FAULT;
    }
}

std::string Crasher::crash_type_to_string(Crasher::CrashType type)
{
    switch(type)
    {
    case CrashType::SEGMENTATION_FAULT: return "segmentation_fault";
    case CrashType::INFINITE_LOOP: return "infinite_loop";
    case CrashType::TERMINATE_PROCESSUS_SUCCESS: return "terminate_processus_success";
    case CrashType::TERMINATE_PROCESSUS_FAILURE: return "terminate_processus_failure";
    case CrashType::ABORT: return "abort";
    case CrashType::SUSPEND_PROCESS: return "suspend_process";
    }

    throw std::invalid_argument("Unknown crash type");
}

Crasher::Crasher(Workload *workload,
                   SchedulingDecision *decision,
                   Queue *queue,
                   ResourceSelector *selector,
                   double rjms_delay,
                   rapidjson::Document *variant_options) :
    Sequencer(workload, decision, queue, selector, rjms_delay, variant_options)
{
    if (variant_options->HasMember("crash_on_start"))
    {
        PPK_ASSERT_ERROR((*variant_options)["crash_on_start"].IsBool(),
                "Invalid options: 'crash_on_start' should be a boolean");
        _crash_on_start = (*variant_options)["crash_on_start"].GetBool();
    }

    if (variant_options->HasMember("crash_on_end"))
    {
        PPK_ASSERT_ERROR((*variant_options)["crash_on_end"].IsBool(),
                "Invalid options: 'crash_on_end' should be a boolean");
        _crash_on_end = (*variant_options)["crash_on_end"].GetBool();
    }

    if (variant_options->HasMember("crash_on_decision_call"))
    {
        PPK_ASSERT_ERROR((*variant_options)["crash_on_decision_call"].IsBool(),
                "Invalid options: 'crash_on_decision_call' should be a boolean");
        _crash_on_decision_call = (*variant_options)["crash_on_decision_call"].GetBool();
    }

    if (variant_options->HasMember("crash_on_decision_call_number"))
    {
        PPK_ASSERT_ERROR((*variant_options)["crash_on_decision_call_number"].IsNumber(),
                "Invalid options: 'crash_on_decision_call_number' should be a number");
        _crash_on_decision_call_number = (*variant_options)["crash_on_decision_call_number"].GetInt();
        PPK_ASSERT_ERROR(_crash_on_decision_call_number >= 0,
                         "Invalid options: 'crash_on_decision_call_number' should be non-negative "
                         "but got value=%d", _crash_on_decision_call_number);
    }

    if (variant_options->HasMember("crash_type"))
    {
        PPK_ASSERT_ERROR((*variant_options)["crash_type"].IsString(),
                "Invalid options: 'crash_type' should be a string");
        std::string crash_type_str = (*variant_options)["crash_type"].GetString();
        _crash_type = crash_type_from_string(crash_type_str);
    }

    LOG_SCOPE_FUNCTION(INFO);
    LOG_F(INFO, "crash_on_start: %d", _crash_on_start);
    LOG_F(INFO, "crash_on_end: %d", _crash_on_end);
    LOG_F(INFO, "crash_on_decision_call: %d", _crash_on_decision_call);
    LOG_F(INFO, "crash_on_decision_call_number: %d", _crash_on_decision_call_number);
    LOG_F(INFO, "crash_type: %s", crash_type_to_string(_crash_type).c_str());
}

Crasher::~Crasher()
{

}

void Crasher::on_simulation_start(double date, const rapidjson::Value &batsim_config)
{
    if (_crash_on_start)
        crash(_crash_type);

    Sequencer::on_simulation_start(date, batsim_config);
}

void Crasher::on_simulation_end(double date)
{
    if (_crash_on_end)
        crash(_crash_type);

    Sequencer::on_simulation_end(date);
}

void Crasher::make_decisions(double date,
                             SortableJobOrder::UpdateInformation *update_info,
                             SortableJobOrder::CompareInformation *compare_info)
{
    ++_decision_call_number;
    if (_crash_on_decision_call && _decision_call_number >= _crash_on_decision_call_number)
        crash(_crash_type);

    Sequencer::make_decisions(date, update_info, compare_info);
}

void Crasher::crash(Crasher::CrashType type)
{
    switch(type)
    {
    case CrashType::SEGMENTATION_FAULT:
        raise(SIGSEGV);
    case CrashType::INFINITE_LOOP:
    { for (;;); }
    case CrashType::TERMINATE_PROCESSUS_SUCCESS:
        exit(EXIT_SUCCESS);
    case CrashType::TERMINATE_PROCESSUS_FAILURE:
        exit(EXIT_FAILURE);
    case CrashType::ABORT:
        abort();
    case CrashType::SUSPEND_PROCESS:
        raise(SIGTSTP);
    }
}
