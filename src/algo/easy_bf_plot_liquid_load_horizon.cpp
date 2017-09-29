#include "easy_bf_plot_liquid_load_horizon.hpp"

#include "../pempek_assert.hpp"

using namespace std;

EasyBackfillingPlotLiquidLoadHorizon::EasyBackfillingPlotLiquidLoadHorizon(Workload * workload,
                                                                           SchedulingDecision * decision,
                                                                           Queue * queue,
                                                                           ResourceSelector * selector,
                                                                           double rjms_delay,
                                                                           rapidjson::Document * variant_options) :
    EasyBackfilling(workload, decision, queue, selector, rjms_delay, variant_options)
{

    PPK_ASSERT_ERROR(variant_options->HasMember("trace_output_filename"),
                     "Invalid options JSON object: Member 'trace_output_filename' cannot be found");
    PPK_ASSERT_ERROR((*variant_options)["trace_output_filename"].IsString(),
            "Invalid options JSON object: Member 'trace_output_filename' must be a string");
    string trace_output_filename = (*variant_options)["trace_output_filename"].GetString();

    _output_file.open(trace_output_filename);
    PPK_ASSERT_ERROR(_output_file.is_open(), "Couldn't open file %s", trace_output_filename.c_str());

    string buf = "date,nb_jobs_in_queue,load_in_queue,liquid_load_horizon\n";
    //string buf = "date,nb_jobs_in_queue,load_in_queue,liquid_load_horizon,qt_mean_wt\n";
    _output_file.write(buf.c_str(), buf.size());
}

EasyBackfillingPlotLiquidLoadHorizon::~EasyBackfillingPlotLiquidLoadHorizon()
{
    _output_file.close();
}

void EasyBackfillingPlotLiquidLoadHorizon::make_decisions(double date,
                                                          SortableJobOrder::UpdateInformation *update_info,
                                                          SortableJobOrder::CompareInformation *compare_info)
{
    for (const string & new_job_id : _jobs_released_recently)
    {
        const Job * new_job = (*_workload)[new_job_id];
        PPK_ASSERT_ERROR(new_job->has_walltime,
                         "This scheduler only supports jobs with walltimes.");
    }

    EasyBackfilling::make_decisions(date, update_info, compare_info);
    write_current_metrics_in_file(date);
}

void EasyBackfillingPlotLiquidLoadHorizon::write_current_metrics_in_file(double date)
{
    Rational liquid_load_horizon = compute_liquid_load_horizon(_schedule, _queue, date);

    /*Rational queueing_theory_period = 60*60*24*10;
    estimator.remove_old(date - queueing_theory_period);
    Rational qt_mean_wt = estimator.estimate_waiting_time(queueing_theory_period);*/

    const int buf_size = 256;
    int nb_printed;
    char * buf = (char *) malloc(sizeof(char) * buf_size);

    nb_printed = snprintf(buf, buf_size, "%g,%d,%g,%g\n", date, _queue->nb_jobs(),
                          (double) _queue->compute_load_estimation(),
                          (double) liquid_load_horizon);
                          //(double) qt_mean_wt);
    PPK_ASSERT_ERROR(nb_printed < buf_size - 1,
                     "Buffer too small, some information might have been lost!");
    _output_file.write(buf, strlen(buf));

    free(buf);
}

Rational EasyBackfillingPlotLiquidLoadHorizon::compute_liquid_load_horizon(const Schedule &schedule,
                                                                           const Queue *queue,
                                                                           Rational starting_time)
{
    // Let's check whether the starting_time is valid
    PPK_ASSERT_ERROR(starting_time >= schedule.first_slice_begin());
    PPK_ASSERT_ERROR(starting_time < schedule.infinite_horizon());

    // Let's compute the total load (area) in the queue
    Rational load_to_distribute = queue->compute_load_estimation();

    // Let's fill the queue load into the schedule by fluidifying it
    auto slice_it = schedule.find_last_time_slice_before_date(starting_time, false);
    Rational current_time = starting_time;

    while (load_to_distribute > 0 && slice_it != schedule.end())
    {
        const Schedule::TimeSlice & slice = *slice_it;

        // If the starting time is in the middle of the schedule, the whole
        // time slice length is not to be considered.
        Rational amount_of_time_to_consider = max(Rational(0), slice.end - max(starting_time, slice.begin));
        Rational slice_empty_area = slice.available_machines.size() * amount_of_time_to_consider;

        if (slice_empty_area <= load_to_distribute)
        {
            load_to_distribute -= slice_empty_area;
            current_time = slice.end;
            ++slice_it;
        }
        else
        {
            PPK_ASSERT_ERROR(slice.available_machines.size() > 0);
            Rational amount_of_time_needed_to_fill_last_slice = load_to_distribute / slice.available_machines.size();
            current_time += amount_of_time_needed_to_fill_last_slice;
            load_to_distribute = 0;
        }
    }

    // Degenerate case: all the machines are probably in a sleep state
    if (load_to_distribute > 0)
        current_time = schedule.infinite_horizon();

    Rational ret_value = current_time - starting_time;
    PPK_ASSERT_ERROR(ret_value >= 0);
    return ret_value;
}
