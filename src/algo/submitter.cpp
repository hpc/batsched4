#include "submitter.hpp"

#include "../pempek_assert.hpp"

using namespace std;

Submitter::Submitter(Workload *workload, SchedulingDecision *decision, Queue *queue,
                     ResourceSelector *selector, double rjms_delay,
                     rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{

}

Submitter::~Submitter()
{

}

void Submitter::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    (void) date;

    available_machines.insert(MachineRange::ClosedInterval(0, _nb_machines - 1));
    PPK_ASSERT_ERROR(available_machines.size() == (unsigned int) _nb_machines);

    PPK_ASSERT_ERROR(batsim_config["job_submission"]["from_scheduler"]["enabled"].GetBool(),
            "This algorithm only works if dynamic job submissions are enabled!");
    PPK_ASSERT_ERROR(batsim_config["job_submission"]["from_scheduler"]["acknowledge"].GetBool(),
            "This algorithm only works if dynamic job submissions acknowledgements are enabled!");
    redis_enabled = batsim_config["redis"]["enabled"].GetBool();
}

void Submitter::on_simulation_end(double date)
{
    (void) date;
}

void Submitter::make_decisions(double date,
                               SortableJobOrder::UpdateInformation *update_info,
                               SortableJobOrder::CompareInformation *compare_info)
{
    // Let's update available machines
    for (const string & ended_job_id : _jobs_ended_recently)
    {
        int nb_available_before = available_machines.size();
        available_machines.insert(current_allocations[ended_job_id]);
        PPK_ASSERT_ERROR(nb_available_before + (*_workload)[ended_job_id]->nb_requested_resources == (int)available_machines.size());
        current_allocations.erase(ended_job_id);
    }

    // Let's handle recently released jobs
    for (const string & new_job_id : _jobs_released_recently)
    {
        const Job * new_job = (*_workload)[new_job_id];

        if (new_job->nb_requested_resources > _nb_machines)
            _decision->add_reject_job(new_job_id, date);
        else
            _queue->append_job(new_job, update_info);
    }

    // Queue sorting
    _queue->sort_queue(update_info, compare_info);

    // Whenever this algorithm execute a job, it submits
    // another one until he submitted enough jobs.
    if (!_queue->is_empty())
    {
        const Job * job = _queue->first_job();

        if (job->nb_requested_resources <= (int)available_machines.size())
        {
            MachineRange used_machines;
            if (_selector->fit(job, available_machines, used_machines))
            {
                _decision->add_execute_job(job->id, used_machines, date);
                current_allocations[job->id] = used_machines;

                available_machines.remove(used_machines);
                _queue->remove_job(job);

                if (nb_submitted_jobs < nb_jobs_to_submit)
                    submit_delay_job(1 + nb_submitted_jobs*30, date);
                else if (!finished_submitting_sent)
                {
                    _decision->add_scheduler_finished_submitting_jobs(date);
                    finished_submitting_sent = true;
                }
            }
        }
    }
}

void Submitter::submit_delay_job(double delay, double date)
{
    string workload_name = "dynamic";

    double submit_time = date;
    double walltime = delay + 5;
    int res = 1;
    string profile = "delay_" + std::to_string(nb_submitted_jobs);

    int buf_size = 128;

    char * buf_job = new char[buf_size];
    int nb_chars = snprintf(buf_job, buf_size,
             R"foo({"id":%d, "subtime":%g, "walltime":%g, "res":%d, "profile":"%s"})foo",
             nb_submitted_jobs, submit_time, walltime, res, profile.c_str());
    PPK_ASSERT_ERROR(nb_chars < buf_size - 1);

    char * buf_profile = new char[buf_size];
    nb_chars = snprintf(buf_profile, buf_size,
            R"foo({"type": "delay", "delay": %g})foo", delay);
    PPK_ASSERT_ERROR(nb_chars < buf_size - 1);

    string job_id = to_string(nb_submitted_jobs);

    _decision->add_submit_job(workload_name, job_id, profile,
                              buf_job, buf_profile, date);

    delete[] buf_job;
    delete[] buf_profile;

    ++nb_submitted_jobs;
}
