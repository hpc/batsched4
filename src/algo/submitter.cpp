#include "submitter.hpp"

#include "../pempek_assert.hpp"

using namespace std;

Submitter::Submitter(Workload *workload, SchedulingDecision *decision, Queue *queue,
                     ResourceSelector *selector, double rjms_delay,
                     rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{

    if (variant_options->HasMember("nb_jobs_to_submit"))
    {
        PPK_ASSERT_ERROR((*variant_options)["nb_jobs_to_submit"].IsInt(),
                         "Bad algo options: nb_jobs_to_submit is not integral");
        nb_jobs_to_submit = (*variant_options)["nb_jobs_to_submit"].GetInt();
        PPK_ASSERT_ERROR(nb_jobs_to_submit >= 0,
                         "Bad algo options: nb_jobs_to_submit is negative (%d)", nb_jobs_to_submit);
    }

    printf("nb_jobs_to_submit: %d\n", nb_jobs_to_submit);
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
    dyn_submit_ack = batsim_config["job_submission"]["from_scheduler"]["acknowledge"].GetBool();
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
    if (_jobs_ended_recently.size() > 0)
    {
        for (const string & ended_job_id : _jobs_ended_recently)
        {
            int nb_available_before = available_machines.size();
            available_machines.insert(current_allocations[ended_job_id]);
            PPK_ASSERT_ERROR(nb_available_before + (*_workload)[ended_job_id]->nb_requested_resources == (int)available_machines.size());
            current_allocations.erase(ended_job_id);
        }
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

    // Trying to execute the priority job if possible
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
            }
        }
    }

    // If the queue is empty, dynamic jobs are submitted if possible
    if (_queue->is_empty() && available_machines.size() >= 1)
    {
        if (nb_submitted_jobs < nb_jobs_to_submit)
        {
            submit_delay_job(1 + nb_submitted_jobs*10, date);

            // If dynamic submissions acknowledgements are disabled, the job is directly executed
            if (!dyn_submit_ack)
            {
                // The execution is done 5 seconds after submitting the job
                date = date + 10;

                MachineRange used_machines = available_machines.left(1);

                string job_id = "dynamic!" + to_string(nb_submitted_jobs);
                _decision->add_execute_job(job_id, used_machines, date);
                current_allocations[job_id] = used_machines;

                available_machines.remove(used_machines);
            }

            ++nb_submitted_jobs;
        }
    }

    // Sending the "end of dynamic submissions" to Batsim if needed
    if ((nb_submitted_jobs >= nb_jobs_to_submit) && !finished_submitting_sent)
    {
        _decision->add_scheduler_finished_submitting_jobs(date);
        finished_submitting_sent = true;
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

    string job_id = to_string(nb_submitted_jobs);
    string unique_job_id = workload_name + "!" + job_id;

    char * buf_job = new char[buf_size];
    int nb_chars = snprintf(buf_job, buf_size,
             R"foo({"id":"%s", "subtime":%g, "walltime":%g, "res":%d, "profile":"%s"})foo",
             unique_job_id.c_str(), submit_time, walltime, res, profile.c_str());
    PPK_ASSERT_ERROR(nb_chars < buf_size - 1);

    char * buf_profile = new char[buf_size];
    nb_chars = snprintf(buf_profile, buf_size,
            R"foo({"type": "delay", "delay": %g})foo", delay);
    PPK_ASSERT_ERROR(nb_chars < buf_size - 1);

    _decision->add_submit_job(workload_name, job_id, profile,
                              buf_job, buf_profile, date);

    // If dynamic submisions ack is disabled, we must add the job in the workload now
    if (!dyn_submit_ack)
    {
        _workload->add_job_from_json_description_string(buf_job, unique_job_id, date);
    }

    delete[] buf_job;
    delete[] buf_profile;
}
