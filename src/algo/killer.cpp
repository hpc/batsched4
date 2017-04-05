#include "killer.hpp"

#include "../pempek_assert.hpp"

using namespace std;

Killer::Killer(Workload *workload,
               SchedulingDecision *decision,
               Queue *queue,
               ResourceSelector *selector,
               double rjms_delay,
               rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{

}

Killer::~Killer()
{

}

void Killer::on_simulation_start(double date)
{
    (void) date;

    available_machines.insert(MachineRange::ClosedInterval(0, _nb_machines - 1));
    PPK_ASSERT_ERROR(available_machines.size() == (unsigned int) _nb_machines);
}

void Killer::on_simulation_end(double date)
{
    (void) date;
}

void Killer::make_decisions(double date,
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
            _decision->add_rejection(new_job_id, date);
        else
            _queue->append_job(new_job, update_info);
    }

    // Queue sorting
    _queue->sort_queue(update_info, compare_info);

    // Let's now do a scheduling pass.
    // This algorithm can only execute one job by call, the priority one.
    // It is executed if it there are enough available resources.
    // The job kill is then queued 10 secondes after its starting.

    if (!_queue->is_empty())
    {
        const Job * job = _queue->first_job();

        if (job->nb_requested_resources <= (int)available_machines.size())
        {
            MachineRange used_machines;
            if (_selector->fit(job, available_machines, used_machines))
            {
                _decision->add_allocation(job->id, used_machines, date);
                _decision->add_kill(job->id, date + 10);
                current_allocations[job->id] = used_machines;

                available_machines.remove(used_machines);
                _queue->remove_job(job);
            }
        }
    }
}
