#include "queue.hpp"

#include <vector>

#include <boost/algorithm/string.hpp>

#include "pempek_assert.hpp"

using namespace std;

SortableJobOrder::UpdateInformation::UpdateInformation(Rational current_date) :
    current_date(current_date)
{

}

SortableJobOrder::UpdateInformation::~UpdateInformation()
{

}

SortableJobOrder::~SortableJobOrder()
{

}

void SortableJob::update_slowdown(Rational current_date)
{
    slowdown = current_date - release_date;
}

void SortableJob::update_bounded_slowdown(Rational current_date, Rational execution_time_lower_bound)
{
    bounded_slowdown = (current_date -  release_date) / execution_time_lower_bound;
}


FCFSOrder::~FCFSOrder()
{

}

bool FCFSOrder::compare(const SortableJob *j1, const SortableJob *j2, const SortableJobOrder::CompareInformation *info) const
{
    (void) info;

    if (j1->release_date == j2->release_date)
        return j1->job->id < j2->job->id;
    else
        return j1->release_date < j2->release_date;
}

void FCFSOrder::updateJob(SortableJob *job, const SortableJobOrder::UpdateInformation *info) const
{
    (void) job;
    (void) info;
}


LCFSOrder::~LCFSOrder()
{

}

bool LCFSOrder::compare(const SortableJob *j1, const SortableJob *j2, const SortableJobOrder::CompareInformation *info) const
{
    (void) info;

    if (j1->release_date == j2->release_date)
        return j1->job->id < j2->job->id;
    else
        return j1->release_date > j2->release_date;
}

void LCFSOrder::updateJob(SortableJob *job, const SortableJobOrder::UpdateInformation *info) const
{
    (void) job;
    (void) info;
}


DescendingBoundedSlowdownOrder::DescendingBoundedSlowdownOrder(Rational min_job_length) :
    _min_job_length(min_job_length)
{

}

DescendingBoundedSlowdownOrder::~DescendingBoundedSlowdownOrder()
{

}

bool DescendingBoundedSlowdownOrder::compare(const SortableJob *j1, const SortableJob *j2, const SortableJobOrder::CompareInformation *info) const
{
    (void) info;

    if (j1->bounded_slowdown == j2->bounded_slowdown)
        return j1->job->id < j2->job->id;
    else
        return j1->bounded_slowdown > j2->bounded_slowdown;
}

void DescendingBoundedSlowdownOrder::updateJob(SortableJob *job, const SortableJobOrder::UpdateInformation *info) const
{
    job->update_bounded_slowdown(info->current_date, _min_job_length);
}


DescendingSlowdownOrder::~DescendingSlowdownOrder()
{

}

bool DescendingSlowdownOrder::compare(const SortableJob *j1, const SortableJob *j2, const SortableJobOrder::CompareInformation *info) const
{
    (void) info;

    if (j1->slowdown == j2->slowdown)
        return j1->job->id < j2->job->id;
    else
        return j1->slowdown > j2->slowdown;
}

void DescendingSlowdownOrder::updateJob(SortableJob *job, const SortableJobOrder::UpdateInformation *info) const
{
    job->update_slowdown(info->current_date);
}


AscendingSizeOrder::~AscendingSizeOrder()
{

}

bool AscendingSizeOrder::compare(const SortableJob *j1, const SortableJob *j2, const SortableJobOrder::CompareInformation *info) const
{
    (void) info;

    if (j1->job->nb_requested_resources == j2->job->nb_requested_resources)
        return j1->job->id < j2->job->id;
    else
        return j1->job->nb_requested_resources < j2->job->nb_requested_resources;
}

void AscendingSizeOrder::updateJob(SortableJob *job, const SortableJobOrder::UpdateInformation *info) const
{
    (void) job;
    (void) info;
}


DescendingSizeOrder::~DescendingSizeOrder()
{

}

bool DescendingSizeOrder::compare(const SortableJob *j1, const SortableJob *j2, const SortableJobOrder::CompareInformation *info) const
{
    (void) info;

    if (j1->job->nb_requested_resources == j2->job->nb_requested_resources)
        return j1->job->id < j2->job->id;
    else
        return j1->job->nb_requested_resources > j2->job->nb_requested_resources;
}

void DescendingSizeOrder::updateJob(SortableJob *job, const SortableJobOrder::UpdateInformation *info) const
{
    (void) job;
    (void) info;
}


AscendingWalltimeOrder::~AscendingWalltimeOrder()
{

}

bool AscendingWalltimeOrder::compare(const SortableJob *j1, const SortableJob *j2, const SortableJobOrder::CompareInformation *info) const
{
    (void) info;

    if (j1->job->walltime == j2->job->walltime)
        return j1->job->id < j2->job->id;
    else
        return j1->job->walltime < j2->job->walltime;
}

void AscendingWalltimeOrder::updateJob(SortableJob *job, const SortableJobOrder::UpdateInformation *info) const
{
    (void) job;
    (void) info;
}


DescendingWalltimeOrder::~DescendingWalltimeOrder()
{

}

bool DescendingWalltimeOrder::compare(const SortableJob *j1, const SortableJob *j2, const SortableJobOrder::CompareInformation *info) const
{
    (void) info;

    if (j1->job->walltime == j2->job->walltime)
        return j1->job->id < j2->job->id;
    else
        return j1->job->walltime > j2->job->walltime;
}

void DescendingWalltimeOrder::updateJob(SortableJob *job, const SortableJobOrder::UpdateInformation *info) const
{
    (void) job;
    (void) info;
}
/**********
** QUEUE **
**********/

Queue::Queue(SortableJobOrder *order) :
    _order(order)
{

}

Queue::~Queue()
{
    auto it = _jobs.begin();

    while (it != _jobs.end())
        it = remove_job((*it)->job);
}

void Queue::append_job(const Job *job, SortableJobOrder::UpdateInformation *update_info)
{
    SortableJob * sjob = new SortableJob;
    sjob->job = job;
    sjob->release_date = update_info->current_date;

    _jobs.push_back(sjob);
}

std::list<SortableJob *>::iterator Queue::remove_job(const Job *job)
{
    auto it = std::find_if(_jobs.begin(), _jobs.end(),
                           [job](SortableJob * sjob)
                           {
                               return sjob->job == job;
                           });

    PPK_ASSERT_ERROR(it != _jobs.end(), "Cannot remove job '%s': not in the queue", job->id.c_str());
    return remove_job(it);
}

std::list<SortableJob *>::iterator Queue::remove_job(std::list<SortableJob *>::iterator job_it)
{
    SortableJob * sjob = *job_it;
    delete sjob;

    return _jobs.erase(job_it);
}

void Queue::sort_queue(SortableJobOrder::UpdateInformation *update_info,
                       SortableJobOrder::CompareInformation *compare_info)
{
    // Update of all jobs
    for (SortableJob * sjob : _jobs)
        _order->updateJob(sjob, update_info);

    // Sort
    _jobs.sort([this, compare_info](const SortableJob * j1, const SortableJob * j2)
                {
                    return _order->compare(j1, j2, compare_info);
    });
}

const Job* Queue::first_job() const
{
    PPK_ASSERT_ERROR(_jobs.size() > 0, "No first job: Queue is empty");
    return (*_jobs.begin())->job;
}

const Job *Queue::first_job_or_nullptr() const
{
    if (_jobs.size() == 0)
        return nullptr;
    else
        return first_job();
}

bool Queue::contains_job(const Job *job) const
{
    auto it = std::find_if(_jobs.begin(), _jobs.end(),
                           [job](SortableJob * sjob)
                           {
                               return sjob->job == job;
                           });
    return it != _jobs.end();
}

bool Queue::is_empty() const
{
    return _jobs.size() == 0;
}

int Queue::nb_jobs() const
{
    return _jobs.size();
}

Rational Queue::compute_load_estimation() const
{
    Rational load = 0;

    for (auto queue_it = _jobs.begin(); queue_it != _jobs.end(); ++queue_it)
    {
        const SortableJob * sjob = *queue_it;
        const Job * job = sjob->job;

        load += job->nb_requested_resources * job->walltime;
    }

    return load;
}

std::string Queue::to_string() const
{
    vector<string> jobs_strings;
    jobs_strings.reserve(_jobs.size());

    for (const SortableJob * sjob : _jobs)
        jobs_strings.push_back(sjob->job->id);

    return "[" + boost::algorithm::join(jobs_strings, ", ") + "]";
}

std::list<SortableJob *>::iterator Queue::begin()
{
    return _jobs.begin();
}

std::list<SortableJob *>::iterator Queue::end()
{
    return _jobs.end();
}

std::list<SortableJob *>::const_iterator Queue::begin() const
{
    return _jobs.begin();
}

std::list<SortableJob *>::const_iterator Queue::end() const
{
    return _jobs.end();
}

