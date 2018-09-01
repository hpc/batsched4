#include "schedule.hpp"

#include <boost/algorithm/string/join.hpp>
#include <fstream>
#include <stdio.h>

#include <loguru.hpp>

#include "pempek_assert.hpp"

using namespace std;

Schedule::Schedule(int nb_machines, Rational initial_time)
{
    PPK_ASSERT_ERROR(nb_machines > 0);
    _nb_machines = nb_machines;

    TimeSlice slice;
    slice.begin = initial_time;
    slice.end = 1e19; // greater than the number of seconds elapsed since the big bang
    slice.length = slice.end - slice.begin;
    slice.available_machines.insert(IntervalSet::ClosedInterval(0, nb_machines - 1));
    slice.nb_available_machines = nb_machines;
    PPK_ASSERT_ERROR(slice.available_machines.size() == (unsigned int)nb_machines);

    _profile.push_back(slice);

    generate_colors();
}

Schedule::Schedule(const Schedule &other)
{
    *this = other;
}

Schedule &Schedule::operator=(const Schedule &other)
{
    _profile = other._profile;
    _nb_machines = other._nb_machines;
    _output_number = other._output_number;
    _colors = other._colors;

    return *this;
}

void Schedule::update_first_slice(Rational current_time)
{
    auto slice = _profile.begin();

    PPK_ASSERT_ERROR(
        current_time >= slice->begin, "current_time=%g, slice->begin=%g", (double)current_time, (double)slice->begin);
    PPK_ASSERT_ERROR(
        current_time <= slice->end, "current_time=%g, slice->end=%g", (double)current_time, (double)slice->end);

    Rational old_time = slice->begin;
    slice->begin = current_time;
    slice->length = slice->end - slice->begin;
    for (auto it = slice->allocated_jobs.begin(); it != slice->allocated_jobs.end(); ++it)
    {
        const Job *job_ref = (it->first);
        auto alloc_it = job_ref->allocations.find(old_time);

        if (alloc_it != job_ref->allocations.end())
        {
            job_ref->allocations[current_time] = alloc_it->second;
            job_ref->allocations.erase(alloc_it);
        }
    }
}

void Schedule::update_first_slice_removing_remaining_jobs(Rational current_time)
{
    PPK_ASSERT_ERROR(current_time < infinite_horizon());

    auto slice = _profile.begin();
    PPK_ASSERT_ERROR(
        current_time >= slice->begin, "current_time=%g, slice->begin=%g", (double)current_time, (double)slice->begin);

    while (current_time >= slice->end)
        slice = _profile.erase(slice);

    slice->begin = current_time;
    slice->length = slice->end - slice->begin;
}

void Schedule::remove_job(const Job *job)
{
    remove_job_first_occurence(job);
}

bool Schedule::remove_job_if_exists(const Job *job)
{
    auto job_first_slice = find_first_occurence_of_job(job, _profile.begin());

    if (job_first_slice != _profile.end())
    {
        remove_job_internal(job, job_first_slice);
        return true;
    }

    return false;
}

void Schedule::remove_job_all_occurences(const Job *job)
{
    auto job_first_slice = find_first_occurence_of_job(job, _profile.begin());

    while (job_first_slice != _profile.end())
    {
        remove_job_internal(job, job_first_slice);
        job_first_slice = find_first_occurence_of_job(job, job_first_slice);
    }
}

void Schedule::remove_job_first_occurence(const Job *job)
{
    auto job_first_slice = find_first_occurence_of_job(job, _profile.begin());
    PPK_ASSERT_ERROR(job_first_slice != _profile.end(),
        "Cannot remove job '%s' from the schedule since it is not in it", job->id.c_str());

    remove_job_internal(job, job_first_slice);
}

void Schedule::remove_job_last_occurence(const Job *job)
{
    auto job_first_slice = find_last_occurence_of_job(job, _profile.begin());
    PPK_ASSERT_ERROR(job_first_slice != _profile.end(),
        "Cannot remove job '%s' from the schedule since it is not in it", job->id.c_str());

    remove_job_internal(job, job_first_slice);
}

Schedule::JobAlloc Schedule::add_job_first_fit(
    const Job *job, ResourceSelector *selector, bool assert_insertion_successful)
{
    PPK_ASSERT_ERROR(!contains_job(job),
        "Invalid Schedule::add_job_first_fit call: Cannot add "
        "job '%s' because it is already in the schedule. %s",
        job->id.c_str(), to_string().c_str());

    return add_job_first_fit_after_time_slice(job, _profile.begin(), selector, assert_insertion_successful);
}

Schedule::JobAlloc Schedule::add_job_first_fit_after_time_slice(const Job *job,
    std::list<TimeSlice>::iterator first_time_slice, ResourceSelector *selector, bool assert_insertion_successful)
{
    if (_debug)
    {
        LOG_F(1, "Adding job '%s' (size=%d, walltime=%g). Output number %d. %s",
            job->id.c_str(), job->nb_requested_resources, (double)job->walltime,
            _output_number, to_string().c_str());
        output_to_svg();
    }

    // Let's scan the profile for an anchor point.
    // An anchor point is a point where enough processors are available to run this job
    for (auto pit = first_time_slice; pit != _profile.end(); ++pit)
    {
        // If the current time slice is an anchor point
        if ((int)pit->nb_available_machines >= job->nb_requested_resources)
        {
            // Let's continue to scan the profile to ascertain that
            // the machines remain available until the job's expected termination

            // If the job has no walltime, its size will be "infinite"
            if (!job->has_walltime)
            {
                // TODO: remove this ugly const_cast?
                const_cast<Job *>(job)->walltime = infinite_horizon() - pit->begin;
            }

            int availableMachinesCount = pit->nb_available_machines;
            Rational totalTime = pit->length;

            // If the job fits in the current time slice (temporarily speaking)
            if (totalTime >= job->walltime)
            {
                // Let's create the job allocation
                Schedule::JobAlloc *alloc = new Schedule::JobAlloc;

                // If the job fits in the current time slice (according to the fitting function)
                if (selector->fit(job, pit->available_machines, alloc->used_machines))
                {
                    Rational beginning = pit->begin;
                    alloc->begin = beginning;
                    alloc->end = alloc->begin + job->walltime;
                    alloc->started_in_first_slice = (pit == _profile.begin()) ? true : false;
                    alloc->job = job;
                    job->allocations[beginning] = alloc;

                    // Let's split the current time slice if needed
                    TimeSliceIterator first_slice_after_split;
                    TimeSliceIterator second_slice_after_split;
                    Rational split_date = pit->begin + job->walltime;
                    split_slice(pit, split_date, first_slice_after_split, second_slice_after_split);

                    // Let's remove the allocated machines from the available machines of the time slice
                    first_slice_after_split->available_machines.remove(alloc->used_machines);
                    first_slice_after_split->nb_available_machines -= job->nb_requested_resources;
                    first_slice_after_split->allocated_jobs[job] = alloc->used_machines;

                    if (_debug)
                    {
                        LOG_F(1, "Added job '%s' (size=%d, walltime=%g). Output number %d. %s", job->id.c_str(),
                            job->nb_requested_resources, (double)job->walltime, _output_number, to_string().c_str());
                        output_to_svg();
                    }

                    // The job has been placed, we can leave this function
                    return *alloc;
                }
            }
            else
            {
                // TODO : merge this big else with its if, as the "else" is a more general case of the "if"
                // The job does not fit in the current time slice (temporarily speaking)
                auto availableMachines = pit->available_machines;

                auto pit2 = pit;
                ++pit2;

                for (; (pit2 != _profile.end()) && ((int)pit2->nb_available_machines >= job->nb_requested_resources);
                     ++pit2)
                {
                    availableMachines &= pit2->available_machines;
                    availableMachinesCount = (int)availableMachines.size();
                    totalTime += pit2->length;

                    if (availableMachinesCount
                        < job->nb_requested_resources) // We don't have enough machines to run the job
                        break;
                    else if (totalTime
                        >= job->walltime) // The job fits in the slices [pit, pit2[ (temporarily speaking)
                    {
                        // Let's create the job allocation
                        JobAlloc *alloc = new JobAlloc;

                        // If the job fits in the current time slice (according to the fitting function)
                        if (selector->fit(job, availableMachines, alloc->used_machines))
                        {
                            alloc->begin = pit->begin;
                            alloc->end = alloc->begin + job->walltime;
                            alloc->started_in_first_slice = (pit == _profile.begin()) ? true : false;
                            alloc->job = job;
                            job->allocations[alloc->begin] = alloc;

                            // Let's remove the used machines from the slices before pit2
                            auto pit3 = pit;
                            for (; pit3 != pit2; ++pit3)
                            {
                                pit3->available_machines -= alloc->used_machines;
                                pit3->nb_available_machines -= job->nb_requested_resources;
                                pit3->allocated_jobs[job] = alloc->used_machines;
                            }

                            // Let's split the current time slice if needed
                            TimeSliceIterator first_slice_after_split;
                            TimeSliceIterator second_slice_after_split;
                            Rational split_date = pit->begin + job->walltime;
                            split_slice(pit2, split_date, first_slice_after_split, second_slice_after_split);

                            // Let's remove the allocated machines from the available machines of the time slice
                            first_slice_after_split->available_machines -= alloc->used_machines;
                            first_slice_after_split->nb_available_machines -= job->nb_requested_resources;
                            first_slice_after_split->allocated_jobs[job] = alloc->used_machines;

                            if (_debug)
                            {
                                LOG_F(1, "Added job '%s' (size=%d, walltime=%g). Output number %d. %s", job->id.c_str(),
                                    job->nb_requested_resources, (double)job->walltime, _output_number,
                                    to_string().c_str());
                                output_to_svg();
                            }

                            // The job has been placed, we can leave this function
                            return *alloc;
                        }
                    }
                }
            }
        }
    }

    if (assert_insertion_successful)
        PPK_ASSERT_ERROR(false, "Error in Schedule::add_job_first_fit: could not add job '%s' into %s", job->id.c_str(),
            to_string().c_str());

    JobAlloc failed_alloc;
    failed_alloc.has_been_inserted = false;
    return failed_alloc;
}

Schedule::JobAlloc Schedule::add_job_first_fit_after_time(
    const Job *job, Rational date, ResourceSelector *selector, bool assert_insertion_successful)
{
    if (_debug)
    {
        LOG_F(1, "Adding job '%s' (size=%d, walltime=%g) after date %g. Output number %d. %s", job->id.c_str(),
            job->nb_requested_resources, (double)job->walltime, (double)date, _output_number, to_string().c_str());
        output_to_svg();
    }

    // Let's first search at each time slice the job should be added
    auto insertion_slice_it = _profile.begin();
    bool insertion_slice_found = false;

    while (insertion_slice_it != _profile.end() && !insertion_slice_found)
    {
        if ((date >= insertion_slice_it->begin) && (date < insertion_slice_it->end))
        {
            insertion_slice_found = true;
        }
        else
            ++insertion_slice_it;
    }

    PPK_ASSERT_ERROR(insertion_slice_found, "Cannot find the insertion slice of date %g. Schedule : %s\n", (double)date,
        to_string().c_str());

    // Let's split the insertion slice in two parts if needed
    TimeSliceIterator first_slice_after_split;
    TimeSliceIterator second_slice_after_split;
    split_slice(insertion_slice_it, date, first_slice_after_split, second_slice_after_split);

    // In both cases (whether a split occured or not), we can simply call add_job_first_fit_after_time_slice on the
    // second slice now
    return add_job_first_fit_after_time_slice(job, second_slice_after_split, selector, assert_insertion_successful);
}

double Schedule::query_wait(int size, Rational time, ResourceSelector *selector)
{
    // very similar to job insertions...

    Job fake_job;
    fake_job.id = "fake";
    fake_job.unique_number = -1;
    fake_job.nb_requested_resources = size;
    fake_job.walltime = time;

    // Let's scan the profile for an anchor point.
    // An anchor point is a point where enough processors are available to run this job
    for (auto pit = _profile.begin(); pit != _profile.end(); ++pit)
    {
        // If the current time slice is an anchor point
        if ((int)pit->nb_available_machines >= size)
        {
            // Let's continue to scan the profile to ascertain that
            // the machines remain available until the job's expected termination

            int availableMachinesCount = pit->nb_available_machines;
            Rational totalTime = pit->length;

            // If the job fits in the current time slice (temporarily speaking)
            if (totalTime >= time)
            {
                IntervalSet used_machines;

                // If the job fits in the current time slice (according to the fitting function)
                if (selector->fit(&fake_job, pit->available_machines, used_machines))
                {
                    return static_cast<double>(pit->begin);
                }
            }
            else
            {
                // TODO : merge this big else with its if, as the "else" is a more general case of the "if"
                // The job does not fit in the current time slice (temporarily speaking)
                auto availableMachines = pit->available_machines;

                auto pit2 = pit;
                ++pit2;

                for (; (pit2 != _profile.end()) && ((int)pit2->nb_available_machines >= size); ++pit2)
                {
                    availableMachines &= pit2->available_machines;
                    availableMachinesCount = (int)availableMachines.size();
                    totalTime += pit2->length;

                    if (availableMachinesCount < size) // We don't have enough machines to run the job
                        break;
                    else if (totalTime >= time) // The job fits in the slices [pit, pit2[ (temporarily speaking)
                    {

                        IntervalSet used_machines;

                        // If the job fits in the current time slice (according to the fitting function)
                        if (selector->fit(&fake_job, availableMachines, used_machines))
                        {
                            return static_cast<double>(pit->begin);
                        }
                    }
                }
            }
        }
    }

    return -1;
}

Rational Schedule::first_slice_begin() const
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto it = _profile.begin();
    return it->begin;
}

Rational Schedule::finite_horizon() const
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto it = _profile.end();
    --it;
    return it->begin;
}

Rational Schedule::infinite_horizon() const
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto it = _profile.end();
    --it;
    return it->end;
}

std::multimap<std::string, Schedule::JobAlloc> Schedule::jobs_allocations() const
{
    multimap<std::string, Schedule::JobAlloc> res;

    map<const Job *, Rational> jobs_starting_times;
    map<const Job *, Rational> jobs_ending_times;
    set<const Job *> current_jobs;
    for (auto mit : _profile.begin()->allocated_jobs)
    {
        const Job *allocated_job = mit.first;
        current_jobs.insert(allocated_job);
        jobs_starting_times[allocated_job] = _profile.begin()->begin;
    }

    // Let's traverse the profile to find the beginning of each job
    for (auto slice_it = _profile.begin(); slice_it != _profile.end(); ++slice_it)
    {
        const TimeSlice &slice = *slice_it;
        set<const Job *> allocated_jobs;
        for (auto mit : slice.allocated_jobs)
        {
            const Job *job = mit.first;
            allocated_jobs.insert(job);
        }

        set<const Job *> finished_jobs;
        set_difference(current_jobs.begin(), current_jobs.end(), allocated_jobs.begin(), allocated_jobs.end(),
            std::inserter(finished_jobs, finished_jobs.end()));

        for (const Job *job : finished_jobs)
        {
            jobs_ending_times[job] = slice_it->begin;

            // Let's find where the job has been allocated
            PPK_ASSERT_ERROR(slice_it != _profile.begin());
            auto previous_slice_it = slice_it;
            --previous_slice_it;

            JobAlloc alloc;
            alloc.job = job;
            alloc.begin = jobs_starting_times[job];
            alloc.end = jobs_ending_times[job];
            alloc.started_in_first_slice = (alloc.begin == first_slice_begin());
            alloc.used_machines = previous_slice_it->allocated_jobs.at(job);

            res.insert({ job->id, alloc });
        }

        set<const Job *> new_jobs;
        set_difference(allocated_jobs.begin(), allocated_jobs.end(), current_jobs.begin(), current_jobs.end(),
            std::inserter(new_jobs, new_jobs.end()));

        for (const Job *job : new_jobs)
        {
            jobs_starting_times[job] = slice.begin;
        }

        // Update current_jobs
        for (const Job *job : finished_jobs)
            current_jobs.erase(job);
        for (const Job *job : new_jobs)
            current_jobs.insert(job);
    }

    return res;
}

bool Schedule::contains_job(const Job *job) const
{
    return find_first_occurence_of_job(job, _profile.begin()) != _profile.end();
}

bool Schedule::split_slice(Schedule::TimeSliceIterator slice_to_split, Rational date,
    Schedule::TimeSliceIterator &first_slice_after_split, Schedule::TimeSliceIterator &second_slice_after_split)
{
    if ((date > slice_to_split->begin) && (date < slice_to_split->end))
    {
        // The split must be done.
        // Let's create the new slice
        TimeSlice new_slice = *slice_to_split;

        new_slice.begin = date;
        new_slice.length = new_slice.end - new_slice.begin;
        PPK_ASSERT_ERROR(new_slice.length > 0);

        // Let's reduce the existing slice length
        slice_to_split->end = date;
        slice_to_split->length = slice_to_split->end - slice_to_split->begin;
        PPK_ASSERT_ERROR(slice_to_split->length > 0);

        // Let's insert the new_slice just after slice_to_split
        // To do so, since list::insert inserts BEFORE the given iterator, we must point after slice 1.
        auto list_insert_it = slice_to_split;
        ++list_insert_it;

        // Let's update returned iterators
        second_slice_after_split = _profile.insert(list_insert_it, new_slice);
        first_slice_after_split = second_slice_after_split;
        --first_slice_after_split;

        return true;
    }
    else
    {
        first_slice_after_split = slice_to_split;
        second_slice_after_split = slice_to_split;
        return false;
    }
}

Schedule::TimeSliceIterator Schedule::find_first_occurence_of_job(
    const Job *job, Schedule::TimeSliceIterator starting_point)
{
    PPK_ASSERT_ERROR(_profile.size() > 0);
    for (auto slice_it = starting_point; slice_it != _profile.end(); ++slice_it)
    {
        const TimeSlice &slice = *slice_it;
        if (slice.allocated_jobs.count(job) == 1)
            return slice_it;
    }

    return _profile.end();
}

Schedule::TimeSliceIterator Schedule::find_last_occurence_of_job(
    const Job *job, Schedule::TimeSliceIterator starting_point)
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto slice_it = _profile.end();
    bool found = false;

    do
    {
        --slice_it;

        const TimeSlice &slice = *slice_it;

        if (slice.allocated_jobs.count(job) == 1)
            found = true;
        else if (found) // If the job is no longer found, its starting point is just after the current slice
            return ++slice_it;

    } while (slice_it != starting_point);

    if (found)
        return starting_point;
    else
        return _profile.end();
}

Schedule::TimeSliceConstIterator Schedule::find_first_occurence_of_job(
    const Job *job, Schedule::TimeSliceConstIterator starting_point) const
{
    PPK_ASSERT_ERROR(_profile.size() > 0);
    for (auto slice_it = starting_point; slice_it != _profile.end(); ++slice_it)
    {
        const TimeSlice &slice = *slice_it;
        if (slice.allocated_jobs.count(job) == 1)
            return slice_it;
    }

    return _profile.end();
}

Schedule::TimeSliceConstIterator Schedule::find_last_occurence_of_job(
    const Job *job, Schedule::TimeSliceConstIterator starting_point) const
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto slice_it = _profile.end();
    bool found = false;

    do
    {
        --slice_it;

        const TimeSlice &slice = *slice_it;

        if (slice.allocated_jobs.count(job) == 1)
            found = true;
        else if (found) // If the job is no longer found, its starting point is just after the current slice
            return ++slice_it;

    } while (slice_it != starting_point);

    if (found)
        return starting_point;
    else
        return _profile.end();
}

Schedule::TimeSliceIterator Schedule::find_last_time_slice_before_date(Rational date, bool assert_not_found)
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto slice_it = _profile.end();

    do
    {
        --slice_it;

        const TimeSlice &slice = *slice_it;

        if (slice.begin <= date)
            return slice_it;

    } while (slice_it != _profile.begin());

    if (assert_not_found)
        PPK_ASSERT_ERROR(false, "No time slice beginning before date %g could be found. Schedule: %s\n", (double)date,
            to_string().c_str());
    return _profile.begin();
}

Schedule::TimeSliceConstIterator Schedule::find_last_time_slice_before_date(Rational date, bool assert_not_found) const
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto slice_it = _profile.end();

    do
    {
        --slice_it;

        const TimeSlice &slice = *slice_it;

        if (slice.begin <= date)
            return slice_it;

    } while (slice_it != _profile.begin());

    if (assert_not_found)
        PPK_ASSERT_ERROR(false, "No time slice beginning before date %g could be found. Schedule: %s\n", (double)date,
            to_string().c_str());
    return slice_it;
}

Schedule::TimeSliceIterator Schedule::find_first_time_slice_after_date(Rational date, bool assert_not_found)
{
    auto slice_it = _profile.begin();

    while (slice_it != _profile.end())
    {
        if (slice_it->begin >= date)
            return slice_it;
        ++slice_it;
    }

    if (assert_not_found)
        PPK_ASSERT_ERROR(false, "No time slice starting after date %g could be found. Schedule: %s\n", (double)date,
            to_string().c_str());

    return _profile.end();
}

Schedule::TimeSliceConstIterator Schedule::find_first_time_slice_after_date(Rational date, bool assert_not_found) const
{
    auto slice_it = _profile.begin();

    while (slice_it != _profile.end())
    {
        if (slice_it->begin >= date)
            return slice_it;
        ++slice_it;
    }

    if (assert_not_found)
        PPK_ASSERT_ERROR(false, "No time slice starting after date %g could be found. Schedule: %s\n", (double)date,
            to_string().c_str());

    return _profile.end();
}

IntervalSet Schedule::available_machines_during_period(Rational begin, Rational end) const
{
    PPK_ASSERT_ERROR(
        begin >= first_slice_begin(), "begin=%f, first_slice_begin()=%f", (double)begin, (double)first_slice_begin());
    PPK_ASSERT_ERROR(
        end <= infinite_horizon(), "end=%f, infinite_horizon()=%f", (double)end, (double)infinite_horizon());

    auto slice_it = find_first_time_slice_after_date(begin);
    IntervalSet available_machines = slice_it->available_machines;

    while (slice_it != _profile.end() && slice_it->begin < end)
    {
        available_machines &= slice_it->available_machines;

        ++slice_it;
    }

    return available_machines;
}

std::list<Schedule::TimeSlice>::iterator Schedule::begin()
{
    return _profile.begin();
}

std::list<Schedule::TimeSlice>::iterator Schedule::end()
{
    return _profile.end();
}

std::list<Schedule::TimeSlice>::const_iterator Schedule::begin() const
{
    return _profile.cbegin();
}

std::list<Schedule::TimeSlice>::const_iterator Schedule::end() const
{
    return _profile.cend();
}

int Schedule::nb_slices() const
{
    return (int) _profile.size();
}

string Schedule::to_string() const
{
    string res = "Schedule:\n";

    for (const TimeSlice &slice : _profile)
        res += slice.to_string(2, 2);

    return res;
}

string Schedule::to_svg() const
{
    Rational x0, x1, y0, y1;
    x0 = y0 = std::numeric_limits<double>::max();
    x1 = y1 = std::numeric_limits<double>::min();

    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto last_finite_slice = _profile.end();
    --last_finite_slice;

    const Rational second_width = 10;
    const Rational machine_height = 10;
    const Rational space_between_machines_ratio(1, 8);
    PPK_ASSERT_ERROR(space_between_machines_ratio >= 0 && space_between_machines_ratio <= 1);

    x0 = _profile.begin()->begin * second_width;
    x1 = last_finite_slice->begin * second_width;

    y0 = 0 * machine_height;
    y1 = _nb_machines * machine_height;

    const Rational width = x1 - x0 + 10;
    const Rational height = y1 - y0;

    const int buf_size = 4096;
    char *buf = new char[buf_size];

    // header
    snprintf(buf, buf_size,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"%g\" height=\"%g\">\n"
        "<title>Schedule</title>\n",
        (double)width, (double)height);

    string res = buf;

    // machines background color
    for (int i = 0; i < _nb_machines; ++i)
    {
        string machine_color;

        if (i % 2 == 0)
            machine_color = "#EEEEEE";
        else
            machine_color = "#DDDDDD";

        snprintf(buf, buf_size,
            "  <rect x=\"%g\" y=\"%g\" width=\"%g\" height=\"%g\" style=\"stroke:none; fill:%s;\"/>\n", (double)0,
            (double)(i * machine_height), (double)width, (double)machine_height, machine_color.c_str());
        res += buf;
    }

    map<const Job *, Rational> jobs_starting_times;
    set<const Job *> current_jobs;
    for (auto mit : _profile.begin()->allocated_jobs)
    {
        const Job *allocated_job = mit.first;
        current_jobs.insert(allocated_job);
        jobs_starting_times[allocated_job] = _profile.begin()->begin;
    }

    // Let's traverse the profile to find the beginning of each job
    for (auto slice_it = _profile.begin(); slice_it != _profile.end(); ++slice_it)
    {
        const TimeSlice &slice = *slice_it;
        set<const Job *> allocated_jobs;
        for (auto mit : slice.allocated_jobs)
        {
            const Job *job = mit.first;
            allocated_jobs.insert(job);
        }

        set<const Job *> finished_jobs;
        set_difference(current_jobs.begin(), current_jobs.end(), allocated_jobs.begin(), allocated_jobs.end(),
            std::inserter(finished_jobs, finished_jobs.end()));

        for (const Job *job : finished_jobs)
        {
            Rational rect_x0 = jobs_starting_times[job] * second_width - x0;
            Rational rect_x1 = slice.begin * second_width - x0;
            Rational rect_width = rect_x1 - rect_x0;
            string rect_color = _colors[job->unique_number % (int)_colors.size()];

            // Let's find where the job has been allocated
            PPK_ASSERT_ERROR(slice_it != _profile.begin());
            auto previous_slice_it = slice_it;
            --previous_slice_it;

            IntervalSet job_machines = previous_slice_it->allocated_jobs.at(job);

            // Let's create a rectangle for each contiguous part of the allocation
            for (auto it = job_machines.intervals_begin(); it != job_machines.intervals_end(); ++it)
            {
                PPK_ASSERT_ERROR(it->lower() <= it->upper());
                Rational rect_y0 = it->lower() * machine_height - y0;
                Rational rect_y1 = ((it->upper() + Rational(1)) * machine_height)
                    - (space_between_machines_ratio * machine_height) - y0;
                Rational rect_height = rect_y1 - rect_y0;

                snprintf(buf, buf_size,
                    "  <rect x=\"%g\" y=\"%g\" width=\"%g\" height=\"%g\" style=\"stroke:black; stroke-width=%g; "
                    "fill:%s;\"/>\n",
                    (double)rect_x0, (double)rect_y0, (double)rect_width, (double)rect_height,
                    (double)(std::min(second_width, machine_height) / 10), rect_color.c_str());

                res += buf;
            }
        }

        set<const Job *> new_jobs;
        set_difference(allocated_jobs.begin(), allocated_jobs.end(), current_jobs.begin(), current_jobs.end(),
            std::inserter(new_jobs, new_jobs.end()));

        for (const Job *job : new_jobs)
        {
            jobs_starting_times[job] = slice.begin;
        }

        // Update current_jobs
        for (const Job *job : finished_jobs)
            current_jobs.erase(job);
        for (const Job *job : new_jobs)
            current_jobs.insert(job);
    }

    res += "</svg>";

    delete[] buf;
    return res;
}

void Schedule::write_svg_to_file(const string &filename) const
{
    ofstream f(filename);

    if (f.is_open())
        f << to_svg() << "\n";

    f.close();
}

void Schedule::output_to_svg(const string &filename_prefix)
{
    const int bufsize = 128;
    char *buf = new char[bufsize];

    snprintf(buf, bufsize, "%s%06d.svg", filename_prefix.c_str(), _output_number);
    ++_output_number %= 10000000;

    write_svg_to_file(buf);

    delete[] buf;
}

int Schedule::nb_machines() const
{
    return _nb_machines;
}

void Schedule::generate_colors(int nb_colors)
{
    PPK_ASSERT_ERROR(nb_colors > 0);
    _colors.reserve(nb_colors);

    double h, s = 1, v = 1, r, g, b;
    const int color_bufsize = 16;
    char color_buf[color_bufsize];

    double hue_fraction = 360.0 / nb_colors;
    for (int i = 0; i < nb_colors; ++i)
    {
        h = i * hue_fraction;
        hsvToRgb(h, s, v, r, g, b);

        unsigned int red = std::max(0, std::min((int)(floor(256 * r)), 255));
        unsigned int green = std::max(0, std::min((int)(floor(256 * g)), 255));
        unsigned int blue = std::max(0, std::min((int)(floor(256 * g)), 255));

        snprintf(color_buf, color_bufsize, "#%02x%02x%02x", red, green, blue);
        _colors.push_back(color_buf);
    }

    random_shuffle(_colors.begin(), _colors.end());
}

void Schedule::remove_job_internal(const Job *job, Schedule::TimeSliceIterator removal_point)
{
    // Let's retrieve the machines used by the job
    PPK_ASSERT_ERROR(removal_point->allocated_jobs.count(job) == 1);
    IntervalSet job_machines = removal_point->allocated_jobs.at(job);

    if (_debug)
    {
        LOG_F(1, "Removing job '%s'. Output number %d. %s", job->id.c_str(), _output_number, to_string().c_str());
        output_to_svg();
    }

    // Let's iterate the time slices until the job is found
    for (auto pit = removal_point; pit != _profile.end(); ++pit)
    {
        // If the job was succesfully erased from the current slice (the job was in it)
        if (pit->allocated_jobs.erase(job) == 1)
        {
            pit->available_machines.insert(job_machines);
            pit->nb_available_machines += job->nb_requested_resources;

            // If the slice is not the first one, let's try to merge it with its preceding slice
            if (pit != _profile.begin())
            {
                auto previous = pit;
                previous--;

                // The slices are merged if they have the same jobs
                if (previous->allocated_jobs == pit->allocated_jobs)
                {
                    PPK_ASSERT_ERROR(previous->available_machines == pit->available_machines,
                        "Two consecutive time slices, do NOT use the same resources "
                        "whereas they contain the same jobs. Slices:\n%s%s",
                        previous->to_string(2).c_str(), pit->to_string(2).c_str());

                    pit->begin = previous->begin;
                    pit->length = pit->end - pit->begin;

                    // pit is updated to ensure --pit points to a valid location after erasure
                    pit = _profile.erase(previous);
                }
            }

            // Let's iterate the slices while the job is in it, and erase it
            for (++pit; pit != _profile.end() && pit->allocated_jobs.erase(job) == 1; ++pit)
            {
                pit->available_machines.insert(job_machines);
                pit->nb_available_machines += job->nb_requested_resources;

                // If the slice is not the first one, let's try to merge it with its preceding slice
                if (pit != _profile.begin())
                {
                    auto previous = pit;
                    previous--;

                    // The slices are merged if they have the same jobs
                    if (previous->allocated_jobs == pit->allocated_jobs)
                    {
                        PPK_ASSERT_ERROR(previous->available_machines == pit->available_machines,
                            "Two consecutive time slices, do NOT use the same resources "
                            "whereas they contain the same jobs. Slices:\n%s%s",
                            previous->to_string(2).c_str(), pit->to_string(2).c_str());

                        pit->begin = previous->begin;
                        pit->length = pit->end - pit->begin;

                        // pit is updated to ensure --pit points to a valid location after erasure
                        pit = _profile.erase(previous);
                    }
                }
            }

            // pit is either profile.end() or does NOT contain the job
            // Let's try to merge it with its previous slice
            if (pit != _profile.end())
            {
                if (pit != _profile.begin())
                {
                    auto previous = pit;
                    previous--;

                    // The slices are merged if they have the same jobs
                    if (previous->allocated_jobs == pit->allocated_jobs)
                    {
                        PPK_ASSERT_ERROR(previous->available_machines == pit->available_machines,
                            "Two consecutive time slices, do NOT use the same resources "
                            "whereas they contain the same jobs. Slices:\n%s%s",
                            previous->to_string(2).c_str(), pit->to_string(2).c_str());

                        pit->begin = previous->begin;
                        pit->length = pit->end - pit->begin;

                        // pit is updated to ensure --pit points to a valid location after erasure
                        pit = _profile.erase(previous);
                    }
                }
            }

            if (_debug)
            {
                LOG_F(1, "Removed job '%s'. Output number %d. %s", job->id.c_str(), _output_number, to_string().c_str());
                output_to_svg();
            }

            return;
        }
    }
}

bool Schedule::TimeSlice::contains_job(const Job *job) const
{
    return allocated_jobs.count(job);
}

bool Schedule::TimeSlice::contains_matching_job(std::function<bool(const Job *)> matching_function) const
{
    for (auto mit : allocated_jobs)
    {
        const Job *job = mit.first;
        if (matching_function(job))
            return true;
    }

    return false;
}

const Job *Schedule::TimeSlice::job_from_job_id(string job_id) const
{
    for (auto mit : allocated_jobs)
    {
        const Job *job = mit.first;
        if (job->id == job_id)
            return job;
    }

    return nullptr;
}

string Schedule::TimeSlice::to_string_interval() const
{
    double ibegin = begin.convert_to<double>();
    double iend = end.convert_to<double>();
    double ilength = length.convert_to<double>();

    char buf[256];
    snprintf(buf, 256, "[%f,%f] (length=%f)", ibegin, iend, ilength);

    return string(buf);
}

string Schedule::TimeSlice::to_string_allocated_jobs() const
{
    vector<string> jobs_str;
    jobs_str.reserve(allocated_jobs.size());
    for (auto mit : allocated_jobs)
    {
        const Job *job = mit.first;
        jobs_str.push_back(job->id);
    }

    return boost::algorithm::join(jobs_str, ",");
}

string Schedule::TimeSlice::to_string(int initial_indent, int indent) const
{
    string res;

    string iistr, istr;

    for (int i = 0; i < initial_indent; ++i)
        iistr += " ";

    for (int i = 0; i < indent; ++i)
        istr += " ";

    res += iistr + "Time slice: ";

    res += to_string_interval() + "\n";
    res += iistr + istr + "available machines: " + available_machines.to_string_brackets() + "\n";
    res += iistr + istr + "allocated jobs: {" + to_string_allocated_jobs() + "}\n";

    return res;
}

void hsvToRgb(double h, double s, double v, double &r, double &g, double &b)
{
    if (s == 0) // Achromatic (grey)
    {
        r = g = b = v;
        return;
    }

    h /= 60; // sector 0 to 5
    int i = floor(h);
    float f = h - i; // factorial part of h
    float p = v * (1 - s);
    float q = v * (1 - s * f);
    float t = v * (1 - s * (1 - f));

    switch (i)
    {
    case 0:
        r = v;
        g = t;
        b = p;
        break;
    case 1:
        r = q;
        g = v;
        b = p;
        break;
    case 2:
        r = p;
        g = v;
        b = t;
        break;
    case 3:
        r = p;
        g = q;
        b = v;
        break;
    case 4:
        r = t;
        g = p;
        b = v;
        break;
    default: // case 5:
        r = v;
        g = p;
        b = q;
        break;
    }
}
