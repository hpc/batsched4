#pragma once

#include <list>
#include <set>

#include "exact_numbers.hpp"
#include "locality.hpp"
#include "json_workload.hpp"
#include <intervalset.hpp>

class Schedule
{
public:
    struct TimeSlice
    {
        Rational begin;
        Rational end;
        Rational length;
        bool has_reservation = false;
        int nb_reservations = 0;

        IntervalSet available_machines;
        int nb_available_machines;
        std::map<const Job *, IntervalSet, JobComparator> allocated_jobs;

        bool contains_job(const Job * job) const;
        bool contains_matching_job(std::function<bool(const Job *)> matching_function) const;
        const Job * job_from_job_id(std::string job_id) const;

        std::string to_string_interval() const;
        std::string to_string_allocated_jobs() const;
        std::string to_string(int initial_indent = 0, int indent = 2) const;
    };

    typedef std::list<TimeSlice>::iterator TimeSliceIterator;
    typedef std::list<TimeSlice>::const_iterator TimeSliceConstIterator;

    typedef struct JobAlloc JobAlloc;

public:
    Schedule(int nb_machines = 1, Rational initial_time = 0);
    Schedule(const Schedule & other);

    Schedule & operator=(const Schedule & other);

    void update_first_slice(Rational current_time);
    void update_first_slice_removing_remaining_jobs(Rational current_time);

    void remove_job(const Job * job);
    bool remove_job_if_exists(const Job * job);
    void remove_job_all_occurences(const Job * job);
    void remove_job_first_occurence(const Job * job);
    void remove_job_last_occurence(const Job * job);
    void set_svg_prefix(std::string svg_prefix);
    bool remove_reservations_if_ready(std::vector<Job*>& jobs_removed);
    JobAlloc add_current_reservation(const Job * job, ResourceSelector * selector,
                               bool assert_insertion_successful = true);
    JobAlloc add_current_reservation_after_time_slice(const Job * job,
                                                std::list<TimeSlice>::iterator first_time_slice,
                                                ResourceSelector * selector,
                                                bool assert_insertion_successful = true);
    JobAlloc add_job_first_fit(const Job * job, ResourceSelector * selector,
                               bool assert_insertion_successful = true);
    JobAlloc reserve_time_slice(const Job * job);
    JobAlloc add_job_first_fit_after_time_slice(const Job * job,
                                                std::list<TimeSlice>::iterator first_time_slice,
                                                ResourceSelector * selector,
                                                bool assert_insertion_successful = true);
    JobAlloc add_job_first_fit_after_time(const Job * job,
                                          Rational date,
                                          ResourceSelector * selector,
                                          bool assert_insertion_successful = true);

  // The coveted query_wait method, bringing an answer (as a double, defined as
  // time away from now) to the question "when will I be able to schedule a job
  // using +size+ processors for +time+ units of time?".
    double query_wait(int size, Rational time, ResourceSelector * selector);
  
    Rational first_slice_begin() const;
    Rational finite_horizon() const;
    Rational infinite_horizon() const;

    std::multimap<std::string, JobAlloc> jobs_allocations() const;
    bool contains_job(const Job * job) const;

    bool split_slice(TimeSliceIterator slice_to_split, Rational date,
                     TimeSliceIterator & first_slice_after_split,
                     TimeSliceIterator & second_slice_after_split);

    TimeSliceIterator find_first_occurence_of_job(const Job * job, TimeSliceIterator starting_point);
    TimeSliceIterator find_last_occurence_of_job(const Job * job, TimeSliceIterator starting_point);
    TimeSliceConstIterator find_first_occurence_of_job(const Job * job, TimeSliceConstIterator starting_point) const;
    TimeSliceConstIterator find_last_occurence_of_job(const Job * job, TimeSliceConstIterator starting_point) const;

    TimeSliceIterator find_last_time_slice_before_date(Rational date, bool assert_not_found = true);
    TimeSliceConstIterator find_last_time_slice_before_date(Rational date, bool assert_not_found = true) const;

    TimeSliceIterator find_first_time_slice_after_date(Rational date, bool assert_not_found = true);
    TimeSliceConstIterator find_first_time_slice_after_date(Rational date, bool assert_not_found = true) const;

    IntervalSet available_machines_during_period(Rational begin, Rational end) const;

    std::list<TimeSlice>::iterator begin();
    std::list<TimeSlice>::iterator end();
    std::list<TimeSlice>::const_iterator begin() const;
    std::list<TimeSlice>::const_iterator end() const;

    int nb_slices() const;

    std::string to_string() const;
    std::string to_svg() const;
    void write_svg_to_file(const std::string & filename) const;
    void output_to_svg(const std::string & filename_prefix = "/tmp/schedule");

    void dump_to_batsim_jobs_file(const std::string & filename) const;
    void incremental_dump_as_batsim_jobs_file(const std::string & filename_prefix = "/tmp/schedule");

    int nb_machines() const;

private:
    void generate_colors(int nb_colors = 32);
    void remove_job_internal(const Job * job, TimeSliceIterator removal_point);

private:
    // The profile is a list of timeslices and a set of job allocations
    std::list<TimeSlice> _profile;
    int _nb_machines;
    bool _debug = false;
    std::string _svg_prefix;

    unsigned int _output_number = 0;
    std::vector<std::string> _colors;
};

/**
 * @brief Give the RGB representation of a color represented in HSV
 * @details This function is greatly inspired by http://www.cs.rit.edu/~ncs/color/t_convert.html
 * @param[in] h The hue, whose value is in [0,360]
 * @param[in] s The saturation, whose value is in [0,1]
 * @param[in] v The value, whose value is in [0,1]
 * @param[out] r The red, whose value is in [0,1]
 * @param[out] g The green, whose value is in [0,1]
 * @param[out] b The blue, whose value is in [0,1]
 */
void hsvToRgb(double h, double s, double v, double & r, double & g, double & b);
