#pragma once

#include <list>
#include <set>

#include "exact_numbers.hpp"
#include "locality.hpp"
#include "json_workload.hpp"
#include <intervalset.hpp>
#include "batsched_tools.hpp"
class Workload;
struct JobAlloc;
namespace batsched_tools{struct start_from_chkpt;}
struct JobComparator
{
    bool operator()(const Job * j1, const Job * j2) const;
};
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
        IntervalSet allocated_machines;//machines in timeslice currently running jobs.  This does not include machines that are down(repair_machines).
        int nb_available_machines;
        std::map<const Job *, IntervalSet, JobComparator> allocated_jobs;
        //std::map<const Job *, IntervalSet, JobComparator> allocated_reservations;

        bool contains_job(const Job * job) const;
        bool contains_matching_job(std::function<bool(const Job *)> matching_function) const;
        bool operator==(const TimeSlice & t);
        const Job * job_from_job_id(std::string job_id) const;

        std::string to_string_interval() const;
        std::string to_string_allocated_jobs() const;
        std::string to_string(int initial_indent = 0, int indent = 2) const;
        std::string to_json_string_interval() const;
        std::string to_json_string(int initial_indent=0,int indent = 2) const;
    };
    enum class RESCHEDULE_POLICY{NONE,AFFECTED,ALL};
    enum class IMPACT_POLICY{NONE,LEAST_KILLING_LARGEST_FIRST,LEAST_KILLING_SMALLEST_FIRST,LEAST_RESCHEDULING};
    
    
    static void convert_policy(std::string policy,RESCHEDULE_POLICY& variable);
    static void convert_policy(std::string policy, IMPACT_POLICY& variable);
    

    typedef std::list<TimeSlice>::iterator TimeSliceIterator;
    typedef std::list<TimeSlice>::const_iterator TimeSliceConstIterator;

    

    struct ReservedTimeSlice{
        bool operator==(const ReservedTimeSlice & r) const;
        bool success = false;
        std::vector<const Job*> jobs_affected;
        std::vector<const Job*> jobs_needed_to_be_killed;
        std::vector<const Job*> jobs_to_reschedule;
        TimeSliceIterator slice_begin;
        TimeSliceIterator slice_end;
        JobAlloc* alloc;
        const Job * job;
        std::string to_string() const;
        std::string to_json_string() const;
         
    };

public:
    Schedule(int nb_machines = 1, Rational initial_time = 0);
    Schedule(const Schedule & other);
    std::string vector_to_json_string(const std::vector<ReservedTimeSlice> *vec) const;
    std::string list_to_json_string(const std::list<ReservedTimeSlice> *lst) const;
    void ingest_schedule(rapidjson::Document & doc);
    TimeSlice TimeSlice_from_json(const rapidjson::Value & Vslice);
    std::map<const Job *, IntervalSet, JobComparator> JobMap_from_json(const rapidjson::Value & Vjobs,Rational begin);
    JobAlloc * JobAlloc_from_json(const rapidjson::Value & Valloc);
    std::vector<const Job *> JobVector_from_json(const rapidjson::Value & Varray);
    Schedule::ReservedTimeSlice ReservedTimeSlice_from_json(const rapidjson::Value & Vslice);

    Schedule & operator=(const Schedule & other);
    void set_now(Rational now);

    void update_first_slice(Rational current_time);
    void update_first_slice_removing_remaining_jobs(Rational current_time);

    void remove_job(const Job * job);
    bool remove_job_if_exists(const Job * job);
    void remove_job_all_occurences(const Job * job);
    void remove_job_first_occurence(const Job * job);
    void remove_job_last_occurence(const Job * job);
   
    
    JobAlloc add_job_first_fit(const Job * job, ResourceSelector * selector,
                               bool assert_insertion_successful = true);
    JobAlloc add_repair_job(Job *);

    void incorrect_call_me_later(double difference);
    
    
    
    //CCU-LANL additions start
    void set_workload(Workload * workload);
    void set_start_from_checkpoint(batsched_tools::start_from_chkpt * sfc);
    IntervalSet add_repair_machine(IntervalSet machine,double duration);
    IntervalSet remove_repair_machines(IntervalSet machines);
    ReservedTimeSlice reserve_time_slice(const Job * job);
    void add_reservation(ReservedTimeSlice reservation);
    void find_least_impactful_fit(JobAlloc * alloc, TimeSliceIterator begin_slice, TimeSliceIterator end_slice,IMPACT_POLICY policy);
    JobAlloc add_job_first_fit_after_time_slice(const Job * job,
                                                std::list<TimeSlice>::iterator first_time_slice,
                                                ResourceSelector * selector,
                                                bool assert_insertion_successful = true);
    JobAlloc add_job_first_fit_after_time(const Job * job,
                                          Rational date,
                                          ResourceSelector * selector,
                                          bool assert_insertion_successful = true);
    void add_reservation_for_svg_outline(const ReservedTimeSlice & reservation_to_be );
    void remove_reservation_for_svg_outline(const ReservedTimeSlice & reservation_to_be);
    void set_output_svg(std::string output_svg);
    void set_output_svg_method(std::string output_svg_method);
    void set_svg_prefix(std::string svg_prefix);
    void set_svg_frame_and_output_start_and_end(long frame_start, long frame_end,long output_start,long output_end);
    void set_policies(RESCHEDULE_POLICY r_policy, IMPACT_POLICY i_policy);
    void add_svg_highlight_machines(IntervalSet machines);
    bool remove_svg_highlight_machines(IntervalSet machines);
    bool remove_reservations_if_ready(std::vector<const Job*>& jobs_removed);

    JobAlloc add_current_reservation(const Job * job, ResourceSelector * selector,
                               bool assert_insertion_successful = true);
    JobAlloc add_current_reservation_after_time_slice(const Job * job,
                                                std::list<TimeSlice>::iterator first_time_slice,
                                                ResourceSelector * selector,
                                                bool assert_insertion_successful = true);
    int get_number_of_running_jobs();
    void get_jobs_running_on_machines(IntervalSet machines, std::vector<std::string>& jobs_running_on_machines);
    void get_jobs_running_on_machines(IntervalSet machines, std::map<const Job*,IntervalSet>& jobs_running_on_machines);
    void get_jobs_affected_on_machines(IntervalSet machines, std::vector<std::string>& jobs_affected_on_machines,bool reservations_too=false);
    void get_jobs_affected_on_machines(IntervalSet machines, std::map<const Job*,IntervalSet>& jobs_affected_on_machines,bool reservations_too=false);
    Rational get_smallest_time_slice_length();
    Rational get_largest_time_slice_length();
    int get_number_of_running_machines();
    double get_utilization();
    double get_utilization_no_resv();
    IntervalSet get_machines_running_reservations();
    IntervalSet get_machines_running_reservations_on_slice(TimeSliceIterator slice);
    void set_smallest_and_largest_time_slice_length(Rational length);
    IntervalSet which_machines_are_allocated_in_time_slice(TimeSliceIterator slice,IntervalSet machine);
    std::vector<std::string> get_reservations_running_on_machines(IntervalSet machines);

    //CCU-LANL additions end


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
    int size();
    int nb_reservations_size();
    int nb_jobs_size();

    int nb_slices() const;

    std::string to_string() const;
    std::string to_json_string() const;
    
    std::string to_svg(const std::string &message, const std::list<ReservedTimeSlice> & svg_reservations) const;
    void write_svg_to_file(const std::string & filename, 
                        const std::string & message,
                        const std::list<ReservedTimeSlice> & svg_reservations) const;
    void output_to_svg(const std::string & message,bool json=false);

    void dump_to_batsim_jobs_file(const std::string & filename) const;
    void incremental_dump_as_batsim_jobs_file(const std::string & filename_prefix = "/tmp/schedule");

    int nb_machines() const;

private:
    void generate_colors(int nb_colors = 32);
    void remove_job_internal(const Job * job, TimeSliceIterator removal_point);

private:
    
    Rational _now = 0;
    // The profile is a list of timeslices and a set of job allocations
    std::list<TimeSlice> _profile;
    int _size = 0;
    int _nb_reservations_size = 0;
    int _nb_jobs_size = 0;
    int _nb_machines;
    bool _debug = false;
    bool _short_debug = false; //make svg's less frequently than _debug
    std::string _output_svg = "none"; //(none||all||short)  affects _debug and _short_debug
    std::string _svg_prefix; //the output path of the svg's output when _debug/_short_debug are true
    RESCHEDULE_POLICY _reschedule_policy = RESCHEDULE_POLICY::AFFECTED;  //whether to reschedule only affected jobs or all the jobs after a reservation addition
    IMPACT_POLICY _impact_policy = IMPACT_POLICY::LEAST_KILLING_LARGEST_FIRST;
    unsigned int _output_number = 1;
    unsigned int _frame_number = 1;
    Rational _previous_time_end = 0;
    std::list<ReservedTimeSlice> _svg_reservations;
    IntervalSet _svg_highlight_machines;
    std::vector<std::string> _colors;
    std::vector<std::string> _reservation_colors;
    IntervalSet _repair_machines;
    Rational _smallest_time_slice_length=0;
    Rational _largest_time_slice_length=1e19;
    std::string _output_svg_method = "svg";
    long _svg_frame_start = 1;
    long _svg_frame_end = -1;
    long _svg_output_start = 1;
    long _svg_output_end = -1;
    Workload * _workload = nullptr;
    batsched_tools::start_from_chkpt * _start_from_checkpoint=nullptr;
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
