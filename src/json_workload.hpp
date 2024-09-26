#pragma once

#include <map>

#include <rapidjson/document.h>

#include "exact_numbers.hpp"
#include "data_storage.hpp"
#include <intervalset.hpp>
//#include "external/batsched_workload.hpp"
//#include "external/batsched_job.hpp"
#include "external/batsched_profile.hpp"
#include "batsched_tools.hpp"
namespace myB = myBatsched;

struct JobAlloc;
namespace batsched_tools{struct checkpoint_job_data; struct start_from_chkpt;};

struct Job
{
    std::string id;  //the job id of the job w<workload#>!<job#>[#<resubmit#>][$<start_from_checkpoint#>]
    int unique_number; //a running count of jobs is kept as _job_number.  This makes it unique to each job and is set as its unique_number
    int nb_requested_resources; //number of requested nodes
    Rational walltime; //walltime
    Rational original_walltime = -1; //the original walltime as put into the workload that was originally submitted
    bool has_walltime = true;  //whether or not this job has a walltime
    double submission_time = 0;  //time that the job is to be submitted in this simulation
    double original_submit = -1;  //time that the job was originally submitted as put into the workload that was originally submitted
    std::vector<double> submission_times ;  //can possibly use for handling resubmitted jobs // all of the submission times this job has gone through
    
    double completion_time = -1; //the time the job completed
    mutable std::map<Rational, JobAlloc*> allocations;
    int cores=1;
    std::string purpose = "job";
    IntervalSet future_allocations;
    double start = -1;
    myBatsched::ProfilePtr profile = nullptr;
    std::string json_description;
    double checkpoint_interval;
    double dump_time;
    double read_time;
    batsched_tools::checkpoint_job_data * checkpoint_job_data;
    bool from_workload=false;
    
};


struct JobAlloc
{
  Rational begin;
  Rational end;
  bool started_in_first_slice;
  bool has_been_inserted = true;
  const Job * job;
  IntervalSet used_machines;
  std::string to_string()const;
};




class Workload
{
public:
    ~Workload();

    Job * operator[] (std::string jobID);
    const Job * operator[] (std::string jobID) const;
    int nb_jobs() const;

    void set_rjms_delay(Rational rjms_delay);

    void add_job_from_redis(RedisStorage &storage, const std::string & job_id, double submission_time);
    void add_job_from_json_object(const rapidjson::Value & object, const std::string & job_id, double submission_time);
    void add_job_from_json_description_string(const std::string & json_string, const std::string & job_id, double submission_time);

    Job * job_from_json_description_string(const std::string & json_string);
    Job * job_from_json_object(const rapidjson::Value & object);
    Job * job_from_json_object(const rapidjson::Value & job_object, const rapidjson::Value & profile_object);
    std::map<std::string, Job*> & get_jobs();

private:
    void put_file_into_buffer(const std::string & filename);
    void deallocate();
public:
    bool _checkpointing_on = false;
    bool _compute_checkpointing = false;
    double _MTBF = -1.0;
    double _SMTBF = -1.0;
    double _repair_time = -1.0;
    double _host_speed;
    double _fixed_failures = -1.0;
    double _checkpointing_interval = -1.0;
    int _seed_failures = -1;
    int _seed_failure_machine = -1;
    int _queue_depth = -1;
    bool _subtract_progress_from_walltime = false;
    int _seed_repair_time = -1;
    double _MTTR = -1.0;
    int _reject_jobs_after_nb_repairs = -1;
    batsched_tools::start_from_chkpt* start_from_checkpoint = nullptr;
private:
    char * _fileContents = nullptr;
    std::map<std::string, Job*> _jobs;
    //myB::Workload * _myWorkload;


    
    Rational _rjms_delay = 0;
    int _job_number = 0;

};

