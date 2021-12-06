#ifndef BATSCHED_JOB_HPP
#define BATSCHED_JOB_HPP

#include <unordered_map>
#include <vector>
#include <rapidjson/document.h>
#include <intervalset.hpp>

#include "pointers.hpp"


namespace myBatsched{
class Profiles;
struct Profile;
class Workload;
struct Job;
enum class JobState
{
     JOB_STATE_NOT_SUBMITTED                //!< The job exists but cannot be scheduled yet.
    ,JOB_STATE_SUBMITTED                    //!< The job has been submitted, it can now be scheduled.
    ,JOB_STATE_RUNNING                      //!< The job has been scheduled and is currently being processed.
    ,JOB_STATE_COMPLETED_SUCCESSFULLY       //!< The job execution finished before its walltime successfully.
    ,JOB_STATE_COMPLETED_FAILED             //!< The job execution finished before its walltime but the job failed.
    ,JOB_STATE_COMPLETED_WALLTIME_REACHED   //!< The job has reached its walltime and has been killed.
    ,JOB_STATE_COMPLETED_KILLED             //!< The job has been killed.
    ,JOB_STATE_REJECTED                     //!< The job has been rejected by the scheduler.
};

class JobIdentifier
{
public:
    
    JobIdentifier() = default;
    /**
     * @brief Creates a JobIdentifier
     * @param[in] workload_name The workload name
     * @param[in] job_name The job name
     */
    explicit JobIdentifier(const std::string & workload_name,
                           const std::string & job_name);

    /**
     * @brief Creates a JobIdentifier from a string to parse
     * @param[in] job_id_str The string to parse
     */
    explicit JobIdentifier(const std::string & job_id_str);

    std::string _workload_name;
    std::string _job_name;
    std::string _representation;

    std::string representation() const;
    std::string to_string() const;
      /**
     * @brief Returns a null-terminated C string of the JobIdentifier representation.
     * @return A null-terminated C string of the JobIdentifier representation.
     */
    const char * to_cstring() const;
    std::string workload_name() const;

    /**
     * @brief Returns the job name within the workload.
     * @return The job name within the workload.
     */
    std::string job_name() const;
    
};
/**
 * @brief Compares two JobIdentifier thanks to their string representations
 * @param[in] ji1 The first JobIdentifier
 * @param[in] ji2 The second JobIdentifier
 * @return ji1.to_string() < ji2.to_string()
 */
bool operator<(const JobIdentifier & ji1, const JobIdentifier & ji2);

/**
 * @brief Compares two JobIdentifier thanks to their string representations
 * @param[in] ji1 The first JobIdentifier
 * @param[in] ji2 The second JobIdentifier
 * @return ji1.to_string() == ji2.to_string()
 */
bool operator==(const JobIdentifier & ji1, const JobIdentifier & ji2);
struct JobIdentifierHasher
{
    /**
     * @brief Hashes a JobIdentifier.
     * @param[in] id The JobIdentifier to hash.
     * @return Whatever is returned by std::hash to match C++ conventions.
     */
    std::size_t operator()(const JobIdentifier & id) const;
};
struct Job
{
        Job()=default;
        
        
        
        Workload * workload = nullptr;
        JobIdentifier id;
        std::string json_description;
        IntervalSet allocation;
        std::string metadata;
        std::vector<int> smpi_ranks_to_hosts_mapping; //!< If the job uses a SMPI profile, stores which host number each MPI rank should use. These numbers must be in [0,required_nb_res[.
        
        JobState state;
        long double starting_time;
        long double runtime;
        bool kill_requested = false;
        ProfilePtr profile;
        long double submission_time;
        long double walltime=-1;
        unsigned int requested_nb_res;
        double checkpoint_time;
        double dump_time;
        double read_time;
public:
    static JobPtr from_json(const rapidjson::Value & json_desc,
                           Workload * workload,
                           const std::string & error_prefix = "Invalid JSON job");

    /**
     * @brief Creates a new-allocated Job from a JSON description
     * @param[in] json_str The JSON description of the job (as a string)
     * @param[in] workload The Workload the job is in
     * @param[in] error_prefix The prefix to display when an error occurs
     * @return The newly allocated Job
     * @pre The JSON description of the job is valid
     */
    static JobPtr from_json(const std::string & json_str,
                           Workload * workload,
                           const std::string & error_prefix = "Invalid JSON job");
    /**
     * @brief Checks whether a job is complete (regardless of the job success)
     * @return true if the job is complete (=has started then finished), false otherwise.
     */
    bool is_complete() const;
    
    static std::string to_json_desc(rapidjson::Document * doc);
    static std::string not_escaped(const std::string & input);

    //! Functor to hash a JobIdentifier
    
    
};

class Jobs
{
public:
    
    Jobs() = default;
    ~Jobs();
public:    
    void set_profiles(Profiles * profiles);
    void set_workload(Workload * workload);
    void load_from_json(const rapidjson::Document & doc, const std::string & filename);
    void add_job(JobPtr job);
    int nb_jobs() const;
    bool exists(const JobIdentifier & job_id) const;
       /**
     * @brief Returns a reference to the map that contains the jobs
     * @return A reference to the map that contains the jobs
     */
    std::unordered_map<JobIdentifier, JobPtr,JobIdentifierHasher> & jobs();
        /**
     * @brief Accesses one job thanks to its identifier
     * @param[in] job_id The job id
     * @return A pointer to the job associated to the given job id
     */
    JobPtr operator[](JobIdentifier job_id);

    /**
     * @brief Accesses one job thanks to its unique name (const version)
     * @param[in] job_id The job id
     * @return A (const) pointer to the job associated to the given job id
     */
    const JobPtr operator[](JobIdentifier job_id) const;

    /**
     * @brief Accesses one job thanks to its unique id
     * @param[in] job_id The job unique id
     * @return A pointer to the job associated to the given job id
     */
    JobPtr at(JobIdentifier job_id);

    /**
     * @brief Accesses one job thanks to its unique id (const version)
     * @param[in] job_id The job unique name
     * @return A (const) pointer to the job associated to the given job
     * name
     */
    const JobPtr at(JobIdentifier job_id) const;
   /**
     * @brief Deletes a job
     * @param[in] job_id The identifier of the job to delete
     * @param[in] garbage_collect_profiles Whether to garbage collect its profiles
     */
    void delete_job(const JobIdentifier & job_id,
                    const bool & garbage_collect_profiles);

private:
    std::unordered_map<JobIdentifier,JobPtr,JobIdentifierHasher> _jobs;
    std::unordered_map<JobIdentifier, bool, JobIdentifierHasher> _jobs_met; //!< Stores the jobs id already met during the simulation
    Profiles * _profiles = nullptr;
    Workload * _workload=nullptr;
    
    
};
}

#endif
