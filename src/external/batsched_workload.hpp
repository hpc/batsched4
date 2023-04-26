#ifndef BATSCHED_WORKLOAD_HPP
#define BATSCHED_WORKLOAD_HPP

#include <string>
#include <vector>
#include <map>
#include "pointers.hpp"
#include <rapidjson/document.h>

namespace r = rapidjson;
namespace myBatsched{
class Jobs;
struct Job;
class Profiles;
struct JobIdentifier;

class Workload
{
   public:
    /**
     * @brief Builds an empty static Workload (via dynamic allocation)
     * @details Static workloads correspond to Batsim input files (workloads or workflows)
     * @param[in] workload_name The workload name
     * @param[in] workload_file The workload file name
     * @return The newly created workload
     */
    static Workload * new_static_workload(const std::string & workload_name,
                                          const std::string & workload_file,
                                          bool checkpointing_on = false
                                         );

    /**
     * @brief Builds an empty dynamic Workload (via dynamic allocation)
     * @details Dynamic workloads are created by the decision process
     * @param[in] workload_name The workload name
     * @return The newly created workload
     */
    static Workload * new_dynamic_workload(const std::string & workload_name,
                                            bool checkpointing_on = false);
    

    /**
     * @brief Destroys a Workload
     */
    ~Workload();
 /**
     * @brief Loads a static workload from a JSON filename
     * @param[in] json_filename The name of the JSON file
     * @param[out] nb_machines The number of machines described in the JSON file
     */
    void load_from_json(const std::string & json_filename);
    
    void load_from_batsim(const std::string& filename, const r::Value & job_json,const r::Value & profile_json);
    /**
     * @brief Returns whether the workload is static (corresponding to a Batsim input workload/workflow) or not
     * @return Whether the workload is static or not
     */
    /**
     * @brief Checks whether a Workload is valid
     */
    void check_validity();

    /**
     * @brief Checks whether a single job is valid
     * @param[in] job The job to examine
     */
    void check_single_job_validity(const JobPtr job);

     bool is_static() const;
public:
std::string name; //!< The Workload name
    std::string file = ""; //!< The Workload file if it exists
    Jobs * jobs = nullptr; //!< The Jobs of the Workload
    Profiles * profiles = nullptr; //!< The Profiles associated to the Jobs of the Workload
    bool _is_static = false; //!< Whether the workload is dynamic or not
    bool _checkpointing_on = false;
    bool _compute_checkpointing = false;
    double _MTBF = -1.0;
    double _SMTBF = -1.0;
    double _repair_time = 0.0;
    double _host_speed;

};

class Workloads
{
public:
    Workloads() = default;
    ~Workloads();
    
    /**
     * @brief Allows to access a Workload thanks to its name
     * @param[in] workload_name The name of the workload to access
     * @return The workload associated with the given workload name
     * @pre The workload exists
     */
    Workload * operator[](const std::string & workload_name);

    /**
     * @brief Allows to access a Workload thanks to its name
     * @param[in] workload_name The name of the workload to access
     * @return The workload associated with the given workload name
     * @pre The workload exists
     */
    const Workload * operator[](const std::string & workload_name) const;

    /**
     * @brief Allows to access a Workload thanks to its name
     * @param[in] workload_name The name of the workload to access
     * @return The workload associated with the given workload name
     * @pre The workload exists
     */
    Workload * at(const std::string & workload_name);

    /**
     * @brief Allows to access a Workload thanks to its name
     * @param[in] workload_name The name of the workload to access
     * @return The workload associated with the given workload name
     * @pre The workload exists
     */
    const Workload * at(const std::string & workload_name) const;

    /**
     * @brief Returns the number of workloads
     * @return The number of workloads
     */
    unsigned int nb_workloads() const;

    /**
     * @brief Returns the number of static workloads
     * @details Static workloads are those corresponding to Batsim input files (input workloads or workflows)
     * @return The number of static workloads
     */
    unsigned int nb_static_workloads() const;

    /**
     * @brief Allows to get a job from the Workloads
     * @param[in] job_id The JobIdentifier
     * @return The job which has been asked
     * @pre The requested job exists
     */
    JobPtr job_at(const JobIdentifier & job_id);

    /**
     * @brief Allows to get a job from the Workloads (const version)
     * @param[in] job_id The JobIdentifier
     * @return The (const) job which has been asked
     * @pre The requested job exists
     */
    const JobPtr job_at(const JobIdentifier & job_id) const;
    
    /**
     * @brief Inserts a new Workload into a Workloads
     * @param[in] workload_name The name of the new Workload to insert
     * @param[in] workload The Workload to insert
     * @pre There should be no existing Workload with the same name in the Workloads
     */
    void insert_workload(const std::string & workload_name,
                         Workload * workload);
    /**
     * @brief Deletes jobs from the associated workloads
     * @param[in] job_ids The vector of identifiers of the jobs to remove
     * @param[in] garbage_collect_profiles Whether to remove profiles that are not used anymore
     */
    void delete_jobs(const std::vector<JobIdentifier> & job_ids,
                     const bool & garbage_collect_profiles);
    /**
     * @brief Checks whether a Workload with the given name exist.
     * @param[in] workload_name The name of the Workload whose existence is checked
     * @return true if a Workload with the given name exists in the Workloads, false otherwise.
     */
    bool exists(const std::string & workload_name) const;
        /**
     * @brief Gets the internal map
     * @return The internal map
     */
    std::map<std::string, Workload*> & workloads();
     /**
     * @brief Checks whether a job is registered in the associated workload
     * @param[in] job_id The JobIdentifier
     * @return True if the given is registered in the associated workload, false otherwise
     */
    bool job_is_registered(const JobIdentifier & job_id);
      /**
     * @brief Checks whether a job profile is registered in the workload it
     * is attached to
     * @param[in] job_id The JobIdentifier
     * @return True if the given is registered in the associated workload, false otherwise
     */
    bool job_profile_is_registered(const JobIdentifier & job_id);
public:
        bool _checkpointing_on = false;
        bool _compute_checkpointing = false;
        double _MTBF = -1.0;
        double _SMTBF = -1.0;
        double _repair_time = 0.0;
        double _fixed_failures = -1.0;
        double _host_speed;
private:
    std::map<std::string, Workload*> _workloads; //!< Associates Workloads with their names



};

}


#endif
