#pragma once

#include <rapidjson/document.h>
#include <vector>
#include <chrono>

#include "decision.hpp"
#include "queue.hpp"
//#include "external/batsched_workload.hpp"
#include "batsched_tools.hpp"
#include "machine.hpp"
#include <random>

/**
 * @brief The base abstract class of (scheduling & machine state) decision algorithms
 */
#define B_LOG_INSTANCE _myBLOG
class ISchedulingAlgorithm
{
public:
    /**
     * @brief Builds a ISchedulingAlgorithm
     * @param[in,out] workload The Workload instance
     * @param[in,out] decision The Decision instance
     * @param[in,out] queue The Queue instance
     * @param[in,out] selector The ResourceSelector instance
     * @param[in] rjms_delay The maximum amount of time the RJMS is supposed to take to do some actions like killing jobs.
     * @param[in,out] variant_options The variant-dependent options
     */
    ISchedulingAlgorithm(Workload * workload,
                         SchedulingDecision * decision,
                         Queue * queue,
                         ResourceSelector * selector,
                         double rjms_delay,
                         rapidjson::Document * variant_options);

    /**
     * @brief Destroys a ISchedulingAlgorithm
     */
    virtual ~ISchedulingAlgorithm();

    /**
     * @brief This function is called when the simulation is about to begin
     * @param[in] date The date at which the simulation is about to begin
     */
    virtual void on_simulation_start(double date, const rapidjson::Value & batsim_config) = 0;
    /**
     * @brief This function is called when the simulation is about to finish
     * @param[in] date The date at which the simulation is about to finish
     */
    virtual void on_simulation_end(double date) = 0;

    /**
     * @brief This function is called when jobs have been released
     * @param[in] date The date at which the jobs have been released
     * @param[in] job_ids The identifiers of the jobs which have been released
     */
    virtual void on_job_release(double date, const std::vector<std::string> & job_ids);
    /**
     * @brief This function is called when jobs have been finished
     * @param[in] date The date at which the jobs have been finished
     * @param[in] job_ids The identifiers of the jobs which have been finished
     */
    virtual void on_job_end(double date, const std::vector<std::string> & job_ids);

    /**
     * @brief This function is called when jobs have been killed (resulting from a decision, not a timeout)
     * @param[in] date The date at which the jobs have been killed
     * @param[in] job_ids The identifiers of the jobs which have been finished
     */
    virtual void on_job_killed(double date, const std::unordered_map<std::string,batsched_tools::Job_Message *> & job_msgs);

    /**
     * @brief This function is called when the power state of some machines have been changed
     * @param[in] date The date at which the power state alteration has occured
     * @param[in] machines The machines involved in the power state alteration
     * @param[in] new_state The new power state of the machines: The state they are in, after the alteration
     */
    virtual void on_machine_state_changed(double date, IntervalSet machines, int new_state);

    /**
     * @brief This function is called when a REQUESTED_CALL message is received
     * @param[in] date The date at which the REQUESTED_CALL message have been received
     */
    virtual void on_requested_call(double date, batsched_tools::CALL_ME_LATERS cml);

    /**
     * @brief This function is called when the no_more_static_job_to_submit
     *        notification is received
     */
    virtual void on_no_more_static_job_to_submit_received(double date);

    /**
     * @brief This function is called when the on_no_more_external_event_to_occur
     *        notification is received
     */
    virtual void on_no_more_external_event_to_occur(double date);

    /**
     * @brief This function is called when an ANSWER message about energy consumption is received
     * @param[in] date The date at which the ANSWER message has been received
     * @param[in] consumed_joules The number of joules consumed since time 0
     */
    virtual void on_answer_energy_consumption(double date, double consumed_joules);

    /**
     * @brief This function is called when a machine_available NOTIFY event is received.
     * @param[in] date The date at which the NOTIFY event has been received.
     * @param[in] machines The machines whose availability has changed.
     */
    virtual void on_machine_available_notify_event(double date, IntervalSet machines);

    /**
     * @brief This function is called when a machine_unavailable NOTIFY event is received.
     * @param[in] date The date at which the NOTIFY event has been received.
     * @param[in] machines The machines whose availability has changed.
     */
    virtual void on_machine_unavailable_notify_event(double date, IntervalSet machines);
    
    virtual void on_job_fault_notify_event(double date, std::string job);   //*********************added
    virtual void on_myKillJob_notify_event(double date);
    /**
     * @brief This function is called when a QUERY message about estimating waiting time of potential jobs is received.
     * @param[in] date The date at which the QUERY message has been received
     * @param[in] job_id The identifier of the potential job
     */
    virtual void on_query_estimate_waiting_time(double date, const std::string & job_id);

    /**
     * @brief This function is called when some decisions need to be made
     * @param[in] date The current date
     * @param[in,out] update_info Some information to sort the jobs
     * @param[in,out] compare_info Some information to sort the jobs
     */
    virtual void make_decisions(double date,
                                SortableJobOrder::UpdateInformation * update_info,
                                SortableJobOrder::CompareInformation * compare_info) = 0;

    /**
     * @brief Allows to set the total number of machines in the platform
     * @details This function is called just before on_simulation_start
     * @param[in] nb_machines The total number of machines in the platform
     */
    void set_nb_machines(int nb_machines);

    /**
     * @brief Allows to set the RedisStorage instance
     * @param[in,out] redis The RedisStorage instance
     */
    void set_redis(RedisStorage * redis);

    /**
     * @brief Clears data structures used to store what happened between two make_decisions calls
     * @details This function should be called between make_decisions calls!
     */
    void clear_recent_data_structures();
    //virtual void set_workloads(myBatsched::Workloads *w);
    virtual void set_machines(Machines *m);
    virtual void set_generators(double date);
    void set_real_time(std::chrono::_V2::system_clock::time_point time);
    void set_checkpoint_time(long seconds,std::string checkpoint_type,bool once);
    bool send_batsim_checkpoint_if_ready(double date);
    bool check_checkpoint_time(double date);
    void checkpoint_batsched(double date);
    void ingest_variables(double date);
    virtual void on_ingest_variables(const rapidjson::Document & doc,double date);
    virtual void on_checkpoint_batsched(double date);
    virtual void on_start_from_checkpoint(double date,const rapidjson::Value & batsim_event) = 0;
    void on_start_from_checkpoint_normal(double date, const rapidjson::Value & batsim_event);
    void on_start_from_checkpoint_schedule(double date, const rapidjson::Value & batsim_event);
    virtual void on_first_jobs_submitted(double date);
    virtual void on_machine_down_for_repair(batsched_tools::KILL_TYPES forWhat,double date){}
    virtual void on_machine_instant_down_up(batsched_tools::KILL_TYPES forWhat,double date){}
    void on_signal_checkpoint();
    void set_failure_map(std::map<double,batsched_tools::failure_tuple> failure_map);
    void normal_start(double date, const rapidjson::Value & batsim_event);
    bool all_submitted_jobs_check_passed();
    bool ingest_variables_if_ready(double date);
    //void set_compute_resources(const rapidjson::Value & batsim_event);
    void schedule_start(double date,const rapidjson::Value & batsim_event);
    void requested_failure_call(double date,batsched_tools::CALL_ME_LATERS cml_in);
    void handle_failures(double date);
    IntervalSet normal_repair(double date);
    IntervalSet normal_downUp(double date);
    void schedule_repair(IntervalSet machine,batsched_tools::KILL_TYPES forWhat,double date);
    void schedule_downUp(IntervalSet machine,batsched_tools::KILL_TYPES forWhat,double date);
    bool schedule_kill_jobs(IntervalSet machine,batsched_tools::KILL_TYPES forWhat, double date);
    void set_index_of_horizons();
    void execute_jobs_in_running_state(double date);
    bool get_clear_recent_data_structures();
    void set_clear_recent_data_structures(bool value);

    //ingest functions
    rapidjson::Document ingestDoc(std::string filename);
    Machines * ingest(Machines * machines,const rapidjson::Value & json);
    double ingest(double aDouble, const rapidjson::Value &json);
    bool ingest(bool aBool,const rapidjson::Value &json);
    int ingest(int aInt,const rapidjson::Value &json);
    long ingest(long aLong,const rapidjson::Value &json);
    std::string ingest(std::string aString,const rapidjson::Value &json);
    Schedule::RESCHEDULE_POLICY ingest(Schedule::RESCHEDULE_POLICY aPolicy, const rapidjson::Value &json);
    Schedule::IMPACT_POLICY ingest(Schedule::IMPACT_POLICY aPolicy,const rapidjson::Value &json);
    Queue * ingest(Queue * queue,const rapidjson::Value & json,double date);
    Job * ingest(Job * aJob, const rapidjson::Value &json);
    IntervalSet ingest(IntervalSet intervalSet,const rapidjson::Value &json);
    std::map<int,IntervalSet> ingest(std::map<int,IntervalSet> &aMap,const rapidjson::Value &json);
    std::vector<std::string> ingest(std::vector<std::string> &aVector,const rapidjson::Value &json);
    std::unordered_map<std::string, batsched_tools::Job_Message *> ingest(std::unordered_map<std::string, batsched_tools::Job_Message *> &aUMap, const rapidjson::Value &json);
    std::vector<double> ingest(std::vector<double> &aVector, const rapidjson::Value &json);
    void ingestCMLS(const rapidjson::Value &json,double date);
    std::vector<batsched_tools::KILL_TYPES> ingest(std::vector<batsched_tools::KILL_TYPES> &aVector, const rapidjson::Value &json);
    std::map<Job *,batsched_tools::Job_Message *> ingest(std::map<Job *,batsched_tools::Job_Message *> &aMap, const rapidjson::Value &json);
    std::map<std::string,batsched_tools::KILL_TYPES> ingest(std::map<std::string,batsched_tools::KILL_TYPES> &aMap, const rapidjson::Value &json);
    std::vector<std::pair<const Job *,batsched_tools::KILL_TYPES>> ingest(std::vector<std::pair<const Job *,batsched_tools::KILL_TYPES>> &aVector, const rapidjson::Value &json);
    std::vector<Schedule::ReservedTimeSlice> ingest(std::vector<Schedule::ReservedTimeSlice> &aVector, const rapidjson::Value &json);
    std::list<Job *> ingest(std::list<Job *> &aList,const rapidjson::Value &json);
    std::unordered_set<std::string> ingest(std::unordered_set<std::string> &aUSet, const rapidjson::Value &json);
    std::unordered_map<std::string,batsched_tools::Allocation> ingest(std::unordered_map<std::string,batsched_tools::Allocation> &aUMap, const rapidjson::Value &json);
    std::list<batsched_tools::FinishedHorizonPoint> ingest(std::list<batsched_tools::FinishedHorizonPoint> &aList, const rapidjson::Value &json);


    //easy_bf3
    std::vector<Job *> ingest(std::vector<Job *> jobs, const rapidjson::Value &json);
    std::vector<batsched_tools::Scheduled_Job *> ingest(std::vector<batsched_tools::Scheduled_Job *> sj,const rapidjson::Value &json);
    batsched_tools::Scheduled_Job * ingest(batsched_tools::Scheduled_Job * sj,const rapidjson::Value &json);
    batsched_tools::Priority_Job * ingest(batsched_tools::Priority_Job* pj, const rapidjson::Value &json);

    






    
    
    
    
    

protected:
    //X = not checkpointed
    //C = checkpointed

    //base_variables
    //***************************************************
    Machines * _machines=nullptr; //C
    Queue * _queue = nullptr; //C
    Workload * _workload; //X
    std::vector<Job *> _waiting_jobs;
    SchedulingDecision * _decision; //X
    std::string _queue_policy; //X
    ResourceSelector * _selector; //X
    double _rjms_delay; //X
    rapidjson::Document * _variant_options; //X
    int _nb_machines = -1; //X
    RedisStorage * _redis = nullptr; //X
    b_log *_myBLOG; //X
    std::string _output_folder; //X
    bool _exit_make_decisions = false; //X
    std::chrono::_V2::system_clock::time_point _start_real_time; //X
    std::chrono::_V2::system_clock::time_point _real_time; //C
    double _consumed_joules; //C
    bool _reject_possible = false; //C
    int _nb_call_me_laters=0; //C
    bool _need_to_backfill = false; //C
    bool _output_extra_info = true; //X
    
    //recently_variables
    //***************************************************
    IntervalSet _machines_that_became_available_recently; //C
    IntervalSet _machines_that_became_unavailable_recently; //C
    std::map<int, IntervalSet> _machines_whose_pstate_changed_recently; //C
    std::vector<std::string> _jobs_whose_waiting_time_estimation_has_been_requested_recently; //C
    std::unordered_map<std::string, batsched_tools::Job_Message *> _jobs_killed_recently; //C
    std::vector<std::string> _jobs_ended_recently; //C
    std::vector<std::string> _jobs_released_recently; //C
    IntervalSet _recently_under_repair_machines; //C
    bool _nopped_recently; //C
    bool _consumed_joules_updated_recently; //C
    //***************************************************

    //failure_variables
    //**************************************************
    bool _need_to_send_finished_submitting_jobs = true; //C
    bool _no_more_static_job_to_submit_received = false; //C
    bool _no_more_external_event_to_occur_received = false; //C
    bool _checkpointing_on=false; //C
    std::map<int,batsched_tools::CALL_ME_LATERS> _call_me_laters; //C
    
    std::vector<batsched_tools::KILL_TYPES> _on_machine_instant_down_ups; //C
    std::vector<batsched_tools::KILL_TYPES> _on_machine_down_for_repairs; //C
    std::map<double,batsched_tools::failure_tuple> _file_failures; //X TODO
    IntervalSet _available_machines; //C
    IntervalSet _unavailable_machines; //C
    int _nb_available_machines = -1; //C
    IntervalSet _repair_machines; //C
    int _repairs_done=0; //C
    std::map<Job *,batsched_tools::Job_Message *> _my_kill_jobs; //C
        //randomness
        //**************************
    std::mt19937 generator_failure; //C
    unsigned int generator_failure_seed; //X
    std::exponential_distribution<double> * failure_exponential_distribution=nullptr; //C
    std::uniform_int_distribution<int> * failure_unif_distribution=nullptr; //C
    std::mt19937 generator_machine; //C
    unsigned int generator_machine_seed; //X
    std::uniform_int_distribution<int> * machine_unif_distribution = nullptr; //C
    std::mt19937 generator_repair_time; //C
    unsigned int generator_repair_time_seed; //X
    std::exponential_distribution<double> * repair_time_exponential_distribution; //C
    bool _set_generators_from_file = false; //X
    //***************************************************

    //schedule_variables
    //***************************************************
    std::string _output_svg; //C   //all|short|none
    std::string _output_svg_method; //C   //svg|text|both
    long _svg_frame_start; //C
    long _svg_frame_end; //C
    long _svg_output_start; //C
    long _svg_output_end; //C
    double _svg_time_start; //X
    double _svg_time_end; //X
    Schedule::RESCHEDULE_POLICY _reschedule_policy; //C
    Schedule::IMPACT_POLICY _impact_policy; //C
    bool _killed_jobs = false; //C
    std::map<std::string,batsched_tools::KILL_TYPES> _resubmitted_jobs; //C
    std::vector<std::pair<const Job *,batsched_tools::KILL_TYPES>>_resubmitted_jobs_released; //C
    bool _dump_provisional_schedules = false; //C
    std::string _dump_prefix = "/tmp/dump"; //C
    Schedule _schedule; //C
    Schedule * _scheduleP = nullptr; //X
    
    //**************************************************

    //backfill variables
    //**************************************************
    bool _horizon_algorithm = false; //X
    Job * _priority_job = nullptr; //C

    //**************************************************


    //reservation variables
    //**************************************************
    bool _reservation_algorithm = false; //X
    Queue * _reservation_queue=nullptr; //C
    bool _start_a_reservation=false; //C
    bool _need_to_compress = false; //C
    std::vector<Schedule::ReservedTimeSlice> _saved_reservations; //C
    std::vector<std::string> _saved_recently_queued_jobs; //C
    std::vector<std::string> _saved_recently_ended_jobs; //C
    //**************************************************

    //Real Checkpoint Variables
    //***************************************************
    int _nb_batsim_checkpoints = 0; //C
    batsched_tools::start_from_chkpt _start_from_checkpoint; //X
    long _batsim_checkpoint_interval_seconds = 0; //X
    std::string _batsim_checkpoint_interval_type = "real"; //X
    bool _batsim_checkpoint_interval_once = false; //X
    bool _need_to_checkpoint = false; //X
    bool _need_to_send_checkpoint = false; //X
    bool _recover_from_checkpoint = false; //X
    bool _block_checkpoint = false; //X
    double _start_from_checkpoint_time=0; //X
    bool _clear_recent_data_structures=true; //X
    bool _clear_jobs_recently_released=true; //X
    int _checkpoint_sync = 0; //X
    bool _debug_real_checkpoint = false; //X
    //***************************************************


    //share-packing,cores variables
    //*************************************************
    bool _share_packing_algorithm = false; //X
    bool _share_packing = false; //X
    double _core_percent = 1.0; //X
    IntervalSet _available_core_machines = IntervalSet::empty_interval_set(); //C
    std::list<Job *> _pending_jobs; //C
    std::list<Job *> _pending_jobs_heldback; //C
    std::unordered_set<std::string> _running_jobs; //C
    std::list<batsched_tools::FinishedHorizonPoint> _horizons; //C
    std::unordered_map<std::string, batsched_tools::Allocation> _current_allocations; //C
    int _share_packing_holdback = 0; //X
    int _p_counter = 0; //pending jobs erased counter  //X
    int _e_counter = 0; //execute job counter  //X
    IntervalSet _heldback_machines; //C
    //*************************************************

  

    
};
