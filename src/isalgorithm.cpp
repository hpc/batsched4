#include "isalgorithm.hpp"

#include "pempek_assert.hpp"
#include "batsched_tools.hpp"
#include <chrono>
#include <ctime>
#include <loguru.hpp>
#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

using namespace std;

void ISchedulingAlgorithm::normal_start(double date, const rapidjson::Value & batsim_event)
{
    CLOG_F(INFO,"on simulation start");
    pid_t pid = batsched_tools::get_batsched_pid();
    _decision->add_generic_notification("PID",std::to_string(pid),date);
    const rapidjson::Value & batsim_config = batsim_event["config"];

    // @note LH: set output folder for logging
    _output_folder=batsim_config["output-folder"].GetString();
    _output_folder.replace(_output_folder.rfind("/out"), std::string("/out").size(), "");
    CLOG_F(CCU_DEBUG_FIN,"output folder %s",_output_folder.c_str());
    ISchedulingAlgorithm::set_generators(date);

    // @note LH: Get the queue policy, only "FCFS" and "ORIGINAL-FCFS" are valid
    _queue_policy=batsim_config["queue-policy"].GetString();
    PPK_ASSERT_ERROR(_queue_policy == "FCFS" || _queue_policy == "ORIGINAL-FCFS");
    CLOG_F(CCU_DEBUG_FIN, "queue-policy = %s", _queue_policy.c_str());
    _myBLOG = new b_log();
    _myBLOG->add_log_file(_output_folder+"/log/Soft_Errors.log",blog_types::SOFT_ERRORS);
    _myBLOG->add_log_file(_output_folder+"/failures.csv",blog_types::FAILURES);
    _myBLOG->add_header(blog_types::FAILURES,"simulated_time,event,data");
    (void) batsim_config;
}
void ISchedulingAlgorithm::schedule_start(double date, const rapidjson::Value & batsim_event)
{
    const rapidjson::Value & batsim_config = batsim_event["config"];
    _output_svg=batsim_config["output-svg"].GetString();
    _output_svg_method = batsim_config["output-svg-method"].GetString();
    _svg_frame_start = batsim_config["svg-frame-start"].GetInt64();
    _svg_frame_end = batsim_config["svg-frame-end"].GetInt64();
    _svg_output_start = batsim_config["svg-output-start"].GetInt64();
    _svg_output_end = batsim_config["svg-output-end"].GetInt64();
    Schedule::convert_policy(batsim_config["reschedule-policy"].GetString(),_reschedule_policy);
    Schedule::convert_policy(batsim_config["impact-policy"].GetString(),_impact_policy);
    CLOG_F(CCU_DEBUG_FIN,"output svg: %s, method: %s",_output_svg.c_str(),_output_svg_method.c_str());
    //was there
    _schedule = Schedule(_nb_machines, date);
    _scheduleP = &_schedule;
    //added
    _schedule.set_output_svg(_output_svg);
    _schedule.set_output_svg_method(_output_svg_method);
    _schedule.set_svg_frame_and_output_start_and_end(_svg_frame_start,_svg_frame_end,_svg_output_start,_svg_output_end);
    _schedule.set_svg_prefix(_output_folder + "/svg/");
    _schedule.set_policies(_reschedule_policy,_impact_policy);
}

void ISchedulingAlgorithm::requested_failure_call(double date, batsched_tools::CALL_ME_LATERS cml_in)
{
    double number = failure_exponential_distribution->operator()(generator_failure);
    batsched_tools::KILL_TYPES killType;
    switch(cml_in.forWhat){
    
        case batsched_tools::call_me_later_types::SMTBF:
            //Log the failure
            BLOG_F(blog_types::FAILURES,"%s,%s",blog_failure_event::FAILURE.c_str(),"SMTBF");
            CLOG_F(CCU_DEBUG,"SMTBF Failure");
            killType=batsched_tools::KILL_TYPES::SMTBF;
        break;
        case batsched_tools::call_me_later_types::MTBF:
            BLOG_F(blog_types::FAILURES, "%s,%s",blog_failure_event::FAILURE.c_str(),"MTBF");
            CLOG_F(CCU_DEBUG,"MTBF Failure");
            killType=batsched_tools::KILL_TYPES::MTBF;
        break;
        case batsched_tools::call_me_later_types::FIXED_FAILURE:
            BLOG_F(blog_types::FAILURES,"%s,%s", blog_failure_event::FAILURE.c_str(),"FIXED_FAILURE");
            CLOG_F(CCU_DEBUG,"Fixed Failure");
            killType=batsched_tools::KILL_TYPES::FIXED_FAILURE;
        break;
        case batsched_tools::call_me_later_types::REPAIR_DONE:
        {
            rapidjson::Document doc;
            doc.Parse(cml_in.extra_data.c_str());
            PPK_ASSERT(doc.HasMember("machine"),"Error, repair done but no 'machine' field in extra_data");
            int machine_number = doc["machine"].GetInt();
            BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::REPAIR_DONE.c_str() ,machine_number);
            CLOG_F(CCU_DEBUG,"Repair Done On Machine: %d",machine_number);
            //a repair is done, all that needs to happen is add the machines to available
            //and remove them from repair machines and add one to the number of available
            IntervalSet machine = machine_number;
            _available_machines += machine;
            _unavailable_machines -= machine;
            _repair_machines -= machine;
            _nb_available_machines=_available_machines.size();
            _machines_that_became_available_recently += machine;
            _need_to_backfill = true;
            if (_scheduleP != nullptr)
            {
                if (_output_svg == "all")
                    _schedule.output_to_svg("top Repair Done  Machine #: "+std::to_string(machine_number));
                _schedule.remove_repair_machines(machine);
                _schedule.remove_svg_highlight_machines(machine);
                if (_output_svg == "all")
                    _schedule.output_to_svg("bottom Repair Done  Machine #: "+std::to_string(machine_number));
                _reservation_algorithm ? _need_to_compress=true: _need_to_compress = false;
            }

            if (_reject_possible)
                _repairs_done++;
            return;
        }
        break;
    }
    if (_workload->_repair_time == -1.0  && _workload->_MTTR == -1.0)
        _on_machine_instant_down_ups.push_back(killType);
    else
        _on_machine_down_for_repairs.push_back(killType);
    batsched_tools::CALL_ME_LATERS cml;
    cml.forWhat = cml_in.forWhat;
    cml.id = _nb_call_me_laters;
    _decision->add_call_me_later(date,number+date,cml);
    
}

void ISchedulingAlgorithm::handle_failures(double date){
    //handle any instant down ups (no repair time on machine going down)
    for(batsched_tools::KILL_TYPES forWhat : _on_machine_instant_down_ups)
    {
        on_machine_instant_down_up(forWhat,date);
    }
    LOG_F(INFO,"here");
    //ok we handled them all, clear the container
    _on_machine_instant_down_ups.clear();
    //handle any machine down for repairs (machine going down with a repair time)
    for(batsched_tools::KILL_TYPES forWhat : _on_machine_down_for_repairs)
    {
        on_machine_down_for_repair(forWhat,date);
    }
    LOG_F(INFO,"here");
}
IntervalSet ISchedulingAlgorithm::normal_repair(double date)
{
    //notes on multiple clusters/partitions
    /*
        this function will need to accept a prefix string to know which cluster/partition to roll the dice on
        we will need multiple machine_unif_distributions and multiple generator_machines
        we will need multiple failure_exp_distributions and failure generators
        we will need to update a prefix->repair_machines and not the ISchedulingAlgorithm::_repair_machines
        as well as the prefix->machineIdsAvailable / prefix->machineIdsUnavailable 
    */
     //get a random number of a machine to kill
     //this will need to be more complex if we aim to have multiple partitions of clusters ...ie multiple Prefixes
    
    std::string prefix = "a";
    int number = machine_unif_distribution->operator()(generator_machine);
    //make it an intervalset so we can find the intersection of it with current allocations
    int id = (*_machines)(prefix,number)->id;
    IntervalSet machine = id;
    BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::MACHINE_REPAIR.c_str(),id);
    
    //if the machine is already down for repairs ignore it.
    
    if ((machine & _repair_machines).is_empty())  
    {
        double repair_time = _workload->_repair_time;
        //if we were doing multiple clusters/partitions
        //double repair_time = (*_machines)[number]->repair_time;
       
        if (_workload->_MTTR != -1.0)
            repair_time = repair_time_exponential_distribution->operator()(generator_repair_time);
        //first check if we are using the schedule 
        if (_scheduleP != nullptr)
        {
            if (_schedule.get_reservations_running_on_machines(machine).empty())
                _schedule.add_repair_machine(machine,repair_time);
            else
                return IntervalSet::empty_interval_set();
        }
        //ok the machine is not down for repairs already so it WAS added
        //the failure/repair will not be happening on a machine that has a reservation on it either
        //it will be going down for repairs now
        
        CLOG_F(CCU_DEBUG,"here, machine going down for repair %d",number);
        //ok the machine is not down for repairs
        //it will be going down for repairs now
        _recently_under_repair_machines+=machine; //haven't found a use for this yet
        _available_machines-=machine;
        _unavailable_machines+=machine;
        _repair_machines+=machine;
        _nb_available_machines=_available_machines.size();
        
        BLOG_F(blog_types::FAILURES,"%s,%f",blog_failure_event::REPAIR_TIME.c_str(),repair_time);
        //call me back when the repair is done
        std::string extra_data = batsched_tools::string_format("{\"machine\":%d}",number);
        batsched_tools::CALL_ME_LATERS cml;
        cml.forWhat = batsched_tools::call_me_later_types::REPAIR_DONE;
        cml.id = _nb_call_me_laters;
        cml.extra_data = extra_data;
        _decision->add_call_me_later(date,date+repair_time,cml);
        return machine;
    }
    else{
        BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::MACHINE_ALREADY_DOWN.c_str(),number);
        return IntervalSet::empty_interval_set();
    }
}
void ISchedulingAlgorithm::schedule_repair(IntervalSet machine,batsched_tools::KILL_TYPES forWhat,double date)
{
    
    if (_output_svg == "all")
            _schedule.output_to_svg("On Machine Down For Repairs  Machine #:  "+machine.to_string_hyphen());
    if (!machine.is_empty())
    {
        _schedule.add_svg_highlight_machines(machine);
        if (_schedule.get_number_of_running_jobs() > 0 )
        {

            if (!ISchedulingAlgorithm::schedule_kill_jobs(machine,forWhat,date) && _reservation_algorithm)
            {
                //ok we didn't have jobs to kill and we are using a reservation algorithm
                auto sort_original_submit_pair = [](const std::pair<const Job *,IntervalSet> j1,const std::pair<const Job *,IntervalSet> j2)->bool{
                    if (j1.first->submission_times[0] == j2.first->submission_times[0])
                        return j1.first->id < j2.first->id;
                    else
                        return j1.first->submission_times[0] < j2.first->submission_times[0];
                };
                if (_reschedule_policy == Schedule::RESCHEDULE_POLICY::AFFECTED)
                {
                    std::map<const Job*,IntervalSet> jobs_affected;
                    _schedule.get_jobs_affected_on_machines(machine, jobs_affected);
                    std::vector<std::pair<const Job *,IntervalSet>> jobs_affectedV;
                    for (auto job_interval_pair : jobs_affected)
                    {
                        jobs_affectedV.push_back(job_interval_pair);
                        _schedule.remove_job(job_interval_pair.first);
                    }
                    std::sort(jobs_affectedV.begin(),jobs_affectedV.end(),sort_original_submit_pair);
                    
                    for (auto job_interval_pair : jobs_affectedV)
                    {
                    JobAlloc  alloc = _schedule.add_job_first_fit(job_interval_pair.first,_selector,false);
                    if (!(alloc.used_machines.is_empty()) && alloc.started_in_first_slice)
                        {
                            _decision->add_execute_job(job_interval_pair.first->id, alloc.used_machines, date);
                            _queue->remove_job(job_interval_pair.first);
                        }
                        
                    }
                }
                else if (_reschedule_policy == Schedule::RESCHEDULE_POLICY::ALL)
                {
                    //lets reschedule everything
                    int scheduled = 0;
                    for (auto job_it = _queue->begin(); job_it != _queue->end(); )
                    {
                        if (_workload->_queue_depth != -1 && scheduled >= _workload->_queue_depth)
                            break;
                        
                        const Job * job = (*job_it)->job;
                        _schedule.remove_job_if_exists(job);
                        //if (_dump_provisional_schedules)
                        // _schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);
                        LOG_F(INFO,"DEBUG line 375");
                        JobAlloc alloc = _schedule.add_job_first_fit(job, _selector,false);   
                        //if (_dump_provisional_schedules)
                        //_schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);
                        if(!alloc.used_machines.is_empty())
                        {
                            scheduled++;
                            if (alloc.started_in_first_slice)
                            {
                                _decision->add_execute_job(job->id, alloc.used_machines, date);
                                job_it = _queue->remove_job(job_it);
                                scheduled--;
                            }
                            else
                                ++job_it;
                        }
                        else
                            ++job_it;
                    }
                    
                }
                if (_output_svg == "all")
                    _schedule.output_to_svg("Finished Machine Down For Repairs, Machine #: "+machine.to_string_hyphen());

            }
        }
        _reservation_algorithm ? _need_to_compress = true: _need_to_compress = false;
        //just detailing something concerning repair here, it isn't addressed here
        //in conservative_bf we reschedule everything
        //in easy_bf2 only backfilled jobs,running jobs and priority job is scheduled
        //but there may not be enough machines to run the priority job
        //we check if priority job could run, and if not, we select the next job in queue until something could run if nodes were available
        
        if (!_reservation_algorithm && _output_svg == "all")
            _schedule.output_to_svg("Finished Machine Down For Repairs, Machine #: "+machine.to_string_hyphen());

      }
    else{
        BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::MACHINE_ALREADY_DOWN.c_str(),machine.to_string_hyphen().c_str());
        //if (!added.is_empty())
        //  _schedule.remove_repair_machines(machine);
        //_schedule.remove_svg_highlight_machines(machine);
        if (_output_svg == "all")
            _schedule.output_to_svg("Finished Machine Down For Repairs, NO REPAIR  Machine #:  "+machine.to_string_hyphen());
    
    }
}
IntervalSet ISchedulingAlgorithm::normal_downUp(double date)
{
    //get a random number of a machine to kill
    int number = machine_unif_distribution->operator()(generator_machine);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = number;
    BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::MACHINE_INSTANT_DOWN_UP.c_str(), number);
    CLOG_F(CCU_DEBUG,"here, machine going down for repair %d",number);
    return machine;
}
void ISchedulingAlgorithm::schedule_downUp(IntervalSet machine, batsched_tools::KILL_TYPES forWhat, double date)
{
    _schedule.add_svg_highlight_machines(machine);
    if (_output_svg == "all")
            _schedule.output_to_svg("On Machine Instant Down Up  Machine #: "+machine.to_string_hyphen());
    
    if (_schedule.get_number_of_running_jobs() > 0)
        ISchedulingAlgorithm::schedule_kill_jobs(machine,forWhat,date);
    _schedule.remove_svg_highlight_machines(machine);
    if (_output_svg == "all")
            _schedule.output_to_svg("END On Machine Instant Down Up  Machine #: "+machine.to_string_hyphen());
}
bool ISchedulingAlgorithm::schedule_kill_jobs(IntervalSet machine,batsched_tools::KILL_TYPES forWhat, double date)
{
    //there are possibly some running jobs to kill
    std::vector<std::string> jobs_to_kill;
    _schedule.get_jobs_running_on_machines(machine,jobs_to_kill);

    std::string jobs_to_kill_str = "";
    if (loguru::g_stderr_verbosity == CCU_DEBUG)
    {
        jobs_to_kill_str = !(jobs_to_kill.empty())? std::accumulate( /* otherwise, accumulate */
        ++jobs_to_kill.begin(), jobs_to_kill.end(), /* the range 2nd to after-last */
        *jobs_to_kill.begin(), /* and start accumulating with the first item */
        [](auto& a, auto& b) { return a + "," + b; }) : "";
    }
    CLOG_F(CCU_DEBUG,"jobs to kill %s",jobs_to_kill_str.c_str());

    if (!jobs_to_kill.empty()){
        std::string killed_jobs;
        _killed_jobs=true;
        std::vector<batsched_tools::Job_Message *> msgs;
        for (auto job_id : jobs_to_kill){
            
            CLOG_F(CCU_DEBUG,"killing job %s",job_id.c_str());
            auto msg = new batsched_tools::Job_Message;
            msg->id = job_id;
            msg->forWhat = forWhat;
            msgs.push_back(msg);
            if (killed_jobs.empty())
                killed_jobs = job_id;
            else
                killed_jobs=batsched_tools::string_format("%s %s",killed_jobs.c_str(),job_id.c_str());
        }


        _decision->add_kill_job(msgs,date);
        for (auto job_id:jobs_to_kill)
            _schedule.remove_job_if_exists((*_workload)[job_id]);
        BLOG_F(blog_types::FAILURES,"%s,\"%s\"",blog_failure_event::KILLING_JOBS.c_str(),killed_jobs.c_str());
    return true; //we have jobs to kill
    }
    return false; //we don't have jobs to kill
}
void ISchedulingAlgorithm::set_failure_map(std::map<double,batsched_tools::failure_tuple> failure_map)
{
 _file_failures = failure_map;
 for (auto myPair: failure_map)
 {
    batsched_tools::CALL_ME_LATERS cml;
    cml.id = _nb_call_me_laters;
    cml.forWhat = myPair.second.type;
    _decision->add_call_me_later(0,myPair.first,cml);
 }   
}
void ISchedulingAlgorithm::set_nb_machines(int nb_machines)
{
    CLOG_F(CCU_DEBUG_FIN,"_nb_machines: %d",_nb_machines);
    PPK_ASSERT_ERROR(_nb_machines == -1);
    _nb_machines = nb_machines;
}

void ISchedulingAlgorithm::set_redis(RedisStorage *redis)
{
    LOG_F(INFO,"here");
    //PPK_ASSERT_ERROR(_redis == nullptr);
    LOG_F(INFO,"here");
    _redis = redis;
    LOG_F(INFO,"here");
}

void ISchedulingAlgorithm::clear_recent_data_structures()
{
    _jobs_released_recently.clear();
    _jobs_ended_recently.clear();
    _jobs_killed_recently.clear();
    _jobs_whose_waiting_time_estimation_has_been_requested_recently.clear();
    _machines_whose_pstate_changed_recently.clear();
    _machines_that_became_available_recently.clear();
    _machines_that_became_unavailable_recently.clear();
    _nopped_recently = false;
    _consumed_joules_updated_recently = false;
    _consumed_joules = -1;
}

ISchedulingAlgorithm::ISchedulingAlgorithm(Workload *workload,
                                           SchedulingDecision *decision,
                                           Queue *queue,
                                           ResourceSelector *selector,
                                           double rjms_delay,
                                           rapidjson::Document *variant_options) :
    _workload(workload), _decision(decision), _queue(queue), _selector(selector),
    _rjms_delay(rjms_delay), _variant_options(variant_options)
{

}

ISchedulingAlgorithm::~ISchedulingAlgorithm()
{

}

void ISchedulingAlgorithm::on_job_release(double date, const vector<string> &job_ids)
{
    (void) date;
    _jobs_released_recently.insert(_jobs_released_recently.end(),
                                   job_ids.begin(),
                                   job_ids.end());
}

void ISchedulingAlgorithm::on_job_end(double date, const vector<string> &job_ids)
{
    (void) date;
    _jobs_ended_recently.insert(_jobs_ended_recently.end(),
                                job_ids.begin(),
                                job_ids.end());
}



void ISchedulingAlgorithm::on_job_killed(double date, const std::unordered_map<std::string,batsched_tools::Job_Message *> &job_msgs)
{
    (void) date;
    _jobs_killed_recently.insert(job_msgs.begin(),
                                 job_msgs.end());
}

void ISchedulingAlgorithm::on_machine_state_changed(double date, IntervalSet machines, int new_state)
{
    (void) date;

    if (_machines_whose_pstate_changed_recently.count(new_state) == 0)
        _machines_whose_pstate_changed_recently[new_state] = machines;
    else
        _machines_whose_pstate_changed_recently[new_state] += machines;
}

void ISchedulingAlgorithm::on_requested_call(double date,batsched_tools::CALL_ME_LATERS cml)
{
    if (cml.forWhat == batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED)
        _need_to_checkpoint = true;
    (void) date;
    _nopped_recently = true;
}

void ISchedulingAlgorithm::on_no_more_static_job_to_submit_received(double date)
{
    (void) date;
    _no_more_static_job_to_submit_received = true;
}

void ISchedulingAlgorithm::on_no_more_external_event_to_occur(double date)
{
    (void) date;
    _no_more_external_event_to_occur_received = true;
}

void ISchedulingAlgorithm::on_answer_energy_consumption(double date, double consumed_joules)
{
    (void) date;
    _consumed_joules = consumed_joules;
    _consumed_joules_updated_recently = true;
}

void ISchedulingAlgorithm::on_machine_available_notify_event(double date, IntervalSet machines)
{
    (void) date;
    _machines_that_became_available_recently += machines;
}
/*void ISchedulingAlgorithm::set_workloads(myBatsched::Workloads *w){
    (void) w;
}
*/
void ISchedulingAlgorithm::set_machines(Machines *m){
    (void) m;
}
void ISchedulingAlgorithm::on_simulation_start(double date, const rapidjson::Value & batsim_event){
    const rapidjson::Value & batsim_config = batsim_event["config"];
    _start_real_time = _real_time;
    _output_folder=batsim_config["output-folder"].GetString();
    _output_folder.replace(_output_folder.rfind("/out"), std::string("/out").size(), "");
    LOG_F(INFO,"output folder %s",_output_folder.c_str());
}
void ISchedulingAlgorithm::on_start_from_checkpoint_normal(double date, const rapidjson::Value & batsim_event)
{
    const rapidjson::Value & batsim_config = batsim_event["config"];
    //lets do all the normal stuff first
    _start_real_time = _real_time;
    _start_from_checkpoint.nb_folder= batsim_config["start-from-checkpoint"]["nb_folder"].GetInt();
    _start_from_checkpoint.nb_checkpoint = batsim_config["start-from-checkpoint"]["nb_checkpoint"].GetInt();
    _start_from_checkpoint.nb_previously_completed = batsim_config["start-from-checkpoint"]["nb_previously_completed"].GetInt();
    _start_from_checkpoint.nb_original_jobs = batsim_config["start-from-checkpoint"]["nb_original_jobs"].GetInt();
    _start_from_checkpoint.nb_actually_completed = _start_from_checkpoint.nb_previously_completed;
    _start_from_checkpoint.started_from_checkpoint = true;
    _start_from_checkpoint.checkpoint_folder =_output_folder+"/previous/checkpoint_"+std::to_string(_start_from_checkpoint.nb_checkpoint);
    _workload->start_from_checkpoint = &_start_from_checkpoint;
    //block call me laters until jobs get submitted
    std::set<batsched_tools::call_me_later_types> blocked_cmls;
    _block_checkpoint = true;
    blocked_cmls.insert(batsched_tools::call_me_later_types::FIXED_FAILURE);
    blocked_cmls.insert(batsched_tools::call_me_later_types::MTBF);
    blocked_cmls.insert(batsched_tools::call_me_later_types::SMTBF);
    blocked_cmls.insert(batsched_tools::call_me_later_types::REPAIR_DONE);
    blocked_cmls.insert(batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED);
    _decision->set_blocked_call_me_laters(blocked_cmls);
    pid_t pid = batsched_tools::get_batsched_pid();
    _decision->add_generic_notification("PID",std::to_string(pid),date);
    const rapidjson::Value & batsim_config = batsim_config["config"];
    CLOG_F(INFO,"***** on_start_from_checkpoint_normal ******");
    //we need to set our generators even though they will be overwritten, so that distributions aren't null
    ISchedulingAlgorithm::set_generators(date);
    _recently_under_repair_machines = IntervalSet::empty_interval_set();
    _myBLOG = new b_log();
    _myBLOG->add_log_file(_output_folder+"/log/Soft_Errors.log",blog_types::SOFT_ERRORS);
    _myBLOG->add_log_file(_output_folder+"/log/simulated_failures.log",blog_types::FAILURES);
    _recover_from_checkpoint = true;
}
void ISchedulingAlgorithm::on_start_from_checkpoint_schedule(double date, const rapidjson::Value & batsim_event)
{
    const rapidjson::Value & batsim_config = batsim_event["config"];
    _output_svg=batsim_config["output-svg"].GetString();
    std::string output_svg_method = batsim_config["output-svg-method"].GetString();
    //output_svg_method = "text";
    _svg_frame_start = batsim_config["svg-frame-start"].GetInt64();
    _svg_frame_end = batsim_config["svg-frame-end"].GetInt64();
    _svg_output_start = batsim_config["svg-output-start"].GetInt64();
    _svg_output_end = batsim_config["svg-output-end"].GetInt64();
    CLOG_F(CCU_DEBUG_FIN,"output svg: %s  output svg method: %s",_output_svg.c_str(),output_svg_method.c_str());
    _output_folder=batsim_config["output-folder"].GetString();
    _output_folder.replace(_output_folder.rfind("/out"), std::string("/out").size(), "");
    CLOG_F(CCU_DEBUG_FIN,"output_folder: %s",_output_folder.c_str());
    Schedule::convert_policy(batsim_config["reschedule-policy"].GetString(),_reschedule_policy);
    Schedule::convert_policy(batsim_config["impact-policy"].GetString(),_impact_policy);

    _schedule = Schedule(_nb_machines, date);
    _schedule.set_output_svg(_output_svg);
    _schedule.set_output_svg_method(output_svg_method);
    _schedule.set_svg_frame_and_output_start_and_end(_svg_frame_start,_svg_frame_end,_svg_output_start,_svg_output_end);
    _schedule.set_svg_prefix(_output_folder + "/svg/");
    _schedule.set_policies(_reschedule_policy,_impact_policy);
    //we will need to look-up jobs in the schedule so set the workload over there
    _schedule.set_workload(_workload);
    _schedule.set_start_from_checkpoint(&_start_from_checkpoint);
}
void ISchedulingAlgorithm::on_start_from_checkpoint(double date, const rapidjson::Value & batsim_config){
    
    
}
void ISchedulingAlgorithm::on_first_jobs_submitted(double date)
{
    //ok we need to update things now
    //workload should have our jobs now
    //lets ingest our schedule
    _decision->clear_blocked_call_me_laters();
    _decision->add_blocked_call_me_later(batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED);
    _block_checkpoint = false;
    ISchedulingAlgorithm::ingest_variables(date);
    _decision->add_generic_notification("recover_from_checkpoint","",date);
    _start_from_checkpoint_time = date;
}
bool ISchedulingAlgorithm::all_submitted_jobs_check_passed()
{
    //this function checks whether all jobs have been submitted.
    for (auto job_id :_jobs_released_recently)
        _start_from_checkpoint.jobs_that_should_have_been_submitted_already.erase(job_id);
    if (_start_from_checkpoint.jobs_that_should_have_been_submitted_already.empty())
        return true;
    else
        return false;
}
bool ISchedulingAlgorithm::ingest_variables_if_ready(double date)
{
    if( _recover_from_checkpoint && _start_from_checkpoint.received_submitted_jobs)
    {
        double epsilon = 1e-6;
        PPK_ASSERT(date - _start_from_checkpoint.first_submitted_time <= epsilon,"Error, waiting on all submitted jobs to come back resulted in simulated time moving too far ahead.");
        if (ISchedulingAlgorithm::all_submitted_jobs_check_passed())
        {
            CLOG_F(CCU_DEBUG,"all jobs submitted, running on first jobs submitted");
            ISchedulingAlgorithm::on_first_jobs_submitted(date);
            _recover_from_checkpoint = false;
        }
        
        return true;
    }
    return false;
}

void ISchedulingAlgorithm::on_signal_checkpoint()
{
    LOG_F(INFO,"on_signal_checkpoint");
    _need_to_send_checkpoint = true;
}
void ISchedulingAlgorithm::set_generators(double date){
    unsigned time_seed = std::chrono::system_clock::now().time_since_epoch().count();
    generator_failure_seed = _workload->_seed_failures;
    generator_machine_seed = _workload->_seed_failure_machine;
    generator_repair_time_seed = _workload->_seed_repair_time;
    if (_workload->_seed_failures == -1)
        generator_failure_seed = time_seed;
    if (_workload->_seed_failure_machine == -1)
        generator_machine_seed = time_seed;
    if (_workload->_seed_repair_time == -1)
        generator_repair_time_seed = time_seed;
        
    generator_failure.seed(generator_failure_seed);
    generator_machine.seed(generator_machine_seed);
    generator_repair_time.seed(generator_repair_time_seed);
    if (_workload->_fixed_failures != -1.0)
     {
        if (machine_unif_distribution == nullptr)
            machine_unif_distribution = new std::uniform_int_distribution<int>(0,_nb_machines-1);
        double number = _workload->_fixed_failures;
        batsched_tools::CALL_ME_LATERS cml;
        cml.forWhat = batsched_tools::call_me_later_types::FIXED_FAILURE;
        cml.id = _nb_call_me_laters;
        _decision->add_call_me_later(date,number+date,cml); 
     }
    if (_workload->_MTTR != -1.0)
    {
        if (repair_time_exponential_distribution == nullptr)
            repair_time_exponential_distribution = new std::exponential_distribution<double>(1.0/_workload->_MTTR);
    }
    if (_workload->_SMTBF != -1.0)
    {
        failure_exponential_distribution = new std::exponential_distribution<double>(1.0/_workload->_SMTBF);
        if (machine_unif_distribution == nullptr)
            machine_unif_distribution = new std::uniform_int_distribution<int>(0,_nb_machines-1);
        std::exponential_distribution<double>::param_type new_lambda(1.0/_workload->_SMTBF);
        failure_exponential_distribution->param(new_lambda);
        double number;         
        number = failure_exponential_distribution->operator()(generator_failure);
       
        batsched_tools::CALL_ME_LATERS cml;
        cml.forWhat = batsched_tools::call_me_later_types::SMTBF;
        cml.id = _nb_call_me_laters;
        _decision->add_call_me_later(date,number+date,cml);
    }
    else if (_workload->_MTBF!=-1.0)
    {
        failure_exponential_distribution = new std::exponential_distribution<double>(1.0/_workload->_MTBF);
        std::exponential_distribution<double>::param_type new_lambda(1.0/_workload->_MTBF);
        failure_exponential_distribution->param(new_lambda);
        double number;         
        number = failure_exponential_distribution->operator()(generator_failure);
        
        batsched_tools::CALL_ME_LATERS cml;
        cml.forWhat = batsched_tools::call_me_later_types::MTBF;
        cml.id = _nb_call_me_laters;
        _decision->add_call_me_later(date,number+date,cml);
    }
}
void ISchedulingAlgorithm::set_real_time(std::chrono::_V2::system_clock::time_point time)
{
    _real_time = time;
}
void ISchedulingAlgorithm::set_checkpoint_time(long seconds,std::string checkpoint_type)
{
    _batsim_checkpoint_interval_type = checkpoint_type;
    _batsim_checkpoint_interval_seconds = seconds;
    _start_real_time = _real_time;
}
bool ISchedulingAlgorithm::check_checkpoint_time(double date)
{
    double epsilon = 1e-4;
    if (_batsim_checkpoint_interval_type == "simulated")
    {
        LOG_F(INFO,"_nb_batsim_checkpoints: %d",_nb_batsim_checkpoints);
        if ((date >= _batsim_checkpoint_interval_seconds*(_nb_batsim_checkpoints + 1)) && (date > _start_from_checkpoint_time+epsilon))
            return true;
        else
            return false;
            
    }
    else
    {
        long duration = std::chrono::duration_cast<std::chrono::seconds>(_real_time - _start_real_time).count();
        if ( duration >= _batsim_checkpoint_interval_seconds*(_nb_batsim_checkpoints + 1))
            return true;
        else
            return false;
    }
}
bool ISchedulingAlgorithm::send_batsim_checkpoint_if_ready(double date){
    LOG_F(INFO,"here");
    if (((_batsim_checkpoint_interval_type != "False" && check_checkpoint_time(date))||_need_to_send_checkpoint) && !_block_checkpoint)
    {
       LOG_F(INFO,"here");
        _decision->add_generic_notification("checkpoint","",date);
        LOG_F(INFO,"here");
        if (!_need_to_send_checkpoint)//check that this is a scheduled checkpoint
            _nb_batsim_checkpoints +=1;
        _need_to_send_checkpoint=false;
        LOG_F(INFO,"here");
        return true;
    }
    else
        return false;
}
void ISchedulingAlgorithm::ingest_variables(double date)
{
    
    std::string filename = _output_folder + "/start_from_checkpoint/batsched_variables.chkpt";
    const rapidjson::Document variablesDoc = ingestDoc(filename);
    ISchedulingAlgorithm::on_ingest_variables(variablesDoc,date);
    LOG_F(INFO,"here");
    on_ingest_variables(variablesDoc,date);
    _decision->remove_blocked_call_me_later(batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED);

}
void ISchedulingAlgorithm::on_ingest_variables(const rapidjson::Document & doc,double date)
{
    std::string checkpoint_dir = _output_folder + "/start_from_checkpoint";
    //first get randomness generators and distributions
    std::ifstream file;
    std::string filename;
    filename= checkpoint_dir+"/generator_failure.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>generator_failure;
        file.close();
    }
    filename = checkpoint_dir+"/generator_machine.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>generator_machine;
        file.close();
    }
    filename = checkpoint_dir+"/generator_repair_time.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>generator_repair_time;
        file.close();
    }
    filename = checkpoint_dir + "/failure_unif_distribution.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>*failure_unif_distribution;
        file.close();
    }
    filename = checkpoint_dir + "/failure_exponential_distribution.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>*failure_exponential_distribution;
        file.close();
    }
    filename = checkpoint_dir + "/machine_unif_distribution.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>(*machine_unif_distribution);
        file.close();
    }
    filename = checkpoint_dir + "/repair_time_exponential_distribution.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>*repair_time_exponential_distribution;
        file.close();
    }
    //next get variables  
    using namespace rapidjson;

        //ingest base_variables
    //********************************

    rapidjson::Document machinesDoc = ingestDoc(checkpoint_dir + "/batsched_machines.chkpt");
    ingest(_machines,machinesDoc);
    
    const rapidjson::Value & base = doc["base_variables"];
    //ingestM(_real_time,base); doesn't need to be ingested
    ingestTTM(_consumed_joules,base,base,Double);
    ingestTTM(_reject_possible,base,base,Bool);
    ingestTTM(_nb_call_me_laters,base,base,Int);
    ingestTTM(_need_to_backfill,base,base,Bool);

    //ingest recently_variables
    //********************************
    const rapidjson::Value & recently = doc["recently_variables"];
    ingestTTM(_machines_that_became_available_recently,recently,recently,String);
    ingestTTM(_machines_that_became_unavailable_recently,recently,recently,String);
    ingestM(_machines_whose_pstate_changed_recently,recently,recently);
    ingestM(_jobs_whose_waiting_time_estimation_has_been_requested_recently,recently,recently);
    ingestM(_jobs_killed_recently,recently,recently);
    ingestM(_jobs_ended_recently,recently,recently);
    ingestM(_jobs_released_recently,recently,recently);
    ingestTTM(_recently_under_repair_machines,recently,recently,String);
    ingestTTM(_nopped_recently,recently,recently,Bool);
    ingestTTM(_consumed_joules_updated_recently,recently,recently,Bool);

    //ingest failure_variables
    //*********************************
    const rapidjson::Value & failure = doc["failure_variables"];
    ingestTTM(_need_to_send_finished_submitting_jobs,failure,failure,Bool);
    ingestTTM(_no_more_static_job_to_submit_received,failure,failure,Bool);
    ingestTTM(_no_more_external_event_to_occur_received,failure,failure,Bool);
    ingestTTM(_checkpointing_on,failure,failure,Bool);
    ingestVDM(failure);
    ingestM(_on_machine_instant_down_ups,failure,failure);
    ingestM(_on_machine_down_for_repairs,failure,failure);
    ingestTTM(_available_machines,failure,failure,String);
    ingestTTM(_unavailable_machines,failure,failure,String);
    ingestTTM(_nb_available_machines,failure,failure,Int);
    ingestTTM(_repair_machines,failure,failure,String);
    ingestTTM(_repairs_done,failure,failure,Int);
    ingestM(_my_kill_jobs,failure,failure);

    //ingest schedule_variables
    //*********************************
    if (_scheduleP != nullptr)
    {
        const rapidjson::Value & schedule = doc["schedule_variables"];
        ingestTTM(_output_svg,schedule,schedule,String);
        ingestTTM(_output_svg_method,schedule,schedule,String);
        ingestTTM(_svg_frame_start,schedule,schedule,Int64);
        ingestTTM(_svg_frame_end,schedule,schedule,Int64);
        ingestTTM(_svg_output_start,schedule,schedule,Int64);
        ingestTTM(_svg_output_end,schedule,schedule,Int64);
        ingestTTM(_reschedule_policy,schedule,schedule,Int);
        ingestTTM(_impact_policy,schedule,schedule,Int);
        ingestTTM(_killed_jobs,schedule,schedule,Bool);
        ingestM(_resubmitted_jobs,schedule,schedule);
        ingestM(_resubmitted_jobs_released,schedule,schedule);
        ingestTTM(_dump_provisional_schedules,schedule,schedule,Bool);
        ingestTTM(_dump_prefix,schedule,schedule,String);
    }
    //we will do the schedule and queues last

    //ingest backfill_variables
    //********************************
    if (_horizon_algorithm)
    {
        const rapidjson::Value & backfill = doc["backfill_variables"];
        ingestM(_priority_job,backfill,backfill);
    }

    //ingest reservation_variables
    //********************************
    if (_reservation_algorithm)
    {
        const rapidjson::Value & reservation = doc["reservation_variables"];
        ingestTTM(_start_a_reservation,reservation,reservation,Bool);
        ingestTTM(_need_to_compress,reservation,reservation,Bool);
        ingestM(_saved_reservations,reservation,reservation);
        ingestM(_saved_recently_queued_jobs,reservation,reservation);
        ingestM(_saved_recently_ended_jobs,reservation,reservation);
    }

    //ingest real_checkpoint_variables
    //*********************************
    const rapidjson::Value & real_checkpoint = doc["real_checkpoint_variables"];
    ingestTTM(_nb_batsim_checkpoints,real_checkpoint,real_checkpoint,Int);

    //ingest share_packing_variables
    //*********************************
    if (_share_packing_algorithm)
    {
        const rapidjson::Value & share_packing = doc["share_packing_variables"];
        ingestTTM(_available_core_machines,share_packing,share_packing,String);
        ingestM(_pending_jobs,share_packing,share_packing);
        ingestM(_pending_jobs_heldback,share_packing,share_packing);
        ingestM(_running_jobs,share_packing,share_packing);
        ingestM(_horizons,share_packing,share_packing);
        ingestM(_current_allocations,share_packing,share_packing);
        ingestTTM(_heldback_machines,share_packing,share_packing,String);

    }
    //ingest the queues
    //********************************
    rapidjson::Document queueDoc = ingestDoc(checkpoint_dir + "/batsched_queues.chkpt");
    const rapidjson::Value & queue = queueDoc["_queue"];
    if(!_queue->is_empty()) _queue->clear();
    ingestDM(_queue,queue,queue);
    if (_reservation_algorithm)
    {
        const rapidjson::Value & reservation_queue = queueDoc["_reservation_queue"];
        if (!_reservation_queue->is_empty()) _reservation_queue->clear();
        ingestDM(_reservation_queue,reservation_queue,reservation_queue);
    }

    //ingest the schedule
    //*******************************
    if (_scheduleP != nullptr)
    {
        rapidjson::Document scheduleDoc = ingestDoc(checkpoint_dir + "/batsched_schedule.chkpt");
        _schedule.ingest_schedule(scheduleDoc);
        //now get the first time slice jobs to execute on the same machines they executed on before
        for (auto kv_pair:_schedule.begin()->allocated_jobs)
        {
        
            _decision->add_execute_job(kv_pair.first->id,kv_pair.second,date);
            //we don't need to delete the jobs from the queue because they shouldn't be in the queue if they are in the first slice
        }
    }
    
    _decision->remove_blocked_call_me_later(batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED);
    

}
void ISchedulingAlgorithm::checkpoint_batsched(double date)
{
    ISchedulingAlgorithm::on_checkpoint_batsched(date);
    on_checkpoint_batsched(date);
    std::string checkpoint_dir = _output_folder + "/checkpoint_latest";
    std::ofstream f(checkpoint_dir+"/batsched_variables.chkpt",std::ios_base::app);
    if (f.is_open())
    {
        f<<std::endl<<"}";
        f.close();
    }

}
void ISchedulingAlgorithm::set_index_of_horizons()
{
    int count = 0;
    for(auto it=_horizons.begin();it != _horizons.end();it++)
    {
        it->index = count;    
        count++;
    }
}
void ISchedulingAlgorithm::on_checkpoint_batsched(double date){
    std::string checkpoint_dir = _output_folder + "/checkpoint_latest";
    std::ofstream f;
    LOG_F(INFO,"here");

    //batsched_machines
    f.open(checkpoint_dir+"/batsched_machines.chkpt",std::ios_base::out);
    if (f.is_open())
    {
        f<<_machines->to_json_string()<<std::endl;
        f.close();
    }

    //batsched_queues
    f.open(checkpoint_dir+"/batsched_queues.chkpt",std::ios_base::out);
    if (f.is_open())
    {
        if (_queue != nullptr && _reservation_queue != nullptr)
        {
            f<<"{\n"
                <<"\t\"_queue\":"<<_queue->to_json_string()<<","<<std::endl
                <<"\t\"_reservation_queue\":"<<_reservation_queue->to_json_string()<<std::endl;
            f<<"}";
            f.close();
        }
        else if (_queue != nullptr)
        {
            f<<"{\n"
                <<"\t\"_queue\":"<<_queue->to_json_string()<<","<<std::endl;
            f<<"}";
            f.close();
        }
    }

    //batsched_variables
    f.open(checkpoint_dir+"/batsched_variables.chkpt",std::ios_base::out);
    if (f.is_open())
    {
        f<<std::fixed<<std::setprecision(15)<<std::boolalpha
        <<"{\n"
        
        //base
        <<"\t\"base_variables\":{\n"
        <<"\t\t\"SIMULATED_CHECKPOINT_TIME\":"                            <<  batsched_tools::to_json_string(date)                            <<","<<std::endl
        <<"\t\t\"REAL_CHECKPOINT_TIME\":"                                 <<  batsched_tools::to_json_string(_real_time)                      <<","<<std::endl
        <<"\t\t\"_consumed_joules\":"                                     <<  batsched_tools::to_json_string(_consumed_joules)                <<","<<std::endl
        <<"\t\t\"_reject_possible\":"                                     <<  _reject_possible                                                <<","<<std::endl
        <<"\t\t\"_nb_call_me_laters\":"                                   <<  batsched_tools::to_json_string(_nb_call_me_laters)              <<","<<std::endl
        <<"\t\t\"_need_to_backfill\":"                                    <<  _need_to_backfill                                               <<std::endl
        <<"\t},"<<std::endl //closes base brace, leaves a brace open
        
        //recently_variables
        <<"\t\"recently_variables\":{"
        <<"\t\t\"_machines_that_became_available_recently\":"             <<  batsched_tools::to_json_string(_machines_that_became_available_recently)     <<","<<std::endl
        <<"\t\t\"_machines_that_became_unavailable_recently\":"           <<  batsched_tools::to_json_string(_machines_that_became_unavailable_recently)   <<","<<std::endl
        <<"\t\t\"_machines_whose_pstate_changed_recently\":"              <<  batsched_tools::map_to_json_string(_machines_whose_pstate_changed_recently)  <<","<<std::endl
        <<"\t\t\"_jobs_whose_waiting_time_estimation_has_been_requested_recently\":"  <<  batsched_tools::vector_to_json_string(_jobs_whose_waiting_time_estimation_has_been_requested_recently)  <<","<<std::endl
        <<"\t\t\"_jobs_killed_recently\":"                                <<  batsched_tools::unordered_map_to_json_string(_jobs_killed_recently)          <<","<<std::endl
        <<"\t\t\"_jobs_ended_recently\":"                                 <<  batsched_tools::vector_to_json_string(_jobs_ended_recently)     <<","<<std::endl
        <<"\t\t\"_jobs_released_recently\":"                              <<  batsched_tools::vector_to_json_string(_jobs_released_recently)  <<","<<std::endl
        <<"\t\t\"_recently_under_repair_machines\":"                      <<  batsched_tools::to_json_string(_recently_under_repair_machines) <<","<<std::endl
        <<"\t\t\"_nopped_recently\":"                                     <<  _nopped_recently                                                <<","<<std::endl
        <<"\t\t\"_consumed_joules_updated_recently\":"                    <<  _consumed_joules_updated_recently                               <<std::endl
        <<"\t},"<<std::endl //closes recently_variables, leaves a brace open

        //failure_variables
        <<"\t\"failure_variables\":{"
        <<"\t\t\"_need_to_send_finished_submitting_jobs\":"       << _need_to_send_finished_submitting_jobs                               <<","<<std::endl
        <<"\t\t\"_no_more_static_job_to_submit_received\":"       << _no_more_static_job_to_submit_received                               <<","<<std::endl
        <<"\t\t\"_no_more_external_event_to_occur_received\":"    << _no_more_external_event_to_occur_received                            <<","<<std::endl
        <<"\t\t\"_checkpointing_on\":"                            << _checkpointing_on                                                    <<","<<std::endl
        <<"\t\t\"_call_me_laters\":"                              << batsched_tools::map_to_json_string(_decision->get_call_me_laters())  <<","<<std::endl
        <<"\t\t\"_on_machine_instant_down_ups\":"                 << batsched_tools::vector_to_json_string(&_on_machine_instant_down_ups) <<","<<std::endl
        <<"\t\t\"_on_machine_down_for_repairs\":"                 << batsched_tools::vector_to_json_string(&_on_machine_down_for_repairs) <<","<<std::endl
        //TODO _file_failures
        <<"\t\t\"_available_machines\":"                          << batsched_tools::to_json_string(_available_machines)                  <<","<<std::endl
        <<"\t\t\"_unavailable_machines\":"                        << batsched_tools::to_json_string(_unavailable_machines)                <<","<<std::endl
        <<"\t\t\"_nb_available_machines\":"                       << batsched_tools::to_json_string(_nb_available_machines)               <<","<<std::endl
        <<"\t\t\"_repair_machines\":"                             << batsched_tools::to_json_string(_repair_machines)                     <<","<<std::endl
        <<"\t\t\"_repairs_done\":"                                << batsched_tools::to_json_string(_repairs_done)                        <<","<<std::endl
        <<"\t\t\"_my_kill_jobs\":"                                << batsched_tools::map_to_json_string(&_my_kill_jobs)                   <<","<<std::endl
        <<"\t},"<<std::endl; //closes failure_variables, leaves a brace open
        
        //randomness gets their own files outside of this if statement below

        //schedule_variables
      if (_scheduleP != nullptr)
      {
        f<<std::fixed<<std::setprecision(15)<<std::boolalpha
        <<"\t\"schedule_variables\":{"
        <<"\t\t\"_output_svg\":"                                  << batsched_tools::to_json_string(_output_svg)                          <<","<<std::endl
        <<"\t\t\"_output_svg_method\":"                           << batsched_tools::to_json_string(_output_svg_method)                   <<","<<std::endl
        <<"\t\t\"_svg_frame_start\":"                             << _svg_frame_start                                                     <<","<<std::endl
        <<"\t\t\"_svg_frame_end\":"                               << _svg_frame_end                                                       <<","<<std::endl
        <<"\t\t\"_svg_output_start\":"                            << _svg_output_start                                                    <<","<<std::endl
        <<"\t\t\"_svg_output_end\":"                              << _svg_output_end                                                      <<","<<std::endl
        <<"\t\t\"_reschedule_policy\":"                           << int(_reschedule_policy)                                              <<","<<std::endl
        <<"\t\t\"_impact_policy\":"                               << int(_impact_policy)                                                  <<","<<std::endl
        <<"\t\t\"_killed_jobs\":"                                 << _killed_jobs                                                         <<","<<std::endl
        <<"\t\t\"_resubmitted_jobs\":"                            << batsched_tools::map_to_json_string(&_resubmitted_jobs)               <<","<<std::endl
        <<"\t\t\"_resubmitted_jobs_released\":"                   << batsched_tools::vector_pair_to_json_string(&_resubmitted_jobs_released)<<","<<std::endl
        <<"\t\t\"_dump_provisional_schedules\":"                  << _dump_provisional_schedules                                          <<","<<std::endl
        <<"\t\t\"_dump_prefix\":"                                 << batsched_tools::to_json_string(_dump_prefix)                         <<","<<std::endl
        <<"\t},"<<std::endl; //closes schedule_variables, leaves a brace open
      }  
        //schedule itself gets its own file outside of this if statement below randomness

        //backfill_variables
      if (_priority_job != nullptr)
      {
        f<<std::fixed<<std::setprecision(15)<<std::boolalpha
        <<"\t\"backfill_variables\":{"
        <<"\t\t\"_priority_job\":"                                << batsched_tools::to_json_string(_priority_job)                        <<std::endl
        <<"\t},"<<std::endl; //closes backfill_variables, leaves a brace open
      } 

       //reservation_queue is put in the queues file outside of this if statement above

       //reservation_variables
      if (_reservation_algorithm)
      {
        f<<std::fixed<<std::setprecision(15)<<std::boolalpha
        <<"\t\"reservation_variables\":{"
        <<"\t\t\"_start_a_reservation\":"                            << _start_a_reservation                                               <<","<<std::endl
        <<"\t\t\"_need_to_compress\":"                               << _need_to_compress                                                  <<","<<std::endl
        <<"\t\t\"_saved_reservations\":"                             << _schedule.vector_to_json_string(&_saved_reservations)              <<","<<std::endl
        <<"\t\t\"_saved_recently_queued_jobs\":"                     << batsched_tools::vector_to_json_string(&_saved_recently_queued_jobs)<<","<<std::endl
        <<"\t\t\"_saved_recently_ended_jobs\":"                      << batsched_tools::vector_to_json_string(&_saved_recently_ended_jobs) <<","<<std::endl
        <<"\t},"<<std::endl; //closes reservation_variables, leaves a brace open
      }

      f<<std::fixed<<std::setprecision(15)<<std::boolalpha
      <<"\t\"real_checkpoint_variables\":{"
      <<"\t\t\"_nb_batsim_checkpoints\":"                            << batsched_tools::to_json_string(_nb_batsim_checkpoints)            <<std::endl
      <<"\t},"<<std::endl; //closes real_checkpoint_variables, leaves a brace open
      
      if (_share_packing_algorithm)
      {
        if (_horizon_algorithm)
            set_index_of_horizons();
        f<<std::fixed<<std::setprecision(15)<<std::boolalpha
        <<"\t\"share_packing_variables\":{"
        <<"\t\t\"_available_core_machines\":"                          << batsched_tools::to_json_string(_available_core_machines)      <<","<<std::endl
        <<"\t\t\"_pending_jobs\":"                                     << batsched_tools::list_to_json_string(_pending_jobs)            <<","<<std::endl
        <<"\t\t\"_pending_jobs_heldback\":"                            << batsched_tools::list_to_json_string(_pending_jobs_heldback)   <<","<<std::endl
        <<"\t\t\"_running_jobs\":"                                     << batsched_tools::unordered_set_to_json_string(_running_jobs)   <<","<<std::endl
        <<"\t\t\"_horizons\":"             << (_horizons.empty()? "\"\"" : batsched_tools::list_to_json_string(_horizons))              <<","<<std::endl
        <<"\t\t\"_current_allocations\":"                              << batsched_tools::unordered_map_to_json_string(_current_allocations) <<","<<std::endl
        <<"\t\t\"_heldback_machines\":"                                << batsched_tools::to_json_string(_heldback_machines)            <<std::endl;
      }



        f<<"}"<<std::endl; // closes batsched_variables
        f.close();
    }

    //randomness_files
    f.open(checkpoint_dir+"/generator_failure.dat");
    f<<generator_failure;
    f.close();
    f.open(checkpoint_dir+"/generator_machine.dat");
    f<<generator_machine;
    f.close();
    f.open(checkpoint_dir + "/generator_repair_time.dat");
    f<<generator_repair_time;
    f.close();
    if (failure_unif_distribution != nullptr)
    {
        f.open(checkpoint_dir + "/failure_unif_distribution.dat");
        f<<*failure_unif_distribution;
        f.close();
    }

    if (failure_exponential_distribution != nullptr)
    {
        f.open(checkpoint_dir + "/failure_exponential_distribution.dat");
        f<<*failure_exponential_distribution;
        f.close();
    }

    if (machine_unif_distribution != nullptr)
    {
        f.open(checkpoint_dir + "/machine_unif_distribution.dat");
        f<<*machine_unif_distribution;
        f.close();
    }

    if (repair_time_exponential_distribution != nullptr)
    {
        f.open(checkpoint_dir + "/repair_time_exponential_distribution.dat");
        f<<*repair_time_exponential_distribution;
        f.close();
    }

    //schedule_file
    if (_scheduleP != nullptr)
    {
        f.open(checkpoint_dir+"/batsched_schedule.chkpt",std::ios_base::out);
        if (f.is_open())
        {
            f<<_schedule.to_json_string()<<std::endl;
            f.close();
        }
    }

    LOG_F(INFO,"here");
    
    _need_to_checkpoint=false;

    //if we need to do something special for the algorithm
    on_checkpoint_batsched(date);


}

void ISchedulingAlgorithm::on_machine_unavailable_notify_event(double date, IntervalSet machines)
{
    (void) date;
    _machines_that_became_unavailable_recently += machines;
}
void ISchedulingAlgorithm::on_job_fault_notify_event(double date, std::string job)  // ****************added
{
    (void) date;
}
void ISchedulingAlgorithm::on_myKillJob_notify_event(double date){
    (void) date;
}
void ISchedulingAlgorithm::on_query_estimate_waiting_time(double date, const string &job_id)
{
    (void) date;
    _jobs_whose_waiting_time_estimation_has_been_requested_recently.push_back(job_id);
}



















    rapidjson::Document ISchedulingAlgorithm::ingestDoc(std::string filename)
    {
        std::string content;
        std::ifstream ifile;
        ifile.open(filename);
        PPK_ASSERT_ERROR(ifile.is_open(), "Cannot read file '%s'", filename.c_str());

        ifile.seekg(0, ios::end);
        content.reserve(static_cast<unsigned long>(ifile.tellg()));
        ifile.seekg(0, ios::beg);

        content.assign((std::istreambuf_iterator<char>(ifile)),
                    std::istreambuf_iterator<char>());

        ifile.close();
        rapidjson::Document jsonDoc;
        jsonDoc.Parse(content.c_str());
        return jsonDoc;
    }


    void ISchedulingAlgorithm::ingest(Machines * machines, const rapidjson::Value & json)
    {
        auto ourJson = json.GetArray();
        for(rapidjson::SizeType i = 0;i<ourJson.Size();i++)
        {
            machines->ingest(ourJson[i]);
        }
    }

    double ISchedulingAlgorithm::ingest(double aDouble, const rapidjson::Value &json)
    {
        return json.GetDouble();
    }
    bool ISchedulingAlgorithm::ingest(bool aBool, const rapidjson::Value &json)
    {
        return json.GetBool();
    }
    int ISchedulingAlgorithm::ingest(int aInt, const rapidjson::Value &json)
    {
        return json.GetInt();
    }
    long ISchedulingAlgorithm::ingest(long aLong, const rapidjson::Value &json)
    {
        return json.GetInt64();
    }
    std::string ISchedulingAlgorithm::ingest(std::string aString,const rapidjson::Value &json)
    {
        return json.GetString();
    }
    Schedule::RESCHEDULE_POLICY ISchedulingAlgorithm::ingest(Schedule::RESCHEDULE_POLICY aPolicy, const rapidjson::Value &json)
    {
        int value = json.GetInt();
        return static_cast<Schedule::RESCHEDULE_POLICY>(value); 
    }
    Schedule::IMPACT_POLICY ISchedulingAlgorithm::ingest(Schedule::IMPACT_POLICY aPolicy, const rapidjson::Value &json)
    {
        int value = json.GetInt();
        return static_cast<Schedule::IMPACT_POLICY>(value);
    }
    Queue * ISchedulingAlgorithm::ingest(Queue * queue,const rapidjson::Value & json,double date)
    {
        const rapidjson::Value & array = json.GetArray();
        SortableJobOrder::UpdateInformation update_info(date);
        for (rapidjson::SizeType i=0;i<array.Size();i++)
        {
            std::string job_id = array[i].GetString();
            auto parts = batsched_tools::get_job_parts(job_id);
            const Job * new_job = (*_workload)[parts.next_checkpoint];
        
            queue->append_job(new_job,&update_info);
    } 
    }
    Job * ISchedulingAlgorithm::ingest(Job * aJob,const rapidjson::Value &json)
    {
        std::string job_id = json.GetString();
        return (*_workload)[job_id];

    }
    IntervalSet ISchedulingAlgorithm::ingest(IntervalSet intervalSet,const rapidjson::Value &json)
    {
        std::string value = json.GetString();
        return IntervalSet::from_string_hyphen(value);
    }
    std::map<int,IntervalSet> ISchedulingAlgorithm::ingest(std::map<int,IntervalSet> &aMap, const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::map<int,IntervalSet> ourMap;
        for(rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            int key = array[i]["key"].GetInt();
            IntervalSet value = IntervalSet::from_string_hyphen(array[i]["value"].GetString());
            ourMap[key] = value;
        }
        return ourMap;
    }
    std::vector<std::string> ISchedulingAlgorithm::ingest(std::vector<std::string> &aVector, const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::vector<std::string> aVector;
        for(rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            aVector.push_back(array[i].GetString());
        }
        return aVector;
    }
    std::unordered_map<std::string, batsched_tools::Job_Message *> ISchedulingAlgorithm::ingest(std::unordered_map<std::string, batsched_tools::Job_Message *> &aUMap, const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::unordered_map<std::string,batsched_tools::Job_Message *> umap;
        for(rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            std::string key = array[i]["key"].GetString();
            batsched_tools::Job_Message * value;
            auto parts = batsched_tools::get_job_parts(array[i]["value"]["id"].GetString());
            value->id = parts.next_checkpoint;
            value->forWhat = static_cast<batsched_tools::KILL_TYPES>(array[i]["value"]["forWhat"].GetInt());
            value->progress = array[i]["value"]["progress"].GetDouble();
            value->progress_str = array[i]["value"]["progress_str"].GetString();
            umap[key] = value;
            
        }
        return umap;
    }
    std::vector<double> ISchedulingAlgorithm::ingest(std::vector<double> &aVector, const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::vector<double> aVector;
        for(rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            aVector.push_back(array[i].GetDouble());
        }
        return aVector;
    }
    void ISchedulingAlgorithm::ingestCMLS(const rapidjson::Value &json,double date)
    {
        const rapidjson::Value & array = json.GetArray();
        std::map<int,batsched_tools::CALL_ME_LATERS> aMap;
        for (rapidjson::SizeType i=0;i<array.Size();i++)
        {
            batsched_tools::CALL_ME_LATERS cml;
            cml.time = array[i]["value"]["time"].GetDouble();
            cml.forWhat = static_cast<batsched_tools::call_me_later_types>(array[i]["value"]["forWhat"].GetInt());
            cml.extra_data = array[i]["value"]["extra_data"].GetString();
            cml.id = array[i]["value"]["id"].GetInt();
            aMap[cml.id]=cml;
        }
        _decision->set_call_me_laters(aMap,date,true);
    }
    std::vector<batsched_tools::KILL_TYPES> ISchedulingAlgorithm::ingest(std::vector<batsched_tools::KILL_TYPES> &aVector, const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::vector<batsched_tools::KILL_TYPES> aVector;
        for(rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            aVector.push_back(static_cast<batsched_tools::KILL_TYPES>(array[i].GetInt()));
        }
        return aVector;
    }
    std::map<Job *,batsched_tools::Job_Message *> ISchedulingAlgorithm::ingest(std::map<Job *,batsched_tools::Job_Message *> &aMap, const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::map<Job *,batsched_tools::Job_Message *> map;
        batsched_tools::Job_Message * jm;
        for(rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            Job * job = (*_workload)[array[i]["key"].GetString()];
            jm = new batsched_tools::Job_Message();
            jm->id = array[i]["value"]["id"].GetString();
            jm->progress_str = array[i]["value"]["progress_str"].GetString();
            jm->progress = array[i]["value"]["progress"].GetDouble();
            jm->forWhat = static_cast<batsched_tools::KILL_TYPES>(array[i]["value"]["forWhat"].GetInt());
            map[job]=jm; 
        }
        return map;
    }
    std::map<std::string,batsched_tools::KILL_TYPES> ISchedulingAlgorithm::ingest(std::map<std::string,batsched_tools::KILL_TYPES> &aMap,const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::map<std::string,batsched_tools::KILL_TYPES> map;
        batsched_tools::KILL_TYPES kt;
        std::string key;
        for(rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            key = array[i]["key"].GetString();
            kt = static_cast<batsched_tools::KILL_TYPES>(array[i]["value"].GetInt());
            map[key]=kt;
        }
        return map;
    }
    std::vector<std::pair<const Job *,batsched_tools::KILL_TYPES>> ISchedulingAlgorithm::ingest(std::vector<std::pair<const Job *,batsched_tools::KILL_TYPES>> &aVector, const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::vector<std::pair<const Job*,batsched_tools::KILL_TYPES>> aVector;

        for(rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            const Job * job= (*_workload)[array[i]["key"].GetString()];
            std::pair<const Job*,batsched_tools::KILL_TYPES> pair{job,static_cast<batsched_tools::KILL_TYPES>(array[i]["value"].GetInt())};
            aVector.push_back(pair);
        }
        return aVector;
    }
    std::vector<Schedule::ReservedTimeSlice> ISchedulingAlgorithm::ingest(std::vector<Schedule::ReservedTimeSlice> &aVector,const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::vector<Schedule::ReservedTimeSlice> aVector;
        for (rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            aVector.push_back(_schedule.ReservedTimeSlice_from_json(array[i]));
        }
        return aVector;
    }
    std::list<Job *> ISchedulingAlgorithm::ingest(std::list<Job *> &aList,const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::list<Job *> aList;
        for (rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            aList.push_back((*_workload)[array[i].GetString()]);
        }
        return aList;
    }
    std::unordered_set<std::string> ISchedulingAlgorithm::ingest(std::unordered_set<std::string> &aUSet, const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::unordered_set<std::string> uset;
        for (rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            uset.insert(array[i].GetString());
        }
        return uset;
    }
    std::list<batsched_tools::FinishedHorizonPoint> ISchedulingAlgorithm::ingest(std::list<batsched_tools::FinishedHorizonPoint> &aList,const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::list<batsched_tools::FinishedHorizonPoint> aList;
        for (rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            batsched_tools::FinishedHorizonPoint fhp;
            fhp.date = array[i]["value"]["date"].GetDouble();
            fhp.nb_released_machines = array[i]["nb_released_machines"].GetInt();
            fhp.machines = IntervalSet::from_string_hyphen(array[i]["machines"].GetString());
            aList.push_back(fhp);
        }
        return aList;
    }
    std::unordered_map<std::string,batsched_tools::Allocation> ISchedulingAlgorithm::ingest(std::unordered_map<std::string,batsched_tools::Allocation> &aUMap, const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::unordered_map<std::string,batsched_tools::Allocation> umap;
        for(rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            std::string key = array[i]["key"].GetString();
            batsched_tools::Allocation value;
            value.machines = IntervalSet::from_string_hyphen(array[i]["value"]["machines"].GetString());
            value.has_horizon = array[i]["value"]["has_horizon"].GetBool();
            int horizon_index = array[i]["value"]["horizon_it"].GetInt();
            if (horizon_index != -1)
            {
                std::list<batsched_tools::FinishedHorizonPoint>::iterator it = _horizons.begin();
                std::advance(it,horizon_index);
                value.horizon_it = it;
            }
            umap[key] = value;
        }
        return umap;
    }

    std::vector<Job *> ISchedulingAlgorithm::ingest(std::vector<Job *> jobs, const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::vector<Job *> vector;
        for(rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            std::string job_id = array[i].GetString();
            batsched_tools::job_parts parts = batsched_tools::get_job_parts(job_id); 
            std::string next_checkpoint = parts.next_checkpoint;
            vector.push_back((*_workload)[next_checkpoint]);
        }
        return vector;
        
    }
    std::vector<batsched_tools::Scheduled_Job *> ISchedulingAlgorithm::ingest(std::vector<batsched_tools::Scheduled_Job *> sj,const rapidjson::Value &json)
    {
        const rapidjson::Value & array = json.GetArray();
        std::vector<batsched_tools::Scheduled_Job *> vector;
        for(rapidjson::SizeType i = 0;i<array.Size();i++)
        {
            const rapidjson::Value & Vsj = array[i].GetObject();
            batsched_tools::Scheduled_Job * sj = new batsched_tools::Scheduled_Job();
            sj = ingest(sj,Vsj);
            vector.push_back(sj);
        }
        return vector;
    }
    batsched_tools::Scheduled_Job * ISchedulingAlgorithm::ingest(batsched_tools::Scheduled_Job * sj,const rapidjson::Value &json)
    {
        std::string id = json["id"].GetString();
        batsched_tools::job_parts parts = batsched_tools::get_job_parts(id);
        id = parts.next_checkpoint;
        sj->id = id;

        sj->requested_resources = json["requested_resources"].GetInt();
        sj->wall_time = json["wall_time"].GetDouble();
        sj->start_time = json["start_time"].GetDouble();
        sj->est_finish_time = json["est_finish_time"].GetDouble();
        sj->allocated_machines = IntervalSet::from_string_hyphen(json["allocated_machines"].GetString());
        return sj;
    }
    batsched_tools::Priority_Job * ISchedulingAlgorithm::ingest(batsched_tools::Priority_Job* pj, const rapidjson::Value &json)
    {
        const rapidjson::Value & Vpj = json.GetObject();
        std::string id = Vpj["id"].GetString();
        batsched_tools::job_parts parts = batsched_tools::get_job_parts(id);
        id = parts.next_checkpoint;
        pj->id = id;

        pj->requested_resources = Vpj["requested_resources"].GetInt();
        pj->extra_resources = Vpj["extra_resources"].GetInt();
        pj->shadow_time = Vpj["shadow_time"].GetDouble();
        pj->est_finish_time = Vpj["est_finish_time"].GetDouble();
        
    }
    
    









