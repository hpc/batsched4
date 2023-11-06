#include <math.h>
#include "easy_bf_fast2_holdback.hpp"

#include "../pempek_assert.hpp"

#include <loguru.hpp>

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <chrono>
#include "../batsched_tools.hpp"


#define B_LOG_INSTANCE _myBLOG
namespace myB = myBatsched;
namespace r = rapidjson;
const int DEBUG = 10;

easy_bf_fast2_holdback::easy_bf_fast2_holdback(Workload *workload,
    SchedulingDecision *decision, Queue *queue, ResourceSelector *selector,
    double rjms_delay, rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay,
        variant_options)
{
    
    //_myWorkloads = new myBatsched::Workloads;
    //batsim log object.  declared in batsched_tools.hpp
    _myBLOG = new b_log();
    
    
}

easy_bf_fast2_holdback::~easy_bf_fast2_holdback()
{
    
}
void easy_bf_fast2_holdback::on_start_from_checkpoint(double date,const rapidjson::Value & batsim_config){

}
void easy_bf_fast2_holdback::on_simulation_start(double date,
    const rapidjson::Value &batsim_event_data)
{
    pid_t pid = batsched_tools::get_batsched_pid();
    _decision->add_generic_notification("PID",std::to_string(pid),date);
    bool seedFailures = false;
    bool logBLog = false;
    const rapidjson::Value & batsim_config = batsim_event_data["config"];
    if (batsim_config.HasMember("share-packing"))
        _share_packing = batsim_config["share-packing"].GetBool();
    if (batsim_config.HasMember("share-packing-holdback"))
        _share_packing_holdback = batsim_config["share-packing-holdback"].GetInt();
    if (batsim_config.HasMember("core-percent"))
        _core_percent = batsim_config["core-percent"].GetDouble();

    if (batsim_config.HasMember("output-folder")){
        _output_folder = batsim_config["output-folder"].GetString();
        _output_folder=_output_folder.substr(0,_output_folder.find_last_of("/"));
    }
    
    if (batsim_config.HasMember("seed-failures"))
        seedFailures = batsim_config["seed-failures"].GetBool();
    if (batsim_config.HasMember("log_b_log"))
        logBLog = batsim_config["log_b_log"].GetBool();
    //log BLogs, add log files that you want logged to.
    if (logBLog){
        _myBLOG->add_log_file(_output_folder+"/log/failures.log",b_log::FAILURES);
    }
    ISchedulingAlgorithm::set_generators(date);
    _available_machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    _nb_available_machines = _nb_machines;
    //LOG_F(INFO,"avail: %d   nb_machines: %d",_available_machines.size(),_nb_machines);
    PPK_ASSERT_ERROR(_available_machines.size() == (unsigned int) _nb_machines);
    const rapidjson::Value& resources = batsim_event_data["compute_resources"];
    for ( auto & itr : resources.GetArray())
    {
	machine* a_machine = new machine();
        if (itr.HasMember("id"))
            a_machine->id = (itr)["id"].GetInt();
        if (itr.HasMember("core_count"))
        {
            a_machine->core_count = (itr)["core_count"].GetInt();
            a_machine->cores_available = int(a_machine->core_count * _core_percent);
        }
        if (itr.HasMember("speed"))
            a_machine->speed = (itr)["speed"].GetDouble();
        if (itr.HasMember("name"))
            a_machine->name = (itr)["name"].GetString();
        machines_by_int[a_machine->id] = a_machine;
        machines_by_name[a_machine->name] = a_machine;
        
    
        //LOG_F(INFO,"machine id = %d, core_count= %d , cores_available= %d",a_machine->id,a_machine->core_count,a_machine->cores_available);
   }
   if (_share_packing_holdback > 0)
   {
        _nb_available_machines -=_share_packing_holdback;
        _heldback_machines = _available_machines.left(_share_packing_holdback);
        _available_machines -= _heldback_machines;
        _unavailable_machines +=_heldback_machines;
    }
     _oldDate=date;

}      
void easy_bf_fast2_holdback::on_simulation_end(double date){
    (void) date;
}
    
 /*void easy_bf_fast2_holdback::on_machine_unavailable_notify_event(double date, IntervalSet machines){
    //LOG_F(INFO,"unavailable %s",machines.to_string_hyphen().c_str());
    _unavailable_machines+=machines;
    _available_machines-=machines;
    for(auto key_value : _current_allocations)
    {
            if (!((key_value.second.machines & machines).is_empty()))
                _decision->add_kill_job({key_value.first},date);
    }
    
}
*/
/*void easy_bf_fast2_holdback::set_workloads(myBatsched::Workloads *w){
    _myWorkloads = w;
    _checkpointing_on = w->_checkpointing_on;
    
}
*/
void easy_bf_fast2_holdback::on_machine_available_notify_event(double date, IntervalSet machines){
    ISchedulingAlgorithm::on_machine_available_notify_event(date, machines);
    _unavailable_machines-=machines;
    _available_machines+=machines;
    _nb_available_machines+=machines.size();
    
}

void easy_bf_fast2_holdback::on_machine_state_changed(double date, IntervalSet machines, int new_state)
{
   

}
void easy_bf_fast2_holdback::on_myKillJob_notify_event(double date){
    
    if (!_running_jobs.empty()){
        auto msg = new batsched_tools::Job_Message;
        msg->id = *_running_jobs.begin();
        msg->forWhat = batsched_tools::KILL_TYPES::NONE;
        _my_kill_jobs.insert(std::make_pair((*_workload)[*_running_jobs.begin()],msg));
    }
        
    
}



void easy_bf_fast2_holdback::on_machine_down_for_repair(double date){
    //get a random number of a machine to kill
    int number = machine_unif_distribution->operator()(generator_machine);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = number;
    //if the machine is already down for repairs ignore it.
    //LOG_F(INFO,"repair_machines.size(): %d    nb_avail: %d  avail:%d running_jobs: %d",_repair_machines.size(),_nb_available_machines,_available_machines.size(),_running_jobs.size());
    BLOG_F(b_log::FAILURES,"Machine Repair: %d",number);
    if ((machine & _repair_machines).is_empty())
    {
        //ok the machine is not down for repairs
        //it will be going down for repairs now
        _available_machines-=machine;
        _unavailable_machines+=machine;
        _repair_machines+=machine;
        _nb_available_machines=_available_machines.size();

        double repair_time = _workload->_repair_time;
        if (_workload->_MTTR != -1.0)
           repair_time = repair_time_exponential_distribution->operator()(generator_repair_time);
        //LOG_F(INFO,"in repair_machines.size(): %d nb_avail: %d  avail: %d running_jobs: %d",_repair_machines.size(),_nb_available_machines,_available_machines.size(),_running_jobs.size());
        //LOG_F(INFO,"date: %f , repair: %f ,repair + date: %f",date,repair_time,date+repair_time);
        //call me back when the repair is done
        _decision->add_call_me_later(batsched_tools::call_me_later_types::REPAIR_DONE,number,date+repair_time,date);
        //now kill the jobs that are running on machines that need to be repaired.        
        //if there are no running jobs, then there are none to kill
        if (!_running_jobs.empty()){
            for(auto key_value : _current_allocations)
            {
                if (!((key_value.second.machines & machine).is_empty())){
                    Job * job_ref = (*_workload)[key_value.first];
                    auto msg = new batsched_tools::Job_Message;
                    msg->id = key_value.first;
                    msg->forWhat = batsched_tools::KILL_TYPES::NONE;
                    _my_kill_jobs.insert(std::make_pair(job_ref,msg));
                    BLOG_F(b_log::FAILURES,"Killing Job: %s",key_value.first.c_str());
                }
            }
        }
    }
    else
    {
        BLOG_F(b_log::FAILURES,"Machine Already Being Repaired: %d",number);
    }
}


void easy_bf_fast2_holdback::on_machine_instant_down_up(double date){
    //get a random number of a machine to kill
    int number = machine_unif_distribution->operator()(generator_machine);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = number;
    BLOG_F(b_log::FAILURES,"Machine Instant Down Up: %d",number);
    //if there are no running jobs, then there are none to kill
    if (!_running_jobs.empty()){
        for(auto key_value : _current_allocations)   
	    {
		    if (!((key_value.second.machines & machine).is_empty())){
                	Job * job_ref = (*_workload)[key_value.first];
                    auto msg = new batsched_tools::Job_Message;
                    msg->id = key_value.first;
                    msg->forWhat = batsched_tools::KILL_TYPES::NONE;
                    _my_kill_jobs.insert(std::make_pair(job_ref,msg));
	                BLOG_F(b_log::FAILURES,"Killing Job: %s",key_value.first.c_str());
            }
	    }
    }
}
/*void easy_bf_fast2_holdback::on_job_fault_notify_event(double date, std::string job){
    std::unordered_set<std::string>::const_iterator found = _running_jobs.find(job);
  //LOG_F(INFO,"on_job_fault_notify_event called");
  if ( found != _running_jobs.end() )    
        _decision->add_kill_job({job},date);
  else
      LOG_F(INFO,"Job %s was not running but was supposed to be killed due to job_fault event",job.c_str());
}
*/

void easy_bf_fast2_holdback::on_requested_call(double date,int id,batsched_tools::call_me_later_types forWhat)
{
    
        switch (forWhat){
            case batsched_tools::call_me_later_types::SMTBF:
                        {
                            //Log the failure
                            BLOG_F(b_log::FAILURES,"FAILURE SMTBF");
                            if (!_running_jobs.empty() || !_pending_jobs.empty() || !_no_more_static_job_to_submit_received)
                                {
                                    double number = failure_exponential_distribution->operator()(generator_failure);
                                    if (_workload->_repair_time == 0.0)
                                        on_machine_instant_down_up(date);
                                    else
                                        on_machine_down_for_repair(date);
                                    _decision->add_call_me_later(batsched_tools::call_me_later_types::SMTBF,1,number+date,date);
                                }
                        }
                        break;
            case batsched_tools::call_me_later_types::MTBF:
                        {
                            if (!_running_jobs.empty() || !_pending_jobs.empty() || !_no_more_static_job_to_submit_received)
                            {
                                double number = failure_exponential_distribution->operator()(generator_failure);
                                on_myKillJob_notify_event(date);
                                _decision->add_call_me_later(batsched_tools::call_me_later_types::MTBF,1,number+date,date);

                            }
                        
                            
                        }
                        break;
            case batsched_tools::call_me_later_types::FIXED_FAILURE:
                        {
                            BLOG_F(b_log::FAILURES,"FAILURE FIXED_FAILURE");
                            if (!_running_jobs.empty() || !_pending_jobs.empty() || !_no_more_static_job_to_submit_received)
                                {
                                    double number = _workload->_fixed_failures;
                                    if (_workload->_repair_time == 0.0)
                                        on_machine_instant_down_up(date);
                                    else
                                        on_machine_down_for_repair(date);
                                    _decision->add_call_me_later(batsched_tools::call_me_later_types::FIXED_FAILURE,1,number+date,date);
                                }
                        }
                        break;
            case batsched_tools::call_me_later_types::REPAIR_DONE:
                        {
                            BLOG_F(b_log::FAILURES,"REPAIR_DONE");
                            //a repair is done, all that needs to happen is add the machines to available
                            //and remove them from repair machines and add one to the number of available
                            IntervalSet machine = id;
                            _available_machines += machine;
                            _unavailable_machines -= machine;
                            _repair_machines -= machine;
                            _nb_available_machines=_available_machines.size();
                            _machines_that_became_available_recently += machine;
                           //LOG_F(INFO,"in repair_machines.size(): %d nb_avail: %d avail: %d  running_jobs: %d",_repair_machines.size(),_nb_available_machines,_available_machines.size(),_running_jobs.size());
                        }
                        break;
        }
    

}
void easy_bf_fast2_holdback::on_no_more_static_job_to_submit_received(double date){
    ISchedulingAlgorithm::on_no_more_static_job_to_submit_received(date);

}
void easy_bf_fast2_holdback::on_no_more_external_event_to_occur(double date){
    
    _wrap_it_up = true;    
    
    
}
void easy_bf_fast2_holdback::on_job_end(double date, std::vector<std::string> job_ids)
{
    (void) date;
    (void) job_ids;
}





/*********************************************************
 *                                                       *
 *                    Make Decisions                     *
 *                                                       *
**********************************************************/



void easy_bf_fast2_holdback::make_decisions(double date,
    SortableJobOrder::UpdateInformation *update_info,
    SortableJobOrder::CompareInformation *compare_info)
{
   ////LOG_F(INFO,"line 322   fcfs_fast2.cpp");
    (void) update_info;
    (void) compare_info;
    std::vector<int> mapping = {0};
    if (_oldDate == -1)
        _oldDate=date;
    // This algorithm is a fast version of FCFS without backfilling.
    // It is meant to be fast in the usual case, not to handle corner cases.
    // It is not meant to be easily readable or hackable ;).

    // This fast FCFS variant in a few words:
    // - only handles the FCFS queue order
    // - only handles the basic resource selection policy
    // - only handles finite jobs (no switchoff)
    // - only handles time as floating-point (-> precision errors).

    

    ////LOG_F(INFO,"line 340  fcfs_fast2.cpp");
    //*****************************************************************
    // Handle newly finished jobs
    //*****************************************************************
    LOG_F(INFO,"line 353");
    bool job_ended = handle_newly_finished_jobs();
    LOG_F(INFO,"line 355");    
    handle_new_jobs_to_kill(date);
    //************************************************************resubmission if killed
    //Handle jobs to queue back up (if killed)
    LOG_F(INFO,"line 359");
    _decision->handle_resubmission(_jobs_killed_recently,_workload,date);    
    //***********************************************************
    LOG_F(INFO,"line 362");
    handle_machines_coming_available(date);
    LOG_F(INFO,"line 364");
    handle_ended_job_execution(job_ended,date);
    LOG_F(INFO,"line 366");
    handle_newly_released_jobs(date);
    LOG_F(INFO,"line 368");
    
    /*if (_jobs_killed_recently.empty() && _wrap_it_up && _need_to_send_finished_submitting_jobs && !_myWorkloads->_checkpointing_on)
    {
        
        _decision->add_scheduler_finished_submitting_jobs(date);
        _need_to_send_finished_submitting_jobs = false;
    }
    */
   LOG_F(INFO,"_e_counter %d _p_counter %d",_e_counter,_p_counter);
    if (_jobs_killed_recently.empty() && _pending_jobs.empty() && _running_jobs.empty() &&
             _need_to_send_finished_submitting_jobs && _no_more_static_job_to_submit_received && !date<1.0 )
    {
        _decision->add_scheduler_finished_submitting_jobs(date);
        _need_to_send_finished_submitting_jobs = false;
    }
      
}




std::string easy_bf_fast2_holdback::to_json_desc(rapidjson::Document * doc)
{
  rapidjson::StringBuffer buffer;

  buffer.Clear();

  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc->Accept(writer);

  return std::string( buffer.GetString() );
}


/**********************************************
 * 
 *          Newly Finished Jobs
 * 
 ***********************************************/

bool easy_bf_fast2_holdback::handle_newly_finished_jobs()
{
   //LOG_F(INFO,"line 410");
   std::vector<int> mapping = {0};
    bool job_ended = false;
    for (const std::string & ended_job_id : _jobs_ended_recently)
    {
        job_ended = true;
        Job * finished_job = (*_workload)[ended_job_id];
        const Allocation & alloc = _current_allocations[ended_job_id];
        if (_share_packing && finished_job->nb_requested_resources == 1)
        {
                //first get the machine it was running on
                int machine_number = alloc.machines[0];
                machine* current_machine = machines_by_int[machine_number];

                //now increase cores_available on that machine
                current_machine->cores_available += 1;
                //if that machine was part of heldback machines then that's all that needs to be done
                bool skip = false;
                if (_share_packing_holdback > 0 && !((_heldback_machines & alloc.machines).is_empty()))
                    skip = true;
                //if that increase means no jobs are running on that machine (all its cores are available) then put it back in the mix
                if (!skip && current_machine->cores_available == int(current_machine->core_count * _core_percent))
                {
                    _available_core_machines -= alloc.machines;  // we subtract a core machine because it is now a regular machine
                    _available_machines.insert(alloc.machines); // we insert the machine into available machines
                    _nb_available_machines += 1; // we increase available machines by 1
                }
                _current_allocations.erase(ended_job_id);
                if (alloc.has_horizon == true)
                    _horizons.erase(alloc.horizon_it);
                _running_jobs.erase(ended_job_id);
        }
            // was not a 1 resource job, do things normally
        else{
                _available_machines.insert(alloc.machines);
                _nb_available_machines += finished_job->nb_requested_resources;
                _current_allocations.erase(ended_job_id);
                _running_jobs.erase(ended_job_id);
                _my_kill_jobs.erase((*_workload)[ended_job_id]);
                _horizons.erase(alloc.horizon_it);
        }
    }

    LOG_F(INFO,"_nb_available_machines %d",_nb_available_machines);
    return job_ended;
}

/**********************************************
 * 
 *          New Jobs To Kill
 * 
 ***********************************************/


void easy_bf_fast2_holdback::handle_new_jobs_to_kill(double date)
{
     if(!_my_kill_jobs.empty()){
         std::vector<batsched_tools::Job_Message *> kills;
        for( auto job_msg_pair:_my_kill_jobs)
        {
            kills.push_back(job_msg_pair.second);
        }
        _decision->add_kill_job(kills,date);
        _my_kill_jobs.clear();
    }
}

/**********************************************
 * 
 *          Machines Coming Available TODO
 * 
 ***********************************************/

void easy_bf_fast2_holdback::handle_machines_coming_available(double date)
{
    
    
}

/**********************************************
 * 
 *          Ended Job Execution
 * 
 ***********************************************/



void easy_bf_fast2_holdback::handle_ended_job_execution(bool job_ended,double date)
{
    std::vector<int> mapping = {0};
    // If jobs have finished, execute jobs as long as they fit
    std::list<Job *>::iterator job_it =_pending_jobs.begin();
    if (job_ended)
    {
        if (_priority_job == nullptr)
            LOG_F(INFO,"line 499 nullptr");
        if (_priority_job != nullptr)
        {
            LOG_F(INFO,"pending_jobs %d _nb_available_machines %d",_pending_jobs.size(),_nb_available_machines);
            //first check if priority job fits
            //it fits if it's a 1 resource job and 
            //share_packing is enabled and
            //we either find an _available_core_machine or we find an available heldback machine or
            //we find a free machine to make an _available_core_machine
            //or
            //it fits if share_packing is disabled, or it's greater than a 1 resource job
            //and
            //there are enough machines available for the priority job's nb_requested_resources
            Allocation alloc;
            FinishedHorizonPoint point;
            bool executed = false;

            if (_share_packing && _priority_job->nb_requested_resources == 1)
            {
                //ok it's a 1 resource job and share packing is enabled
                //let's check if we can run it on a heldback machine
                bool found = false;
                if (_share_packing_holdback > 0)
                {
                    for (auto it = _heldback_machines.elements_begin(); it != _heldback_machines.elements_end(); ++it)
                    {
                        machine* current_machine = machines_by_int[*it];
                        if (current_machine->cores_available >= 1)
                        {
                            found = true;
                            alloc.machines = *it;
                            break;
                        }
                    }
                    if (found == true)
                    {
                        _decision->add_execute_job(_priority_job->id,alloc.machines,date,mapping);
                        _e_counter+=1;
                        executed = true;
                        //update data structures
                        //the job doesn't get put into the horizons because it is not part of backfilling
                        alloc.has_horizon = false;

                        machine * current_machine = machines_by_int[alloc.machines[0]];
                        current_machine->cores_available -=1;
                        _current_allocations[_priority_job->id] = alloc;
                        _running_jobs.insert(_priority_job->id);
                        _priority_job = nullptr;
                        
                    }
                }


                //ok the job can be share-packed:
                    if (executed == false) //(not able to run on heldback machines )
                    {
                        //first check if there is a share-packing machine available:
                        //it is a 1 resource job, iterate over the available core machines until it finds one to put the job on.
                        for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
                        {
                            //is this machine able to handle another job?
                            machine* current_machine = machines_by_int[*it];
                            if (current_machine->cores_available >= 1)
                            {                                
                                //it is able to handle another job
                                found = true;
                                alloc.machines = *it;
                                break;
                            }
                                        
                        }
                        //LOG_F(INFO,"line 529");
                        if (found == true)
                        {
                            //yes it can be executed right away
                            _decision->add_execute_job(_priority_job->id,alloc.machines,date,mapping);
                            _e_counter+=1;
                            executed = true;
                            //update data structures
                            machine* current_machine = machines_by_int[alloc.machines[0]];
                            point.nb_released_machines = _priority_job->nb_requested_resources;
                            point.date = date + (double)_priority_job->walltime;
                            point.machines = alloc.machines;
                            alloc.horizon_it = insert_horizon_point(point);

                            
                            current_machine->cores_available -=1;
                            _current_allocations[_priority_job->id] = alloc;
                            _running_jobs.insert(_priority_job->id);

                            _priority_job = nullptr;    
                            
                        }
                        if (found == false && _nb_available_machines > 0)
                        {
                            
                            //first get a machine
                            alloc.machines = _available_machines.left(1);
                        
                            _decision->add_execute_job(_priority_job->id,alloc.machines,date,mapping);
                            _e_counter+=1;
                            executed = true;
                            
                            point.nb_released_machines = _priority_job->nb_requested_resources;
                            point.date = date + (double)_priority_job->walltime;
                            point.machines = alloc.machines;
                            alloc.horizon_it = insert_horizon_point(point);

                            //update data structures
                            machine* current_machine = machines_by_int[alloc.machines[0]];
                            current_machine->cores_available -= 1;
                            _available_core_machines += alloc.machines;
                            _available_machines -= alloc.machines;
                            _nb_available_machines -= 1;
                        
                            _current_allocations[_priority_job->id] = alloc;
                        
                            _running_jobs.insert(_priority_job->id);
                            _priority_job = nullptr;
                        
                        }
                    }//end not executed
                
            }//end share-packing
            //ok not share-packing or resources > 1
            else if (_priority_job->nb_requested_resources <= _nb_available_machines)
            {
                //LOG_F(INFO, "Priority job fits!");
                alloc.machines = _available_machines.left(
                    _priority_job->nb_requested_resources);
                _decision->add_execute_job(_priority_job->id, alloc.machines,
                    date);
                executed = true;
                _e_counter+=1;
                point.nb_released_machines = _priority_job->nb_requested_resources;
                point.date = date + (double)_priority_job->walltime;
                point.machines = alloc.machines;
                alloc.horizon_it = insert_horizon_point(point);

                // Update data structures
                _available_machines -= alloc.machines;
                _nb_available_machines -= _priority_job->nb_requested_resources;
                _current_allocations[_priority_job->id] = alloc;
                _priority_job = nullptr;
            }
            //LOG_F(INFO,"line 597");
              
            if (executed)
            {
                //ok priority job got to run, now execute the whole queue until a priority job cannot fit
                std::list<Job *>::iterator job_it =_pending_jobs.begin();
                bool erased = false;
                bool executed2;
                while(job_it!=_pending_jobs.end())
                {
                    Job * pending_job = *job_it;
                    Allocation alloc;
                    executed2 = false;
            
                    std::string pending_job_id = pending_job->id;
                    //can the job be share-packed?
                    if (_share_packing && pending_job->nb_requested_resources==1)
                    {
                        //it can be share-packed, can we run it on a heldback machine?
                        LOG_F(INFO,"line 611");
                        bool found = false;
                        if (_share_packing_holdback > 0)
                        {
                            //we can run it on a heldback machine as long as one is available.
                            for (auto it = _heldback_machines.elements_begin(); it != _heldback_machines.elements_end(); ++it)
                            {
                                machine* current_machine = machines_by_int[*it];
                                if (current_machine->cores_available >= 1)
                                {
                                    found = true;
                                    alloc.machines = *it;
                                    break;
                                }
                            }
                            if (found == true)
                            {
                                //a heldback machine is available, execute the job on it.
                                _decision->add_execute_job(pending_job_id,alloc.machines,date,mapping);
                                _e_counter+=1;
                                executed2 = true;
                                //update data structures
                                //the job doesn't get put into the horizons because it is not part of backfilling
                                alloc.has_horizon = false;

                                machine * current_machine = machines_by_int[alloc.machines[0]];
                                current_machine->cores_available -=1;
                                _current_allocations[pending_job_id] = alloc;
                                _running_jobs.insert(pending_job_id);
                                 job_it = _pending_jobs.erase(job_it);
                                 erased = true;
                                
                            }
                        }//end share-packing holdback
                        //was the job able to be put on a heldback machine?
                        if (executed2 == false)
                        {
                            //no it was not able to be put on a heldback machine, try putting it on a normal share-packing machine.
                            //it is a 1 resource job, iterate over the available core machines until it finds one to put the job on.
                            for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
                            {
                                //is this machine able to handle another job?
                                machine* current_machine = machines_by_int[*it];
                                if (current_machine->cores_available >= 1)
                                {                                
                                    //it is able to handle another job, execute a job on it and subtract from cores_available
                                    alloc.machines = *it;
                                    
                                    _decision->add_execute_job(pending_job_id,alloc.machines,date,mapping);
                                    executed2 = true;
                                    _e_counter+=1;
                                    point.nb_released_machines = pending_job->nb_requested_resources;
                                    point.date = date + (double)pending_job->walltime;
                                    point.machines = alloc.machines;
                                    alloc.horizon_it = insert_horizon_point(point);
                                    
                                    //update data structures
                                    current_machine->cores_available -=1;
                                    _current_allocations[pending_job_id] = alloc;
                                    _running_jobs.insert(pending_job_id);
                                    job_it = _pending_jobs.erase(job_it);
                                    _p_counter+=1;
                                    erased = true;
                                    found = true;
                                        
                                }
                                if (found == true)
                                    break; 
                            }  
                            // there were no available core machines to put it on, try to put on a new core machine
                            if (found == false && _nb_available_machines > 0)
                            {
                        
                            //LOG_F(INFO,"line 645");
                                //first get a machine
                                alloc.machines = _available_machines.left(1);
                            
                                _decision->add_execute_job(pending_job_id,alloc.machines,date,mapping);
                                executed2 = true;
                                _e_counter+=1;
                                point.nb_released_machines = pending_job->nb_requested_resources;
                                point.date = date + (double)pending_job->walltime;
                                point.machines = alloc.machines;
                                alloc.horizon_it = insert_horizon_point(point);
                                //update data structures
                                machine* current_machine = machines_by_int[alloc.machines[0]];
                                current_machine->cores_available -= 1;
                                _available_core_machines += alloc.machines;
                                _available_machines -= alloc.machines;
                                _nb_available_machines -= 1;
                            
                                _current_allocations[pending_job_id] = alloc;
                            
                                _running_jobs.insert(pending_job_id);
                                
                                job_it = _pending_jobs.erase(job_it);
                                _p_counter+=1;
                                erased = true;
                            }
                        }// end not executed     

                    } // end of pending jobs share-packing block
                
                    else if (pending_job->nb_requested_resources <= _nb_available_machines)
                    {
                    
                        alloc.machines = _available_machines.left(
                            pending_job->nb_requested_resources);
                        _decision->add_execute_job(pending_job->id,
                            alloc.machines, date);
                        executed2 = true;
                        _e_counter+=1;
                        point.nb_released_machines = pending_job->nb_requested_resources;
                        point.date = date + (double)pending_job->walltime;
                        point.machines = alloc.machines;
                        alloc.horizon_it = insert_horizon_point(point);
                        //LOG_F(INFO,"line 683");

                        // Update data structures
                        _available_machines -= alloc.machines;
                        _nb_available_machines -= pending_job->nb_requested_resources;
                        _current_allocations[pending_job_id] = alloc;
                        job_it = _pending_jobs.erase(job_it);
                        _p_counter+=1;
                        erased = true;
                        _running_jobs.insert(pending_job->id);
                    
                    }
                    if (executed2==false)
                    {
                        //ok we have a priority job, now stop traversing pending jobs
                        _priority_job = pending_job;
                        _priority_job->completion_time = compute_priority_job_expected_earliest_starting_time();
                        LOG_F(INFO,"line 699");
                        
                        job_it = _pending_jobs.erase(job_it);
                        _p_counter+=1;
                        //LOG_F(INFO,"line 701");
                        // Stop first queue traversal.
                        break;
                    }
                    if (!erased)
                        job_it++;
                    else
                        erased = false;
                }
            }
            //now let's backfill jobs that don't hinder priority job
           bool erased = false;
           job_it =_pending_jobs.begin();
            while(job_it!=_pending_jobs.end())
            {
                bool execute = false;
                Job * pending_job = *job_it;
                Allocation alloc;
                //LOG_F(INFO,"line 715");
                std::string pending_job_id = pending_job->id;
                //can we share-pack it?
                if (_share_packing && pending_job->nb_requested_resources==1)
                {
                        //yes we can share-pack it
                        //can we use a heldback machine?
                        bool found = false;
                        if (_share_packing_holdback > 0)
                        {
                            for (auto it = _heldback_machines.elements_begin(); it != _heldback_machines.elements_end(); ++it)
                            {
                                machine* current_machine = machines_by_int[*it];
                                if (current_machine->cores_available >= 1)
                                {
                                    found = true;
                                    alloc.machines = *it;
                                    break;
                                }
                            }
                            if (found == true)
                            {
                                //yes we can use a heldback machine
                                _decision->add_execute_job(pending_job_id,alloc.machines,date,mapping);
                                _e_counter+=1;
                                execute = true;
                                //update data structures
                                //the job doesn't get put into the horizons because it is not part of backfilling
                                alloc.has_horizon = false;

                                machine * current_machine = machines_by_int[alloc.machines[0]];
                                current_machine->cores_available -=1;
                                _current_allocations[pending_job_id] = alloc;
                                _running_jobs.insert(pending_job_id);
                                 job_it = _pending_jobs.erase(job_it);
                                 erased = true;
                                
                            }
                        }
                   if (execute == false)//ok can't backfill on the heldback machines, try the normal machines
                   {
                        bool found = false;
                        //it is a 1 resource job, iterate over the available core machines until it finds one to put the job on.
                        for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
                        {
                            //is this machine able to handle another job?
                            machine* current_machine = machines_by_int[*it];
                            //LOG_F(INFO,"line 728");
                            if (current_machine->cores_available >= 1)
                            {                                
                                found = true;
                                //LOG_F(INFO,"line 731");
                                if (date + pending_job->walltime <= _priority_job->completion_time)
                                {
                                    //it is able to handle another job, execute a job on it and subtract from cores_available
                                    alloc.machines = *it;
                                    
                                    _decision->add_execute_job(pending_job_id,alloc.machines,date,mapping);
                                    execute = true;
                                    _e_counter+=1;
                                    point.nb_released_machines = pending_job->nb_requested_resources;
                                    point.date = date + (double)pending_job->walltime;
                                    point.machines = alloc.machines;
                                    alloc.horizon_it = insert_horizon_point(point);
                                    //LOG_F(INFO,"line 744");
                                    //update data structures
                                    current_machine->cores_available -=1;
                                    _current_allocations[pending_job_id] = alloc;
                                    _running_jobs.insert(pending_job_id);
                                    job_it = _pending_jobs.erase(job_it);
                                    _p_counter+=1;
                                    erased = true;
                                    
                                    
                                }
                                else
                                    LOG_F(INFO,"date %f walltime %f completion_time %f",
                                    date,pending_job->walltime,_priority_job->completion_time);
                            }
                            if (found == true)
                                break; 
                        }  
                        // there were no available core machines to put it on, try to put on a new core machine
                        if (found == false && _nb_available_machines > 0)
                        {
                            LOG_F(INFO,"line 760");
                            if (date + pending_job->walltime <= _priority_job->completion_time)
                            {
                                //first get a machine
                                alloc.machines = _available_machines.left(1);
                            
                                _decision->add_execute_job(pending_job_id,alloc.machines,date,mapping);
                                _e_counter+=1;
                                execute=true;
                                point.nb_released_machines = pending_job->nb_requested_resources;
                                point.date = date + (double)pending_job->walltime;
                                point.machines = alloc.machines;
                                alloc.horizon_it = insert_horizon_point(point);
                                //update data structures
                                machine* current_machine = machines_by_int[alloc.machines[0]];
                                current_machine->cores_available -= 1;
                                _available_core_machines += alloc.machines;
                                _available_machines -= alloc.machines;
                                _nb_available_machines -= 1;
                            //LOG_F(INFO,"line 778");
                                _current_allocations[pending_job_id] = alloc;
                            
                                _running_jobs.insert(pending_job_id);
                                
                                job_it = _pending_jobs.erase(job_it);
                                _p_counter+=1;
                                erased = true;
                            }
                            else
                                LOG_F(INFO,"date %f walltime %f completion_time %f",
                                    date,pending_job->walltime,_priority_job->completion_time);
                        }
                    }// end of not executed 
                }// end of backfilling jobs share-packing block
                else if (pending_job->nb_requested_resources <= _nb_available_machines &&
                        date + pending_job->walltime <= _priority_job->completion_time)
                {
                    // Yes, it can be backfilled!
                    //LOG_F(INFO,"line 791");
                    alloc.machines = _available_machines.left(
                        pending_job->nb_requested_resources);
                    _decision->add_execute_job(pending_job->id,
                        alloc.machines, date);
                    _e_counter+=1;
                    execute = true;
                    point.nb_released_machines = pending_job->nb_requested_resources;
                    point.date = date + (double)pending_job->walltime;
                    point.machines = alloc.machines;
                    //LOG_F(INFO,"line 804");
                    alloc.horizon_it = insert_horizon_point(point);
                    //LOG_F(INFO,"line 806");
                    // Update data structures
                    _available_machines -= alloc.machines;
                    _nb_available_machines -= pending_job->nb_requested_resources;
                    _current_allocations[pending_job->id] = alloc;
                    //LOG_F(INFO,"line 811");
                    _running_jobs.insert(pending_job_id);
                    job_it = _pending_jobs.erase(job_it);
                    _p_counter+=1;
                    //LOG_F(INFO,"line 814");
                    erased = true;
                }
                else{
                    LOG_F(INFO,"date %f walltime %f completion_time %f",
                                 date,pending_job->walltime,_priority_job->completion_time);
                }
                if (erased==false)
                    job_it++;
                else
                   erased = false;
            }
        }
    }
}

/*************************************************
 * 
 *              Newly Released Jobs
 * 
 **************************************************/

void easy_bf_fast2_holdback::handle_newly_released_jobs(double date)
{
    int counter = 0;
    int pending = 0;
    std::vector<int> mapping = {0};
    // Handle newly released jobs
    for (const std::string & new_job_id : _jobs_released_recently)
    {
        Job * new_job = (*_workload)[new_job_id];

        // Is this job valid?
        if (new_job->nb_requested_resources > (_nb_machines-_share_packing_holdback))
        {
            // Invalid!
            //LOG_F(INFO,"Job being rejected HERE %s",new_job_id.c_str());
            _decision->add_reject_job(new_job_id, date);
            continue;
        }
        Allocation alloc;
        bool executed = false;
        //Are we share-packing?
        if (_share_packing && new_job->nb_requested_resources == 1)
        {
            //Can the job be executed right now?
            //first check _heldback_machines, if there are any
            bool found = false;
            if (_share_packing_holdback > 0)
            {
                for (auto it = _heldback_machines.elements_begin(); it != _heldback_machines.elements_end(); ++it)
                {
                    machine* current_machine = machines_by_int[*it];
                    if (current_machine->cores_available >= 1)
                    {
                        found = true;
                        alloc.machines = *it;
                        break;
                    }
                }
                if (found == true)
                {
                    _decision->add_execute_job(new_job_id,alloc.machines,date,mapping);
                    _e_counter+=1;
                    executed = true;
                    //update data structures
                    //the job doesn't get put into the horizons because it is not part of backfilling
                    alloc.has_horizon = false;

                    machine * current_machine = machines_by_int[alloc.machines[0]];
                    current_machine->cores_available -=1;
                    _current_allocations[new_job_id] = alloc;
                    _running_jobs.insert(new_job_id);
                     
                }
            }
            //then check all core machines running right now
            //if not executed on heldback machines
            if (executed == false)
            {
            
                for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
                {
                    //is this machine able to handle another job?
                    machine* current_machine = machines_by_int[*it];
                    if (current_machine->cores_available >= 1)
                    {                                
                        //it is able to handle another job
                        found = true;
                        alloc.machines = *it;
                        break;
                    }
                                    
                }
                if (found == true)
                {
                    if(_priority_job == nullptr ||
                            date + new_job->walltime<= _priority_job->completion_time)
                    {
                        //yes it can be executed right away
                        _decision->add_execute_job(new_job_id,alloc.machines,date,mapping);
                        _e_counter+=1;
                        executed = true;
                        //update data structures
                        FinishedHorizonPoint point;
                        point.nb_released_machines = new_job->nb_requested_resources;
                        point.date = date + (double)new_job->walltime;
                        point.machines = alloc.machines;
                        alloc.horizon_it = insert_horizon_point(point);

                        machine * current_machine = machines_by_int[alloc.machines[0]];
                        current_machine->cores_available -=1;
                        _current_allocations[new_job_id] = alloc;
                        _running_jobs.insert(new_job_id);
                        
                    }
                }
                if (found == false && _nb_available_machines > 0)
                {
                        
                    //Can the job be executed without hindering priority job?
                    if(_priority_job == nullptr ||
                        date + new_job->walltime<= _priority_job->completion_time)
                    {
                        //yes it can
                        //first get a machine
                        alloc.machines = _available_machines.left(1);
                        _decision->add_execute_job(new_job_id,alloc.machines,date,mapping);
                        _e_counter+=1;
                        executed = true;
                        FinishedHorizonPoint point;
                        point.nb_released_machines = new_job->nb_requested_resources;
                        point.date = date + (double) new_job->walltime;
                        point.machines = alloc.machines;
                        alloc.horizon_it = insert_horizon_point(point);


                        //update data structures
                        machine* current_machine = machines_by_int[alloc.machines[0]];
                        current_machine->cores_available -= 1;
                        _available_core_machines += alloc.machines;
                        _available_machines -= alloc.machines;
                        _nb_available_machines -= 1;
                        
                        _current_allocations[new_job_id] = alloc;
                        
                        _running_jobs.insert(new_job_id);
                        
                                                    
                    }

                }
            }   

        }//end share-packing block
        //we are not share-packing or the number of resources != 1
        else if (new_job->nb_requested_resources <=_nb_available_machines)
        {
            if (_priority_job == nullptr ||
                date + new_job->walltime <= _priority_job->completion_time)
            {
                alloc.machines = _available_machines.left(
                    new_job->nb_requested_resources);
                _decision->add_execute_job(new_job_id, alloc.machines, date);
                _e_counter+=1;
                executed = true;

                FinishedHorizonPoint point;
                point.nb_released_machines = new_job->nb_requested_resources;
                point.date = date + (double)new_job->walltime;
                point.machines = alloc.machines;
                alloc.horizon_it = insert_horizon_point(point);

                // Update data structures
                _available_machines -= alloc.machines;
                _nb_available_machines -= new_job->nb_requested_resources;
                _current_allocations[new_job_id] = alloc;
                _running_jobs.insert(new_job_id);
            }
        }
        if (executed==false)
        {
             
            if (_priority_job == nullptr)
            {
                //if this job is a 1 resource job then the completion time really doesn't matter
                //as nothing can really be backfilled, so it doesn't matter if the completion time
                //is computed from heldback machines or normal machines.  we compute it here from normal machines
                //regardless of number of resources.
                    _priority_job = new_job;
                    _priority_job->completion_time = compute_priority_job_expected_earliest_starting_time();
                
            }
            // submitted job is a resubmitted one, put at front of pending jobs
            else if(new_job_id.find("#")!=std::string::npos)
                _pending_jobs.push_front(new_job);
                
            else
                _pending_jobs.push_back(new_job);
                
        }
    }//end released jobs loop
    LOG_F(INFO,"pending_jobs size %d",_pending_jobs.size());
}

        





std::list<easy_bf_fast2_holdback::FinishedHorizonPoint>::iterator easy_bf_fast2_holdback::insert_horizon_point(const easy_bf_fast2_holdback::FinishedHorizonPoint &point)
{
    // The data structure is sorted, we can therefore traverse it in order
    // until finding an insertion point.
    for (auto it = _horizons.begin(); it != _horizons.end(); ++it)
    {
        if (point.date < it->date)
        {
            // Insertion point is before the current iterator.
            return _horizons.insert(it, point);
        }
    }

    // Insertion point not found. Insertion at end.
    return _horizons.insert(_horizons.end(), point);
}

double easy_bf_fast2_holdback::compute_priority_job_expected_earliest_starting_time()
{
    int nb_available = _nb_available_machines;
    int required = _priority_job->nb_requested_resources;
    //LOG_F(INFO,"line 1294");
    //make a shallow copy of machines_by_int if share-packing
    std::map<int,machine *> machines_by_int_copy;
    if (_share_packing)
    {
        for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
            {
                machine* current_machine = machines_by_int[*it];
                machine* a_machine = new machine();
                //all we need to copy are cores_available and core_count
                a_machine->cores_available = current_machine->cores_available;
                a_machine->core_count = current_machine->core_count;
                machines_by_int_copy[*it]=a_machine;
            }

    }   
         //LOG_F(INFO,"line 1310");   

    for (auto it = _horizons.begin(); it != _horizons.end(); ++it)
    {
        //is this the case that a single core is being released?
        if (_share_packing && it->nb_released_machines == 1)
        {
            //LOG_F(INFO,"line 1324");
            //ok a single core is released
            //is that all we needed for the priority_job (unlikely for a priority job but not out of the question)
            if (required == 1)
                return it->date;
            //ok that isn't all we needed, let's keep track of these released cores on each machine
            int machine_number = it->machines[0];
            machine * current_machine = machines_by_int_copy[machine_number];
            current_machine->cores_available +=1;
            //ok so we added a core to the released machine
            //does this bring a whole machine available?
            //LOG_F(INFO,"line 1335");
            if (current_machine->cores_available == int(current_machine->core_count * _core_percent))
                {
                    //yes it did make a whole machine available
                    nb_available += 1;
                }
        }
        //we are not doing share-packing or more than a single core was released
        else 
            nb_available += it->nb_released_machines;
        //do we now have enough full machines to run the priority job?
        if (nb_available >= required)
        {
            //yes we do, return the time that this will occur
            return it->date;
        }
    }

    PPK_ASSERT_ERROR(false, "The job will never be executable.");
    return 0;
}
/*(Document, Swap) {
    Document d1;
    Document::AllocatorType& a = d1.GetAllocator();

    d1.SetArray().PushBack(1, a).PushBack(2, a);

    Value o;
    o.SetObject().AddMember("a", 1, a);

    // Swap between Document and Value
    d1.Swap(o);
*/
