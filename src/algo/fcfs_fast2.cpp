#include <math.h>
#include "fcfs_fast2.hpp"

#include "../pempek_assert.hpp"

#include <loguru.hpp>

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <chrono>
#include <utility>
#include "../batsched_tools.hpp"


#define B_LOG_INSTANCE _myBLOG
namespace myB = myBatsched;
namespace r = rapidjson;
const int DEBUG = 10;
FCFSFast2::FCFSFast2(Workload *workload,
    SchedulingDecision *decision, Queue *queue, ResourceSelector *selector,
    double rjms_delay, rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay,
        variant_options)
{
    //LOG_F(INFO,"created 4");
    //_myWorkloads = new myBatsched::Workloads;
    //batsim log object.  declared in batsched_tools.hpp
    _myBLOG = new b_log();
    
    
}

FCFSFast2::~FCFSFast2()
{
    
}
void FCFSFast2::on_start_from_checkpoint(double date,const rapidjson::Value & batsim_config){

}
void FCFSFast2::on_simulation_start(double date,
    const rapidjson::Value &batsim_event_data)
{
    
    LOG_F(INFO,"On simulation start");
    pid_t pid = batsched_tools::get_batsched_pid();
    _decision->add_generic_notification("PID",std::to_string(pid),date);
    bool seedFailures = false;
    bool logBLog = false;
    const rapidjson::Value & batsim_config = batsim_event_data["config"];
    if (batsim_config.HasMember("share-packing"))
        _share_packing = batsim_config["share-packing"].GetBool();

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
    LOG_F(INFO,"_available_machines %d",_available_machines.size());
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
     _oldDate=date;
     
}      
void FCFSFast2::on_simulation_end(double date){
    (void) date;
}
    
 /*void FCFSFast2::on_machine_unavailable_notify_event(double date, IntervalSet machines){
    //LOG_F(INFO,"unavailable %s",machines.to_string_hyphen().c_str());
    _unavailable_machines+=machines;
    _available_machines-=machines;
    for(auto key_value : _current_allocations)
    {
            if (!((key_value.second & machines).is_empty()))
                _decision->add_kill_job({key_value.first},date);
    }
    
}
*/
/*void FCFSFast2::set_workloads(myBatsched::Workloads *w){
    _myWorkloads = w;
    _checkpointing_on = w->_checkpointing_on;
    
}
*/
void FCFSFast2::on_machine_available_notify_event(double date, IntervalSet machines){
    ISchedulingAlgorithm::on_machine_available_notify_event(date, machines);
    _unavailable_machines-=machines;
    _available_machines+=machines;
    _nb_available_machines+=machines.size();
    
}

void FCFSFast2::on_machine_state_changed(double date, IntervalSet machines, int new_state)
{
   

}
void FCFSFast2::on_myKillJob_notify_event(double date){
    
    if (!_running_jobs.empty()){
        batsched_tools::Job_Message * msg = new batsched_tools::Job_Message;
        msg->id = *_running_jobs.begin();
        msg->forWhat = batsched_tools::KILL_TYPES::NONE;
        _my_kill_jobs.insert(std::make_pair((*_workload)[*_running_jobs.begin()], msg));
    }
        
    
}



void FCFSFast2::on_machine_down_for_repair(double date){
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
            //ok there are jobs to kill
            for(auto key_value : _current_allocations)
            {
                if (!((key_value.second & machine).is_empty())){
                    Job * job_ref = (*_workload)[key_value.first];
                    batsched_tools::Job_Message * msg = new batsched_tools::Job_Message;
                    msg->id = key_value.first;
                    msg->forWhat = batsched_tools::KILL_TYPES::NONE;
                    _my_kill_jobs.insert(std::make_pair(job_ref,msg));
                    LOG_F(INFO,"Killing Job: %s",key_value.first.c_str());
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


void FCFSFast2::on_machine_instant_down_up(double date){
    //get a random number of a machine to kill
    int number = machine_unif_distribution->operator()(generator_machine);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = number;
    BLOG_F(b_log::FAILURES,"Machine Instant Down Up: %d",number);
    //if there are no running jobs, then there are none to kill
    if (!_running_jobs.empty()){
        for(auto key_value : _current_allocations)   
	{
		if (!((key_value.second & machine).is_empty())){
                    Job * job_ref = (*_workload)[key_value.first];
                    batsched_tools::Job_Message* msg = new batsched_tools::Job_Message;
                    msg->id = key_value.first;
                    msg->forWhat = batsched_tools::KILL_TYPES::NONE;
                    _my_kill_jobs.insert(std::make_pair(job_ref,msg));
	                BLOG_F(b_log::FAILURES,"Killing Job: %s",key_value.first.c_str());
            	}
	}
    }
}
/*void FCFSFast2::on_job_fault_notify_event(double date, std::string job){
    std::unordered_set<std::string>::const_iterator found = _running_jobs.find(job);
  //LOG_F(INFO,"on_job_fault_notify_event called");
  if ( found != _running_jobs.end() )    
        _decision->add_kill_job({job},date);
  else
      LOG_F(INFO,"Job %s was not running but was supposed to be killed due to job_fault event",job.c_str());
}
*/

void FCFSFast2::on_requested_call(double date,int id,batsched_tools::call_me_later_types forWhat)
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
void FCFSFast2::on_no_more_static_job_to_submit_received(double date){
    ISchedulingAlgorithm::on_no_more_static_job_to_submit_received(date);

}
void FCFSFast2::on_no_more_external_event_to_occur(double date){
    
    _wrap_it_up = true;    
    
    
}
void FCFSFast2::on_job_end(double date, std::vector<std::string> job_ids)
{
    (void) date;
    (void) job_ids;
}

void FCFSFast2::make_decisions(double date,
    SortableJobOrder::UpdateInformation *update_info,
    SortableJobOrder::CompareInformation *compare_info)
{
   LOG_F(INFO,"Line 322   fcfs_fast2.cpp");
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

    

//LOG_F(INFO,"Line 340  fcfs_fast2.cpp");
    //*****************************************************************
    // Handle newly finished jobs
    //*****************************************************************
    bool job_ended = false;
    for (const std::string & ended_job_id : _jobs_ended_recently)
    {
        job_ended = true;
        Job * finished_job = (*_workload)[ended_job_id];
        if (_share_packing && finished_job->nb_requested_resources == 1)
        {
                //first get the machine it was running on
                int machine_number = (_current_allocations[ended_job_id])[0];
                machine* current_machine = machines_by_int[machine_number];

                //now increase cores_available on that machine
                current_machine->cores_available += 1;
                //if that increase means no jobs are running on that machine (all its cores are available) then put it back in the mix
                if (current_machine->cores_available == int(current_machine->core_count * _core_percent))
                {
                    _available_core_machines -= _current_allocations[ended_job_id];  // we subtract a core machine because it is now a regular machine
                    _available_machines.insert(_current_allocations[ended_job_id]); // we insert the machine into available machines
                    _nb_available_machines += 1; // we increase available machines by 1
                }
                _current_allocations.erase(ended_job_id);
                _running_jobs.erase(ended_job_id);
        }
            // was not a 1 resource job, do things normally
        else{
                IntervalSet machines_to_add = _current_allocations[ended_job_id];
                machines_to_add-=_unavailable_machines;
                _available_machines.insert(machines_to_add);

                _available_machines-=_unavailable_machines;
                _nb_available_machines += machines_to_add.size();
                _current_allocations.erase(ended_job_id);
                _running_jobs.erase(ended_job_id);
                _my_kill_jobs.erase((*_workload)[ended_job_id]);
        }
    }
    

    
    //LOG_F(INFO,"Line 379  fcfs_fast2.cpp");
    //Handle new jobs to kill
   
    if(!_my_kill_jobs.empty()){
         std::vector<batsched_tools::Job_Message *> kills;
        for( auto job_msg_pair:_my_kill_jobs)
        {
            LOG_F(INFO,"adding kill job %s",job_msg_pair.first->id.c_str());
            kills.push_back(job_msg_pair.second);
        }
        _decision->add_kill_job(kills,date);
        _my_kill_jobs.clear();
    }
    //************************************************************resubmission if killed
    //Handle jobs to queue back up (if killed)  
    _decision->handle_resubmission(_jobs_killed_recently,_workload,date);    
    //*************************************************************
    
    
    
    
    //LOG_F(INFO,"Line 397  fcfs_fast2.cpp");
    
    if (!(_machines_that_became_available_recently.is_empty()) && !(_pending_jobs.empty()))
    {
        std::list<Job *>::iterator job_it =_pending_jobs.begin();
        bool erased = false;
        while(job_it!=_pending_jobs.end())
        {
            Job * pending_job = *job_it;
            std::string pending_job_id = pending_job->id;
            if (_share_packing && pending_job->nb_requested_resources==1)
            {
                 bool found = false;
                //it is a 1 resource job, iterate over the available core machines until it finds one to put the job on.
                for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
                {
                    //is this machine able to handle another job?
                    machine* current_machine = machines_by_int[*it];
                    if (current_machine->cores_available >= 1)
                    {                                
                            //it is able to handle another job, execute a job on it and subtract from cores_available
                            IntervalSet machines = *it;
                            
                            _decision->add_execute_job(pending_job_id,machines,date,mapping);
                            //update data structures
                            current_machine->cores_available -=1;
                            _current_allocations[pending_job_id] = machines;
                            _running_jobs.insert(pending_job_id);
                            job_it = _pending_jobs.erase(job_it);
                            erased = true;
                            found = true;
                    }
                    if (found == true)
                        break; 
                }  
                // there were no available core machines to put it on, try to put on a new core machine
                if (found == false && _nb_available_machines > 0)
                {
                    
                    //first get a machine
                    LOG_F(INFO,"here");
                    IntervalSet machines = _available_machines.left(1);
                    
                    _decision->add_execute_job(pending_job_id,machines,date,mapping);

                    //update data structures
                    machine* current_machine = machines_by_int[machines[0]];
                    current_machine->cores_available -= 1;
                    _available_core_machines += machines;
                    _available_machines -= machines;
                    _nb_available_machines -= 1;
                    _current_allocations[pending_job_id] = machines;
                    _running_jobs.insert(pending_job_id);
                    job_it = _pending_jobs.erase(job_it);
                    erased = true;

                } 
            }
            else if (pending_job->nb_requested_resources <= _nb_available_machines)
            {
                LOG_F(INFO,"here");
                IntervalSet machines = _available_machines.left(
                    pending_job->nb_requested_resources);
                _decision->add_execute_job(pending_job->id,
                    machines, date);
                

                // Update data structures
                _available_machines -= machines;
                _nb_available_machines -= pending_job->nb_requested_resources;
                 _current_allocations[pending_job_id] = machines;
                job_it = _pending_jobs.erase(job_it);
                erased = true;
                _running_jobs.insert(pending_job->id);
                
            }
            else
            {
                // The job becomes priority!
                // As there is no backfilling, we can simply leave this loop.
                break;
            }
            if (!erased)
                job_it++;
            else
                erased = false;
        }
    }
    
//LOG_F(INFO,"Line 476  fcfs_fast2.cpp");
    // If jobs have finished, execute jobs as long as they fit
    std::list<Job *>::iterator job_it =_pending_jobs.begin();
    if (job_ended)
    {
        bool erased = false;
        while(job_it!=_pending_jobs.end())
            
        {
            //LOG_F(INFO,"Line 483  fcfs_fast2.cpp");
            Job * pending_job = *job_it;
            //LOG_F(INFO,"Line 485  fcfs_fast2.cpp");
            //LOG_F(INFO,"Line 486 pending job %p",static_cast<void *>(pending_job));
            //LOG_F(INFO,"Line 487 pending job %s",(*job_it)->id.c_str());
            //LOG_F(INFO,"Line 488 pending job %d ",pending_job->nb_requested_resources);
            std::string pending_job_id = pending_job->id;
            //LOG_F(INFO,"Line 489  fcfs_fast2.cpp");
            if (_share_packing && pending_job->nb_requested_resources==1)
            {
                //LOG_F(INFO,"Line 492 fcfs_fast2.cpp");
                 bool found = false;
                //it is a 1 resource job, iterate over the available core machines until it finds one to put the job on.
                for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
                {
                    //is this machine able to handle another job?
                    machine* current_machine = machines_by_int[*it];
                    if (current_machine->cores_available >= 1)
                    {                                
                            //it is able to handle another job, execute a job on it and subtract from cores_available
                            IntervalSet machines = *it;
                            
                            _decision->add_execute_job(pending_job_id,machines,date,mapping);
                            //update data structures
                            current_machine->cores_available -=1;
                            _current_allocations[pending_job_id] = machines;
                            _running_jobs.insert(pending_job_id);
                            job_it = _pending_jobs.erase(job_it);
                            erased = true;
                            found = true;
                            //LOG_F(INFO,"Line 511  fcfs_fast2.cpp");
                    }
                    if (found == true)
                        break; 
                }  
                // there were no available core machines to put it on, try to put on a new core machine
                if (found == false && _nb_available_machines > 0)
                {
                    LOG_F(INFO,"Line 519  fcfs_fast2.cpp");
                    //first get a machine
                    
                    IntervalSet machines = _available_machines.left(1);
                    //LOG_F(INFO,"Line 522  fcfs_fast2.cpp");
                    _decision->add_execute_job(pending_job_id,machines,date,mapping);

                    //update data structures
                    machine* current_machine = machines_by_int[machines[0]];
                    current_machine->cores_available -= 1;
                    _available_core_machines += machines;
                    _available_machines -= machines;
                    _nb_available_machines -= 1;
                    //LOG_F(INFO,"Line 531  fcfs_fast2.cpp");
                    _current_allocations[pending_job_id] = machines;
                    //LOG_F(INFO,"Line 533  fcfs_fast2.cpp");
                    _running_jobs.insert(pending_job_id);
                    //LOG_F(INFO,"Line 535  fcfs_fast2.cpp");
                    //LOG_F(INFO,"Line 536  fcfs_fast2.cpp pending_job: %p",static_cast<void *>(*job_it));
                    //LOG_F(INFO,"Line   fcfs_fast2.cpp pending_job_id: %s",pending_job->id.c_str());
                    job_it = _pending_jobs.erase(job_it);
                    erased = true;
                    //LOG_F(INFO,"Line 537  fcfs_fast2.cpp");

                } 
            }
            else if (pending_job->nb_requested_resources <= _nb_available_machines)
            {
                LOG_F(INFO,"Line 543  fcfs_fast2.cpp"); 
                IntervalSet machines = _available_machines.left(
                    pending_job->nb_requested_resources);
                _decision->add_execute_job(pending_job->id,
                    machines, date);
                

                // Update data structures
                _available_machines -= machines;
                _nb_available_machines -= pending_job->nb_requested_resources;
                 _current_allocations[pending_job_id] = machines;
                job_it = _pending_jobs.erase(job_it);
                erased = true;
                _running_jobs.insert(pending_job->id);
                //LOG_F(INFO,"Line 556  fcfs_fast2.cpp");
            }
            else
            {
                // The job becomes priority!
                // As there is no backfilling, we can simply leave this loop.
                break;
            }
            if (!erased)
                job_it++;
            else
                erased = false;
        }
        //LOG_F(INFO,"Line 566  fcfs_fast2.cpp");
    }
    //LOG_F(INFO,"Line 567  fcfs_fast2.cpp");
    // Handle newly released jobs
    for (const std::string & new_job_id : _jobs_released_recently)
    {
        Job * new_job = (*_workload)[new_job_id];

        // Is this job valid?
        if (new_job->nb_requested_resources > _nb_machines)
        {
            // Invalid!
            //LOG_F(INFO,"Job being rejected HERE %s",new_job_id.c_str());
            _decision->add_reject_job(new_job_id, date);
            continue;
        }

        // Is there a waiting job?
        if (!_pending_jobs.empty())
        {   
            // submitted job is a resubmitted one, put at front of pending jobs
           if (new_job_id.find("#")!=std::string::npos)
               _pending_jobs.push_front(new_job);
            else{
                // Yes. The new job is queued up.
                //LOG_F(INFO,"Line 590  fcfs_fast2.cpp  new_job: %p  new_job_id: %s",static_cast<void *>(new_job),new_job_id.c_str());
                _pending_jobs.push_back(new_job);
            }
        }
        else
        {
            // No, the queue is empty.
            // Can the new job be executed now?
            
            if (_share_packing && new_job->nb_requested_resources==1)
            {
                 bool found = false;
                //it is a 1 resource job, iterate over the available core machines until it finds one to put the job on.
                for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
                {
                    //is this machine able to handle another job?
                    machine* current_machine = machines_by_int[*it];
                    if (current_machine->cores_available >= 1)
                    {                                
                            //it is able to handle another job, execute a job on it and subtract from cores_available
                            IntervalSet machines = *it;
                            
                            _decision->add_execute_job(new_job_id,machines,date,mapping);
                            //update data structures
                            current_machine->cores_available -=1;
                            _current_allocations[new_job_id] = machines;
                            _running_jobs.insert(new_job_id);
                            found = true;
                    }
                    if (found == true)
                        break; 
                }  
                // there were no available core machines to put it on, try to put on a new core machine
                if (found == false && _nb_available_machines > 0)
                {
                    
                    //first get a machine
                    LOG_F(INFO,"here");
                    IntervalSet machines = _available_machines.left(1);
                    
                    _decision->add_execute_job(new_job_id,machines,date,mapping);

                    //update data structures
                    machine* current_machine = machines_by_int[machines[0]];
                    current_machine->cores_available -= 1;
                    _available_core_machines += machines;
                    _available_machines -= machines;
                    _nb_available_machines -= 1;
                    _current_allocations[new_job_id] = machines;
                    _running_jobs.insert(new_job_id);

                } 
                else if (found == false){ // there was no machine available...queue it up
                    _pending_jobs.push_back(new_job);
                }

            } // do things normally, we don't have share-packing or job isn't 1 resource
            else if (new_job->nb_requested_resources <= _nb_available_machines)
            {
                LOG_F(INFO,"here");
                IntervalSet machines = _available_machines.left(
                    new_job->nb_requested_resources);
                _decision->add_execute_job(new_job->id,
                        machines, date);
                

                // Update data structures
                _available_machines -= machines;
                _nb_available_machines -= new_job->nb_requested_resources;
                 _current_allocations[new_job_id] = machines;
                _running_jobs.insert(new_job->id);
            }
            
            else
            {
                // No. The job is queued up.
                //LOG_F(INFO,"Line 656   fcfs_fast2.cpp  new job:%p  new job id: %s",static_cast<void *>(new_job),new_job_id.c_str());
                _pending_jobs.push_back(new_job);
            }
        }
    }
    //LOG_F(INFO,"Line 650  fcfs_fast2.cpp");
    /*if (_jobs_killed_recently.empty() && _wrap_it_up && _need_to_send_finished_submitting_jobs && !_myWorkloads->_checkpointing_on)
    {
        
        _decision->add_scheduler_finished_submitting_jobs(date);
        _need_to_send_finished_submitting_jobs = false;
    }
    */
    if (_jobs_killed_recently.empty() && _pending_jobs.empty() && _running_jobs.empty() &&
             _need_to_send_finished_submitting_jobs && _no_more_static_job_to_submit_received && !date<1.0 )
    {
        _decision->add_scheduler_finished_submitting_jobs(date);
        _need_to_send_finished_submitting_jobs = false;
    }
      
}
std::string FCFSFast2::to_json_desc(rapidjson::Document * doc)
{
  rapidjson::StringBuffer buffer;

  buffer.Clear();

  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc->Accept(writer);

  return std::string( buffer.GetString() );
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
