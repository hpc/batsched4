#include <math.h>
#include "fcfs_fast2.hpp"

#include "../pempek_assert.hpp"

#include <loguru.hpp>
#include "../external/batsched_workload.hpp"
#include "../external/batsched_job.hpp"
#include "../external/batsched_profile.hpp"
#include "../external/pointers.hpp"
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <chrono>
#include "../batsched_tools.hpp"

#define B_LOG_INSTANCE _myBLOG
namespace myB = myBatsched;
namespace r = rapidjson;

FCFSFast2::FCFSFast2(Workload *workload,
    SchedulingDecision *decision, Queue *queue, ResourceSelector *selector,
    double rjms_delay, rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay,
        variant_options)
{
    LOG_F(INFO,"created 4");
    _myWorkloads = new myBatsched::Workloads;
    //batsim log object.  declared in batsched_tools.hpp
    _myBLOG = new b_log();
    
    
}

FCFSFast2::~FCFSFast2()
{
    
}

void FCFSFast2::on_simulation_start(double date,
    const rapidjson::Value &batsim_event_data)
{
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
    unsigned seed = 0;
    if (seedFailures)
        seed = std::chrono::system_clock::now().time_since_epoch().count();
         
    generator.seed(seed);
    generator2.seed(seed);
    _available_machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    _nb_available_machines = _nb_machines;
    LOG_F(INFO,"avail: %d   nb_machines: %d",_available_machines.size(),_nb_machines);
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
        LOG_F(INFO,"machine id = %d, core_count= %d , cores_available= %d",a_machine->id,a_machine->core_count,a_machine->cores_available);
   }
     _oldDate=date;
     if (_myWorkloads->_fixed_failures != -1.0)
     {
        if (unif_distribution == nullptr)
            unif_distribution = new std::uniform_int_distribution<int>(0,_nb_machines-1);
        double number = _myWorkloads->_fixed_failures;
        _decision->add_call_me_later(batsched_tools::FIXED_FAILURE,1,number+date,date);  
     }
    if (_myWorkloads->_SMTBF != -1.0)
    {
        distribution = new std::exponential_distribution<double>(1.0/_myWorkloads->_SMTBF);
        if (unif_distribution == nullptr)
            unif_distribution = new std::uniform_int_distribution<int>(0,_nb_machines-1);
        std::exponential_distribution<double>::param_type new_lambda(1.0/_myWorkloads->_SMTBF);
        distribution->param(new_lambda);
        double number;         
        number = distribution->operator()(generator);
        _decision->add_call_me_later(batsched_tools::SMTBF,1,number+date,date);
    }
    else if (_myWorkloads->_MTBF!=-1.0)
    {
        distribution = new std::exponential_distribution<double>(1.0/_myWorkloads->_MTBF);
        std::exponential_distribution<double>::param_type new_lambda(1.0/_myWorkloads->_MTBF);
        distribution->param(new_lambda);
        double number;         
        number = distribution->operator()(generator);
        _decision->add_call_me_later(batsched_tools::MTBF,1,number+date,date);
    }
}      
void FCFSFast2::on_simulation_end(double date){
    (void) date;
}
    
 void FCFSFast2::on_machine_unavailable_notify_event(double date, IntervalSet machines){
    LOG_F(INFO,"unavailable %s",machines.to_string_hyphen().c_str());
    _unavailable_machines+=machines;
    _available_machines-=machines;
    for(auto key_value : _current_allocations)
    {
            if (!((key_value.second & machines).is_empty()))
                _decision->add_kill_job({key_value.first},date);
    }
    
}
void FCFSFast2::set_workloads(myBatsched::Workloads *w){
    _myWorkloads = w;
    _checkpointing_on = w->_checkpointing_on;
    
}
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
        _my_kill_jobs.push_back((*_workload)[*_running_jobs.begin()]);
    }
        
    
}



void FCFSFast2::on_machine_down_for_repair(double date){
    //get a random number of a machine to kill
    int number = unif_distribution->operator()(generator2);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = number;
    //if the machine is already down for repairs ignore it.
    LOG_F(INFO,"repair_machines.size(): %d    nb_avail: %d  avail:%d running_jobs: %d",_repair_machines.size(),_nb_available_machines,_available_machines.size(),_running_jobs.size());
    BLOG_F(b_log::FAILURES,"Machine Repair: %d",number);
    if ((machine & _repair_machines).is_empty())
    {
        //ok the machine is not down for repairs
        //it will be going down for repairs now
        _available_machines-=machine;
        _unavailable_machines+=machine;
        _repair_machines+=machine;
        _nb_available_machines=_available_machines.size();

        double repair_time = _myWorkloads->_repair_time;
        LOG_F(INFO,"in repair_machines.size(): %d nb_avail: %d  avail: %d running_jobs: %d",_repair_machines.size(),_nb_available_machines,_available_machines.size(),_running_jobs.size());
        LOG_F(INFO,"date: %f , repair: %f ,repair + date: %f",date,repair_time,date+repair_time);
        //call me back when the repair is done
        _decision->add_call_me_later(batsched_tools::REPAIR_DONE,number,date+repair_time,date);
        //now kill the jobs that are running on machines that need to be repaired.        
        //if there are no running jobs, then there are none to kill
        if (!_running_jobs.empty()){
            for(auto key_value : _current_allocations)
            {
                if (!((key_value.second & machine).is_empty())){
                    _my_kill_jobs.push_back((*_workload)[key_value.first]);
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
    int number = unif_distribution->operator()(generator2);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = number;
    BLOG_F(b_log::FAILURES,"Machine Instant Down Up: %d",number);
    //if there are no running jobs, then there are none to kill
    if (!_running_jobs.empty()){
        for(auto key_value : _current_allocations)   
	{
		if (!((key_value.second & machine).is_empty())){
                	_my_kill_jobs.push_back((*_workload)[key_value.first]);
	                BLOG_F(b_log::FAILURES,"Killing Job: %s",key_value.first.c_str());
            	}
	}
    }
}
void FCFSFast2::on_job_fault_notify_event(double date, std::string job){
    std::unordered_set<std::string>::const_iterator found = _running_jobs.find(job);
  LOG_F(INFO,"on_job_fault_notify_event called");
  if ( found != _running_jobs.end() )    
        _decision->add_kill_job({job},date);
  else
      LOG_F(INFO,"Job %s was not running but was supposed to be killed due to job_fault event",job.c_str());
}

void FCFSFast2::on_requested_call(double date,int id,batsched_tools::call_me_later_types forWhat)
{
    
        switch (forWhat){
            case batsched_tools::SMTBF:
                        {
                            //Log the failure
                            BLOG_F(b_log::FAILURES,"FAILURE SMTBF");
                            if (!_running_jobs.empty() || !_pending_jobs.empty() || !_no_more_static_job_to_submit_received)
                                {
                                    double number = distribution->operator()(generator);
                                    if (_myWorkloads->_repair_time == 0.0)
                                        on_machine_instant_down_up(date);
                                    else
                                        on_machine_down_for_repair(date);
                                    _decision->add_call_me_later(batsched_tools::SMTBF,1,number+date,date);
                                }
                        }
                        break;
            case batsched_tools::MTBF:
                        {
                            if (!_running_jobs.empty() || !_pending_jobs.empty() || !_no_more_static_job_to_submit_received)
                            {
                                double number = distribution->operator()(generator);
                                on_myKillJob_notify_event(date);
                                _decision->add_call_me_later(batsched_tools::MTBF,1,number+date,date);

                            }
                        
                            
                        }
                        break;
            case batsched_tools::FIXED_FAILURE:
                        {
                            BLOG_F(b_log::FAILURES,"FAILURE FIXED_FAILURE");
                            if (!_running_jobs.empty() || !_pending_jobs.empty() || !_no_more_static_job_to_submit_received)
                                {
                                    double number = _myWorkloads->_fixed_failures;
                                    if (_myWorkloads->_repair_time == 0.0)
                                        on_machine_instant_down_up(date);
                                    else
                                        on_machine_down_for_repair(date);
                                    _decision->add_call_me_later(batsched_tools::FIXED_FAILURE,1,number+date,date);
                                }
                        }
                        break;
            case batsched_tools::REPAIR_DONE:
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
                           LOG_F(INFO,"in repair_machines.size(): %d nb_avail: %d avail: %d  running_jobs: %d",_repair_machines.size(),_nb_available_machines,_available_machines.size(),_running_jobs.size());
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
                    _available_core_machines -= _current_allocations[ended_job_id];
                    _available_machines.insert(_current_allocations[ended_job_id]);
                    _nb_available_machines += 1;
                }
                _current_allocations.erase(ended_job_id);
                _running_jobs.erase(ended_job_id);
        }
            // was not a 1 resource job, do things normally
        else{
                _available_machines.insert(_current_allocations[ended_job_id]);
                _nb_available_machines += finished_job->nb_requested_resources;
                _current_allocations.erase(ended_job_id);
                _running_jobs.erase(ended_job_id);
                _my_kill_jobs.remove((*_workload)[ended_job_id]);
        }
    }
    

    
    
    //Handle new jobs to kill
   
    if(!_my_kill_jobs.empty()){
         std::vector<std::string> kills;
        for( Job* kill:_my_kill_jobs)
            kills.push_back(kill->id);
        _decision->add_kill_job(kills,date);
        _my_kill_jobs.clear();
    }
    //************************************************************resubmission if killed
    //Handle jobs to queue back up (if killed)  
    handle_resubmission(date);    
    //*************************************************************
    
    
    
    
    
    
    if (!(_machines_that_became_available_recently.is_empty()) && !(_pending_jobs.empty()))
    {
        for (auto job_it = _pending_jobs.begin();
             job_it != _pending_jobs.end(); )
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
                            
                            _decision->add_execute_job(PARALLEL,pending_job_id,machines,date,mapping);
                            //update data structures
                            current_machine->cores_available -=1;
                            _current_allocations[pending_job_id] = machines;
                            _running_jobs.insert(pending_job_id);
                            _pending_jobs.erase(job_it);
                            found = true;
                    }
                    if (found == true)
                        break; 
                }  
                // there were no available core machines to put it on, try to put on a new core machine
                if (found == false && _nb_available_machines > 0)
                {
                    
                    //first get a machine
                    IntervalSet machines = _available_machines.left(1);
                    
                    _decision->add_execute_job(PARALLEL,pending_job_id,machines,date,mapping);

                    //update data structures
                    machine* current_machine = machines_by_int[machines[0]];
                    current_machine->cores_available -= 1;
                    _available_core_machines += machines;
                    _available_machines -= machines;
                    _nb_available_machines -= 1;
                    _current_allocations[pending_job_id] = machines;
                    _running_jobs.insert(pending_job_id);
                    _pending_jobs.erase(job_it);

                } 
            }
            else if (pending_job->nb_requested_resources <= _nb_available_machines)
            {
                IntervalSet machines = _available_machines.left(
                    pending_job->nb_requested_resources);
                _decision->add_execute_job(PARALLEL,pending_job->id,
                    machines, date);
                

                // Update data structures
                _available_machines -= machines;
                _nb_available_machines -= pending_job->nb_requested_resources;
                 _current_allocations[pending_job_id] = machines;
                job_it = _pending_jobs.erase(job_it);
                _running_jobs.insert(pending_job->id);
                
            }
            else
            {
                // The job becomes priority!
                // As there is no backfilling, we can simply leave this loop.
                break;
            }
        }
    }
    

    // If jobs have finished, execute jobs as long as they fit
    if (job_ended)
    {
        for (auto job_it = _pending_jobs.begin();
             job_it != _pending_jobs.end(); )
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
                            
                            _decision->add_execute_job(PARALLEL,pending_job_id,machines,date,mapping);
                            //update data structures
                            current_machine->cores_available -=1;
                            _current_allocations[pending_job_id] = machines;
                            _running_jobs.insert(pending_job_id);
                            _pending_jobs.erase(job_it);
                            found = true;
                    }
                    if (found == true)
                        break; 
                }  
                // there were no available core machines to put it on, try to put on a new core machine
                if (found == false && _nb_available_machines > 0)
                {
                    
                    //first get a machine
                    IntervalSet machines = _available_machines.left(1);
                    
                    _decision->add_execute_job(PARALLEL,pending_job_id,machines,date,mapping);

                    //update data structures
                    machine* current_machine = machines_by_int[machines[0]];
                    current_machine->cores_available -= 1;
                    _available_core_machines += machines;
                    _available_machines -= machines;
                    _nb_available_machines -= 1;
                    _current_allocations[pending_job_id] = machines;
                    _running_jobs.insert(pending_job_id);
                    _pending_jobs.erase(job_it);

                } 
            }
            else if (pending_job->nb_requested_resources <= _nb_available_machines)
            {
                IntervalSet machines = _available_machines.left(
                    pending_job->nb_requested_resources);
                _decision->add_execute_job(PARALLEL,pending_job->id,
                    machines, date);
                

                // Update data structures
                _available_machines -= machines;
                _nb_available_machines -= pending_job->nb_requested_resources;
                 _current_allocations[pending_job_id] = machines;
                job_it = _pending_jobs.erase(job_it);
                _running_jobs.insert(pending_job->id);
                
            }
            else
            {
                // The job becomes priority!
                // As there is no backfilling, we can simply leave this loop.
                break;
            }
        }
    }

    // Handle newly released jobs
    for (const std::string & new_job_id : _jobs_released_recently)
    {
        Job * new_job = (*_workload)[new_job_id];

        // Is this job valid?
        if (new_job->nb_requested_resources > _nb_machines)
        {
            // Invalid!
            LOG_F(INFO,"Job being rejected HERE %s",new_job_id.c_str());
            _decision->add_reject_job(new_job_id, date);
            continue;
        }

        // Is there a waiting job?
        if (!_pending_jobs.empty())
        {   
            // submitted job is a resubmitted one, put at front of pending jobs
           if (new_job_id.find("#")!=std::string::npos)
               _pending_jobs.push_front(new_job);
            else
            // Yes. The new job is queued up.
            
            _pending_jobs.push_back(new_job);
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
                            
                            _decision->add_execute_job(PARALLEL,new_job_id,machines,date,mapping);
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
                    IntervalSet machines = _available_machines.left(1);
                    
                    _decision->add_execute_job(PARALLEL,new_job_id,machines,date,mapping);

                    //update data structures
                    machine* current_machine = machines_by_int[machines[0]];
                    current_machine->cores_available -= 1;
                    _available_core_machines += machines;
                    _available_machines -= machines;
                    _nb_available_machines -= 1;
                    _current_allocations[new_job_id] = machines;
                    _running_jobs.insert(new_job_id);

                } 
            }
            else if (new_job->nb_requested_resources <= _nb_available_machines)
            {
                IntervalSet machines = _available_machines.left(
                    new_job->nb_requested_resources);
                _decision->add_execute_job(PARALLEL,new_job->id,
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
                _pending_jobs.push_back(new_job);
            }
        }
    }
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
void FCFSFast2::handle_resubmission(double date)
{
 for(const auto & killed_map:_jobs_killed_recently)
    {
        std::string killed_job=killed_map.first;
        double progress = killed_map.second;
        LOG_F(INFO,"REPAIR  progress: %f",progress);
        auto start = killed_job.find("!")+1;
        auto end = killed_job.find("#");
        std::string basename = (end ==std::string::npos) ? killed_job.substr(start) : killed_job.substr(start,end-start); 
        
        const std::string workload_str = killed_job.substr(0,start-1); 
        //get the workload
        myB::Workload * w0= (*_myWorkloads)[workload_str];
        //get the conversion from seconds to cpu instructions
        double one_second = w0->_host_speed;
        //get the job that was killed
        myB::JobPtr job_to_queue =(*(w0->jobs))[myB::JobIdentifier(killed_job)];
        //get the job identifier of the job that was killed
        myB::JobIdentifier ji = job_to_queue->id;
        
        std::string profile_jd=job_to_queue->profile->json_description;
        std::string job_jd=job_to_queue->json_description;
        r::Document profile_doc;
        profile_doc.Parse(profile_jd.c_str());
        r::Document doc;
        doc.Parse(job_jd.c_str());
        
        if (job_to_queue->profile->type == myB::ProfileType::DELAY )
        {
            if (_checkpointing_on)
            {
                double progress_time = 0;
                if (progress > 0)
                {
                    
                    
                    progress_time =progress * profile_doc["delay"].GetDouble();
                    LOG_F(INFO,"REPAIR progress is > 0  progress: %f  progress_time: %f",progress,progress_time);
                    
                    bool has_checkpointed = false;
                    std::string meta_str = "null";
                    int num_checkpoints_completed = 0;
                    r::Document meta_doc;
                    //check whether there is a checkpointed value and set has_checkpointed if so
                    if (doc.HasMember("metadata"))
                    {
                        
                        meta_str = doc["metadata"].GetString();
                        std::replace(meta_str.begin(),meta_str.end(),'\'','\"');
                        meta_doc.Parse(meta_str.c_str());
                        if (meta_doc.HasMember("checkpointed"))
                        {
                            has_checkpointed = meta_doc["checkpointed"].GetBool();
                            
                        }
                    }
                    //if has checkpointed we need to alter how we check num_checkpoints_completed and progress time
                    if (has_checkpointed)
                    {
                        
                        //progress_time must be subtracted by read_time to see how many checkpoints we have gone through
                        num_checkpoints_completed = floor((progress_time-job_to_queue->read_time)/(job_to_queue->checkpoint_interval + job_to_queue->dump_time));
                        if (meta_doc.HasMember("work_progress"))
                        {
                            double work = meta_doc["work_progress"].GetDouble();
                            if (num_checkpoints_completed > 0)
                                work += num_checkpoints_completed * job_to_queue->checkpoint_interval;
                            meta_doc["work_progress"] = work;
                        }
                        else if (num_checkpoints_completed > 0)
                        {
                            meta_doc.AddMember("work_progress",r::Value().SetDouble(num_checkpoints_completed * job_to_queue->checkpoint_interval),meta_doc.GetAllocator());
                        }
                        if (meta_doc.HasMember("num_dumps"))
                        {
                                int num_dumps = meta_doc["num_dumps"].GetInt();
                                if (num_checkpoints_completed > 0)
                                    num_dumps += num_checkpoints_completed;
                                meta_doc["num_dumps"] = num_dumps;
                                
                        }
                        else if (num_checkpoints_completed > 0)
                        {
                                meta_doc.AddMember("num_dumps",r::Value().SetInt(num_checkpoints_completed),meta_doc.GetAllocator());
                            
                        }
                        std::string meta_str = to_json_desc(&meta_doc);
                        doc["metadata"].SetString(meta_str.c_str(),doc.GetAllocator());
                        // the progress_time needs to add back in the read_time
                        progress_time = num_checkpoints_completed * (job_to_queue->checkpoint_interval + job_to_queue->dump_time) + job_to_queue->read_time;
                    
                    }
                    else // there hasn't been any checkpoints in the past, do normal check on num_checkpoints_completed
                    {
                        num_checkpoints_completed = floor(progress_time/(job_to_queue->checkpoint_interval + job_to_queue->dump_time ));
                        progress_time = num_checkpoints_completed * (job_to_queue->checkpoint_interval + job_to_queue->dump_time);
                        
                        
                        //if a checkpoint has completed set the metadata to reflect this
                        if (num_checkpoints_completed > 0)
                        {
                            meta_doc.SetObject();
                            //if there was previous metadata make sure to include it
                            if (meta_str!="null")
                            {
                                meta_doc.Parse(meta_str.c_str());
                            }    
                            r::Document::AllocatorType& myAlloc = meta_doc.GetAllocator();
                            meta_doc.AddMember("checkpointed",r::Value().SetBool(true),myAlloc);
                            meta_doc.AddMember("num_dumps",r::Value().SetInt(num_checkpoints_completed),meta_doc.GetAllocator());
                            meta_doc.AddMember("work_progress",r::Value().SetDouble(num_checkpoints_completed * job_to_queue->checkpoint_interval),meta_doc.GetAllocator());
                            std::string myString = to_json_desc(&meta_doc);
                            r::Document::AllocatorType& myAlloc2 = doc.GetAllocator();
                                                
                            if (meta_str=="null")
                                doc.AddMember("metadata",r::Value().SetString(myString.c_str(),myAlloc2),myAlloc2);
                            else
                                doc["metadata"].SetString(myString.c_str(),myAlloc2);
                        }
        
                    }        
                    //only if a new checkpoint has been reached does the delay time change
                    LOG_F(INFO,"REPAIR num_checkpoints_completed: %d",num_checkpoints_completed);
                    if (num_checkpoints_completed > 0)
                    {
                        
                        double delay = profile_doc["delay"].GetDouble() - progress_time + job_to_queue->read_time;
                        LOG_F(INFO,"REPAIR delay: %f  readtime: %f",delay,job_to_queue->read_time);
                        profile_doc["delay"].SetDouble(delay);
                        
                        
                    }
                }
            
            }
                                          
                    
        }
    
        if (job_to_queue->profile->type == myB::ProfileType::PARALLEL_HOMOGENEOUS)
        {
            if (_checkpointing_on)
                {
                    double progress_time = 0;
                    if (progress > 0)
                    {
                        
                        
                        progress_time =(progress * profile_doc["cpu"].GetDouble())/one_second;
                        LOG_F(INFO,"REPAIR progress is > 0  progress: %f  progress_time: %f",progress,progress_time);
                        LOG_F(INFO,"profile_doc[cpu]: %f    , one_second: %f",profile_doc["cpu"].GetDouble(),one_second);
                        
                        bool has_checkpointed = false;
                        std::string meta_str = "null";
                        int num_checkpoints_completed = 0;
                        r::Document meta_doc;
                        //check whether there is a checkpointed value and set has_checkpointed if so
                        if (doc.HasMember("metadata"))
                        {
                            
                            meta_str = doc["metadata"].GetString();
                            std::replace(meta_str.begin(),meta_str.end(),'\'','\"');
                            meta_doc.Parse(meta_str.c_str());
                            if (meta_doc.HasMember("checkpointed"))
                            {
                                has_checkpointed = meta_doc["checkpointed"].GetBool();
                                
                            }
                        }
                        //if has checkpointed we need to alter how we check num_checkpoints_completed and progress time
                        if (has_checkpointed)
                        {
                            
                            //progress_time must be subtracted by read_time to see how many checkpoints we have gone through
                            num_checkpoints_completed = floor((progress_time-job_to_queue->read_time)/(job_to_queue->checkpoint_interval + job_to_queue->dump_time));
                            if (meta_doc.HasMember("work_progress"))
                            {
                                double work = meta_doc["work_progress"].GetDouble();
                                if (num_checkpoints_completed > 0)
                                    work += num_checkpoints_completed * job_to_queue->checkpoint_interval;
                                work = work * one_second;
                                meta_doc["work_progress"] = work;
                            }
                            else if (num_checkpoints_completed > 0)
                            {
                                meta_doc.AddMember("work_progress",r::Value().SetDouble(num_checkpoints_completed * job_to_queue->checkpoint_interval*one_second),meta_doc.GetAllocator());
                            }
                            if (meta_doc.HasMember("num_dumps"))
                            {
                                    int num_dumps = meta_doc["num_dumps"].GetInt();
                                    if (num_checkpoints_completed > 0)
                                        num_dumps += num_checkpoints_completed;
                                    meta_doc["num_dumps"] = num_dumps;
                                    
                            }
                            else if (num_checkpoints_completed > 0)
                            {
                                    meta_doc.AddMember("num_dumps",r::Value().SetInt(num_checkpoints_completed),meta_doc.GetAllocator());
                                
                            }
                            std::string meta_str = to_json_desc(&meta_doc);
                            doc["metadata"].SetString(meta_str.c_str(),doc.GetAllocator());
                            // the progress_time needs to add back in the read_time
                            progress_time = num_checkpoints_completed * (job_to_queue->checkpoint_interval + job_to_queue->dump_time) + job_to_queue->read_time;
                        
                        }
                        else // there hasn't been any checkpoints in the past, do normal check on num_checkpoints_completed
                        {
                            num_checkpoints_completed = floor(progress_time/(job_to_queue->checkpoint_interval + job_to_queue->dump_time ));
                            progress_time = num_checkpoints_completed * (job_to_queue->checkpoint_interval + job_to_queue->dump_time);
                            
                            
                            //if a checkpoint has completed set the metadata to reflect this
                            if (num_checkpoints_completed > 0)
                            {
                                meta_doc.SetObject();
                                //if there was previous metadata make sure to include it
                                if (meta_str!="null")
                                {
                                    meta_doc.Parse(meta_str.c_str());
                                }    
                                r::Document::AllocatorType& myAlloc = meta_doc.GetAllocator();
                                meta_doc.AddMember("checkpointed",r::Value().SetBool(true),myAlloc);
                                meta_doc.AddMember("num_dumps",r::Value().SetInt(num_checkpoints_completed),meta_doc.GetAllocator());
                                meta_doc.AddMember("work_progress",r::Value().SetDouble(num_checkpoints_completed * job_to_queue->checkpoint_interval * one_second),meta_doc.GetAllocator());
                                std::string myString = to_json_desc(&meta_doc);
                                r::Document::AllocatorType& myAlloc2 = doc.GetAllocator();
                                                    
                                if (meta_str=="null")
                                    doc.AddMember("metadata",r::Value().SetString(myString.c_str(),myAlloc2),myAlloc2);
                                else
                                    doc["metadata"].SetString(myString.c_str(),myAlloc2);
                            }
            
                        }        
                        //only if a new checkpoint has been reached does the delay time change
                        LOG_F(INFO,"REPAIR num_checkpoints_completed: %d",num_checkpoints_completed);
                        if (num_checkpoints_completed > 0)
                        {
                            double cpu = profile_doc["cpu"].GetDouble();
                            double cpu_time = cpu / one_second;
                            cpu_time = cpu_time - progress_time + job_to_queue->read_time;
                            LOG_F(INFO,"REPAIR cpu_time: %f  readtime: %f",cpu_time,job_to_queue->read_time);
                            profile_doc["cpu"].SetDouble(cpu_time*one_second);
                            
                            
                        }
                    }
                
                }
        }


        doc["subtime"]=date;
                
        //check if resubmitted and get the next resubmission number
        int resubmit = 1;
        if (end!=std::string::npos) //if job name has # in it...was resubmitted b4
        {
            resubmit = std::stoi(killed_job.substr(end+1));   // then get the resubmitted number
            resubmit++; // and add 1 to it
        }
        std::string resubmit_str = std::to_string(resubmit);
        
        
        std::string profile_name = basename + "#" + resubmit_str;
        std::string job_name = basename + "#" + resubmit_str;
        std::string job_id = workload_str+"!" + basename + "#" + resubmit_str;
        std::string workload_name = workload_str;
        doc["profile"].SetString(profile_name.data(), profile_name.size(), doc.GetAllocator());
        doc["id"].SetString(job_id.data(),job_id.size(),doc.GetAllocator());
        std::string error_prefix = "Invalid JSON job '" + killed_job + "'";
        profile_jd = to_json_desc(&profile_doc);
        myB::ProfilePtr p = myB::Profile::from_json(profile_name,profile_jd);
        w0->profiles->add_profile(p);
        myB::JobPtr j = myB::Job::from_json(doc,w0,error_prefix);
        w0->jobs->add_job(j);
        job_jd = to_json_desc(&doc);
        LOG_F(INFO,"workload: %s  job: %s, profile: %s",workload_name.c_str(),job_name.c_str(),profile_name.c_str());
        _decision->add_submit_profile(workload_name,
                                    profile_name,
                                    profile_jd,
                                    date);
                                    
        _decision->add_submit_job(workload_name,
                                    job_name,
                                    profile_name,
                                    job_jd,
                                    profile_jd,
                                    date,
                                    true);
        if (doc.HasMember("metadata"))
        {
            std::string meta = doc["metadata"].GetString();
            //must replace double quotes with single quotes.  Remember to
            //replace single quotes with double quotes before parsing metadata
            std::replace( meta.begin(), meta.end(), '\"', '\'');
        _decision->add_set_job_metadata(job_id,
                                        meta,
                                        date);
        }                               
    }            
    
       
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
