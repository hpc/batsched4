#include <math.h>
#include "easy_bf_fast2.hpp"

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

easy_bf_fast2::easy_bf_fast2(Workload *workload,
    SchedulingDecision *decision, Queue *queue, ResourceSelector *selector,
    double rjms_delay, rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay,
        variant_options)
{
    
    _share_packing_algorithm = true;
    _horizon_algorithm = true;
    
    
}

easy_bf_fast2::~easy_bf_fast2()
{
    
}
void easy_bf_fast2::on_start_from_checkpoint(double date,const rapidjson::Value & batsim_event){
    ISchedulingAlgorithm::on_start_from_checkpoint_normal(date,batsim_event);

}
void easy_bf_fast2::on_ingest_variables(const rapidjson::Document & doc,double date)
{
    ISchedulingAlgorithm::execute_jobs_in_running_state(date);
}
void easy_bf_fast2::on_simulation_start(double date,
    const rapidjson::Value &batsim_event)
{
    ISchedulingAlgorithm::normal_start(date,batsim_event);
    
    const rapidjson::Value & batsim_config = batsim_event["config"];
    if (batsim_config.HasMember("share-packing"))
        _share_packing = batsim_config["share-packing"].GetBool();

    if (batsim_config.HasMember("core-percent"))
        _core_percent = batsim_config["core-percent"].GetDouble();
   
    _available_machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    _nb_available_machines = _nb_machines;
    //LOG_F(INFO,"avail: %d   nb_machines: %d",_available_machines.size(),_nb_machines);
    PPK_ASSERT_ERROR(_available_machines.size() == (unsigned int) _nb_machines);
      
}      
void easy_bf_fast2::on_simulation_end(double date){
    (void) date;
}
  
void easy_bf_fast2::on_machine_available_notify_event(double date, IntervalSet machines){
    ISchedulingAlgorithm::on_machine_available_notify_event(date, machines);
    _unavailable_machines-=machines;
    _available_machines+=machines;
    _nb_available_machines+=machines.size();
    
}

void easy_bf_fast2::on_machine_state_changed(double date, IntervalSet machines, int new_state)
{
   

}
void easy_bf_fast2::on_myKillJob_notify_event(double date){
    
    if (!_running_jobs.empty()){
       auto msg = new batsched_tools::Job_Message;
        msg->id = *_running_jobs.begin();
        msg->forWhat = batsched_tools::KILL_TYPES::NONE;
        _my_kill_jobs.insert(std::make_pair((*_workload)[*_running_jobs.begin()],msg));
    }
        
    
}



void easy_bf_fast2::on_machine_down_for_repair(batsched_tools::KILL_TYPES forWhat,double date){
    //do we do a normal repair?
    IntervalSet machine = ISchedulingAlgorithm::normal_repair(date);
    
    //now kill the jobs that are running on machines that need to be repaired.        
    //if there are no running jobs, then there are none to kill
    if(!machine.is_empty() && !_running_jobs.empty())
    {
        
        //ok there are jobs to kill
        std::string killed_jobs;
            for(auto key_value : _current_allocations)
            {
                if (!((key_value.second.machines & machine).is_empty())){
                    Job * job_ref = (*_workload)[key_value.first];
                    auto msg = new batsched_tools::Job_Message;
                    msg->id = key_value.first;
                    msg->forWhat = forWhat;
                    _my_kill_jobs.insert(std::make_pair(job_ref,msg));
                   if (killed_jobs.empty())
                    killed_jobs = job_ref->id;
                else
                    killed_jobs=batsched_tools::string_format("%s %s",killed_jobs.c_str(),job_ref->id.c_str());
            }
        }
        BLOG_F(blog_types::FAILURES,"%s,\"%s\"",blog_failure_event::KILLING_JOBS.c_str(),killed_jobs.c_str());
    }
}


void easy_bf_fast2::on_machine_instant_down_up(batsched_tools::KILL_TYPES forWhat,double date){
    IntervalSet machine = ISchedulingAlgorithm::normal_downUp(date);
    //if there are no running jobs, then there are none to kill
    if (!_running_jobs.empty()){
        std::string killed_jobs;
        for(auto key_value : _current_allocations)   
	    {
		    if (!((key_value.second.machines & machine).is_empty())){
                Job * job_ref = (*_workload)[key_value.first];
                auto msg = new batsched_tools::Job_Message;
                msg->id = key_value.first;
                msg->forWhat = forWhat;
                _my_kill_jobs.insert(std::make_pair(job_ref,msg));
                if (killed_jobs.empty())
                    killed_jobs = job_ref->id;
                else
                    killed_jobs=batsched_tools::string_format("%s %s",killed_jobs.c_str(),job_ref->id.c_str());
            }
        }
        BLOG_F(blog_types::FAILURES,"%s,\"%s\"",blog_failure_event::KILLING_JOBS.c_str(), killed_jobs.c_str());
    }
}


void easy_bf_fast2::on_requested_call(double date,batsched_tools::CALL_ME_LATERS cml_in)
{
     
        
    switch (cml_in.forWhat){
        case batsched_tools::call_me_later_types::SMTBF: 
        case batsched_tools::call_me_later_types::MTBF:
        case batsched_tools::call_me_later_types::FIXED_FAILURE:
            if (!_running_jobs.empty() || !_pending_jobs.empty() || !_no_more_static_job_to_submit_received)
                ISchedulingAlgorithm::requested_failure_call(date,cml_in);
                ISchedulingAlgorithm::handle_failures(date);
            break;
        
        case batsched_tools::call_me_later_types::REPAIR_DONE:
            ISchedulingAlgorithm::requested_failure_call(date,cml_in);
            break;
        case batsched_tools::call_me_later_types::CHECKPOINT_SYNC:
            _checkpoint_sync++;
            break;
        case batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED:
            _need_to_checkpoint = true;
            break;
    }
}
    


void easy_bf_fast2::on_no_more_static_job_to_submit_received(double date){
    ISchedulingAlgorithm::on_no_more_static_job_to_submit_received(date);

}
void easy_bf_fast2::on_no_more_external_event_to_occur(double date){
    
    (void)date;  
    
    
}
void easy_bf_fast2::on_job_end(double date, std::vector<std::string> job_ids)
{
    (void) date;
    (void) job_ids;
}





/*********************************************************
 *                                                       *
 *                    Make Decisions                     *
 *                                                       *
**********************************************************/



void easy_bf_fast2::make_decisions(double date,
    SortableJobOrder::UpdateInformation *update_info,
    SortableJobOrder::CompareInformation *compare_info)
{
   ////LOG_F(INFO,"line 322   fcfs_fast2.cpp");
    (void) update_info;
    (void) compare_info;
    if (_exit_make_decisions)
    {   
        _exit_make_decisions = false;     
        return;
    }
    CLOG_F(CCU_DEBUG_ALL,"batsim_checkpoint_seconds: %d",_batsim_checkpoint_interval_seconds);
    send_batsim_checkpoint_if_ready(date);
    CLOG_F(CCU_DEBUG_ALL,"here");
    if (_need_to_checkpoint){
        checkpoint_batsched(date);
    }
    std::vector<int> mapping = {0};
   
    // This algorithm is a fast version of EASY backfilling.
    // It is meant to be fast in the usual case, not to handle corner cases.
    // It is not meant to be easily readable or hackable ;).

    // This fast EASY backfilling variant in a few words:
    // - only handles the FCFS queue order
    // - only handles the basic resource selection policy
    // - only handles finite jobs (no switchoff), with walltimes
    // - only handles one priority job (the first of the queue)
    // - only handles time as floating-point (-> precision errors).

    

    ////LOG_F(INFO,"line 340  fcfs_fast2.cpp");
    //*****************************************************************
    // Handle newly finished jobs
    //*****************************************************************
    LOG_F(INFO,"line 360");
    //do we need to switch out priority_job?
    if (_priority_job != nullptr && _priority_job->nb_requested_resources > (_nb_machines - _repair_machines.size()))
    {
        //ok we need to switch out _priority_job
        _pending_jobs.push_front(_priority_job);
        _priority_job = nullptr;
        handle_null_priority_job(date);
    }
    handle_machines_coming_available(date);
    LOG_F(INFO,"line 362");
    bool job_ended = handle_newly_finished_jobs();
    LOG_F(INFO,"line 364");    
    handle_new_jobs_to_kill(date);
    //************************************************************resubmission if killed
    //Handle jobs to queue back up (if killed)
    LOG_F(INFO,"line 368");
    _decision->handle_resubmission(_jobs_killed_recently,_workload,date);    
    //***********************************************************

    LOG_F(INFO,"line 372");
    handle_ended_job_execution(job_ended,date);
    LOG_F(INFO,"line 374");
    if (_recover_from_checkpoint && _start_from_checkpoint.started_from_checkpoint && !_jobs_released_recently.empty())
    {
        if (!_start_from_checkpoint.received_submitted_jobs)
            _start_from_checkpoint.first_submitted_time = date;
        _start_from_checkpoint.received_submitted_jobs = true;
        ISchedulingAlgorithm::ingest_variables_if_ready(date);
        _clear_jobs_recently_released = false; //not safe to clear since it is handled below
        return;
    }
    handle_newly_released_jobs(date);

    LOG_F(INFO,"line 376");
    
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
void easy_bf_fast2::handle_null_priority_job(double date)
{
    
}



std::string easy_bf_fast2::to_json_desc(rapidjson::Document * doc)
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

bool easy_bf_fast2::handle_newly_finished_jobs()
{
   //LOG_F(INFO,"line 410");
   std::vector<int> mapping = {0};
   std::string prefix = "a";
    bool job_ended = false;
    for (const std::string & ended_job_id : _jobs_ended_recently)
    {
        job_ended = true;
        Job * finished_job = (*_workload)[ended_job_id];
        const batsched_tools::Allocation & alloc = _current_allocations[ended_job_id];
        if (_share_packing && finished_job->nb_requested_resources == 1)
        {
                //first get the machine it was running on
                int machine_number = alloc.machines[0];
                Machine* current_machine = (*_machines)(prefix,machine_number);

                //now increase cores_available on that machine
                current_machine->cores_available += 1;
                //if that increase means no jobs are running on that machine (all its cores are available) then put it back in the mix
                if (current_machine->cores_available == int(current_machine->core_count * _core_percent))
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


void easy_bf_fast2::handle_new_jobs_to_kill(double date)
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

void easy_bf_fast2::handle_machines_coming_available(double date)
{
    //if (!_machines_that_became_available_recently.is_empty())
    

    
}

/**********************************************
 * 
 *          Ended Job Execution
 * 
 ***********************************************/



void easy_bf_fast2::handle_ended_job_execution(bool job_ended,double date)
{
    std::vector<int> mapping = {0};
    std::string prefix = "a";
    // If jobs have finished, execute jobs as long as they fit
    std::list<Job *>::iterator job_it =_pending_jobs.begin();
    if (job_ended || (!_machines_that_became_available_recently.is_empty()))
    {
        if (_priority_job == nullptr)
            LOG_F(INFO,"line 499 nullptr");
        if (_priority_job != nullptr)
        {
            LOG_F(INFO,"pending_jobs %d _nb_available_machines %d",_pending_jobs.size(),_nb_available_machines);
            //first check if priority job fits
            //it fits if it's a 1 resource job and share_packing is enabled and
            //we either find an _available_core_machine or
            //we find a free machine to make an _available_core_machine
            //or
            //it fits if share_packing is disabled, or it's greater than a 1 resource job
            //and
            //there are enough machines available for the priority job's nb_requested_resources
            batsched_tools::Allocation alloc;
            batsched_tools::FinishedHorizonPoint point;
            bool executed = false;
            if (_share_packing && _priority_job->nb_requested_resources == 1)
            {
                //ok the job can be share-packed:
                bool found = false;
                //first check if there is a share-packing machine available:
                //it is a 1 resource job, iterate over the available core machines until it finds one to put the job on.
                for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
                {
                    //is this machine able to handle another job?
                    Machine* current_machine = (*_machines)(prefix,*it);
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
                    Machine* current_machine = (*_machines)(prefix,alloc.machines[0]);
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
                    Machine* current_machine = (*_machines)(prefix,alloc.machines[0]);
                    current_machine->cores_available -= 1;
                    _available_core_machines += alloc.machines;
                    _available_machines -= alloc.machines;
                    _nb_available_machines -= 1;
                  
                    _current_allocations[_priority_job->id] = alloc;
                   
                    _running_jobs.insert(_priority_job->id);
                    _priority_job = nullptr;
                   
                }
            }
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
            //ok priority job got to run, now execute the whole queue until a priority job cannot fit  
            if (executed)
            {
                std::list<Job *>::iterator job_it =_pending_jobs.begin();
                bool erased = false;
                bool executed2;
                while(job_it!=_pending_jobs.end())
                {
                    Job * pending_job = *job_it;
                    batsched_tools::Allocation alloc;
                    executed2 = false;
            
                    std::string pending_job_id = pending_job->id;
                
                    if (_share_packing && pending_job->nb_requested_resources==1)
                    {
                    LOG_F(INFO,"line 611");
                        bool found = false;
                        //it is a 1 resource job, iterate over the available core machines until it finds one to put the job on.
                        for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
                        {
                            //is this machine able to handle another job?
                            Machine* current_machine = (*_machines)(prefix,*it);
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
                            Machine* current_machine = (*_machines)(prefix,alloc.machines[0]);
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
                    if (executed2==false && pending_job->nb_requested_resources <= (_nb_machines - _repair_machines.size()))
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
                batsched_tools::Allocation alloc;
           //LOG_F(INFO,"line 715");
                std::string pending_job_id = pending_job->id;
            
                if (_share_packing && pending_job->nb_requested_resources==1)
                {
                   
                    LOG_F(INFO,"line 721");
                    bool found = false;
                    //it is a 1 resource job, iterate over the available core machines until it finds one to put the job on.
                    for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
                    {
                        //is this machine able to handle another job?
                        Machine* current_machine = (*_machines)(prefix,*it);
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
                            Machine* current_machine = (*_machines)(prefix,alloc.machines[0]);
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
                } // end of backfilling jobs share-packing block
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

void easy_bf_fast2::handle_newly_released_jobs(double date)
{
    int counter = 0;
    int pending = 0;
    std::vector<int> mapping = {0};
    std::string prefix = "a";
    // Handle newly released jobs
    for (const std::string & new_job_id : _jobs_released_recently)
    {
        Job * new_job = (*_workload)[new_job_id];

        // Is this job valid?
        if (new_job->nb_requested_resources > _nb_machines)
        {
            // Invalid!
            //LOG_F(INFO,"Job being rejected HERE %s",new_job_id.c_str());
            _decision->add_reject_job(date,new_job_id, batsched_tools::REJECT_TYPES::NOT_ENOUGH_RESOURCES);
            continue;
        }
        batsched_tools::Allocation alloc;
        bool executed = false;
        //Are we share-packing?
        if (_share_packing && new_job->nb_requested_resources == 1)
        {
            //Can the job be executed right now?
            //first check all core machines running right now
            
            bool found = false;
            for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
            {
                //is this machine able to handle another job?
                Machine* current_machine = (*_machines)(prefix,*it);
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
                    batsched_tools::FinishedHorizonPoint point;
                    point.nb_released_machines = new_job->nb_requested_resources;
                    point.date = date + (double)new_job->walltime;
                    point.machines = alloc.machines;
                    alloc.horizon_it = insert_horizon_point(point);

                    Machine * current_machine = (*_machines)(prefix,alloc.machines[0]);
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
                    batsched_tools::FinishedHorizonPoint point;
                    point.nb_released_machines = new_job->nb_requested_resources;
                    point.date = date + (double) new_job->walltime;
                    point.machines = alloc.machines;
                    alloc.horizon_it = insert_horizon_point(point);


                    //update data structures
                    Machine* current_machine = (*_machines)(prefix,alloc.machines[0]);
                    current_machine->cores_available -= 1;
                    _available_core_machines += alloc.machines;
                    _available_machines -= alloc.machines;
                    _nb_available_machines -= 1;
                    
                    _current_allocations[new_job_id] = alloc;
                    
                    _running_jobs.insert(new_job_id);
                    
                                                
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

                batsched_tools::FinishedHorizonPoint point;
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
            if ((_priority_job == nullptr) && (new_job->nb_requested_resources <= (_nb_machines - _repair_machines.size())))
            {
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

        





std::list<batsched_tools::FinishedHorizonPoint>::iterator easy_bf_fast2::insert_horizon_point(const batsched_tools::FinishedHorizonPoint &point)
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

double easy_bf_fast2::compute_priority_job_expected_earliest_starting_time()
{
    std::string prefix = "a";
    int nb_available = _nb_available_machines;
    int required = _priority_job->nb_requested_resources;
    //LOG_F(INFO,"line 1294");
    //make a shallow copy of machines_by_int if share-packing
    std::map<int,Machine *> machines_by_int_copy;
    if (_share_packing)
    {
        for (auto it = _available_core_machines.elements_begin(); it != _available_core_machines.elements_end(); ++it)
            {
                Machine* current_machine = (*_machines)(prefix,*it);
                Machine* a_machine = new Machine();
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
            Machine * current_machine = machines_by_int_copy[machine_number];
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
