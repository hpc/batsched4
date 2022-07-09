#include "conservative_bf.hpp"

#include <loguru.hpp>

#include "../pempek_assert.hpp"
#include "../batsched_tools.hpp"
#define B_LOG_INSTANCE _myBLOG
using namespace std;

ConservativeBackfilling::ConservativeBackfilling(Workload *workload, SchedulingDecision *decision,
                                                 Queue *queue, ResourceSelector * selector, double rjms_delay, rapidjson::Document *variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{
    if (variant_options->HasMember("dump_previsional_schedules"))
    {
        PPK_ASSERT_ERROR((*variant_options)["dump_previsional_schedules"].IsBool(),
                "Invalid options: 'dump_previsional_schedules' should be a boolean");
        _dump_provisional_schedules = (*variant_options)["dump_previsional_schedules"].GetBool();
    }

    if (variant_options->HasMember("dump_prefix"))
    {
        PPK_ASSERT_ERROR((*variant_options)["dump_prefix"].IsString(),
                "Invalid options: 'dump_prefix' should be a string");
        _dump_prefix = (*variant_options)["dump_prefix"].GetString();
    }
    //initialize reservation queue
    SortableJobOrder * order = new FCFSOrder;
    _reservation_queue = new Queue(order);
    
}

ConservativeBackfilling::~ConservativeBackfilling()
{
}

void ConservativeBackfilling::on_simulation_start(double date, const rapidjson::Value & batsim_config)
{
    LOG_F(INFO,"ON simulation start");
    _output_svg=batsim_config["output-svg"].GetString();
    LOG_F(INFO,"output svg %s",_output_svg.c_str());
    
    _output_folder=batsim_config["output-folder"].GetString();
    
    _output_folder.replace(_output_folder.find("/out"), std::string("/out").size(), "");
    
    LOG_F(INFO,"output folder %s",_output_folder.c_str());
    
    Schedule::convert_policy(batsim_config["reschedule-policy"].GetString(),_reschedule_policy);
    Schedule::convert_policy(batsim_config["impact-policy"].GetString(),_impact_policy);

    _schedule = Schedule(_nb_machines, date);
    _schedule.set_output_svg(_output_svg);
    _schedule.set_svg_prefix(_output_folder + "/svg/");
    _schedule.set_policies(_reschedule_policy,_impact_policy);
    
    (void) batsim_config;
}


void ConservativeBackfilling::on_simulation_end(double date)
{
    (void) date;
}
void ConservativeBackfilling::set_workloads(myBatsched::Workloads *w){
    _myWorkloads = w;
    _checkpointing_on = w->_checkpointing_on;
    
}
void ConservativeBackfilling::on_machine_down_for_repair(double date){
    (void) date;
    /* not ready yet TODO
    //get a random number of a machine to kill
    int number = unif_distribution->operator()(generator2);
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

        double repair_time = _myWorkloads->_repair_time;
        //LOG_F(INFO,"in repair_machines.size(): %d nb_avail: %d  avail: %d running_jobs: %d",_repair_machines.size(),_nb_available_machines,_available_machines.size(),_running_jobs.size());
        //LOG_F(INFO,"date: %f , repair: %f ,repair + date: %f",date,repair_time,date+repair_time);
        //call me back when the repair is done
        _decision->add_call_me_later(batsched_tools::call_me_later_types::REPAIR_DONE,number,date+repair_time,date);
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
    */
}


void ConservativeBackfilling::on_machine_instant_down_up(double date){
    (void) date;
    /* not ready yet TODO
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
    */
}
void ConservativeBackfilling::on_requested_call(double date,int id,batsched_tools::call_me_later_types forWhat)
{
    
        switch (forWhat){
            /* TODO
            case batsched_tools::call_me_later_types::SMTBF:
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
                                    _decision->add_call_me_later(batsched_tools::call_me_later_types::SMTBF,1,number+date,date);
                                }
                        }
                        break;
            case batsched_tools::call_me_later_types::MTBF:
                        {
                            if (!_running_jobs.empty() || !_pending_jobs.empty() || !_no_more_static_job_to_submit_received)
                            {
                                double number = distribution->operator()(generator);
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
                                    double number = _myWorkloads->_fixed_failures;
                                    if (_myWorkloads->_repair_time == 0.0)
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
            */
            case batsched_tools::call_me_later_types::RESERVATION_START:
                        {
                            _start_a_reservation = true;
                            //SortableJobOrder::UpdateInformation update_info(date);
                            //make_decisions(date,&update_info,nullptr);
                            
                        }
                        break;
        }
    

}
void ConservativeBackfilling::make_decisions(double date,
                                             SortableJobOrder::UpdateInformation *update_info,
                                             SortableJobOrder::CompareInformation *compare_info)
{
    

    LOG_F(INFO,"make decisions");
    // Let's remove finished jobs from the schedule
    // not including killed jobs
    
    for (const string & ended_job_id : _jobs_ended_recently)
    {
        _schedule.remove_job_if_exists((*_workload)[ended_job_id]);
    }
    
    LOG_F(INFO,"after jobs ended");
    // Let's handle recently released jobs
    std::vector<std::string> recently_queued_jobs;
    std::vector<std::string> recently_released_reservations;
    for (const string & new_job_id : _jobs_released_recently)
    {
        
        LOG_F(INFO,"jobs released");
        const Job * new_job = (*_workload)[new_job_id];
        LOG_F(INFO,"job %s has purpose %s  and walltime: %g",new_job->id.c_str(),new_job->purpose.c_str(),(double)new_job->walltime);
        
        if (new_job->purpose!="reservation")
        {
            if (new_job->nb_requested_resources > _nb_machines)
            {
                _decision->add_reject_job(new_job_id, date);
            }
            else if (!new_job->has_walltime)
            {
                LOG_SCOPE_FUNCTION(INFO);
                LOG_F(INFO, "Date=%g. Rejecting job '%s' as it has no walltime", date, new_job_id.c_str());
                _decision->add_reject_job(new_job_id, date);
            }
            else
            {
                _queue->append_job(new_job, update_info);
                recently_queued_jobs.push_back(new_job_id);
            }
        }
        else
        {
            _reservation_queue->append_job(new_job,update_info);
            recently_released_reservations.push_back(new_job_id);
        }
    }
    
    
    // Let's update the schedule's present
    _schedule.update_first_slice(date);
    if (_output_svg == "short")
        _schedule.output_to_svg("make_decisions");
    // Queue sorting
    _queue->sort_queue(update_info, compare_info);
    _reservation_queue->sort_queue(update_info,compare_info);
     //ok this is if there are reservations to deal with: 

    //insert reservations into schedule whether jobs have finished or not
    for (const string & new_job_id : recently_released_reservations)
    {
        LOG_F(INFO,"new reservation: %s queue:%s",new_job_id.c_str(),_queue->to_string().c_str());
        const Job * new_job = (*_workload)[new_job_id];
        LOG_F(INFO,"job %s has walltime %g  start %f and alloc %s",new_job->id.c_str(),(double)new_job->walltime,new_job->start,new_job->future_allocations.to_string_hyphen(" ","-").c_str());
        //reserve a time slice 
        LOG_F(INFO,"resched policy %d",_reschedule_policy);

        if (_reschedule_policy == Schedule::RESCHEDULE_POLICY::AFFECTED)
        {
            LOG_F(INFO,"DEBUG line 200");
            Schedule::ReservedTimeSlice reservation = _schedule.reserve_time_slice(new_job);
            _schedule.add_reservation_for_svg_outline(reservation);
            LOG_F(INFO,"DEBUG line 201");
            LOG_F(INFO,"size of jobs_to_resch %d",reservation.jobs_to_reschedule.size());
            for(auto job : reservation.jobs_to_reschedule)
            {
                LOG_F(INFO,"size of submission times %d",job->submission_times.size());
                LOG_F(INFO,"job %s  sub times[0] %f",job->id.c_str(),job->submission_times[0]);
            }
            if (reservation.success)
            {
                //sort the jobs to reschedule;
                auto sort_function = [](const Job * j1,const Job * j2)->bool{
                    if (j1->submission_times[0] == j2->submission_times[0])
                        return j1->id < j2->id;
                    else
                        return j1->submission_times[0] < j2->submission_times[0];
                };
                std::sort(reservation.jobs_to_reschedule.begin(), reservation.jobs_to_reschedule.end(),sort_function);
                LOG_F(INFO,"DEBUG line 209");
                //we need to wait on rescheduling if jobs are killed
                //so check if any jobs need to be killed
                if (reservation.jobs_needed_to_be_killed.empty())
                {//ok no jobs need to be killed, reschedule
                    
                    //remove the jobs in order all at once
                    for(auto job : reservation.jobs_to_reschedule)
                        _schedule.remove_job(job);
                    LOG_F(INFO,"DEBUG line 218");
                   
                    Schedule::ReservedTimeSlice reservation2 = _schedule.reserve_time_slice(new_job);
                    reservation.slice_begin = reservation2.slice_begin;
                    reservation.slice_end = reservation2.slice_end;
                    _schedule.add_reservation(reservation);
                    _schedule.remove_reservation_for_svg_outline(reservation);
                    Schedule::JobAlloc alloc;
                    //now add the rescheduled jobs in order
                    for(auto job : reservation.jobs_to_reschedule)
                    {
                        alloc = _schedule.add_job_first_fit(job,_selector);
                        //if this job starts in first slice execute it
                        if (alloc.started_in_first_slice)
                        {
                            _queue->remove_job(job);
                            _decision->add_execute_job(job->id,alloc.used_machines,date);
                        }
                    }
                    LOG_F(INFO,"DEBUG line 234");
                    //if reservation started in first slice
                    if (reservation.alloc->started_in_first_slice)
                    {
                        LOG_F(INFO,"line 447 started first slice?");
                        _reservation_queue->remove_job(new_job);
                        _decision->add_execute_job(new_job->id,reservation.alloc->used_machines,date);
                    }
                    else if (new_job->start > date)
                        _decision->add_call_me_later(batsched_tools::call_me_later_types::RESERVATION_START,
                                                    new_job->unique_number,
                                                    new_job->start,
                                                    date);
                    else
                        _start_a_reservation = true;
                }
                else{//ok some jobs do need to be killed
                    
                    //make decisions will run a couple times with the same date:
                    //  right now when job is killed -> when progress comes back and we resubmit -> when notified of resubmit and we reschedule
                    
                    //first kill jobs
                    LOG_F(INFO,"DEBUG line 248");
                    std::vector<std::string> kill_jobs;
                    for(auto job : reservation.jobs_needed_to_be_killed)
                    {
                        kill_jobs.push_back(job->id);
                    }
                    _decision->add_kill_job(kill_jobs,date);
                    LOG_F(INFO,"DEBUG line 253");
                    //remove kill jobs from schedule
                    for(auto job : reservation.jobs_needed_to_be_killed)
                    {
                        LOG_F(INFO,"DEBUG id %s",job->id.c_str());
                        _schedule.remove_job(job);
                    }
                    LOG_F(INFO,"DEBUG line 257");
                    //remove jobs to reschedule
                    for (auto job : reservation.jobs_to_reschedule)
                        _schedule.remove_job(job);
                    LOG_F(INFO,"DEBUG line 261");
                    //we can make the reservation now
                    Schedule::ReservedTimeSlice reservation2 = _schedule.reserve_time_slice(new_job);
                    reservation.slice_begin = reservation2.slice_begin;
                    reservation.slice_end = reservation2.slice_end;
                    _schedule.add_reservation(reservation);
                    _schedule.remove_reservation_for_svg_outline(reservation);
                    //get things ready for once killed_jobs are resubmitted
                    LOG_F(INFO,"DEBUG line 265");
                    _saved_reservations.push_back(reservation);
                    LOG_F(INFO,"line 307");
                    _killed_jobs = true;
                    _saved_recently_queued_jobs = recently_queued_jobs;
                }
            }
        }
        if (_reschedule_policy == Schedule::RESCHEDULE_POLICY::ALL)
        {
            
        }
              
    }
    recently_released_reservations.clear();

    if (_start_a_reservation)
    {
        Schedule::JobAlloc alloc;
        std::vector<const Job *> jobs_removed;
        LOG_F(INFO,"line 322");
        if(_schedule.remove_reservations_if_ready(jobs_removed))
        {
            LOG_F(INFO,"DEBUG line 323");
                for(const Job * job : jobs_removed)
                {
                    LOG_F(INFO,"DEBUG line 326");
                    alloc = _schedule.add_current_reservation(job,_selector);
                    LOG_F(INFO,"DEBUG line 328");
                    _reservation_queue->remove_job(job);
                    _decision->add_execute_job(alloc.job->id,alloc.used_machines,date);
                    
                }
            //only set this to false if we  remove and execute some reservations
            //keep true in case it comes back around after a job completes
            _start_a_reservation = false;
        }
        
    }

    
     
    
    
    
    
    
    
    
    
    
    
    
    for ( auto job_progress_pair : _jobs_killed_recently)
    {
        batsched_tools::id_separation separation = batsched_tools::tools::separate_id(job_progress_pair.first);
        LOG_F(INFO,"next_resubmit_string %s",separation.next_resubmit_string.c_str());
        _resubmitted_jobs.push_back(separation.next_resubmit_string);
    }
    _decision->handle_resubmission(_jobs_killed_recently,_workload,date);

    //take care of killed jobs and reschedule jobs
    //lifecycle of killed job:
    //make_decisions() kill job -> make_decisions() submit job -> make_decisions() add jobs to schedule in correct order
    // it is the third invocation that this function should run 
    LOG_F(INFO,"killed_jobs %d",_killed_jobs);
    if (_killed_jobs && !_resubmitted_jobs.empty())
    {
        LOG_F(INFO,"killed_jobs !_jobs_release empty");
        if (_reschedule_policy == Schedule::RESCHEDULE_POLICY::AFFECTED)
        {
            
            //reservations are in place, killed jobs have been re-submitted
            //now add the jobs back to the schedule
            //first get all jobs_to_reschedule in one lump so we can sort them
            //and all the killed jobs
            std::vector<const Job *> all_jobs_to_reschedule;
            std::vector<const Job *> all_killed_jobs;
            for(auto reservation : _saved_reservations)
            {
                
                for (auto job : reservation.jobs_to_reschedule)
                        all_jobs_to_reschedule.push_back(job);
                
            }
            //have to remove resubmitted jobs since we are taking care of them
            //store unkilled jobs in recently_queued_jobs2
            std::vector<std::string> recently_queued_jobs2;
            //go through recently_queued_jobs and get all the resubmitted ones
            for (std::string job_id : recently_queued_jobs)
            {
                //check if it's a resubmitted job
                if (job_id.find("#") != std::string::npos)
                {
                    
                    all_killed_jobs.push_back((*_workload)[job_id]);

                }
                else
                    recently_queued_jobs2.push_back(job_id);
            }
            //set the recently_queued_jobs to a vector without the resubmitted jobs
            recently_queued_jobs = recently_queued_jobs2;

            //define a sort function
            auto sort_function = [](const Job * j1, const Job *j2)->bool{
                if (j1->submission_times[0] == j2->submission_times[0])
                    return j1->id < j2->id;
                return j1->submission_times[0]<j2->submission_times[0];
            };
            //now sort them based on original_submit_time
            std::sort(all_jobs_to_reschedule.begin(),all_jobs_to_reschedule.end(),sort_function);
            std::sort(all_killed_jobs.begin(),all_killed_jobs.end(),sort_function);
            //add the killed_jobs first
            for (auto job : all_killed_jobs)
            {    
                _resubmitted_jobs.remove(job->id);
                LOG_F(INFO,"all killed jobs job: %s",job->id.c_str());
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job,_selector);
                if (alloc.started_in_first_slice)
                {
                    _queue->remove_job(job);
                    _decision->add_execute_job(job->id,alloc.used_machines,date);
                }

            }
            //add the reschedule jobs next only after the killed jobs
            if (_resubmitted_jobs.empty())
            {
                for(auto job: all_jobs_to_reschedule)
                {
                        Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job,_selector);
                        if (alloc.started_in_first_slice)
                        {
                            _queue->remove_job(job);
                            _decision->add_execute_job(job->id,alloc.used_machines,date);
                        }
                }
                //everything should be rescheduled, now add callbacks for each reservation
                for (auto reservation : _saved_reservations)
                {
                    if (reservation.job->start > date)
                    _decision->add_call_me_later(batsched_tools::call_me_later_types::RESERVATION_START,
                                                reservation.job->unique_number,
                                                reservation.job->start,
                                                date );
                    else
                        _start_a_reservation = true;
                }
            }
            
            
        }
        if (_resubmitted_jobs.empty())
        {
            LOG_F(INFO,"Setting _killed_jobs to false");
            _saved_reservations.clear();
            _killed_jobs = false;
        }
    }  
     
    
    
   
    
    
    // If no resources have been released, we can just insert the new jobs into the schedule
    if (_jobs_ended_recently.empty() && !_killed_jobs)
    {
        //if there were some saved queued jobs from killing jobs take care of them
        if (recently_queued_jobs.empty() && !_saved_recently_queued_jobs.empty())
        {
            for (const string & new_job_id : _saved_recently_queued_jobs)
            {
                const Job * new_job = (*_workload)[new_job_id];
                LOG_F(INFO,"DEBUG line 321");
                    
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(new_job, _selector);

                // If the job should start now, let's say it to the resource manager
                if (alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(new_job->id, alloc.used_machines, date);
                    _queue->remove_job(new_job);
                }
            }
        }
        else
        {
            for (const string & new_job_id : recently_queued_jobs)
            {
                const Job * new_job = (*_workload)[new_job_id];
                LOG_F(INFO,"DEBUG line 337");
                    
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(new_job, _selector);

                // If the job should start now, let's say it to the resource manager
                if (alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(new_job->id, alloc.used_machines, date);
                    _queue->remove_job(new_job);
                }
            }
        }
    }
    else if (!_jobs_ended_recently.empty() && !_killed_jobs)
    {
        // Since some resources have been freed,
        // Let's compress the schedule following conservative backfilling rules:
        // For each non running job j
        //   Remove j from the schedule
        //   Add j into the schedule
        //   If j should be executed now
        //     Take the decision to run j now
        

        for (auto job_it = _queue->begin(); job_it != _queue->end(); )
        {
            const Job * job = (*job_it)->job;
            _schedule.remove_job_if_exists(job);
    //            if (_dump_provisional_schedules)
    //                _schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);
            LOG_F(INFO,"DEBUG line 375");
            Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, _selector);   
    //            if (_dump_provisional_schedules)
    //                _schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);

            if (alloc.started_in_first_slice)
            {
                _decision->add_execute_job(job->id, alloc.used_machines, date);
                job_it = _queue->remove_job(job_it);
            }
            else
                ++job_it;
        }
    }
    
      

    // And now let's see if we can estimate some waiting times
    
    for (const std::string & job_id : _jobs_whose_waiting_time_estimation_has_been_requested_recently)
    {
        const Job * new_job = (*_workload)[job_id];
        double answer = _schedule.query_wait(new_job->nb_requested_resources, new_job->walltime, _selector);
            _decision->add_answer_estimate_waiting_time(job_id, answer, date);
    }

    if (_dump_provisional_schedules)
        _schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);
    /*
    LOG_F(INFO,"jkr = %d  qie = %d rqie = %d ss = %d ntsfsj = %d nmsjtsr = %d",
    _jobs_killed_recently.empty(), _queue->is_empty(), _reservation_queue->is_empty() , _schedule.size(),
             _need_to_send_finished_submitting_jobs , _no_more_static_job_to_submit_received);
    LOG_F(INFO,"res_queue %s",_reservation_queue->to_string().c_str());
    */
    if (_jobs_killed_recently.empty() && _queue->is_empty() && _reservation_queue->is_empty() && _schedule.size() == 0 &&
             _need_to_send_finished_submitting_jobs && _no_more_static_job_to_submit_received && !(date<1.0) )
    {
        _decision->add_scheduler_finished_submitting_jobs(date);
        _need_to_send_finished_submitting_jobs = false;
    }

    
}

