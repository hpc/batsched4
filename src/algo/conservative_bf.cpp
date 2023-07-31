#include "conservative_bf.hpp"

#include <loguru.hpp>

#include "../pempek_assert.hpp"
#include "../batsched_tools.hpp"
#include <chrono>
#include <ctime>
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

void ConservativeBackfilling::on_simulation_start(double date, const rapidjson::Value & batsim_event)
{
    pid_t pid = batsched_tools::get_batsched_pid();
    _decision->add_generic_notification("PID",std::to_string(pid),date);
    const rapidjson::Value & batsim_config = batsim_event["config"];
    LOG_F(INFO,"ON simulation start");
    _output_svg=batsim_config["output-svg"].GetString();
    _svg_frame_start = batsim_config["svg-frame-start"].GetInt64();
    _svg_frame_end = batsim_config["svg-frame-end"].GetInt64();
    _svg_output_start = batsim_config["svg-output-start"].GetInt64();
    _svg_output_end = batsim_config["svg-output-end"].GetInt64();
    LOG_F(INFO,"output svg %s",_output_svg.c_str());
    
    _output_folder=batsim_config["output-folder"].GetString();
    
    _output_folder.replace(_output_folder.rfind("/out"), std::string("/out").size(), "");
    
    LOG_F(INFO,"output folder %s",_output_folder.c_str());
    
    Schedule::convert_policy(batsim_config["reschedule-policy"].GetString(),_reschedule_policy);
    Schedule::convert_policy(batsim_config["impact-policy"].GetString(),_impact_policy);

    _schedule = Schedule(_nb_machines, date);
    _schedule.set_output_svg(_output_svg);
    _schedule.set_svg_frame_and_output_start_and_end(_svg_frame_start,_svg_frame_end,_svg_output_start,_svg_output_end);
    _schedule.set_svg_prefix(_output_folder + "/svg/");
    _schedule.set_policies(_reschedule_policy,_impact_policy);
    ISchedulingAlgorithm::set_generators(date);
    
    _recently_under_repair_machines = IntervalSet::empty_interval_set();
    
    (void) batsim_config;
}


void ConservativeBackfilling::on_simulation_end(double date)
{
    (void) date;
}
/*
void ConservativeBackfilling::set_workloads(myBatsched::Workloads *w){
    _myWorkloads = w;
    _checkpointing_on = w->_checkpointing_on;
    
}
*/
void ConservativeBackfilling::set_machines(Machines *m){
    _machines = m;
}
void ConservativeBackfilling::on_machine_down_for_repair(batsched_tools::KILL_TYPES forWhat,double date){
    (void) date;
    auto sort_original_submit_pair = [](const std::pair<const Job *,IntervalSet> j1,const std::pair<const Job *,IntervalSet> j2)->bool{
            if (j1.first->submission_times[0] == j2.first->submission_times[0])
                return j1.first->id < j2.first->id;
            else
                return j1.first->submission_times[0] < j2.first->submission_times[0];
    };
   
    //get a random number of a machine to kill
    int number = unif_distribution->operator()(generator2);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = (*_machines)[number]->id;
    
    if (_output_svg == "all")
            _schedule.output_to_svg("On Machine Down For Repairs  Machine #:  "+std::to_string((*_machines)[number]->id));
    
    IntervalSet added = IntervalSet::empty_interval_set() ;
    if (_schedule.get_reservations_running_on_machines(machine).empty())
        added = _schedule.add_repair_machines(machine);
    LOG_F(INFO,"here");
    //if the machine is already down for repairs ignore it.
    //LOG_F(INFO,"repair_machines.size(): %d    nb_avail: %d  avail:%d running_jobs: %d",_repair_machines.size(),_nb_available_machines,_available_machines.size(),_running_jobs.size());
    //BLOG_F(b_log::FAILURES,"Machine Repair: %d",number);
    if (!added.is_empty())
    {
        
        _schedule.add_svg_highlight_machines(machine);
        //ok the machine is not down for repairs already so it WAS added
        //the failure/repair will not be happening on a machine that has a reservation on it either
        //it will be going down for repairs now
        double repair_time = (*_machines)[number]->repair_time;
        //if there is a global repair time set that as the repair time
        if (_workload->_repair_time != -1.0)
            repair_time = _workload->_repair_time;
        if (_workload->_MTTR != -1.0)
            repair_time = repair_time_exponential_distribution->operator()(generator_repair_time);
        //call me back when the repair is done
        _decision->add_call_me_later(batsched_tools::call_me_later_types::REPAIR_DONE,number,date+repair_time,date);
       
        if (_schedule.get_number_of_running_jobs() > 0 )
        {
            //there are possibly some running jobs to kill
             
            std::vector<std::string> jobs_to_kill;
            _schedule.get_jobs_running_on_machines(machine,jobs_to_kill);
              
              std::string jobs_to_kill_str = !(jobs_to_kill.empty())? std::accumulate( /* otherwise, accumulate */
    ++jobs_to_kill.begin(), jobs_to_kill.end(), /* the range 2nd to after-last */
    *jobs_to_kill.begin(), /* and start accumulating with the first item */
    [](auto& a, auto& b) { return a + "," + b; }) : "";
              
            LOG_F(INFO,"jobs to kill %s",jobs_to_kill_str.c_str());

            if (!jobs_to_kill.empty()){
                _recently_under_repair_machines+=machine;
                std::vector<batsched_tools::Job_Message *> msgs;
                for (auto job_id : jobs_to_kill){
                    auto msg = new batsched_tools::Job_Message;
                    msg->id = job_id;
                    msg->forWhat = forWhat;
                    msgs.push_back(msg);
                }
                _killed_jobs=true;
                _decision->add_kill_job(msgs,date);
                for (auto job_id:jobs_to_kill)
                    _schedule.remove_job_if_exists((*_workload)[job_id]);
            }
            else{
                    //ok there are no jobs to kill
                    //lets reschedule
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
                        Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job_interval_pair.first,_selector,false);
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
                    //            if (_dump_provisional_schedules)
                    //                _schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);
                            LOG_F(INFO,"DEBUG line 375");
                            Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, _selector,false);   
                    //            if (_dump_provisional_schedules)
                    //                _schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);
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
                        _schedule.output_to_svg("Finished Machine Down For Repairs, Machine #: "+std::to_string(number));

                }
        }
    }
    else{
        //if (!added.is_empty())
        //  _schedule.remove_repair_machines(machine);
        //_schedule.remove_svg_highlight_machines(machine);
        if (_output_svg == "all")
            _schedule.output_to_svg("Finished Machine Down For Repairs, NO REPAIR  Machine #:  "+std::to_string(number));
    
    }
    
        
  
    
}


void ConservativeBackfilling::on_machine_instant_down_up(batsched_tools::KILL_TYPES forWhat,double date){
    (void) date;
    //get a random number of a machine to kill
    int number = unif_distribution->operator()(generator2);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = number;
    _schedule.add_svg_highlight_machines(machine);
    if (_output_svg == "all")
            _schedule.output_to_svg("On Machine Instant Down Up  Machine #: "+std::to_string(number));
    
    //BLOG_F(b_log::FAILURES,"Machine Instant Down Up: %d",number);
    LOG_F(INFO,"instant down up machine number %d",number);
    //if there are no running jobs, then there are none to kill
    if (_schedule.get_number_of_running_jobs() > 0){
        //ok so there are running jobs
        LOG_F(INFO,"instant down up");
        std::vector<std::string> jobs_to_kill;
        _schedule.get_jobs_running_on_machines(machine,jobs_to_kill);
         LOG_F(INFO,"instant down up");
        
        LOG_F(INFO,"instant down up");
        if (!jobs_to_kill.empty())
        {
            _killed_jobs = true;
            std::vector<batsched_tools::Job_Message *> msgs;
            for (auto job_id : jobs_to_kill){
                auto msg = new batsched_tools::Job_Message;
                msg->id = job_id;
                msg->forWhat = forWhat;
                msgs.push_back(msg);
            }
            _decision->add_kill_job(msgs,date);
            std::string jobs_to_kill_string;
            //remove jobs to kill from schedule and add to our log string
             LOG_F(INFO,"instant down up");
            for (auto job_id:jobs_to_kill)
            {
                jobs_to_kill_string += ", " + job_id;
                _schedule.remove_job_if_exists((*_workload)[job_id]);

            }
             LOG_F(INFO,"instant down up");
            //BLOG_F(b_log::FAILURES,"Killing Jobs: %s",jobs_to_kill_string.c_str());
    
        }
            	
	}
    _schedule.remove_svg_highlight_machines(machine);
    if (_output_svg == "all")
            _schedule.output_to_svg("END On Machine Instant Down Up  Machine #: "+std::to_string(number));
    
}
void ConservativeBackfilling::on_requested_call(double date,int id,batsched_tools::call_me_later_types forWhat)
{
        if (_output_svg != "none")
            _schedule.set_now((Rational)date);
        switch (forWhat){
            
            case batsched_tools::call_me_later_types::SMTBF:
                        {
                            //Log the failure
                            //BLOG_F(b_log::FAILURES,"FAILURE SMTBF");
                            if (_schedule.get_number_of_running_jobs() > 0 || !_queue->is_empty() || !_no_more_static_job_to_submit_received)
                                {
                                    double number = distribution->operator()(generator);
                                    LOG_F(INFO,"%f %f",_workload->_repair_time,_workload->_MTTR);
                                    if (_workload->_repair_time == 0.0 && _workload->_MTTR == -1.0)
                                        _on_machine_instant_down_ups.push_back(batsched_tools::KILL_TYPES::SMTBF);                                        
                                    else
                                        _on_machine_down_for_repairs.push_back(batsched_tools::KILL_TYPES::SMTBF);
                                    _decision->add_call_me_later(batsched_tools::call_me_later_types::SMTBF,1,number+date,date);
                                }
                        }
                        break;
            /* TODO
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
            */
            case batsched_tools::call_me_later_types::FIXED_FAILURE:
                        {
                            //BLOG_F(b_log::FAILURES,"FAILURE FIXED_FAILURE");
                            LOG_F(INFO,"DEBUG");
                            if (_schedule.get_number_of_running_jobs() > 0 || !_queue->is_empty() || !_no_more_static_job_to_submit_received)
                                {
                                    LOG_F(INFO,"DEBUG");
                                    double number = _workload->_fixed_failures;
                                    if (_workload->_repair_time == 0.0 & _workload->_MTTR == -1.0)
                                        _on_machine_instant_down_ups.push_back(batsched_tools::KILL_TYPES::FIXED_FAILURE);//defer to after make_decisions
                                    else
                                        _on_machine_down_for_repairs.push_back(batsched_tools::KILL_TYPES::FIXED_FAILURE);
                                    _decision->add_call_me_later(batsched_tools::call_me_later_types::FIXED_FAILURE,1,number+date,date);
                                }
                        }
                        break;
            
            case batsched_tools::call_me_later_types::REPAIR_DONE:
                        {
                            //BLOG_F(b_log::FAILURES,"REPAIR_DONE");
                            //a repair is done, all that needs to happen is add the machines to available
                            //and remove them from repair machines and add one to the number of available
                            if (_output_svg == "all")
                                _schedule.output_to_svg("top Repair Done  Machine #: "+std::to_string(id));
                            IntervalSet machine = id;
                            _schedule.remove_repair_machines(machine);
                            _schedule.remove_svg_highlight_machines(machine);
                             if (_output_svg == "all")
                                _schedule.output_to_svg("bottom Repair Done  Machine #: "+std::to_string(id));
                            _need_to_compress = true;
                           //LOG_F(INFO,"in repair_machines.size(): %d nb_avail: %d avail: %d  running_jobs: %d",_repair_machines.size(),_nb_available_machines,_available_machines.size(),_running_jobs.size());
                        }
                        break;
            
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
    
    if (_output_svg != "none")
        _schedule.set_now((Rational)date);
    LOG_F(INFO,"make decisions");
    //define a sort function for sorting jobs based on original submit times
    auto sort_original_submit = [](const Job * j1,const Job * j2)->bool{
            if (j1->submission_times[0] == j2->submission_times[0])
                return j1->id < j2->id;
            else
                return j1->submission_times[0] < j2->submission_times[0];
    };
    
    // Let's remove finished jobs from the schedule
    // not including killed jobs
    time_t start = time(NULL);
    for (const string & ended_job_id : _jobs_ended_recently)
    {
        _schedule.remove_job_if_exists((*_workload)[ended_job_id]);
    }
    time_t end = time(NULL);
    
    
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
    
    //before we update the first slice make sure we have jobs in the schedule other than reservations

    //int schedule_size = _schedule.size() - _schedule.nb_reservations_size();
    //if (!_killed_jobs && _jobs_killed_recently.empty() && _reservation_queue->is_empty && schedule_size > 0 && _no_more_static_job_to_submit_received)
    
    //if we have a reservation that needs to be run, first make sure 
  
    // Let's update the schedule's present
    
    _schedule.update_first_slice(date);
    //check if the first slice has a reservation to run
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

    
    if (_output_svg == "short")
        _schedule.output_to_svg("make_decisions");
    // Queue sorting
    _queue->sort_queue(update_info, compare_info);
    //LOG_F(INFO,"queue: %s",_queue->to_string().c_str());
    _reservation_queue->sort_queue(update_info,compare_info);
    for ( auto job_message_pair : _jobs_killed_recently)
    {
        batsched_tools::id_separation separation = batsched_tools::tools::separate_id(job_message_pair.first);
        LOG_F(INFO,"next_resubmit_string %s",separation.next_resubmit_string.c_str());
        _resubmitted_jobs[separation.next_resubmit_string]=job_message_pair.second->forWhat;
    }
    
   
    
    //take care of killed jobs and reschedule jobs
    //lifecycle of killed job:
    //make_decisions() kill job -> make_decisions() submit job -> make_decisions() add jobs to schedule in correct order
    // it is the third invocation that this function should run 
    
    handle_killed_jobs(recently_queued_jobs,date);
    
   // LOG_F(ERROR,"handle_killed_jobs time: %d",end-start);
    auto compare_reservations = [this](const std::string j1,const std::string j2)->bool{
            
            Job * job1= (*_workload)[j1];
            Job * job2= (*_workload)[j2];
            if (job1->future_allocations.is_empty() && job2->future_allocations.is_empty())
                return j1 < j2;
            else if (job1->future_allocations.is_empty())
                return false;  // j1 has no allocation so it must be set up after j2
            else if (job2->future_allocations.is_empty())
                return true;
            else
                return j1 < j2;

    };
LOG_F(INFO,"here");
    //sort reservations with jobs that have allocations coming first 
    if (!recently_released_reservations.empty())           
    {
        LOG_F(INFO,"here size: %d",recently_released_reservations.size());
        for(std::string resv:recently_released_reservations)
            LOG_F(INFO,"resv: %s",resv.c_str());
        std::sort(recently_released_reservations.begin(),recently_released_reservations.end(),compare_reservations);
        handle_reservations(recently_released_reservations,recently_queued_jobs,date);
    }
    LOG_F(INFO,"here");
    //insert reservations into schedule whether jobs have finished or not
    
    LOG_F(INFO,"here");
    
    for(batsched_tools::KILL_TYPES forWhat : _on_machine_instant_down_ups)
    {
        on_machine_instant_down_up(forWhat,date);
    }
    
    //LOG_F(ERROR,"on_machine_instant_down_up time: %d",end-start);
    _on_machine_instant_down_ups.clear();
    for(batsched_tools::KILL_TYPES forWhat : _on_machine_down_for_repairs)
    {
        on_machine_down_for_repair(forWhat,date);
    }
    _on_machine_down_for_repairs.clear();
    LOG_F(INFO,"here");
    
    _decision->handle_resubmission(_jobs_killed_recently,_workload,date);
    //end=time(NULL);
    //LOG_F(ERROR,"resubmission time: %d",end-start);
    //start=time(NULL);

    handle_schedule(recently_queued_jobs,date);
   //end=time(NULL);
   //LOG_F(ERROR,"handle_schedule time: %d",end-start);


LOG_F(INFO,"here");


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
    
    //LOG_F(INFO,"res_queue %s",_reservation_queue->to_string().c_str());
    */

   //this is what normally has to happen to end the simulation
    if (!_killed_jobs && _jobs_killed_recently.empty() && _queue->is_empty() && _reservation_queue->is_empty() && _schedule.size() == 0 &&
             _need_to_send_finished_submitting_jobs && _no_more_static_job_to_submit_received && !(date<1.0) )
    {
      //  LOG_F(INFO,"finished_submitting_jobs sent");
        _decision->add_scheduler_finished_submitting_jobs(date);
        if (_output_svg == "all" || _output_svg == "short")
            _schedule.output_to_svg("Simulation Finished");
        _schedule.set_output_svg("none");
        _output_svg = "none";
        _need_to_send_finished_submitting_jobs = false;
    }
    LOG_F(INFO,"here");
    //LOG_F(INFO,"!killed= %d  jkr = %d  qie = %d rqie = %d ss = %d ntsfsj = %d nmsjtsr = %d",
    //!_killed_jobs,_jobs_killed_recently.empty(), _queue->is_empty(), _reservation_queue->is_empty() , _schedule.size(),
    //         _need_to_send_finished_submitting_jobs , _no_more_static_job_to_submit_received);
    //if there are jobs that can't run then we need to start rejecting them at this point
    if (!_killed_jobs && _jobs_killed_recently.empty() && _reservation_queue->is_empty() && _schedule.size() == 0 &&
             _need_to_send_finished_submitting_jobs && _no_more_static_job_to_submit_received && !(date<1.0) )
    {
      //  LOG_F(INFO,"here");
        bool able=false; //this will stay false unless there is a job that can run
        auto previous_to_end = _schedule.end();
        previous_to_end--;
        for (auto itr = _queue->begin();itr!=_queue->end();++itr)
        {
        //     LOG_F(INFO,"here");
            
            if ((*itr)->job->nb_requested_resources <= previous_to_end->available_machines.size())
                able=true;
          //   LOG_F(INFO,"here");
        }
        if (!able)
        {
            // LOG_F(INFO,"here");
            //ok we are not able to run things, start rejecting the jobs
            for ( auto itr = _queue->begin();itr!=_queue->end();++itr)
            {
              //   LOG_F(INFO,"here");
              //  LOG_F(INFO,"Rejecting job %s",(*itr)->job->id.c_str());
                _decision->add_reject_job((*itr)->job->id,date);
                itr=_queue->remove_job(itr);
                // LOG_F(INFO,"here");
            }
        }
        
    }

    _decision->add_generic_notification("queue_size",std::to_string(_queue->nb_jobs()),date);
    _decision->add_generic_notification("schedule_size",std::to_string(_schedule.size()),date);
    _decision->add_generic_notification("number_running_jobs",std::to_string(_schedule.get_number_of_running_jobs()),date);
    _decision->add_generic_notification("utilization",std::to_string(_schedule.get_utilization()),date);
    _decision->add_generic_notification("utilization_no_resv",std::to_string(_schedule.get_utilization_no_resv()),date);
}







void ConservativeBackfilling::handle_schedule(std::vector<std::string> & recently_queued_jobs,double date)
{

auto sort_original_submit = [](const Job * j1,const Job * j2)->bool{
            if (j1->submission_times[0] == j2->submission_times[0])
                return j1->id < j2->id;
            else
                return j1->submission_times[0] < j2->submission_times[0];
    };
// If no resources have been released, we can just insert the new jobs into the schedule
    if (_jobs_ended_recently.empty() && !_killed_jobs)
    {
        //if there were some saved queued jobs from killing jobs take care of them
    
        if (recently_queued_jobs.empty() && !_saved_recently_queued_jobs.empty())
        {
            
            int scheduled = _schedule.nb_jobs_size() - _schedule.get_number_of_running_jobs();
            if(_output_svg == "all")
                _schedule.output_to_svg("CONSERVATIVE_BF saved queued ADDING");
            for (const string & new_job_id : _saved_recently_queued_jobs)
            {
                if (_workload->_queue_depth != -1 && scheduled >=_workload->_queue_depth)
                    break;
                
                const Job * new_job = (*_workload)[new_job_id];
                //LOG_F(INFO,"DEBUG line 321");
                    
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(new_job, _selector,false);

                // If the job should start now, let's say it to the resource manager
                if (!alloc.used_machines.is_empty())
                {
                    scheduled++;
                
                    if (alloc.started_in_first_slice)
                    {
                        _decision->add_execute_job(new_job->id, alloc.used_machines, date);
                        _queue->remove_job(new_job);
                        scheduled--;
                    }
                }
            }
            if(_output_svg == "all")
                _schedule.output_to_svg("CONSERVATIVE_BF saved queued ADDING DONE");
            _saved_recently_queued_jobs.clear();
        }
        else
        {
            int scheduled = _schedule.nb_jobs_size() - _schedule.get_number_of_running_jobs();
            if(_output_svg == "all")
                _schedule.output_to_svg("CONSERVATIVE_BF recent queued ADDING");
            for (const string & new_job_id : recently_queued_jobs)
            {
                if (_workload->_queue_depth != -1 && scheduled >=_workload->_queue_depth)
                    break;
                const Job * new_job = (*_workload)[new_job_id];
                //LOG_F(INFO,"DEBUG line 337");
                    
                Schedule::JobAlloc alloc = _schedule.add_job_first_fit(new_job, _selector,false);

                // If the job should start now, let's say it to the resource manager
                 if (!alloc.used_machines.is_empty())
                {
                    scheduled++;
                    if (alloc.started_in_first_slice)
                    {
                        _decision->add_execute_job(new_job->id, alloc.used_machines, date);
                        _queue->remove_job(new_job);
                        scheduled--;
                    }
                }
            }
            if(_output_svg == "all")
                _schedule.output_to_svg("CONSERVATIVE_BF recent queued ADDING DONE");
        }
    }
    if ((!_jobs_ended_recently.empty() || _need_to_compress) && !_killed_jobs)
    {
        // Since some resources have been freed,
        // Let's compress the schedule following conservative backfilling rules:
        // For each non running job j
        //   Remove j from the schedule
        //   Add j into the schedule
        //   If j should be executed now
        //     Take the decision to run j now
        if (_output_svg == "all")
            _schedule.output_to_svg("CONSERVATIVE_BF  " + std::string( _need_to_compress? "needed":"") + " compress");
        int scheduled = 0;
        for (auto job_it = _queue->begin(); job_it != _queue->end(); )
        {
            if (_workload->_queue_depth != -1 && scheduled >= _workload->_queue_depth)
                break;
            
            const Job * job = (*job_it)->job;
            _schedule.remove_job_if_exists(job);
    //            if (_dump_provisional_schedules)
    //                _schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);
            //LOG_F(INFO,"DEBUG line 375");
            Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, _selector,false);   
    //            if (_dump_provisional_schedules)
    //                _schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);
            if (!alloc.used_machines.is_empty())
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
        if (_output_svg == "all")
            _schedule.output_to_svg("CONSERVATIVE_BF  " + std::string(_need_to_compress? "needed":"") + "compress, DONE");
        _need_to_compress = false;
    }
}











void ConservativeBackfilling::handle_killed_jobs(std::vector<std::string> & recently_queued_jobs, double date)
{
    auto sort_original_submit = [](const Job * j1,const Job * j2)->bool{
            if (j1->submission_times[0] == j2->submission_times[0])
                return j1->id < j2->id;
            else
                return j1->submission_times[0] < j2->submission_times[0];
    };
    auto sort_original_submit_pair = [](const std::pair<const Job *,batsched_tools::KILL_TYPES> j1,const std::pair<const Job *,batsched_tools::KILL_TYPES> j2)->bool{
            if (j1.first->submission_times[0] == j2.first->submission_times[0])
                return j1.first->id < j2.first->id;
            else
                return j1.first->submission_times[0] < j2.first->submission_times[0];
    };
    //LOG_F(INFO,"killed_jobs %d",_killed_jobs);
    LOG_F(INFO,"line 385 _resubmitted_jobs.size %d",_resubmitted_jobs.size());
            std::string resub_jobs_str;
            for( auto mypair : _resubmitted_jobs)
            {
                resub_jobs_str += mypair.first +",";
            }
    //        LOG_F(INFO,"line 811 resub_jobs: %s",resub_jobs_str.c_str());
    LOG_F(INFO,"here");
    if (_killed_jobs && !_resubmitted_jobs.empty())
    {
      //  LOG_F(INFO,"killed_jobs !_jobs_release empty");
        if (_reschedule_policy == Schedule::RESCHEDULE_POLICY::AFFECTED)
        {
            //first we take care of sorting out resubmitted jobs from the
            //regular submitted jobs, since we will be handling the resubmitted jobs                      
            std::vector<std::string> recently_queued_jobs2;
            //go through recently_queued_jobs and get all the resubmitted ones
            for (std::string job_id : recently_queued_jobs)
            {
                //check if it's a resubmitted job
                if (job_id.find("#") != std::string::npos)
                {

                   auto forWhat = _resubmitted_jobs.at(job_id);
                   _resubmitted_jobs.erase(job_id);
                   _resubmitted_jobs_released.push_back(std::make_pair((*_workload)[job_id],forWhat));

                }
                else
                    recently_queued_jobs2.push_back(job_id); // it's a normal job, push it back
            }
            //set the recently_queued_jobs to a vector without the resubmitted jobs
            recently_queued_jobs = recently_queued_jobs2;

            //have all the resubmitted_jobs come back?
            if (_resubmitted_jobs.empty())
            {
                // _resubmitted_jobs_released should now contain all the resubmitted jobs
                //add the killed_jobs(resubmitted jobs) first

                //first sort all the resubmitted jobs based on their original submit date
                std::sort(_resubmitted_jobs_released.begin(),_resubmitted_jobs_released.end(),sort_original_submit_pair);
                //now get all jobs to reschedule that weren't killed
                std::vector<const Job *> all_jobs_to_reschedule;
                for(auto reservation : _saved_reservations)
                {
                
                    for (auto job : reservation.jobs_to_reschedule)
                            all_jobs_to_reschedule.push_back(job);
                
                }
                //now get all the jobs to reschedule that were affected by a machine going down
                std::map<const Job *,IntervalSet> jobs_affected;
                _schedule.get_jobs_affected_on_machines(_recently_under_repair_machines,jobs_affected);
                for(auto job_interval_pair : jobs_affected){
                    _schedule.remove_job_if_exists(job_interval_pair.first);
                    all_jobs_to_reschedule.push_back(job_interval_pair.first);
                }
                //now sort them based on original_submit_time
                std::sort(all_jobs_to_reschedule.begin(),all_jobs_to_reschedule.end(),sort_original_submit);

                //now add the kill jobs
                int queue_size = _queue->nb_jobs();
                int scheduled = _schedule.nb_jobs_size()-_schedule.get_number_of_running_jobs();
                for (auto job_forWhat_pair : _resubmitted_jobs_released)
                {   
                    if (_workload->_queue_depth != -1 && scheduled >=_workload->_queue_depth)
                        break;
                    
                    Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job_forWhat_pair.first,_selector,false);
                    if (!alloc.used_machines.is_empty())
                    {
                        scheduled++;
                        if (alloc.started_in_first_slice)
                        {
                            _queue->remove_job(job_forWhat_pair.first);
                            _decision->add_execute_job(job_forWhat_pair.first->id,alloc.used_machines,date);
                            scheduled--;
                        }
                    }
                }
                
                //now add them to the schedule
                for(auto job: all_jobs_to_reschedule)
                {
                    if (_workload->_queue_depth != -1 && scheduled >=_workload->_queue_depth)
                        break;
                   
                    Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job,_selector,false);
                    if (!alloc.used_machines.is_empty())
                    {
                        scheduled++;
                        if (alloc.started_in_first_slice)
                        {
                            _queue->remove_job(job);
                            _decision->add_execute_job(job->id,alloc.used_machines,date);
                            scheduled--;
                        }
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
                    else if (reservation.alloc->started_in_first_slice)
                    {
                        _reservation_queue->remove_job(reservation.job);
                        _decision->add_execute_job(reservation.job->id,reservation.alloc->used_machines,date);
                    }
                    else
                        _start_a_reservation = true;
                        
                }
                //make sure to clear the _resubmitted_jobs_released
                _resubmitted_jobs_released.clear();
                //we need to compress since things were moved around
                _need_to_compress = true;
            }
            
            
        }
        if (_reschedule_policy == Schedule::RESCHEDULE_POLICY::ALL)
        {
        //    LOG_F(INFO,"handle killed jobs sched_policy all");
            //first we take care of sorting out resubmitted jobs from the
            //regular submitted jobs, since we will be handling the resubmitted jobs                      
            std::vector<std::string> recently_queued_jobs2;
            //go through recently_queued_jobs and get all the resubmitted ones
            for (std::string job_id : recently_queued_jobs)
            {
                //check if it's a resubmitted job
                if (job_id.find("#") != std::string::npos)
                {
                   auto forWhat = _resubmitted_jobs.at(job_id);
                   _resubmitted_jobs.erase(job_id);
                   _resubmitted_jobs_released.push_back(std::pair((*_workload)[job_id],forWhat));

                }
                else
                    recently_queued_jobs2.push_back(job_id); // it's a normal job, push it back
            }
            //set the recently_queued_jobs to a vector without the resubmitted jobs
            recently_queued_jobs = recently_queued_jobs2;
          //  LOG_F(INFO,"line 385 _resubmitted_jobs.size %d",_resubmitted_jobs.size());
            
            for( auto mypair : _resubmitted_jobs)
            {
                resub_jobs_str += mypair.first +",";
            }
            //LOG_F(INFO,"line 811 resub_jobs: %s",resub_jobs_str.c_str());
            //have all the resubmitted_jobs come back?
            if (_resubmitted_jobs.empty())
            {
              //  LOG_F(INFO,"line 389");
                // _resubmitted_jobs_released should now contain all the resubmitted jobs

                //we have a sorted queue including the resubmitted jobs.   
                //we should be using the OriginalFCFSOrder on the queue so
                //the queue should be sorted by original submission time
                //remove all jobs from the schedule that are in the queue
                if (_output_svg == "all")
                        _schedule.output_to_svg("CONSERVATIVE_BF started removing");
                for (auto job_it = _queue->begin(); job_it != _queue->end();++job_it )
                {
                //    LOG_F(INFO,"job: %s ",(*job_it)->job->id.c_str());
                    //jobs that were affected by the reservation won't be in the schedule
                    //so we must use the _if_exists version
                    _schedule.remove_job_if_exists((*job_it)->job);
                }
                if (_output_svg == "all")
                        _schedule.output_to_svg("CONSERVATIVE_BF finished removing, adding them back in");
                //LOG_F(INFO,"line 402");
                //so, only the jobs running right now that weren't affected by the reservation
                //and the reservation itself and other reservations are still in the schedule
                //now add everything else back to the schedule
                int scheduled = 0;
                for (auto job_it = _queue->begin(); job_it != _queue->end(); )
                {
                    if (_workload->_queue_depth != -1 && scheduled >= _workload->_queue_depth)
                        break;
                    
                    const Job * job = (*job_it)->job;
                    Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, _selector,false);   
                    if (!alloc.used_machines.is_empty())
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
                if (_output_svg == "all")
                        _schedule.output_to_svg("CONSERVATIVE_BF adding them back in finished");
                //ok clear recently_queued_jobs since we just went through the entire queue
                recently_queued_jobs.clear();
                //everything should be rescheduled, now add callbacks for each reservation
                for (auto reservation : _saved_reservations)
                {
                    if (reservation.job->start > date)
                    _decision->add_call_me_later(batsched_tools::call_me_later_types::RESERVATION_START,
                                                reservation.job->unique_number,
                                                reservation.job->start,
                                                date );
                    else if (reservation.alloc->started_in_first_slice)
                    {
                        _reservation_queue->remove_job(reservation.job);
                        _decision->add_execute_job(reservation.job->id,reservation.alloc->used_machines,date);
                    }
                    else
                        _start_a_reservation = true;
                }
                //LOG_F(INFO,"line 429");
            }
        }
        if (_resubmitted_jobs.empty())
        {
            //LOG_F(INFO,"Setting _killed_jobs to false");
            _saved_reservations.clear();
            _killed_jobs = false;
        }
    } 
}



















void ConservativeBackfilling::handle_reservations(std::vector<std::string> & recently_released_reservations, 
                                                std::vector<std::string> &recently_queued_jobs,
                                                 double date)
{
    auto sort_original_submit = [](const Job * j1,const Job * j2)->bool{
            if (j1->submission_times[0] == j2->submission_times[0])
                return j1->id < j2->id;
            else
                return j1->submission_times[0] < j2->submission_times[0];
    };
    for (const string & new_job_id : recently_released_reservations)
    {
        LOG_F(INFO,"new reservation: %s queue:%s",new_job_id.c_str(),_queue->to_string().c_str());
        Job * new_job = (*_workload)[new_job_id];
        LOG_F(INFO,"job %s has walltime %g  start %f and alloc %s",new_job->id.c_str(),(double)new_job->walltime,new_job->start,new_job->future_allocations.to_string_hyphen(" ","-").c_str());
        //reserve a time slice 
        LOG_F(INFO,"resched policy %d",_reschedule_policy);

        if (_reschedule_policy == Schedule::RESCHEDULE_POLICY::AFFECTED)
        {
            
            Schedule::ReservedTimeSlice reservation = _schedule.reserve_time_slice(new_job);
            if (reservation.success == false)
            {
                _decision->add_reject_job(new_job_id,date);
                continue;
            }
            if (new_job->future_allocations.is_empty() && reservation.success)
                new_job->future_allocations = reservation.alloc->used_machines;

            _schedule.add_reservation_for_svg_outline(reservation);
            
            if (reservation.success)
            {
                //sort the jobs to reschedule;
                
                std::sort(reservation.jobs_to_reschedule.begin(), reservation.jobs_to_reschedule.end(),sort_original_submit);
                
                //we need to wait on rescheduling if jobs are killed
                //so check if any jobs need to be killed
                LOG_F(INFO,"here");
                if (reservation.jobs_needed_to_be_killed.empty())
                {//ok no jobs need to be killed, reschedule
                    
                    //remove the jobs in order all at once
                    for(auto job : reservation.jobs_to_reschedule)
                        _schedule.remove_job(job);
                    
                   
                    Schedule::ReservedTimeSlice reservation2 = _schedule.reserve_time_slice(new_job);
                    reservation.slice_begin = reservation2.slice_begin;
                    reservation.slice_end = reservation2.slice_end;
                    _schedule.add_reservation(reservation);
                    _schedule.remove_reservation_for_svg_outline(reservation);
                    Schedule::JobAlloc alloc;
                    //now add the rescheduled jobs in order
                    LOG_F(INFO,"here vec");
                    std::stringstream ss;
                    bool skip=true;
                    for(auto job : reservation.jobs_to_reschedule)
                    { 
                        if(skip)
                        { 
                            ss<<" ";
                            skip=false;
                        }
                        ss<< job->id;
                    }
                    LOG_F(INFO,"jobs to reschedule: %s",ss.str().c_str());
                    for(auto job : reservation.jobs_to_reschedule)
                    {
                        alloc = _schedule.add_job_first_fit(job,_selector,false);
                        //if this job starts in first slice execute it
                        if (!alloc.used_machines.is_empty())
                        {
                            if (alloc.started_in_first_slice)
                            {
                                _queue->remove_job(job);
                                _decision->add_execute_job(job->id,alloc.used_machines,date);
                            }
                        }
                    }
                   LOG_F(INFO,"here");
                    //if reservation started in first slice
                    if (reservation.alloc->started_in_first_slice)
                    {
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
                    //we need to compress since things moved around
                    _need_to_compress = true;
                }
                else{//ok some jobs do need to be killed
                    
                    //make decisions will run a couple times with the same date:
                    //  right now when job is killed -> when progress comes back and we resubmit -> when notified of resubmit and we reschedule
                    
                    //first kill jobs that need to be killed
                    LOG_F(INFO,"DEBUG line 248");
                    std::vector<batsched_tools::Job_Message *> msgs;
                    for(auto job : reservation.jobs_needed_to_be_killed)
                    {
                        auto msg = new batsched_tools::Job_Message;
                        msg->id = job->id;
                        msg->forWhat = batsched_tools::KILL_TYPES::RESERVATION;
                        msgs.push_back(msg);
                    }
                     
                    _decision->add_kill_job(msgs,date);//as long as kill jobs happen before an execute job we should be good
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
                    //we need to add the recently_queued_jobs to the schedule eventually
                    //but not until the killed jobs come back as resubmitted
                    for (std::string job : recently_queued_jobs)
                        _saved_recently_queued_jobs.push_back(job);
                }
            }
        }
        if (_reschedule_policy == Schedule::RESCHEDULE_POLICY::ALL)
        {
            Schedule::ReservedTimeSlice reservation = _schedule.reserve_time_slice(new_job);
            if (reservation.success == false)
            {
                _decision->add_reject_job(new_job_id,date);
                continue;
            }
            if (new_job->future_allocations.is_empty() && reservation.success)
                new_job->future_allocations = reservation.alloc->used_machines;
            _schedule.add_reservation_for_svg_outline(reservation);
            if (reservation.success)
            {
                // do we need to kill jobs
                if (reservation.jobs_needed_to_be_killed.empty())
                {
                    //we don't need to kill jobs
                    //remove jobs in the reservation's way
                    for (auto job : reservation.jobs_to_reschedule)
                        _schedule.remove_job(job);
                    //we can make the reservation now
                    Schedule::ReservedTimeSlice reservation2 = _schedule.reserve_time_slice(new_job);
                    reservation.slice_begin = reservation2.slice_begin;
                    reservation.slice_end = reservation2.slice_end;
                    _schedule.add_reservation(reservation);
                    _schedule.remove_reservation_for_svg_outline(reservation);
                    if (_output_svg == "all")
                        _schedule.output_to_svg("CONSERVATIVE_BF about to remove all jobs in the queue");
                    //we have a sorted queue including resubmitted jobs.   
                    //we should be using the OriginalFCFSOrder on the queue so
                    //the queue should be sorted by original submission time
                    //remove all jobs from the schedule that are in the queue
                    for (auto job_it = _queue->begin(); job_it != _queue->end(); ++job_it)
                    {
                        //jobs that were affected by the reservation won't be in the schedule
                        //so we must use the _if_exists version
                        _schedule.remove_job_if_exists((*job_it)->job);
                    }
                    if (_output_svg == "all")
                        _schedule.output_to_svg("CONSERVATIVE_BF finished removing, adding them back in");
                    
                    //so, only the jobs running right now
                    //and the reservation itself and other reservations are still in the schedule
                    //now add everything else back to the schedule
                    int scheduled=0;
                    for (auto job_it = _queue->begin(); job_it != _queue->end(); )
                    {
                        const Job * job = (*job_it)->job;
                        Schedule::JobAlloc alloc = _schedule.add_job_first_fit(job, _selector,false);   
                        if (!alloc.used_machines.is_empty())
                        {
                            scheduled++;
                        
                            if (alloc.started_in_first_slice)
                            {
                                _decision->add_execute_job(job->id, alloc.used_machines, date);
                                job_it = _queue->remove_job(job_it);
                            }
                            else
                                ++job_it;
                        }
                        else
                            ++job_it;
                    }
                    if (_output_svg == "all")
                        _schedule.output_to_svg("CONSERVATIVE_BF finished adding them back in");
                    //ok clear recently_queued_jobs since we just went through the entire queue
                    recently_queued_jobs.clear();
                    //take care of reservation that is in first slice
                    if (reservation.alloc->started_in_first_slice)
                    {
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
                else
                {
                    //ok we need to kill jobs
                    //first kill jobs
                    LOG_F(INFO,"DEBUG line 248");
                    std::vector<batsched_tools::Job_Message *> msgs;
                    for(auto job : reservation.jobs_needed_to_be_killed)
                    {
                        auto msg = new batsched_tools::Job_Message;
                        msg->id = job->id;
                        msg->forWhat = batsched_tools::KILL_TYPES::RESERVATION;
                        msgs.push_back(msg);
                    }
                     
                    _decision->add_kill_job(msgs,date);//as long as kill jobs happen before an execute job we should be good
                    LOG_F(INFO,"DEBUG line 253");
                    //make room for the reservation first
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
                    //we need this for issuing the callbacks
                    _saved_reservations.push_back(reservation);
                    LOG_F(INFO,"line 307");
                    //we need to signal we are in a killed_jobs event so keep
                    _killed_jobs = true;
                    //we don't need to save the recently_queued_jobs
                    //since once we get the killed jobs resubmitted we are adding in all of the jobs back into the queue
                    //for (std::string job : recently_queued_jobs)
                    //    _saved_recently_queued_jobs.push_back(job);
                    
                }
                    
                
            }
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
}

