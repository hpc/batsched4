#include "easy_bf2.hpp"

#include <loguru.hpp>

#include "../pempek_assert.hpp"

//added
#include "../batsched_tools.hpp"

using namespace std;

EasyBackfilling2::EasyBackfilling2(Workload * workload,
                                 SchedulingDecision * decision,
                                 Queue * queue,
                                 ResourceSelector * selector,
                                 double rjms_delay,
                                 rapidjson::Document * variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{
        //initialize reservation queue
    SortableJobOrder * order = new FCFSOrder;//reservations do not get killed so we do not need OriginalFCFSOrder for this
    _reservation_queue = new Queue(order);
}

EasyBackfilling2::~EasyBackfilling2()
{

}

void EasyBackfilling2::on_simulation_start(double date, const rapidjson::Value & batsim_event)
{
   //added
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
    
    
    //was there
    _schedule = Schedule(_nb_machines, date);
    //added
    _schedule.set_output_svg(_output_svg);
    _schedule.set_svg_frame_and_output_start_and_end(_svg_frame_start,_svg_frame_end,_svg_output_start,_svg_output_end);
    _schedule.set_svg_prefix(_output_folder + "/svg/");
    _schedule.set_policies(_reschedule_policy,_impact_policy);
    ISchedulingAlgorithm::set_generators(date);
    
    _recently_under_repair_machines = IntervalSet::empty_interval_set();

    //re-intialize queue if necessary
    if (batsim_config["queue-policy"].GetString() == "ORIGINAL-FCFS")
    {
        //ok we need to delete the _queue pointer and make a new queue
        delete _queue;
        SortableJobOrder * order = new OriginalFCFSOrder;
        _queue = new Queue(order);

    }
    
    (void) batsim_config;
}

void EasyBackfilling2::on_simulation_end(double date)
{
    (void) date;
}
void EasyBackfilling2::set_machines(Machines *m){
    _machines = m;
}
void EasyBackfilling2::on_machine_down_for_repair(batsched_tools::KILL_TYPES forWhat,double date){
    (void) date;
    auto sort_original_submit_pair = [](const std::pair<const Job *,IntervalSet> j1,const std::pair<const Job *,IntervalSet> j2)->bool{
            if (j1.first->submission_times[0] == j2.first->submission_times[0])
                return j1.first->id < j2.first->id;
            else
                return j1.first->submission_times[0] < j2.first->submission_times[0];
    };
   
    //get a random number of a machine to kill
    int number = machine_unif_distribution->operator()(generator_machine);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = (*_machines)[number]->id;
    
    if (_output_svg == "all")
            _schedule.output_to_svg("On Machine Down For Repairs  Machine #:  "+std::to_string((*_machines)[number]->id));
    double repair_time = (*_machines)[number]->repair_time;
    //if there is a global repair time set that as the repair time
    if (_workload->_repair_time != -1.0)
        repair_time = _workload->_repair_time;
    if (_workload->_MTTR != -1.0)
        repair_time = repair_time_exponential_distribution->operator()(generator_repair_time);
    IntervalSet added = IntervalSet::empty_interval_set() ;
    if (_schedule.get_reservations_running_on_machines(machine).empty())
        added = _schedule.add_repair_machine(machine,repair_time);

    LOG_F(INFO,"here");
    //if the machine is already down for repairs ignore it.
    //LOG_F(INFO,"repair_machines.size(): %d    nb_avail: %d  avail:%d running_jobs: %d",_repair_machines.size(),_nb_available_machines,_available_machines.size(),_running_jobs.size());
    //BLOG_F(b_log::FAILURES,"Machine Repair: %d",number);
    if (!added.is_empty())
    {
        
        _recently_under_repair_machines+=machine; //haven't found a use for this yet
        _schedule.add_svg_highlight_machines(machine);
        //ok the machine is not down for repairs already so it WAS added
        //the failure/repair will not be happening on a machine that has a reservation on it either
        //it will be going down for repairs now
        
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
            //in conservative_bf we reschedule everything
            //in easy_bf only backfilled jobs,running jobs and priority job is scheduled
            //but there may not be enough machines to run the priority job
            //move the priority job to after the repair time and let things backfill ahead of that.
            



                    if (_output_svg == "all")
                        _schedule.output_to_svg("Finished Machine Down For Repairs, Machine #: "+std::to_string(number));

                
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


void EasyBackfilling2::on_machine_instant_down_up(batsched_tools::KILL_TYPES forWhat,double date){
    (void) date;
    //get a random number of a machine to kill
    int number = machine_unif_distribution->operator()(generator_machine);
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
void EasyBackfilling2::on_requested_call(double date,int id,batsched_tools::call_me_later_types forWhat)
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
                                    double number = failure_exponential_distribution->operator()(generator_failure);
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

void EasyBackfilling2::make_decisions(double date,
                                     SortableJobOrder::UpdateInformation *update_info,
                                     SortableJobOrder::CompareInformation *compare_info)
{
    const Job * priority_job_before = _queue->first_job_or_nullptr();

    // Let's remove finished jobs from the schedule
    for (const string & ended_job_id : _jobs_ended_recently)
        _schedule.remove_job((*_workload)[ended_job_id]);

    // Let's handle recently released jobs
    std::vector<std::string> recently_queued_jobs;
    for (const string & new_job_id : _jobs_released_recently)
    {
        const Job * new_job = (*_workload)[new_job_id];

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

    // Let's update the schedule's present
    _schedule.update_first_slice(date);



    //We will want to handle any Failures before we start allowing anything new to run
    //This is very important for when there are repair times, as the machine may be down


    //handle any instant down ups (no repair time on machine going down)
    for(batsched_tools::KILL_TYPES forWhat : _on_machine_instant_down_ups)
    {
        on_machine_instant_down_up(forWhat,date);
    }
    //ok we handled them all, clear the container
    _on_machine_instant_down_ups.clear();
    //handle any machine down for repairs (machine going down with a repair time)
    for(batsched_tools::KILL_TYPES forWhat : _on_machine_down_for_repairs)
    {
        on_machine_down_for_repair(forWhat,date);
    }
    //ok we handled them all, clear the container
    _on_machine_down_for_repairs.clear();




    // Queue sorting
    const Job * priority_job_after = nullptr;
    sort_queue_while_handling_priority_job(priority_job_before, priority_job_after, update_info, compare_info);

    // If no resources have been released, we can just try to backfill the newly-released jobs
    if (_jobs_ended_recently.empty())
    {
        int nb_available_machines = _schedule.begin()->available_machines.size();

        for (unsigned int i = 0; i < recently_queued_jobs.size() && nb_available_machines > 0; ++i)
        {
            const string & new_job_id = recently_queued_jobs[i];
            const Job * new_job = (*_workload)[new_job_id];

            // The job could have already been executed by sort_queue_while_handling_priority_job,
            // that's why we check whether the queue contains the job.
            if (_queue->contains_job(new_job) &&
                new_job != priority_job_after &&
                new_job->nb_requested_resources <= nb_available_machines)
            {
                JobAlloc alloc = _schedule.add_job_first_fit(new_job, _selector);
                if ( alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(new_job_id, alloc.used_machines, date);
                    _queue->remove_job(new_job);
                    nb_available_machines -= new_job->nb_requested_resources;
                }
                else
                    _schedule.remove_job(new_job);
            }
        }
    }
    else
    {
        // Some resources have been released, the whole queue should be traversed.
        auto job_it = _queue->begin();
        int nb_available_machines = _schedule.begin()->available_machines.size();

        // Let's try to backfill all the jobs
        while (job_it != _queue->end() && nb_available_machines > 0)
        {
            const Job * job = (*job_it)->job;

            if (_schedule.contains_job(job))
                _schedule.remove_job(job);

            if (job == priority_job_after) // If the current job is priority
            {
                JobAlloc alloc = _schedule.add_job_first_fit(job, _selector);

                if (alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(job->id, alloc.used_machines, date);
                    job_it = _queue->remove_job(job_it); // Updating job_it to remove on traversal
                    priority_job_after = _queue->first_job_or_nullptr();
                }
                else
                    ++job_it;
            }
            else // The job is not priority
            {
                JobAlloc alloc = _schedule.add_job_first_fit(job, _selector);

                if (alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(job->id, alloc.used_machines, date);
                    job_it = _queue->remove_job(job_it);
                }
                else
                {
                    _schedule.remove_job(job);
                    ++job_it;
                }
            }
        }
    }
}


void EasyBackfilling2::sort_queue_while_handling_priority_job(const Job * priority_job_before,
                                                             const Job *& priority_job_after,
                                                             SortableJobOrder::UpdateInformation * update_info,
                                                             SortableJobOrder::CompareInformation * compare_info)
{
    if (_debug)
        LOG_F(1, "sort_queue_while_handling_priority_job beginning, %s", _schedule.to_string().c_str());

    // Let's sort the queue
    _queue->sort_queue(update_info, compare_info);

    // Let the new priority job be computed
    priority_job_after = _queue->first_job_or_nullptr();

    // If the priority job has changed
    if (priority_job_after != priority_job_before)
    {
        // If there was a priority job before, let it be removed from the schedule
        if (priority_job_before != nullptr)
            _schedule.remove_job_if_exists(priority_job_before);

        // Let us ensure the priority job is in the schedule.
        // To do so, while the priority job can be executed now, we keep on inserting it into the schedule
        for (bool could_run_priority_job = true; could_run_priority_job && priority_job_after != nullptr; )
        {
            could_run_priority_job = false;

            // Let's add the priority job into the schedule
            JobAlloc alloc = _schedule.add_job_first_fit(priority_job_after, _selector);

            if (alloc.started_in_first_slice)
            {
                _decision->add_execute_job(priority_job_after->id, alloc.used_machines, (double)update_info->current_date);
                _queue->remove_job(priority_job_after);
                priority_job_after = _queue->first_job_or_nullptr();
                could_run_priority_job = true;
            }
        }
    }

    if (_debug)
        LOG_F(1, "sort_queue_while_handling_priority_job ending, %s", _schedule.to_string().c_str());
}
