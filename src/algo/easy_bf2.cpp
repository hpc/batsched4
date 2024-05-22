#include "easy_bf2.hpp"
#include <loguru.hpp>

#include "../pempek_assert.hpp"
//added
#include "../batsched_tools.hpp"
using namespace std;
#define B_LOG_INSTANCE _myBLOG

EasyBackfilling2::EasyBackfilling2(Workload * workload,
                                 SchedulingDecision * decision,
                                 Queue * queue,
                                 ResourceSelector * selector,
                                 double rjms_delay,
                                 rapidjson::Document * variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{
    SortableJobOrder * order = new FCFSOrder;//reservations do not get killed so we do not need OriginalFCFSOrder for this
    // @note LESLIE commented out 
    //_reservation_queue = new Queue(order);
}

EasyBackfilling2::~EasyBackfilling2()
{

}

/*********************************************************
 *                  STATE HANDLING FUNCTIONS             *
**********************************************************/

void EasyBackfilling2::on_simulation_start(double date, const rapidjson::Value & batsim_event)
{
   //added
    ISchedulingAlgorithm::normal_start(date,batsim_event);
    ISchedulingAlgorithm::schedule_start(date,batsim_event);
    _recently_under_repair_machines = IntervalSet::empty_interval_set();
}

void EasyBackfilling2::on_simulation_end(double date)
{
    (void) date;
}

void EasyBackfilling2::set_machines(Machines *m){
    _machines = m;
}

/*********************************************************
 *               REAL CHECKPOINTING FUNCTIONS            *
**********************************************************/
// @note Leslie added on_start_from_checkpoint(double date,const rapidjson::Value & batsim_event)
void EasyBackfilling2::on_start_from_checkpoint(double date,const rapidjson::Value & batsim_event){
    ISchedulingAlgorithm::on_start_from_checkpoint_normal(date,batsim_event);
    ISchedulingAlgorithm::on_start_from_checkpoint_schedule(date,batsim_event);

}
// @note Leslie added on_checkpoint_batsched(double date)
void EasyBackfilling2::on_checkpoint_batsched(double date)
{
    CLOG_F(CCU_DEBUG_ALL,"here");
        
}
// @note Leslie added on_ingest_variables(const rapidjson::Document & doc,double date)
void EasyBackfilling2::on_ingest_variables(const rapidjson::Document & doc,double date)
{
    //this code should not be run because the doc, at this point (5-13-2024) does not have a "derived" member
    //using namespace rapidjson;
    //const Value & derived = doc["derived"];
    
}

// @note Leslie added on_first_jobs_submitted(double date)
void EasyBackfilling2::on_first_jobs_submitted(double date)
{
    
}

/*********************************************************
 *            SIMULATED FAILURES FUNCTIONS               *
**********************************************************/

void EasyBackfilling2::on_machine_down_for_repair(batsched_tools::KILL_TYPES forWhat,double date){
    (void) date;
    
    IntervalSet machine = ISchedulingAlgorithm::normal_repair(date);
    
    ISchedulingAlgorithm::schedule_repair(machine,forWhat,date);
    

}

void EasyBackfilling2::on_machine_instant_down_up(batsched_tools::KILL_TYPES forWhat,double date){
    (void) date;
    IntervalSet machine = ISchedulingAlgorithm::normal_downUp(date);
    ISchedulingAlgorithm::schedule_downUp(machine,forWhat,date);
}

// @note Leslie modified on_requested_call(double date,int id,batsched_tools::call_me_later_types forWhat)
void EasyBackfilling2::on_requested_call(double date,batsched_tools::CALL_ME_LATERS cml_in)
{
        if (_output_svg != "none")
            _schedule.set_now((Rational)date);
        LOG_F(INFO,"DEBUG");
        switch (cml_in.forWhat){
            case batsched_tools::call_me_later_types::SMTBF: 
            case batsched_tools::call_me_later_types::MTBF:
            case batsched_tools::call_me_later_types::FIXED_FAILURE:
                if (_schedule.get_number_of_running_jobs() > 0 || !_queue->is_empty() || !_no_more_static_job_to_submit_received)
                       ISchedulingAlgorithm::requested_failure_call(date,cml_in);
                break;
                          
            case batsched_tools::call_me_later_types::REPAIR_DONE:
                ISchedulingAlgorithm::requested_failure_call(date,cml_in);
                break;
            case batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED:
                _need_to_checkpoint = true;
                break;
        }


}

/*********************************************************
 *                  DECICSION FUNCTIONS                  *
**********************************************************/

void EasyBackfilling2::make_decisions(double date,
                                     SortableJobOrder::UpdateInformation *update_info,
                                     SortableJobOrder::CompareInformation *compare_info)
{

    // @note Leslie added 
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
        
    CLOG_F(CCU_DEBUG_ALL,"here");
    if (_output_svg != "none")
        _schedule.set_now((Rational)date);
    CLOG_F(CCU_DEBUG_FIN,"make decisions");

    //define a sort function for sorting jobs based on original submit times
    auto sort_original_submit = [](const Job * j1,const Job * j2)->bool{
        if (j1->submission_times[0] == j2->submission_times[0])
            return j1->id < j2->id;
        else
            return j1->submission_times[0] < j2->submission_times[0];
    };


    const Job * priority_job_before = _queue->first_job_or_nullptr();
    CLOG_F(CCU_DEBUG,"removing finished jobs");
    // Let's remove finished jobs from the schedule
    for (const string & ended_job_id : _jobs_ended_recently)
        _schedule.remove_job_if_exists((*_workload)[ended_job_id]);

    // Let's handle recently released jobs
    CLOG_F(CCU_DEBUG,"handling released recently");
    std::vector<std::string> recently_queued_jobs;
    for (const string & new_job_id : _jobs_released_recently)
    {
        // @note Leslie added 
        if (!_start_from_checkpoint.received_submitted_jobs)
            _start_from_checkpoint.first_submitted_time = date;
        _start_from_checkpoint.received_submitted_jobs = true;
        
        const Job * new_job = (*_workload)[new_job_id];

        if (new_job->nb_requested_resources > _nb_machines)
        {
            _decision->add_reject_job(date,new_job_id, batsched_tools::REJECT_TYPES::NOT_ENOUGH_RESOURCES);
        }
        else if (!new_job->has_walltime)
        {
            LOG_SCOPE_FUNCTION(INFO);
            CLOG_F(INFO, "Date=%g. Rejecting job '%s' as it has no walltime", date, new_job_id.c_str());
            _decision->add_reject_job(date,new_job_id, batsched_tools::REJECT_TYPES::NO_WALLTIME);
        }
        else
        {
            _queue->append_job(new_job, update_info);
            recently_queued_jobs.push_back(new_job_id);
        }
    }
    // @note Leslie added 
    if (ISchedulingAlgorithm::ingest_variables_if_ready(date))
        return;


    CLOG_F(CCU_DEBUG_FIN,"updating first slice");
    // Let's update the schedule's present
    _schedule.update_first_slice(date);

    //We will want to handle any Failures before we start allowing anything new to run
    //This is very important for when there are repair times, as the machine may be down


    ISchedulingAlgorithm::handle_failures(date);
    LOG_F(INFO,"here");
    CLOG_F(CCU_DEBUG,"handled instant down ups and down for repairs. handling resubmission");
    //ok we handled them all, clear the container
    _on_machine_down_for_repairs.clear();
    for ( auto job_message_pair : _jobs_killed_recently)
    {
        batsched_tools::id_separation separation = batsched_tools::tools::separate_id(job_message_pair.first);
        LOG_F(INFO,"next_resubmit_string %s",separation.next_resubmit_string.c_str());
        _resubmitted_jobs[separation.next_resubmit_string]=job_message_pair.second->forWhat;
    }
    if (!_resubmitted_jobs.empty())
    {
        for ( auto job_id : recently_queued_jobs)
        {
            _resubmitted_jobs.erase(job_id);
        }
        if (_resubmitted_jobs.empty())
            _killed_jobs = false;
    }
    _decision->handle_resubmission(_jobs_killed_recently,_workload,date);
    LOG_F(INFO,"here");
    CLOG_F(CCU_DEBUG,"handled resubmission. bout to sort queue while handling priority job");
    if (_output_svg == "short")
        _schedule.output_to_svg("before");
    // Queue sorting
    const Job * priority_job_after = nullptr;
    sort_queue_while_handling_priority_job(priority_job_before, priority_job_after, update_info, compare_info);
    LOG_F(INFO,"here");
    CLOG_F(CCU_DEBUG,"bout to backfill");
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
                    _schedule.remove_job_if_exists(new_job);
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
            std::string message = "backfill job: " + job->id;
            if (_output_svg == "all")
                _schedule.output_to_svg(message);
            CLOG_F(CCU_DEBUG_ALL,"backfill remove job first %s",job->id.c_str());
            if (_schedule.contains_job(job))
                _schedule.remove_job_if_exists(job);
            CLOG_F(CCU_DEBUG_ALL,"after remove job");

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
                if (_output_svg == "all")
                    _schedule.output_to_svg("added job first fit");
                if (alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(job->id, alloc.used_machines, date);
                    job_it = _queue->remove_job(job_it);
                }
                else
                {
                    CLOG_F(CCU_DEBUG_ALL,"backfill remove job second %s",job->id.c_str());
                    _schedule.remove_job_if_exists(job);
                    ++job_it;
                }
            }
        }
    }
    LOG_F(INFO,"here");
    if (_output_svg == "short")
        _schedule.output_to_svg("after");
    if (!_killed_jobs && _jobs_killed_recently.empty() && _queue->is_empty()  && _schedule.size() == 0 &&
            _need_to_send_finished_submitting_jobs && _no_more_static_job_to_submit_received && !(date<1.0) )
    {
      //  LOG_F(INFO,"finished_submitting_jobs sent");
      LOG_F(INFO,"here");
        _decision->add_scheduler_finished_submitting_jobs(date);
        if (_output_svg == "all" || _output_svg == "short")
            _schedule.output_to_svg("Simulation Finished");
        _schedule.set_output_svg("none");
        _output_svg = "none";
        _need_to_send_finished_submitting_jobs = false;
    }
    LOG_F(INFO,"here");
    CLOG_F(CCU_DEBUG_ALL,"here");
    //descriptive log statement
    
    LOG_F(INFO,"!killed= %d  jkr = %d  qie = %d ss = %d ntsfsj = %d nmsjtsr = %d\n _queue: %s",
    !_killed_jobs,_jobs_killed_recently.empty(), _queue->is_empty(), _schedule.size(),
             _need_to_send_finished_submitting_jobs , _no_more_static_job_to_submit_received,_queue->to_string().c_str());

    //if there are jobs that can't run then we need to start rejecting them at this point
    if (!_killed_jobs && _jobs_killed_recently.empty() && _schedule.size() == 0 &&
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
            for ( auto itr = _queue->begin();itr!=_queue->end();)
            {
              //   LOG_F(INFO,"here");
              //  LOG_F(INFO,"Rejecting job %s",(*itr)->job->id.c_str());
                _decision->add_reject_job(date,(*itr)->job->id,batsched_tools::REJECT_TYPES::NOT_ENOUGH_AVAILABLE_RESOURCES);
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


void EasyBackfilling2::sort_queue_while_handling_priority_job(const Job * priority_job_before,
                                                             const Job *& priority_job_after,
                                                             SortableJobOrder::UpdateInformation * update_info,
                                                             SortableJobOrder::CompareInformation * compare_info)
{
    CLOG_F(CCU_DEBUG_MAX, "sort_queue_while_handling_priority_job beginning, %s", _schedule.to_string().c_str());

    // Let's sort the queue
    _queue->sort_queue(update_info, compare_info);

    // Let the new priority job be computed
    priority_job_after = _queue->first_job_or_nullptr();
    if (_output_svg == "short")
        _schedule.output_to_svg("sort queue while handling priority job");
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

    CLOG_F(CCU_DEBUG_MAX, "sort_queue_while_handling_priority_job ending, %s", _schedule.to_string().c_str());
}
