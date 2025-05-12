#include "conservative_bf_metrics_roci.hpp"

#include <loguru.hpp>

#include "../pempek_assert.hpp"
#include <fstream>
#include <chrono>
#include <ctime>
#include "../json_workload.hpp"
#include "../schedule.hpp"
#define B_LOG_INSTANCE _myBLOG
using namespace std;
void ConservativeBackfilling_metrics_roci::on_checkpoint_batsched(double date)
{
    (void)date;
}
void ConservativeBackfilling_metrics_roci::on_ingest_variables(const rapidjson::Document & doc,double date)
{
 


}

ConservativeBackfilling_metrics_roci::ConservativeBackfilling_metrics_roci(Workload *workload, SchedulingDecision *decision,
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
    SortableJobOrder * order = new FCFSOrder;//reservations do not get killed so we do not need OriginalFCFSOrder for this
    _reservation_queue = new Queue(order);
    
}

ConservativeBackfilling_metrics_roci::~ConservativeBackfilling_metrics_roci()
{
}

void ConservativeBackfilling_metrics_roci::on_start_from_checkpoint(double date,const rapidjson::Value & batsim_event){
    ISchedulingAlgorithm::on_start_from_checkpoint_normal(date,batsim_event);
    batsched_tools::CALL_ME_LATERS cml;
    cml.forWhat = batsched_tools::call_me_later_types::METRICS;
    cml.id = _decision->get_nb_call_me_laters();
    _decision->add_call_me_later(date,date+3600,cml);
    _myBLOG->add_log_file(_output_folder+"/metrics.csv",_metrics_type,blog_open_method::APPEND,true,";");
    ISchedulingAlgorithm::on_start_from_checkpoint_schedule(date,batsim_event);

}

void ConservativeBackfilling_metrics_roci::on_first_jobs_submitted(double date)
{
    
}
void ConservativeBackfilling_metrics_roci::on_simulation_start(double date, const rapidjson::Value & batsim_event)
{
    ISchedulingAlgorithm::normal_start(date,batsim_event);
    ISchedulingAlgorithm::schedule_start(date,batsim_event);
    
    _recently_under_repair_machines = IntervalSet::empty_interval_set();
    _reservation_algorithm = true;
    
   batsched_tools::CALL_ME_LATERS cml;
    cml.forWhat = batsched_tools::call_me_later_types::METRICS;
    cml.id = _decision->get_nb_call_me_laters();
    _decision->add_call_me_later(date,date+3600,cml);
    _myBLOG->add_log_file(_output_folder+"/metrics.csv",_metrics_type,blog_open_method::OVERWRITE,true,";");
    std::string header = "sim_time;hour;state";
    _myBLOG->add_header(_metrics_type,header);
}


void ConservativeBackfilling_metrics_roci::on_simulation_end(double date)
{
    (void) date;
}
/*
void ConservativeBackfilling_metrics_roci::set_workloads(myBatsched::Workloads *w){
    _myWorkloads = w;
    _checkpointing_on = w->_checkpointing_on;
    
}
*/
void ConservativeBackfilling_metrics_roci::set_machines(Machines *m){
    _machines = m;
}
void ConservativeBackfilling_metrics_roci::on_machine_down_for_repair(batsched_tools::KILL_TYPES forWhat,double date){
    (void) date;
    
    IntervalSet machine = ISchedulingAlgorithm::normal_repair(date);
    ISchedulingAlgorithm::schedule_repair(machine,forWhat,date);
}


void ConservativeBackfilling_metrics_roci::on_machine_instant_down_up(batsched_tools::KILL_TYPES forWhat,double date){
    (void) date;
    //get a random number of a machine to kill
    LOG_F(INFO,"here");
    IntervalSet machine = ISchedulingAlgorithm::normal_downUp(date);
    ISchedulingAlgorithm::schedule_downUp(machine,forWhat,date);
    
    
}
void ConservativeBackfilling_metrics_roci::on_requested_call(double date,batsched_tools::CALL_ME_LATERS cml_in)
{
          
    LOG_F(ERROR,"here");
    if (_output_svg != "none")
        _schedule.set_now((Rational)date);
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
        case batsched_tools::call_me_later_types::RESERVATION_START:
            _start_a_reservation = true;
            break;
        case batsched_tools::call_me_later_types::CHECKPOINT_SYNC:
            _checkpoint_sync++;
            break;
        case batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED:
            _need_to_checkpoint = true;
            break;
        case batsched_tools::call_me_later_types::METRICS:
            if (!_recover_from_checkpoint)
                _need_to_write_metrics = true;
            _metrics.hour++;
            batsched_tools::CALL_ME_LATERS cml;
            cml.forWhat = batsched_tools::call_me_later_types::METRICS;
            cml.id = _decision->get_nb_call_me_laters();
            _decision->add_call_me_later(date,date+3600,cml);
            break;
        }
        //sometimes we get back a call me later at the wrong time, this handles that
        double difference = _decision->remove_call_me_later(cml_in,date,_workload);
        if (difference != 0 && cml_in.forWhat == batsched_tools::call_me_later_types::RESERVATION_START)
        {
            LOG_F(INFO,"difference: %f",difference);
            BLOG_F(blog_types::SOFT_ERRORS,"There was a requested_call error. The original date was %.15f, but was pushed out %.15f seconds, resulting in time %.15f",date-difference,difference,date);
            _schedule.incorrect_call_me_later(difference);
        }
}
void ConservativeBackfilling_metrics_roci::get_queue_metrics(metrics_roci& metrics)
{
    std::string running_string = "[";
    std::string queue_string = "[";
    bool first = true;
    LOG_F(INFO,"here");
    int queue_count=0;
    for (auto job_it = _queue->begin();job_it !=_queue->end();++job_it)
    {
        
        const Job * job = (*job_it)->job;
        //total_nodes += job->nb_requested_resources;
        //myBatsched::ParallelHomogeneousProfileData * data = static_cast<myBatsched::ParallelHomogeneousProfileData *>(job->profile->data);
        //time +=data->cpu*SPEED;
        //LOG_F(INFO,"here");
            auto time_slice_it = _schedule.find_first_occurence_of_job(job,_schedule.begin());
            LOG_F(INFO,"here");
            Rational start_time = time_slice_it->begin;
            Rational end_time = start_time + job->walltime;
            if (first)
            {
                queue_string += batsched_tools::string_format("{\"id\":%d,\"start\":%f,\"stop\":%f}",batsched_tools::get_job_parts(job->id).job_number,start_time.convert_to<double>(),end_time.convert_to<double>());
                first = false;
            }
            else
                queue_string += batsched_tools::string_format(",{\"id\":%d,\"start\":%f,\"stop\":%f}",batsched_tools::get_job_parts(job->id).job_number,start_time.convert_to<double>(),end_time.convert_to<double>());
            LOG_F(INFO,"here");
        queue_count++;
        if (queue_count == _workload->_queue_depth)
            break;

        
    }
    queue_string +="]";
    first = true;
    for (auto job_it = _schedule.begin()->allocated_jobs.begin();job_it != _schedule.begin()->allocated_jobs.end();++job_it)
    {
        const Job * job = (*job_it).first;
        Rational end_time = _schedule.find_last_occurence_of_job(job,_schedule.begin())->end;
        if (first)
        {
            running_string += batsched_tools::string_format("{\"id\":%d,\"stop\":%f}",batsched_tools::get_job_parts(job->id).job_number,end_time.convert_to<double>());
            first = false;
        }
        else
            running_string += batsched_tools::string_format(",{\"id\":%d,\"stop\":%f}",batsched_tools::get_job_parts(job->id).job_number,end_time.convert_to<double>());
    }
    running_string +="]";
    metrics.queue_string = queue_string;
    metrics.running_string = running_string;

}


void ConservativeBackfilling_metrics_roci::make_decisions(double date,
                                             SortableJobOrder::UpdateInformation *update_info,
                                             SortableJobOrder::CompareInformation *compare_info)
{
        
    if (_exit_make_decisions)
    {   
        _exit_make_decisions = false;     
        return;
    }
    LOG_F(INFO,"batsim_checkpoint_seconds: %d",_batsim_checkpoint_interval_seconds);
    send_batsim_checkpoint_if_ready(date);
    LOG_F(INFO,"here");
    if (_need_to_checkpoint)
        checkpoint_batsched(date);
    LOG_F(INFO,"here");
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
    
    for (const std::string & ended_job_id : _jobs_ended_recently)
    {
        //_end_metrics.arrived = true;
        if (_schedule.remove_job_if_exists((*_workload)[ended_job_id]))
            LOG_F(INFO,"ended_job_id: %s",ended_job_id.c_str());
        else
            LOG_F(INFO,"ended_no_find: %s",ended_job_id.c_str());
    }
    //get_x_y_z_queue_metrics(xyz::QUEUE_AND_UTIL_METRICS,_end_metrics);
    
    LOG_F(INFO,"after jobs ended");
    // Let's handle recently released jobs
    //keep track of what was added to both queues this time around
    std::vector<std::string> recently_queued_jobs;
    std::vector<std::string> recently_released_reservations;
    bool first_loop = true;
    for (const std::string & new_job_id : _jobs_released_recently)
    {
        
        if (!_start_from_checkpoint.received_submitted_jobs)
            _start_from_checkpoint.first_submitted_time = date;
        _start_from_checkpoint.received_submitted_jobs = true;
        LOG_F(INFO,"here");
        const Job * new_job = (*_workload)[new_job_id];
        LOG_F(INFO,"job %s has purpose %s  and walltime: %g",new_job->id.c_str(),new_job->purpose.c_str(),(double)new_job->walltime);
        
        if (new_job->purpose!="reservation")
        {
            if (new_job->nb_requested_resources > _nb_machines)
            {
                _decision->add_reject_job(date,new_job_id, batsched_tools::REJECT_TYPES::NOT_ENOUGH_RESOURCES);
            }
            else if (!new_job->has_walltime)
            {
                LOG_SCOPE_FUNCTION(INFO);
                LOG_F(INFO, "Date=%g. Rejecting job '%s' as it has no walltime", date, new_job_id.c_str());
                _decision->add_reject_job(date,new_job_id, batsched_tools::REJECT_TYPES::NO_WALLTIME);
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
    LOG_F(INFO,"here");
    //get_x_y_z_queue_metrics(xyz::QUEUE_AND_UTIL_METRICS,_arrive_metrics);
    //ingesting variables after a start_from_checkpoint starts when all jobs that were running have been submitted
    if (ISchedulingAlgorithm::ingest_variables_if_ready(date))
        return;

    /********* The following are just thoughts I don't want to get rid of
     * they don't explain the update_first_slice() call
        //before we update the first slice make sure we have jobs in the schedule other than reservations

        //int schedule_size = _schedule.size() - _schedule.nb_reservations_size();
        //if (!_killed_jobs && _jobs_killed_recently.empty() && _reservation_queue->is_empty && schedule_size > 0 && _no_more_static_job_to_submit_received)
    
        
    **********/
        if (_output_svg == "short")
        _schedule.output_to_svg("make_decisions",true);
    // Let's update the schedule's present. it may put a reservation in a position to run
    _schedule.update_first_slice(date);
    if (_output_svg == "all" )
    _schedule.output_to_svg("after update first slice",true);
    //check if the first slice has a reservation to run (by checking _start_a_reservation)
    //this starts out as false
    if (_start_a_reservation)
    {
        JobAlloc alloc; //holds the allocation of a reservation that is ready to start
        std::vector<const Job *> jobs_removed;//holds the jobs_removed 
        
        if(_schedule.remove_reservations_if_ready(jobs_removed))
        {
                if (_output_svg == "all")
                _schedule.output_to_svg("after reservations if ready",true);
                //traverse the reservations removed         
                for(const Job * job : jobs_removed)
                {
                    //add the reservation to schedule in the first time slice
                    alloc = _schedule.add_current_reservation(job,_selector);
                    LOG_F(INFO,"allocated reservation %s, %s",alloc.job->id.c_str(),alloc.used_machines.to_string_hyphen().c_str());
                    //we are about to run the reservation, remove it from the reservation queue
                    _reservation_queue->remove_job(job);
                    //execute the reservation
                    _decision->add_execute_job(alloc.job->id,alloc.used_machines,date);
                    
                }
            //only set this to false if we  remove and execute some reservations
            //it was triggered true for a reason
            //keep true in case it comes back around after a job completes
            _start_a_reservation = false;
        }
        
    }

    
    if (_output_svg == "short")
        _schedule.output_to_svg("after reservation added back",true);
    // Queue sorting
    _queue->sort_queue(update_info, compare_info);
    _reservation_queue->sort_queue(update_info,compare_info);
    //all jobs that are killed need to be resubmitted.  Get the jobs killed ready for resubmitting
    //jobs are added to _resubmitted_jobs which can be seen in the entire class
    //jobs will be removed from this map once they are seen as resubmitted
    //handle killed jobs will only start adding jobs to schedule once _resubmitted_jobs is empty
    for ( auto job_message_pair : _jobs_killed_recently)
    {
        batsched_tools::id_separation separation = batsched_tools::tools::separate_id(job_message_pair.first);
        LOG_F(INFO,"next_resubmit_string %s",separation.next_resubmit_string.c_str());
        _resubmitted_jobs[separation.next_resubmit_string]=job_message_pair.second->forWhat;
    }
        
    //take care of killed jobs and reschedule jobs
    //lifecycle of killed job:
    //make_decisions() kill job -> make_decisions() submit job -> make_decisions() add jobs to schedule in correct order
    // it is the third invocation that this function should run in full 
    handle_killed_jobs(recently_queued_jobs,date);
    
   
    auto compare_reservations = [this](const std::string j1,const std::string j2)->bool{
            
            Job * job1= (*_workload)[j1];
            Job * job2= (*_workload)[j2];
            if (job1->future_allocations.is_empty() && job2->future_allocations.is_empty())
                return j1 < j2;
            else if (job1->future_allocations.is_empty())
                return false;  // j1 has no allocation so it must be set up after j2: j1<j2 ordering is false
            else if (job2->future_allocations.is_empty())
                return true; //j2 has no allocation so j1 must be set up before j2: j1<j2 ordering is true
            else
                return j1 < j2;

    };
    
    //sort reservations with jobs that have allocations coming first 
    if (!recently_released_reservations.empty())           
    {
        LOG_F(INFO,"here size: %d",recently_released_reservations.size());
        //log all the recent reservations
        for(std::string resv:recently_released_reservations)
            LOG_F(INFO,"resv: %s",resv.c_str());
        //here we sort using the above compare lambda method
        std::sort(recently_released_reservations.begin(),recently_released_reservations.end(),compare_reservations);
        //handle the reservations by putting them into the schedule
        handle_reservations(recently_released_reservations,recently_queued_jobs,date);
    }
    LOG_F(INFO,"here");
    
    ISchedulingAlgorithm::handle_failures(date);
   
    //lifecycle of killed job:
    //make_decisions() kill job -> make_decisions() submit job -> make_decisions() add jobs to schedule in correct order
    //this function is the submit job part, the second invocation of make_decisions()
    _decision->handle_resubmission(_jobs_killed_recently,_workload,date);
    LOG_F(INFO,"here");
    //this handles the schedule including any compressing that might need to be done
    //LOG_F(INFO,"%s",_queue->to_json_string().c_str());
    if (_output_svg == "all")
    _schedule.output_to_svg("before handle",true);
    handle_schedule(recently_queued_jobs,date);
   
    //queue is based on ended jobs being taken out
    //queue is based on submitted jobs being added
    //estimated start is based on submitted jobs being added to schedule, ended jobs being out of the schedule
    //and started jobs being in the first timeslice.
    
    if (_need_to_write_metrics)
    {
        _need_to_write_metrics = false;
        get_queue_metrics(_metrics);
        //import into pandas as 'df = pd.read_csv("file.csv",sep=";",header=0)'
        //and json as myjson = json.loads(df["state"].values[<hour>])["running"||"queued"][<index>]["id"||"start"||"stop"]
        BLOG_F(_metrics_type,"%d;{\"running\":%s,\"queued\":%s}",int( date/3600),_metrics.running_string.c_str(),_metrics.queue_string.c_str());
    }
    
    /*
    if (_start_metrics.arrived)
    {
        for (const std::string & start_job_id : _executed_jobs)
        {
            Job * new_job = (*_workload)[start_job_id];
            new_job->start = date;
            BLOG_F(_metrics_type,"%s,%s,%d,%f,%f,%f,%f,%f,%d,%f,%f,%s",
            "S",new_job->id.c_str(),new_job->nb_requested_resources,new_job->walltime.convert_to<double>(),-1.0,new_job->submission_time,date, -1.0,
            _start_metrics._nb_jobs_in_queue, _start_metrics._work_in_queue,_start_metrics._util,
            _start_metrics._queue_to_json_string.c_str());

        }
    }
    _executed_jobs.clear();
    if (_end_metrics.arrived)
    {
        for (const std::string & end_job_id : _jobs_ended_recently)
        {
            const Job * new_job = (*_workload)[end_job_id];
            BLOG_F(_metrics_type,"%s,%s,%d,%f,%f,%f,%f,%f,%d,%f,%f,%s",
            "E",new_job->id.c_str(), new_job->nb_requested_resources,new_job->walltime.convert_to<double>(),date - new_job->start,new_job->submission_time,new_job->start, date,
            _end_metrics._nb_jobs_in_queue, _end_metrics._work_in_queue,_end_metrics._util,
            _end_metrics._queue_to_json_string.c_str());
        }
    }
    */
    if (_output_svg == "all")
    _schedule.output_to_svg("after handle",true);

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
    //descriptive log statement
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







void ConservativeBackfilling_metrics_roci::handle_schedule(std::vector<std::string> & recently_queued_jobs,double date)
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
                    
                JobAlloc alloc = _schedule.add_job_first_fit(new_job, _selector,false);

                // If the job should start now, let's say it to the resource manager
                if (!alloc.used_machines.is_empty())
                {
                    scheduled++;
                
                    if (alloc.started_in_first_slice)
                    {
                        _decision->add_execute_job(new_job->id, alloc.used_machines, date);
                        _executed_jobs.push_back(new_job->id);
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
                    
                JobAlloc alloc = _schedule.add_job_first_fit(new_job, _selector,false);
                
                LOG_F(INFO,"ALLOC: %s  begin:%f  end:%f used:%s",new_job->id.c_str(),alloc.begin.convert_to<double>(),alloc.end.convert_to<double>(),alloc.used_machines.to_string_hyphen().c_str());

                // If the job should start now, let's say it to the resource manager
                 if (!alloc.used_machines.is_empty())
                {
                    scheduled++;
                    if (alloc.started_in_first_slice)
                    {
                        _decision->add_execute_job(new_job->id, alloc.used_machines, date);
                        _executed_jobs.push_back(new_job->id);
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
            JobAlloc alloc = _schedule.add_job_first_fit(job, _selector,false); 
            
            //LOG_F(INFO,"ALLOC: %s  begin:%f  end:%f used:%s",job->id.c_str(),alloc.begin.convert_to<double>(),alloc.end.convert_to<double>(),alloc.used_machines.to_string_hyphen().c_str());  
    //            if (_dump_provisional_schedules)
    //                _schedule.incremental_dump_as_batsim_jobs_file(_dump_prefix);
            if (!alloc.used_machines.is_empty())
            {
                scheduled++;
                if (alloc.started_in_first_slice)
                {
                    _decision->add_execute_job(job->id, alloc.used_machines, date);
                    _executed_jobs.push_back(job->id);
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











void ConservativeBackfilling_metrics_roci::handle_killed_jobs(std::vector<std::string> & recently_queued_jobs, double date)
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
            LOG_F(INFO,"here");
            for (std::string job_id : recently_queued_jobs)
            {
                LOG_F(INFO,"%s",job_id.c_str());
                //check if it's a resubmitted job
                if (job_id.find("#") != std::string::npos)
                {
                   LOG_F(INFO,"here"); 
                   auto forWhat = _resubmitted_jobs.at(job_id);
                   LOG_F(INFO,"here");
                   _resubmitted_jobs.erase(job_id);
                   LOG_F(INFO,"here");
                   _resubmitted_jobs_released.push_back(std::make_pair((*_workload)[job_id],forWhat));
                   LOG_F(INFO,"here");

                }
                else
                    recently_queued_jobs2.push_back(job_id); // it's a normal job, push it back
            }
            //set the recently_queued_jobs to a vector without the resubmitted jobs
            recently_queued_jobs = recently_queued_jobs2;
            LOG_F(INFO,"here");
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
                LOG_F(INFO,"here");
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
                    
                    JobAlloc alloc = _schedule.add_job_first_fit(job_forWhat_pair.first,_selector,false);
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
                   
                    JobAlloc alloc = _schedule.add_job_first_fit(job,_selector,false);
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
                    {
                        batsched_tools::CALL_ME_LATERS cml;
                        cml.forWhat = batsched_tools::call_me_later_types::RESERVATION_START;
                        cml.id = _nb_call_me_laters;
                        cml.extra_data = batsched_tools::string_format("{\"job_id\":'%s'}",reservation.job->id);
                        _decision->add_call_me_later(date,reservation.job->start,cml);
                    }
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
                    JobAlloc alloc = _schedule.add_job_first_fit(job, _selector,false);   
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
                    {
                        batsched_tools::CALL_ME_LATERS cml;
                        cml.forWhat = batsched_tools::call_me_later_types::RESERVATION_START;
                        cml.id = _nb_call_me_laters;
                        cml.extra_data = batsched_tools::string_format("{\"job_id\":\"%s\"}",reservation.job->id);
                        _decision->add_call_me_later(date,reservation.job->start,cml);
                    }
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



















void ConservativeBackfilling_metrics_roci::handle_reservations(std::vector<std::string> & recently_released_reservations, 
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
                _decision->add_reject_job(date,new_job_id,batsched_tools::REJECT_TYPES::NO_RESERVATION_ALLOCATION);
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
                    JobAlloc alloc;
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
                    {
                        batsched_tools::CALL_ME_LATERS cml;
                        cml.forWhat = batsched_tools::call_me_later_types::RESERVATION_START;
                        cml.id = _nb_call_me_laters;
                        cml.extra_data = batsched_tools::string_format("{'job_id':'%s'}",reservation.job->id);
                        _decision->add_call_me_later(date,reservation.job->start,cml);
                    }
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
                _decision->add_reject_job(date,new_job_id,batsched_tools::REJECT_TYPES::NO_RESERVATION_ALLOCATION);
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
                        JobAlloc alloc = _schedule.add_job_first_fit(job, _selector,false);   
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
                    {
                        batsched_tools::CALL_ME_LATERS cml;
                        cml.forWhat = batsched_tools::call_me_later_types::RESERVATION_START;
                        cml.id = _nb_call_me_laters;
                        cml.extra_data = batsched_tools::string_format("{'job_id':'%s'}",reservation.job->id);
                        _decision->add_call_me_later(date,reservation.job->start,cml);
                    }
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
        JobAlloc alloc;
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



