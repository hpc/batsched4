#include "easy_bf3.hpp"
#include <loguru.hpp>
#include "../pempek_assert.hpp"
#include "../batsched_tools.hpp"
#define B_LOG_INSTANCE _myBLOG
using namespace std;

EasyBackfilling3::EasyBackfilling3(Workload * workload,
                                 SchedulingDecision * decision,
                                 Queue * queue,
                                 ResourceSelector * selector,
                                 double rjms_delay,
                                 rapidjson::Document * variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{
    // @note allocate priority job
    _p_job = new Priority_Job();
    // @note failures logs
    _logFailure = new b_log();
    (void) queue;
}

EasyBackfilling3::~EasyBackfilling3()
{
    // @note deallocate priority job struct
    delete _p_job;
}

/*********************************************************
 *            MODIFIED STATE HANDLING FUNCTIONS          *
**********************************************************/

void EasyBackfilling3::on_simulation_start(double date, const rapidjson::Value & batsim_event)
{
    CLOG_F(INFO,"on simulation start");
    pid_t pid = batsched_tools::get_batsched_pid();
    _decision->add_generic_notification("PID",std::to_string(pid),date);
    const rapidjson::Value & batsim_config = batsim_event["config"];

    // @note LH: set output folder for logging
    _output_folder=batsim_config["output-folder"].GetString();
    _output_folder.replace(_output_folder.rfind("/out"), std::string("/out").size(), "");
    CLOG_F(CCU_DEBUG_FIN,"output folder %s",_output_folder.c_str());

    // @note LH: config options for logging failures
    if(batsim_config["log_b_log"].GetBool()){
        _logFailure->add_log_file(_output_folder+"/log/failures.log",blog_types::FAILURES);
    }

    // @note get interval set of machine id's and total number of machines
    _available_machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    _nb_available_machines = _nb_machines;
    PPK_ASSERT_ERROR(_available_machines.size() == (unsigned int) _nb_machines);

    /* @note LH: total number of jobs in queue/schedule will be at most the number of machines.
        Removing the memory reservations will decrease performance and it is worth noting that
        both the schedule and queue are pointer vectors, so reserved memory will be negligible */
    _scheduled_jobs.reserve(_nb_machines);
    _waiting_jobs.reserve(_nb_machines);

    // @note LH: for seeding failures
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

void EasyBackfilling3::on_simulation_end(double date)
{
    (void) date;
}

/*********************************************************
 *      MODIFIED SIMULATED CHECKPOINTING FUNCTIONS       *
**********************************************************/

void EasyBackfilling3::on_machine_down_for_repair(double date){
    //get a random number of a machine to kill
    int number = machine_unif_distribution->operator()(generator_machine);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = number;
    //if the machine is already down for repairs ignore it.
    BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::MACHINE_REPAIR.c_str(),number);
    if ((machine & _repair_machines).is_empty())
    {
        CLOG_F(CCU_DEBUG_FIN,"here, machine going down for repair %d",number);
        //ok the machine is not down for repairs
        //it will be going down for repairs now
        _available_machines-=machine;
        _repair_machines+=machine;
        _nb_available_machines=_available_machines.size();
        double repair_time = _workload->_repair_time;

        if (_workload->_MTTR != -1.0)
        repair_time = repair_time_exponential_distribution->operator()(generator_repair_time);
        BLOG_F(blog_types::FAILURES,"%s,%f",blog_failure_event::REPAIR_TIME.c_str(),repair_time);
        //call me back when the repair is done
        std::string extra_data = batsched_tools::string_format("{\"machine\":%d}",number);
        batsched_tools::CALL_ME_LATERS cml;
        cml.forWhat = batsched_tools::call_me_later_types::REPAIR_DONE;
        cml.id = _nb_call_me_laters;
        cml.extra_data = extra_data;
        _decision->add_call_me_later(date,date+repair_time,cml);
        //now kill the jobs that are running on machines that need to be repaired.        
        //if there are no running jobs, then there are none to kill
        if (!_scheduled_jobs.empty()) {
            //ok there are jobs to kill
            std::string killed_jobs;
            for(auto sj : _scheduled_jobs) {
                if (!((sj->allocated_machines & machine).is_empty())) {
                    Job * job_ref = (*_workload)[sj->id];
                    batsched_tools::Job_Message * msg = new batsched_tools::Job_Message;
                    msg->id = sj->id;
                    msg->forWhat = batsched_tools::KILL_TYPES::NONE;
                    _my_kill_jobs.insert(std::make_pair(job_ref,msg));
                    LOG_F(3,"Killing Job: %s",sj->id.c_str());
                    if (killed_jobs.empty())
                        killed_jobs = sj->id;
                    else
                        killed_jobs=batsched_tools::string_format("%s %s",killed_jobs.c_str(),sj->id.c_str());
                }
            }
            BLOG_F(blog_types::FAILURES,"%s,\"%s\"",blog_failure_event::KILLING_JOBS.c_str(),killed_jobs.c_str());
        }
    }
    else BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::MACHINE_ALREADY_DOWN.c_str(),number);
}

void EasyBackfilling3::on_machine_instant_down_up(double date){
    //get a random number of a machine to kill
    int number = machine_unif_distribution->operator()(generator_machine);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = number;
    BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::MACHINE_INSTANT_DOWN_UP.c_str(), number);
    //if there are no running jobs, then there are none to kill
    std::string killed_jobs;
    if (!_scheduled_jobs.empty()){
        //ok there are jobs to kill
        for(auto sj : _scheduled_jobs)
        {
            if (!((sj->allocated_machines & machine).is_empty())){
                Job * job_ref = (*_workload)[sj->id];
                batsched_tools::Job_Message * msg = new batsched_tools::Job_Message;
                msg->id = sj->id;
                msg->forWhat = batsched_tools::KILL_TYPES::NONE;
                _my_kill_jobs.insert(std::make_pair(job_ref,msg));
                if (killed_jobs.empty())
                    killed_jobs = sj->id;
                else
                    killed_jobs=batsched_tools::string_format("%s %s",killed_jobs.c_str(),sj->id.c_str());
            }
        }
        BLOG_F(blog_types::FAILURES,"%s,\"%s\"",blog_failure_event::KILLING_JOBS.c_str(), killed_jobs.c_str());
    }
}

void EasyBackfilling3::on_requested_call(double date,batsched_tools::CALL_ME_LATERS cml_in)
{
    switch (cml_in.forWhat){
        case batsched_tools::call_me_later_types::SMTBF: {
            //Log the failure
            BLOG_F(blog_types::FAILURES,"%s,%s",blog_failure_event::FAILURE.c_str(),"SMTBF");
            if ( !_scheduled_jobs.empty() || !_waiting_jobs.empty() || !_no_more_static_job_to_submit_received)
                {
                    double number = failure_exponential_distribution->operator()(generator_failure);
                    if (_workload->_repair_time == -1.0  && _workload->_MTTR == -1.0)
                        on_machine_instant_down_up(date);
                    else
                        on_machine_down_for_repair(date);
                    batsched_tools::CALL_ME_LATERS cml;
                    cml.forWhat = batsched_tools::call_me_later_types::SMTBF;
                    cml.id = _nb_call_me_laters;
                    _decision->add_call_me_later(date,number+date,cml);
                }
        }
        break;
        case batsched_tools::call_me_later_types::MTBF: {
            BLOG_F(blog_types::FAILURES, "%s,%s",blog_failure_event::FAILURE.c_str(),"MTBF");
            if (! _scheduled_jobs.empty() || !_waiting_jobs.empty() || !_no_more_static_job_to_submit_received) {
                double number = failure_exponential_distribution->operator()(generator_failure);
                on_myKillJob_notify_event(date);
                batsched_tools::CALL_ME_LATERS cml;
                cml.forWhat = batsched_tools::call_me_later_types::MTBF;
                cml.id = _nb_call_me_laters;
                _decision->add_call_me_later(date,number+date,cml);
            }
        }
        break;
        case batsched_tools::call_me_later_types::FIXED_FAILURE: {
                BLOG_F(blog_types::FAILURES,"%s,%s", blog_failure_event::FAILURE.c_str(),"FIXED_FAILURE");
                if (! _scheduled_jobs.empty() || !_waiting_jobs.empty() || !_no_more_static_job_to_submit_received){
                    double number = _workload->_fixed_failures;
                    if (_workload->_repair_time == 0.0)
                        on_machine_instant_down_up(date);
                    else
                        on_machine_down_for_repair(date);
                    batsched_tools::CALL_ME_LATERS cml;
                    cml.forWhat = batsched_tools::call_me_later_types::FIXED_FAILURE;
                    cml.id = _nb_call_me_laters;
                    _decision->add_call_me_later(date,number+date,cml);
                }
        }
        break;
        case batsched_tools::call_me_later_types::REPAIR_DONE: {
            
            rapidjson::Document doc;
            doc.Parse(cml_in.extra_data.c_str());
            PPK_ASSERT(doc.HasMember("machine"),"Error, repair done but no 'machine' field in extra_data");
            int machine_number = doc["machine"].GetInt();
            BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::REPAIR_DONE.c_str() ,machine_number);
            //a repair is done, all that needs to happen is add the machines to available
            //and remove them from repair machines and add one to the number of available
            IntervalSet machine = machine_number;
            _available_machines += machine;
            _repair_machines -= machine;
            _nb_available_machines=_available_machines.size();
            _machines_that_became_available_recently += machine;
            _need_to_backfill = true;
            if (_reject_possible)
                _repairs_done++;
        }
        break;
    }
}

void EasyBackfilling3::on_myKillJob_notify_event(double date){
    if (!_scheduled_jobs.empty()){
        batsched_tools::Job_Message * msg = new batsched_tools::Job_Message;
        msg->id = (*_scheduled_jobs.begin())->id;
        msg->forWhat = batsched_tools::KILL_TYPES::NONE;
        _my_kill_jobs.insert(std::make_pair((*_workload)[(*_scheduled_jobs.begin())->id], msg));
    }
}

void EasyBackfilling3::on_no_more_static_job_to_submit_received(double date){
    ISchedulingAlgorithm::on_no_more_static_job_to_submit_received(date);
}

/*********************************************************
 *         REAL CHECKPOINTING FUNCTIONS (TBA)            *
**********************************************************/

void EasyBackfilling3::on_start_from_checkpoint(double date,const rapidjson::Value & batsim_config){}

void EasyBackfilling3::on_checkpoint_batsched(double date){}

void EasyBackfilling3::on_ingest_variables(const rapidjson::Document & doc,double date){}

void EasyBackfilling3::on_first_jobs_submitted(double date){}

/*********************************************************
 *             MODIFIED DECISION FUNCTIONS               *
**********************************************************/

void EasyBackfilling3::make_decisions(double date,
                                     SortableJobOrder::UpdateInformation *update_info,
                                     SortableJobOrder::CompareInformation *compare_info)
{
    (void) compare_info;
    Job * priority_job_before = get_first_waiting_job();

    // Let's remove finished jobs from the schedule
    for (const string & ended_job_id : _jobs_ended_recently){
        handle_finished_job(ended_job_id, date);
    }

    //  Handle any killed jobs
    if(!_my_kill_jobs.empty()){
        std::vector<batsched_tools::Job_Message *> kills;
        for( auto job_msg_pair:_my_kill_jobs)
        {
            CLOG_F(CCU_DEBUG_FIN,"adding kill job %s",job_msg_pair.first->id.c_str());
            kills.push_back(job_msg_pair.second);
        }
        _decision->add_kill_job(kills,date);
        _my_kill_jobs.clear();
    }

    // Handle resubmitting killed jobs to queue back up
    _decision->handle_resubmission(_jobs_killed_recently,_workload,date);

    // Let's handle recently released jobs
    std::vector<std::string> recently_queued_jobs;
    for (const string & new_job_id : _jobs_released_recently)
    {
        Job * new_job = (*_workload)[new_job_id];

        if (new_job->nb_requested_resources > _nb_machines)
        {
            _decision->add_reject_job(date,new_job_id, batsched_tools::REJECT_TYPES::NOT_ENOUGH_RESOURCES);
        }
        else if (!new_job->has_walltime)
        {
            LOG_SCOPE_FUNCTION(INFO);
            CLOG_F(CCU_DEBUG_FIN, "Date=%g. Rejecting job '%s' as it has no walltime", date, new_job_id.c_str());
            _decision->add_reject_job(date,new_job_id, batsched_tools::REJECT_TYPES::NO_WALLTIME);
        }
        else
        {
            _waiting_jobs.push_back(new_job);
            recently_queued_jobs.push_back(new_job_id);
        }
    }    

    // Queue sorting
    Job * priority_job_after = nullptr;
    sort_queue_while_handling_priority_job(priority_job_before, priority_job_after, update_info);

    if (get_first_waiting_job() != nullptr)
        CLOG_F(CCU_DEBUG_ALL,"first waiting job: %s  resources: %d",get_first_waiting_job()->id.c_str(),get_first_waiting_job()->nb_requested_resources);

    // If no resources have been released, we can just try to backfill the newly-released jobs
    if (_jobs_ended_recently.empty() && !_need_to_backfill)
    {
        for (unsigned int i = 0; i < recently_queued_jobs.size() && _nb_available_machines > 0; ++i)
        {
            const string & new_job_id = recently_queued_jobs[i];
            const Job * new_job = (*_workload)[new_job_id];

            // The job could have already been executed by sort_queue_while_handling_priority_job,
            // that's why we check whether the queue contains the job.
            auto wj_it = find_waiting_job(new_job_id);
            if (wj_it != _waiting_jobs.end() && new_job != priority_job_after)
            {
                check_backfill_job(new_job, date);

                if(_can_run){
                    _decision->add_execute_job(new_job_id, _tmp_job->allocated_machines, date);
                    _waiting_jobs.erase(wj_it);
                    _reject_possible = false;
                    _repairs_done = 0;
                }
            }
        }
    }
    else
    {   
        /* Some resources have been released, the whole queue should be traversed.
           Let's try to backfill all the jobs */
        auto job_it = _waiting_jobs.begin();
        while (job_it != _waiting_jobs.end() && _nb_available_machines > 0)
        {
            Job * job = *job_it;

            if(job == priority_job_after) check_priority_job(job, date);
            else check_backfill_job(job, date);
            
            if(_can_run){

                _decision->add_execute_job(job->id, _tmp_job->allocated_machines, date);
                _reject_possible = false;
                _repairs_done = 0;
                job_it = delete_waiting_job(job_it);
                if(job == priority_job_after) priority_job_after = get_first_waiting_job();
            }else ++job_it;  
        }
    }
    _need_to_backfill = false;

    //we need to start rejecting jobs if the _waiting_jobs don't fit
    if ((_workload->_reject_jobs_after_nb_repairs!= -1) && _jobs_killed_recently.empty() && (priority_job_after == nullptr)  && _scheduled_jobs.empty() &&
            _need_to_send_finished_submitting_jobs && _no_more_static_job_to_submit_received && !date<1.0 )
    {
        _reject_possible = true;
        if (_repairs_done > _workload->_reject_jobs_after_nb_repairs)
        {
            for (auto iter = _waiting_jobs.begin();iter != _waiting_jobs.end();)
            {

                _decision->add_reject_job(date,(*iter)->id,batsched_tools::REJECT_TYPES::NOT_ENOUGH_AVAILABLE_RESOURCES);
                iter = _waiting_jobs.erase(iter);
            }
        }
    }
    
    // @note LH: conditions for ending the simulation with dynamic jobs
    if (_jobs_killed_recently.empty() && _waiting_jobs.empty()  && _scheduled_jobs.empty() &&
            _need_to_send_finished_submitting_jobs && _no_more_static_job_to_submit_received && !date<1.0 )
    {
        _decision->add_scheduler_finished_submitting_jobs(date);
        _need_to_send_finished_submitting_jobs = false;
    }

    // @note LH: adds queuing info to the out_jobs_extra.csv file
    _decision->add_generic_notification("queue_size",to_string(_waiting_jobs.size()),date);
    _decision->add_generic_notification("schedule_size",to_string(_scheduled_jobs.size()),date);
    _decision->add_generic_notification("number_running_jobs",to_string(_scheduled_jobs.size()),date);
    _decision->add_generic_notification("utilization",to_string(NOTIFY_MACHINE_UTIL), date);
}

void EasyBackfilling3::sort_queue_while_handling_priority_job(Job * priority_job_before,
                                                             Job *& priority_job_after,
                                                             SortableJobOrder::UpdateInformation * update_info)
{
    // @note LH: set how queue should compare jobs
    CompareQueue compare(_queue_policy == "ORIGINAL-FCFS");
    // @note LH: sort the queue
    heap_sort_queue(_waiting_jobs.size(), compare);

    // Let the new priority job be computed
    priority_job_after = get_first_waiting_job();

    // If the priority job has changed
    if (priority_job_after != priority_job_before)
    {
        // Let us ensure the priority job is in the schedule.
        // To do so, while the priority job can be executed now, we keep on inserting it into the schedule
        for (bool could_run_priority_job = true; could_run_priority_job && priority_job_after != nullptr; )
        {
            could_run_priority_job = false;
            check_priority_job(priority_job_after, C2DBL(update_info->current_date));

            if(_can_run){
                _decision->add_execute_job(priority_job_after->id, _tmp_job->allocated_machines, C2DBL(update_info->current_date));
                delete_waiting_job(priority_job_after->id);
                priority_job_after = get_first_waiting_job();
                could_run_priority_job = true;
            }
        }
    }
}

/*********************************************************
 *         EASY BACKFILLING DECISION ADDITIONS           *
**********************************************************/

//@note LH: added function check priority job and "reserve" it's spot in the schedule
void EasyBackfilling3::check_priority_job(const Job * priority_job, double date){   

    int machine_count = _nb_available_machines;

    // @note update priority job if it changed
    if(_p_job->id != priority_job->id){
        _p_job->id = priority_job->id;
        _p_job->requested_resources = priority_job->nb_requested_resources;
        // @note reinitailize predicted values
        _p_job->shadow_time = 0.0;
        _p_job->est_finish_time = 0.0;
        _p_job->extra_resources = 0;
    }

    /* @note 
        priority job can run if the following is true:
            - requested resources <=  current available 
    */
    _can_run = _p_job->requested_resources <= machine_count;

    if(_can_run){
        // @note priority job can run so add it to the schedule
        handle_scheduled_job(priority_job,date);
    }else{

        // @note if the priority job can't run then calculate when it will 
        for(auto & sj: _scheduled_jobs){
            machine_count += sj->requested_resources;
            if(machine_count >= _p_job->requested_resources){
                // @note predicted start time
                _p_job->shadow_time = sj->est_finish_time;
                // @note predicted end time
                _p_job->est_finish_time = sj->est_finish_time + C2DBL(priority_job->walltime);
                // @note available resources after priority jobs reserved start time
                _p_job->extra_resources = machine_count - priority_job->nb_requested_resources;
                break;
            }
        }
    }
}

//@note LH: added function check if next backfill job can be backfilled
void EasyBackfilling3::check_backfill_job(const Job * backfill_job, double date){   

    /* @note LH:
        job can be backfilled if the following is true:
            case A: job will finish after priority jobs start time 
                -AND- the requested resources are <= MIN[current available nodes, waiting priority job extra nodes]
            case B: otherwise job will finish before the priority jobs reserved start (shadow) time 
                -AND- the requested resources are <= current available resources
                ***Note: This case requires subtracting "extra resources" when priority job starts
    */
    bool subtract_resources = ((date+C2DBL(backfill_job->walltime)) > _p_job->shadow_time);
    _can_run = (subtract_resources)
        ? (backfill_job->nb_requested_resources <= MIN(_nb_available_machines, _p_job->extra_resources))
        : (backfill_job->nb_requested_resources <= _nb_available_machines);

    /* @note LH:
        job can be backfilled, 
            - add it to the schedule
            - sort the schedule
            - increase backfilled jobs count
            - substract extra resources 
    */ 
    if(_can_run) {
        handle_scheduled_job(backfill_job, date);
        /*  @note LH: 
            this handles case A for when backfill job will end after priority job starts
            and there is enough resources to run now, but not after priority job starts
        */
        if(subtract_resources) _p_job->extra_resources -= backfill_job->nb_requested_resources;
    }
}

//@note LH: added function to add jobs to the schedule
void EasyBackfilling3::handle_scheduled_job(const Job * job, double date){
    // allocate space for scheduled job
    _tmp_job = new Scheduled_Job();
    // @note convert wall time to a double
    double tmp_walltime = C2DBL(job->walltime);

    // @note set the scheduled jobs info
    _tmp_job->id = job->id;
    _tmp_job->requested_resources = job->nb_requested_resources;
    _tmp_job->wall_time = tmp_walltime;
    _tmp_job->start_time = date;
    _tmp_job->est_finish_time = date + tmp_walltime;
    _tmp_job->allocated_machines = _available_machines.left(job->nb_requested_resources);
    // @note add the job to the schedule
    _scheduled_jobs.push_back(_tmp_job);

    // @note remove allocated nodes from intervalset and subtract from machine count
    _available_machines -= _tmp_job->allocated_machines;
    _nb_available_machines -= _tmp_job->requested_resources;
    heap_sort_schedule(_scheduled_jobs.size());
}

//@note LH: added function that handles deallocating finished jobs
void EasyBackfilling3::handle_finished_job(string job_id, double date){
    // @note LH: get finished job from schedule
    auto fj_it = find_if(_scheduled_jobs.begin(), _scheduled_jobs.end(), 
        [job_id](Scheduled_Job *sj){ 
            return (sj->id == job_id);
    });

    // @note LH: if the job exists, find it, and retrieve it
    if(fj_it != _scheduled_jobs.end()){
        auto fj_idx = distance(_scheduled_jobs.begin(), fj_it);
        _tmp_job = _scheduled_jobs.at(fj_idx);
        
        // @note LH: return allocated machines to intervalset and add to machine count
        _available_machines.insert(_tmp_job->allocated_machines - _repair_machines);
        _nb_available_machines = _available_machines.size();

        // @note deallocate finished job struct
        delete _tmp_job;
        // @note LH: delete the job pointer in the schedule
        _scheduled_jobs.erase(fj_it);
    }
}

/*********************************************************
 *         SCHEDULE & QUEUE HEAP SORT ADDITIONS          *
**********************************************************/

//@note LH: added helper sub-function to turn schedule into a maximum heap
void EasyBackfilling3::max_heapify_schedule(int root, int size){
    // @note find the max root node, left node, and right node
    int max = root, left = (2 * root) + 1, right = left + 1;

    // @note LH: if left exists -AND- left is more than max, set left as new max
    if (left < size && _scheduled_jobs[left]->est_finish_time > _scheduled_jobs[max]->est_finish_time){
        max = left;
    }

    // @note LH: if right exists -AND- right is more than max, set right as new max
    if (right < size && _scheduled_jobs[right]->est_finish_time  > _scheduled_jobs[max]->est_finish_time){
        max = right;
    }

    // @note swap[max_root(max),last_node(size)] and continue sorting
    if (max != root) {
        swap(_scheduled_jobs[root], _scheduled_jobs[max]);
        max_heapify_schedule(max, size);
    }
}

//@note LH: added heaper function for heap sorting the schedule
void EasyBackfilling3::heap_sort_schedule(int size){
    //@note LH: no need to sort if there is less than 2 jobs
    if(size < 2) return;

    // @note LH: build the new max heap
    for(int i = (size/2)-1; i >= 0; --i){
        max_heapify_schedule(i, size);
    }

    // @note LH: sort the new max heap
    for(int j = size-1; j >= 0; --j){
        swap(_scheduled_jobs[0], _scheduled_jobs[j]);
        max_heapify_schedule(0, j);
    }
}

// @note LH: added helper function to set if queue should sort by orginal submission times 
EasyBackfilling3::CompareQueue::CompareQueue(bool compare_original) : compare_original(compare_original) {}

inline bool EasyBackfilling3::CompareQueue::operator()(Job* jobA, Job* jobB) const {
    // @note LH: get the numeric part of a jobs id string 
    auto extractNumericId = [](const std::string& str_id) {
        size_t pos1 = str_id.find('!') + 1;
        size_t pos2 = str_id.find_first_not_of("0123456789", pos1);
        return std::stoi(str_id.substr(pos1, pos2 - pos1));
    };
    // @note LH: compare_orginal is true if _queue_policy is "ORIGINAL_FCFS"
    if (compare_original) {
        // @note LH: if submission times are the same, sort by id 
        if (jobA->submission_times[0] == jobB->submission_times[0]) {
            return extractNumericId(jobA->id) > extractNumericId(jobB->id);
        }
         // @note LH: otherwise sort by subbmission time
        return jobA->submission_times[0] > jobB->submission_times[0];
    } else {
        if (jobA->submission_time == jobB->submission_time) {
            return extractNumericId(jobA->id) > extractNumericId(jobB->id);
        }
        return jobA->submission_time > jobB->submission_time;
    }
}

//@note LH: added helper sub-function to turn the queue into a maximum heap
void EasyBackfilling3::max_heapify_queue(int root, int size, const CompareQueue &comp){  
    // @note LH: find the max root node, left node, and right node
    int max = root, left = (2 * root) + 1, right = left + 1;

    // @note LH: if left exists -AND- left is more than max, set left as new max
    if (left < size && comp(_waiting_jobs[left], _waiting_jobs[max])) {
        max = left;
    }

    // @note LH: if right exists -AND- right is more than max, set right as new max
    if (right < size && comp(_waiting_jobs[right], _waiting_jobs[max])) {
        max = right;
    }

    // @note swap[max_root(max),last_node(size)] and continue sorting
    if (max != root) {
        swap(_waiting_jobs[root], _waiting_jobs[max]);
        max_heapify_queue(max, size, comp);
    }
}

//@note LH: added helper function for heap sorting the queue
void EasyBackfilling3::heap_sort_queue(int size, const CompareQueue &comp){
    //@note LH: no need to sort if there is less than 2 jobs
    if(size < 2) return;

    // @note LH: build the new max heap
    for(int i = (size/2)-1; i >= 0; --i){
        max_heapify_queue(i, size, comp);
    }

    // @note LH: sort the new max heap
    for(int j = size-1; j >= 0; --j){
        swap(_waiting_jobs[0], _waiting_jobs[j]);
        max_heapify_queue(0, j, comp);
    }
}

/*********************************************************
 *              QUEUE REPLACEMENT ADDITIONS              *
**********************************************************/

//@note LH: added helper function to return the first waiting job 
Job * EasyBackfilling3::get_first_waiting_job(){
    if (_waiting_jobs.empty())
        return nullptr;
    else
    {
        for(auto iter=_waiting_jobs.begin();iter != _waiting_jobs.end();iter++)
        {
            if ((*iter)->nb_requested_resources <= (_nb_machines - _repair_machines.size()))
                return (*iter);
            else
                continue;
        }
        return nullptr;
    }
}

//@note LH: added helper function to find and return waiting job iterator 
vector<Job *>::iterator EasyBackfilling3::find_waiting_job(string job_id){
    auto iter = find_if(_waiting_jobs.begin(), _waiting_jobs.end(), [job_id](Job *wj){ 
        return wj->id == job_id;
    });
    return iter;
}

//@note LH: added overloaded helper function to delete waiting job by string id
vector<Job *>::iterator EasyBackfilling3::delete_waiting_job(string wjob_id){
    vector<Job *>::iterator wj_it = find_waiting_job(wjob_id);
    return delete_waiting_job(wj_it);
}

//@note LH: added overloaded helper function to delete waiting job by list iterator
vector<Job *>::iterator EasyBackfilling3::delete_waiting_job(vector<Job *>::iterator wj_iter){
    return _waiting_jobs.erase(wj_iter);
}
