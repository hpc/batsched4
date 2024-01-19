#include "easy_bf3.hpp"
#include <loguru.hpp>
#include "../pempek_assert.hpp"
#include "../batsched_tools.hpp"
using namespace std;


EasyBackfilling3::EasyBackfilling3(Workload * workload,
                                 SchedulingDecision * decision,
                                 Queue * queue,
                                 ResourceSelector * selector,
                                 double rjms_delay,
                                 rapidjson::Document * variant_options) :
    ISchedulingAlgorithm(workload, decision, queue, selector, rjms_delay, variant_options)
{
    // @note LH: test csv file
    _logTime = new b_log();
    _p_job = new Priority_Job();
    (void) queue;
}

EasyBackfilling3::~EasyBackfilling3()
{
    // @note deallocate priority job struct
    delete _p_job;
}

/*********************************************************
 *                   MODIFIED FUNCTIONS                  *
**********************************************************/

void EasyBackfilling3::on_simulation_start(double date, const rapidjson::Value & batsim_event)
{
    // @note LH added for time analysis
    GET_TIME(_begin_overall);

    LOG_F(INFO,"on simulation start");
    pid_t pid = batsched_tools::get_batsched_pid();
    _decision->add_generic_notification("PID",std::to_string(pid),date);
    const rapidjson::Value & batsim_config = batsim_event["config"];

    _output_folder=batsim_config["output-folder"].GetString();
    _output_folder.replace(_output_folder.rfind("/out"), std::string("/out").size(), "");
    LOG_F(INFO,"output folder %s",_output_folder.c_str());

    //@note LH: added csv file to update timing data (using append mode to record multiple simulations)
    string time_dir="experiments/";
    string time_path= _output_folder.substr(0,_output_folder.find(time_dir));
    _logTime->update_log_file(time_path+time_dir+SRC_FILE+"_time_data.csv",b_log::TIME);

    //was there
    ISchedulingAlgorithm::set_generators(date);
    _available_machines.insert(IntervalSet::ClosedInterval(0, _nb_machines - 1));
    _nb_available_machines = _nb_machines;
    PPK_ASSERT_ERROR(_available_machines.size() == (unsigned int) _nb_machines);

    /* @note LH: total number of jobs in queue/schedule will be at most the number of machines.
        Removing the memory reservations will decrease performance and it is worth noting that
        both the schedule and queue are pointer vectors, so reserved memory will be negligible */
    _scheduled_jobs.reserve(_nb_machines);
    _waiting_jobs.reserve(_nb_machines);
    
    _queue_policy=batsim_config["queue-policy"].GetString();
    LOG_F(ERROR, "QUEUE POLICY = %s", _queue_policy.c_str());
    (void) batsim_config;
}

void EasyBackfilling3::on_simulation_end(double date)
{

    // @note LH added for time analysis
    double end_overall = 0.0;
    GET_TIME(end_overall);
    // @note create csv row with simulation timing data
    string row_fmt = "%d,%d,%d,%.15f,%.15f";
    auto time_str = batsched_tools::string_format(
            row_fmt,
                _workload->nb_jobs(),
                _nb_machines,
                _backfill_counter,
                end_overall-_begin_overall,
                _decision_time
    );
    //  @note update csv file with the timing data
    TCSV_F(b_log::TIME, date, "%s", time_str.c_str());
    (void) date;
}

void EasyBackfilling3::make_decisions(double date,
                                     SortableJobOrder::UpdateInformation *update_info,
                                     SortableJobOrder::CompareInformation *compare_info)
{
    // @note LH added for time analysis
    GET_TIME(_begin_decision);

    Job * priority_job_before = get_first_waiting_job();

    // Let's remove finished jobs from the schedule
    for (const string & ended_job_id : _jobs_ended_recently){
        handle_finished_job(ended_job_id, date);
    }
    
    // Let's handle recently released jobs
    std::vector<std::string> recently_queued_jobs;
    for (const string & new_job_id : _jobs_released_recently)
    {
        Job * new_job = (*_workload)[new_job_id];

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
            _waiting_jobs.push_back(new_job);
            recently_queued_jobs.push_back(new_job_id);
        }
    }

    // Queue sorting
    Job * priority_job_after = nullptr;
    sort_queue_while_handling_priority_job(priority_job_before, priority_job_after, update_info);
    
    // If no resources have been released, we can just try to backfill the newly-released jobs
    if (_jobs_ended_recently.empty())
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
                check_next_job(new_job, date);
                if(_can_run){
                    _decision->add_execute_job(new_job_id, _tmp_job->allocated_machines, date);
                    _waiting_jobs.erase(wj_it);
                }
            }
        }
    }
    else
    {   
        // Some resources have been released, the whole queue should be traversed.
        auto job_it = _waiting_jobs.begin();
        // Let's try to backfill all the jobs
        while (job_it != _waiting_jobs.end() && _nb_available_machines > 0)
        {
            Job * job = *job_it;

            if(job == priority_job_after) check_priority_job(job, date);
            else check_next_job(job, date);
            
            if(_can_run){
                _decision->add_execute_job(job->id, _tmp_job->allocated_machines, date);
                job_it = delete_waiting_job(job_it);
                if(job == priority_job_after) priority_job_after = get_first_waiting_job();
            }else ++job_it;  
        }
    }
    

    // @note LH: adds queuing info to the out_jobs_extra.csv file
    _decision->add_generic_notification("queue_size",std::to_string(_waiting_jobs.size()),date);
    _decision->add_generic_notification("schedule_size",std::to_string(_scheduled_jobs.size()),date);
/*    // @todo LH: fix this
    _decision->add_generic_notification("number_running_jobs",std::to_string(_schedule.get_number_of_running_jobs()),date);
    _decision->add_generic_notification("utilization",std::to_string(_schedule.get_utilization()),date);
    _decision->add_generic_notification("utilization_no_resv",std::to_string(_schedule.get_utilization_no_resv()),date);
*/
    // @note LH added for time analysis
    GET_TIME(_end_decision);
    _decision_time += (_end_decision-_begin_decision);
}

void EasyBackfilling3::sort_queue_while_handling_priority_job(Job * priority_job_before,
                                                             Job *& priority_job_after,
                                                             SortableJobOrder::UpdateInformation * update_info)
{
    // Let's sort the queue
    sort_max_heap(_waiting_jobs);

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

            // @note LH: (1) Initial scheduling of jobs
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
    }

    /* @note 
        priority job can run if the following is true:
            - requested resources <=  current available 
    */
    _can_run = _p_job->requested_resources <= machine_count;

    if(_can_run){

        // @note priority job can run so add it to the schedule
        handle_scheduled_job(priority_job,date);

        // @note sort the schedule
        sort_max_heap(_scheduled_jobs);
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

//@note LH: added function check if next job can be backfilled
void EasyBackfilling3::check_next_job(const Job * next_job, double date){   

    /* @note LH:
        job can be backfilled if the following is true:
            - job will finish before the priority jobs reserved start (shadow) time 
                -AND- the requested resources are <= current available resources
            - otherwise job finishs after priority jobs start time 
                -AND- the requested resources are <= MIN[current available nodes, waiting priority job extra nodes]
    */
    _can_run = ((date+C2DBL(next_job->walltime)) <= _p_job->shadow_time)
        ? (next_job->nb_requested_resources <= _nb_available_machines) 
        : (next_job->nb_requested_resources <= MIN(_nb_available_machines ,_p_job->extra_resources));

    /* @note LH:
        job can be backfilled, 
            - add it to the schedule
            - sort the schedule
            - increase backfilled jobs count
    */ 
    if(_can_run){
        handle_scheduled_job(next_job, date);
        sort_max_heap(_scheduled_jobs);
        _backfill_counter++;
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
}

//@notw LH: added function that handles deallocating finished jobs
void EasyBackfilling3::handle_finished_job(string job_id, double date){
    // @note LH: get finished job from scheduler
    auto fj_it = find_if(_scheduled_jobs.begin(), _scheduled_jobs.end(), [job_id](Scheduled_Job *sj) { 
        return (sj->id == job_id);
    });
    if(fj_it != _scheduled_jobs.end()){
        auto fj_idx = distance(_scheduled_jobs.begin(), fj_it);
        _tmp_job = _scheduled_jobs.at(fj_idx);
        // @note LH: return allocated machines to intervalset and add to machine count
        _available_machines.insert(_tmp_job->allocated_machines);
        _nb_available_machines += _tmp_job->requested_resources;

        // @note deallocate finished job struct
        delete _tmp_job;
        // @note LH: delete the pointer to the job struct from the vector
        _scheduled_jobs.erase(fj_it);
    }
}

/*********************************************************
 *         SCHEDULE & QUEUE HEAP SORT ADDITIONS          *
**********************************************************/

//@note LH: added helper sub-function to turn schedule into a maximum heap
void EasyBackfilling3::max_heapify(int root, int size, vector<Scheduled_Job *> job_vect){
    // @note find the max root node, left node, and right node
    int max = root, left = (2 * root) + 1, right = left + 1;

    // @note LH: if not the last node -AND- the left is more than the root, set left node as new max root
    if (left < size && job_vect[left]->est_finish_time > job_vect[max]->est_finish_time){
        max = left;
    }
    // @note LH: if not the last node -AND- right node is more than the root, set right node as new max root
    if (right < size && job_vect[right]->est_finish_time  > job_vect[max]->est_finish_time){
        max = right;
    }

    // @note swap[max_root(max),last_node(size)] and heapify the new max root
    if (max != root) {
        swap(job_vect[root], job_vect[max]);
        max_heapify(max, size, job_vect);
    }
}

//@note LH: added heaper function for heap sorting the schedule
void EasyBackfilling3::sort_max_heap(vector<Scheduled_Job *> job_vect){
    int size = job_vect.size();
    //@note LH: no need to sort if there is less than 2 jobs
    if(size < 2) return;

    // @note LH: build the new max heap
    for(int i = size / 2; i >= 0; i--){
        max_heapify(i, size, job_vect);
    }

    // @note LH: sort the new max heap
    for(int j = size - 1; j > 0; j--){
        swap(job_vect[0], job_vect[j]);
        max_heapify(0, j, job_vect);
    }
}


//@note LH: added helper sub-function to turn the queue into a maximum heap
void EasyBackfilling3::max_heapify(int root, int size, vector<Job *> job_vect){  

    // @note find the max root node, left node, and right node
    int max = root, left = (2 * root) + 1, right = left + 1;
    bool left_exists = left < size, right_exists =  right < size;
    double left_node = -1, right_node = -1, max_node = -1;

    if(_queue_policy == "FCFS"){
        if(left_exists) left_node = job_vect[left]->submission_time;
        if(right_exists) right_node = job_vect[right]->submission_time;
        max_node = job_vect[max]->submission_time;
    }

    if(_queue_policy == "ORGINAL-FCFS"){
        if(left_exists) left_node = job_vect[left]->submission_times[0];
        if(right_exists) right_node = job_vect[right]->submission_times[0];
        max_node = job_vect[max]->submission_times[0];
    }

    if((left_exists && right_exists) && left_node == right_node){
        left_node = stod(job_vect[left]->id.substr(3));
        right_node = stod(job_vect[right]->id.substr(3));
        max_node = stod(job_vect[max]->id.substr(3));
    }
    
    // @note LH: if not the last node -AND- the left is more than the root, set left node as new max root
    if (left_exists && left_node > max_node) max = left;

    // @note LH: if not the last node -AND- right node is more than the root, set right node as new max root
    if (right_exists && right_node > max_node) max = right;

    // @note swap[max_root(max),last_node(size)] and heapify the new max root
    if (max != root) {
        swap(job_vect[root], job_vect[max]);
        max_heapify(max, size, job_vect);
    }
}

//@note LH: added heaper function for heap sorting the queue
void EasyBackfilling3::sort_max_heap(vector<Job *> job_vect){
    int size = job_vect.size();
    //@note LH: no need to sort if there is less than 2 jobs
    if(size < 2) return;

    // @note LH: build the new max heap
    for(int i = size / 2; i >= 0; i--){
        max_heapify(i, size, job_vect);
    }

    // @note LH: sort the new max heap
    for(int j = size - 1; j > 0; j--){
        swap(job_vect[0], job_vect[j]);
        max_heapify(0, j, job_vect);
    }
}

/*********************************************************
 * QUEUE REPLACEMENT ADDITIONS
**********************************************************/

//@note LH: added helper function to return the first waiting job 
Job * EasyBackfilling3::get_first_waiting_job(){
    return _waiting_jobs.empty() ? nullptr : *_waiting_jobs.begin();
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
