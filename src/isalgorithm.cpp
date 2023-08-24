#include "isalgorithm.hpp"

#include "pempek_assert.hpp"
#include "batsched_tools.hpp"
#include <chrono>
#include <ctime>

using namespace std;

void ISchedulingAlgorithm::set_nb_machines(int nb_machines)
{
    PPK_ASSERT_ERROR(_nb_machines == -1);
    _nb_machines = nb_machines;
}

void ISchedulingAlgorithm::set_redis(RedisStorage *redis)
{
    PPK_ASSERT_ERROR(_redis == nullptr);
    _redis = redis;
}

void ISchedulingAlgorithm::clear_recent_data_structures()
{
    _jobs_released_recently.clear();
    _jobs_ended_recently.clear();
    _jobs_killed_recently.clear();
    _jobs_whose_waiting_time_estimation_has_been_requested_recently.clear();
    _machines_whose_pstate_changed_recently.clear();
    _machines_that_became_available_recently.clear();
    _machines_that_became_unavailable_recently.clear();
    _nopped_recently = false;
    _consumed_joules_updated_recently = false;
    _consumed_joules = -1;
}

ISchedulingAlgorithm::ISchedulingAlgorithm(Workload *workload,
                                           SchedulingDecision *decision,
                                           Queue *queue,
                                           ResourceSelector *selector,
                                           double rjms_delay,
                                           rapidjson::Document *variant_options) :
    _workload(workload), _decision(decision), _queue(queue), _selector(selector),
    _rjms_delay(rjms_delay), _variant_options(variant_options)
{

}

ISchedulingAlgorithm::~ISchedulingAlgorithm()
{

}

void ISchedulingAlgorithm::on_job_release(double date, const vector<string> &job_ids)
{
    (void) date;
    _jobs_released_recently.insert(_jobs_released_recently.end(),
                                   job_ids.begin(),
                                   job_ids.end());
}

void ISchedulingAlgorithm::on_job_end(double date, const vector<string> &job_ids)
{
    (void) date;
    _jobs_ended_recently.insert(_jobs_ended_recently.end(),
                                job_ids.begin(),
                                job_ids.end());
}



void ISchedulingAlgorithm::on_job_killed(double date, const std::unordered_map<std::string,batsched_tools::Job_Message *> &job_msgs)
{
    (void) date;
    _jobs_killed_recently.insert(job_msgs.begin(),
                                 job_msgs.end());
}

void ISchedulingAlgorithm::on_machine_state_changed(double date, IntervalSet machines, int new_state)
{
    (void) date;

    if (_machines_whose_pstate_changed_recently.count(new_state) == 0)
        _machines_whose_pstate_changed_recently[new_state] = machines;
    else
        _machines_whose_pstate_changed_recently[new_state] += machines;
}

void ISchedulingAlgorithm::on_requested_call(double date,int id, batsched_tools::call_me_later_types forWhat)
{
    if (forWhat == batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED)
        _need_to_checkpoint = true;
    (void) date;
    _nopped_recently = true;
}

void ISchedulingAlgorithm::on_no_more_static_job_to_submit_received(double date)
{
    (void) date;
    _no_more_static_job_to_submit_received = true;
}

void ISchedulingAlgorithm::on_no_more_external_event_to_occur(double date)
{
    (void) date;
    _no_more_external_event_to_occur_received = true;
}

void ISchedulingAlgorithm::on_answer_energy_consumption(double date, double consumed_joules)
{
    (void) date;
    _consumed_joules = consumed_joules;
    _consumed_joules_updated_recently = true;
}

void ISchedulingAlgorithm::on_machine_available_notify_event(double date, IntervalSet machines)
{
    (void) date;
    _machines_that_became_available_recently += machines;
}
/*void ISchedulingAlgorithm::set_workloads(myBatsched::Workloads *w){
    (void) w;
}
*/
void ISchedulingAlgorithm::set_machines(Machines *m){
    (void) m;
}
void ISchedulingAlgorithm::on_simulation_start(double date, const rapidjson::Value & batsim_config){
    _start_real_time = _real_time;
}
void ISchedulingAlgorithm::set_generators(double date){
    unsigned seed = 0;
    if (_workload->_seed_failures)
        seed = std::chrono::system_clock::now().time_since_epoch().count();
    generator.seed(seed);
    generator2.seed(seed);
    unsigned seed_repair_time = 10;
    if (_workload->_seed_repair_time)
        seed_repair_time = std::chrono::system_clock::now().time_since_epoch().count();
    generator_repair_time.seed(seed_repair_time);
    if (_workload->_fixed_failures != -1.0)
     {
        if (unif_distribution == nullptr)
            unif_distribution = new std::uniform_int_distribution<int>(0,_nb_machines-1);
        double number = _workload->_fixed_failures;
        _decision->add_call_me_later(batsched_tools::call_me_later_types::FIXED_FAILURE,1,number+date,date);  
     }
    if (_workload->_MTTR != -1.0)
    {
        if (repair_time_exponential_distribution == nullptr)
            repair_time_exponential_distribution = new std::exponential_distribution<double>(1.0/_workload->_MTTR);
    }
    if (_workload->_SMTBF != -1.0)
    {
        distribution = new std::exponential_distribution<double>(1.0/_workload->_SMTBF);
        if (unif_distribution == nullptr)
            unif_distribution = new std::uniform_int_distribution<int>(0,_nb_machines-1);
        std::exponential_distribution<double>::param_type new_lambda(1.0/_workload->_SMTBF);
        distribution->param(new_lambda);
        double number;         
        number = distribution->operator()(generator);
        _decision->add_call_me_later(batsched_tools::call_me_later_types::SMTBF,1,number+date,date);
    }
    else if (_workload->_MTBF!=-1.0)
    {
        distribution = new std::exponential_distribution<double>(1.0/_workload->_MTBF);
        std::exponential_distribution<double>::param_type new_lambda(1.0/_workload->_MTBF);
        distribution->param(new_lambda);
        double number;         
        number = distribution->operator()(generator);
        _decision->add_call_me_later(batsched_tools::call_me_later_types::MTBF,1,number+date,date);
    }
}
void ISchedulingAlgorithm::set_real_time(std::chrono::_V2::system_clock::time_point time)
{
    _real_time = time;
}
void ISchedulingAlgorithm::set_checkpoint_time(long seconds,std::string checkpoint_type)
{
    _batsim_checkpoint_interval_type = checkpoint_type;
    _batsim_checkpoint_interval_seconds = seconds;
    _start_real_time = _real_time;
}
bool ISchedulingAlgorithm::check_checkpoint_time(double date)
{
    if (_batsim_checkpoint_interval_type == "simulated")
    {
        if (date >= _batsim_checkpoint_interval_seconds*(_nb_batsim_checkpoints + 1))
            return true;
        else
            return false;
            
    }
    else
    {
        long duration = std::chrono::duration_cast<std::chrono::seconds>(_real_time - _start_real_time).count();
        if ( duration >= _batsim_checkpoint_interval_seconds*(_nb_batsim_checkpoints + 1))
            return true;
        else
            return false;
    }
}
bool ISchedulingAlgorithm::send_batsim_checkpoint_if_ready(double date){
    if (_batsim_checkpoint_interval_type != "False" && check_checkpoint_time(date))
    {
        _decision->add_generic_notification("checkpoint","",date);
        //_decision->add_call_me_later(batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED,_nb_batsim_checkpoints,date,date);
        _nb_batsim_checkpoints +=1;
        return true;
    }
    else
        return false;
}
void ISchedulingAlgorithm::checkpoint_batsched(double date){

}

void ISchedulingAlgorithm::on_machine_unavailable_notify_event(double date, IntervalSet machines)
{
    (void) date;
    _machines_that_became_unavailable_recently += machines;
}
void ISchedulingAlgorithm::on_job_fault_notify_event(double date, std::string job)  // ****************added
{
    (void) date;
}
void ISchedulingAlgorithm::on_myKillJob_notify_event(double date){
    (void) date;
}
void ISchedulingAlgorithm::on_query_estimate_waiting_time(double date, const string &job_id)
{
    (void) date;
    _jobs_whose_waiting_time_estimation_has_been_requested_recently.push_back(job_id);
}
