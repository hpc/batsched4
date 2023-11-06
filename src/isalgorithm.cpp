#include "isalgorithm.hpp"

#include "pempek_assert.hpp"
#include "batsched_tools.hpp"
#include <chrono>
#include <ctime>
#include <loguru.hpp>
#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

using namespace std;

void ISchedulingAlgorithm::set_nb_machines(int nb_machines)
{
    PPK_ASSERT_ERROR(_nb_machines == -1);
    _nb_machines = nb_machines;
}

void ISchedulingAlgorithm::set_redis(RedisStorage *redis)
{
    LOG_F(INFO,"here");
    //PPK_ASSERT_ERROR(_redis == nullptr);
    LOG_F(INFO,"here");
    _redis = redis;
    LOG_F(INFO,"here");
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
    _output_folder=batsim_config["output-folder"].GetString();
    _output_folder.replace(_output_folder.rfind("/out"), std::string("/out").size(), "");
    LOG_F(INFO,"output folder %s",_output_folder.c_str());
}
void ISchedulingAlgorithm::on_start_from_checkpoint(double date, const rapidjson::Value & batsim_config){
    
    _start_real_time = _real_time;
    _start_from_checkpoint.nb_folder= batsim_config["start-from-checkpoint"]["nb_folder"].GetInt();
    _start_from_checkpoint.nb_checkpoint = batsim_config["start-from-checkpoint"]["nb_checkpoint"].GetInt();
    _start_from_checkpoint.nb_previously_completed = batsim_config["start-from-checkpoint"]["nb_previously_completed"].GetInt();
    _start_from_checkpoint.nb_original_jobs = batsim_config["start-from-checkpoint"]["nb_original_jobs"].GetInt();
    _start_from_checkpoint.nb_actually_completed = _start_from_checkpoint.nb_previously_completed;
    _start_from_checkpoint.started_from_checkpoint = true;
    _start_from_checkpoint.checkpoint_folder =_output_folder+"/previous/checkpoint_"+std::to_string(_start_from_checkpoint.nb_checkpoint);
    _workload->start_from_checkpoint = &_start_from_checkpoint;
}

void ISchedulingAlgorithm::on_signal_checkpoint()
{
    LOG_F(INFO,"on_signal_checkpoint");
    _need_to_send_checkpoint = true;
}
void ISchedulingAlgorithm::set_generators(double date){
    unsigned seed = 0;
    if (_workload->_seed_failures)
        seed = std::chrono::system_clock::now().time_since_epoch().count();
    generator_failure_seed = seed;
    generator_machine_seed = seed;
    generator_failure.seed(seed);
    generator_machine.seed(seed);
    unsigned seed_repair_time = 10;
    if (_workload->_seed_repair_time)
        seed_repair_time = std::chrono::system_clock::now().time_since_epoch().count();
    generator_repair_time_seed = seed_repair_time;
    generator_repair_time.seed(seed_repair_time);
    if (_workload->_fixed_failures != -1.0)
     {
        if (machine_unif_distribution == nullptr)
            machine_unif_distribution = new std::uniform_int_distribution<int>(0,_nb_machines-1);
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
        failure_exponential_distribution = new std::exponential_distribution<double>(1.0/_workload->_SMTBF);
        if (machine_unif_distribution == nullptr)
            machine_unif_distribution = new std::uniform_int_distribution<int>(0,_nb_machines-1);
        std::exponential_distribution<double>::param_type new_lambda(1.0/_workload->_SMTBF);
        failure_exponential_distribution->param(new_lambda);
        double number;         
        number = failure_exponential_distribution->operator()(generator_failure);
        nb_failure_exponential_distribution++;
        _decision->add_call_me_later(batsched_tools::call_me_later_types::SMTBF,1,number+date,date);
    }
    else if (_workload->_MTBF!=-1.0)
    {
        failure_exponential_distribution = new std::exponential_distribution<double>(1.0/_workload->_MTBF);
        std::exponential_distribution<double>::param_type new_lambda(1.0/_workload->_MTBF);
        failure_exponential_distribution->param(new_lambda);
        double number;         
        number = failure_exponential_distribution->operator()(generator_failure);
        nb_failure_exponential_distribution++;
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
    LOG_F(INFO,"here");
    if ((_batsim_checkpoint_interval_type != "False" && check_checkpoint_time(date))||_need_to_send_checkpoint)
    {
       LOG_F(INFO,"here");
        _decision->add_generic_notification("checkpoint","",date);
        LOG_F(INFO,"here");
        _need_to_send_checkpoint=false;
        //_decision->add_call_me_later(batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED,_nb_batsim_checkpoints,date,date);
        if (!_need_to_send_checkpoint)//check that this is a scheduled checkpoint
            _nb_batsim_checkpoints +=1;
        LOG_F(INFO,"here");
        return true;
    }
    else
        return false;
}
void ISchedulingAlgorithm::ingest_variables()
{
    
    std::string filename = _output_folder + "/start_from_checkpoint/batsched_variables.chkpt";
    ifstream ifile(filename);
    PPK_ASSERT_ERROR(ifile.is_open(), "Cannot read batsched_variables file '%s'", filename.c_str());
    std::string content;

    ifile.seekg(0, ios::end);
    content.reserve(static_cast<unsigned long>(ifile.tellg()));
    ifile.seekg(0, ios::beg);

    content.assign((std::istreambuf_iterator<char>(ifile)),
                std::istreambuf_iterator<char>());
    ifile.close();
    rapidjson::Document variablesDoc;
    variablesDoc.Parse(content.c_str());
    LOG_F(INFO,"here");
    ISchedulingAlgorithm::on_ingest_variables(variablesDoc);
    LOG_F(INFO,"here");
    if (variablesDoc.HasMember("derived"))
        on_ingest_variables(variablesDoc);

}
void ISchedulingAlgorithm::on_ingest_variables(const rapidjson::Document & doc)
{
    std::string checkpoint_dir = _output_folder + "/start_from_checkpoint";
    //first get generators and distributions
    std::ifstream file;
    std::string filename;
    filename= checkpoint_dir+"/generator_failure.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>generator_failure;
        file.close();
    }
    filename = checkpoint_dir+"/generator_machine.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>generator_machine;
        file.close();
    }
    filename = checkpoint_dir+"/generator_repair_time.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>generator_repair_time;
        file.close();
    }
    filename = checkpoint_dir + "/failure_unif_distribution.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>*failure_unif_distribution;
        file.close();
    }
    filename = checkpoint_dir + "/failure_exponential_distribution.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>*failure_exponential_distribution;
        file.close();
    }
    filename = checkpoint_dir + "/machine_unif_distribution.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>(*machine_unif_distribution);
        file.close();
    }
    filename = checkpoint_dir + "/repair_time_exponential_distribution.dat";
    LOG_F(INFO,"here");
    if (fs::exists(filename))
    {
        file.open(filename);
        file>>*repair_time_exponential_distribution;
        file.close();
    }
    //next get variables  
    LOG_F(INFO,"here");
    using namespace rapidjson;
    const rapidjson::Value & base = doc["base"];
LOG_F(INFO,"here");
    

    
    _consumed_joules = base["_consumed_joules"].GetDouble();
    LOG_F(INFO,"here");
    std::string mtbar = base["_machines_that_became_available_recently"].GetString();
    LOG_F(INFO,"here");
    if (mtbar == "")
        _machines_that_became_available_recently = IntervalSet::empty_interval_set();
    else
        _machines_that_became_available_recently = IntervalSet::from_string_hyphen(mtbar);
        LOG_F(INFO,"here");
    std::string mtbur = base["_machines_that_became_unavailable_recently"].GetString();
    LOG_F(INFO,"here");
    if (mtbur == "")
        _machines_that_became_unavailable_recently = IntervalSet::empty_interval_set();
    else
        _machines_that_became_unavailable_recently = IntervalSet::from_string_hyphen(mtbar); 
    _machines_whose_pstate_changed_recently.clear();
    LOG_F(INFO,"here");
    const rapidjson::Value & Vmwpcr = base["_machines_whose_pstate_changed_recently"].GetArray();
    LOG_F(INFO,"here");
    for(SizeType i = 0;i<Vmwpcr.Size();i++)
    {
        int key = Vmwpcr[i]["key"].GetInt();
        IntervalSet value = IntervalSet::from_string_hyphen(Vmwpcr[i]["value"].GetString());
        
        _machines_whose_pstate_changed_recently[key]=value;
    }
    LOG_F(INFO,"here");
    //hopefully this works.  questionable since we are only running this after the first jobs submitted
    _jobs_killed_recently.clear();
    LOG_F(INFO,"here");
    const rapidjson::Value & Vjkr = base["_jobs_killed_recently"].GetArray();
    for(SizeType i = 0;i<Vjkr.Size();i++)
    {
        std::string key = Vjkr[i]["key"].GetString();
        batsched_tools::Job_Message * value;
        auto parts = batsched_tools::get_job_parts(Vjkr[i]["value"]["id"].GetString());
        value->id = parts.next_checkpoint;
        value->forWhat = static_cast<batsched_tools::KILL_TYPES>(Vjkr[i]["value"]["forWhat"].GetInt());
        value->progress = Vjkr[i]["value"]["progress"].GetDouble();
        value->progress_str = Vjkr[i]["value"]["progress_str"].GetString();
        
    }
    LOG_F(INFO,"here");
    _nb_call_me_laters = base["_nb_call_me_laters"].GetInt();
    //not doing any of the other variables
    //end: jobs shouldn't have been ended between the time batsched tells batsim to checkpoint and then batsched checkpoints
    //released: should've already been handled

    



}
void ISchedulingAlgorithm::checkpoint_batsched(double date)
{
    ISchedulingAlgorithm::on_checkpoint_batsched(date);
    on_checkpoint_batsched(date);
    std::string checkpoint_dir = _output_folder + "/checkpoint_latest";
    std::ofstream f(checkpoint_dir+"/batsched_variables.chkpt",std::ios_base::app);
    if (f.is_open())
    {
        f<<std::endl<<"}";
        f.close();
    }

}
void ISchedulingAlgorithm::on_checkpoint_batsched(double date){
    std::string checkpoint_dir = _output_folder + "/checkpoint_latest";
    std::ofstream file;
    file.open(checkpoint_dir+"/generator_failure.dat");
    file<<generator_failure;
    file.close();
    file.open(checkpoint_dir+"/generator_machine.dat");
    file<<generator_machine;
    file.close();
    file.open(checkpoint_dir + "/generator_repair_time.dat");
    file<<generator_repair_time;
    file.close();
    if (failure_unif_distribution != nullptr)
    {
        file.open(checkpoint_dir + "/failure_unif_distribution.dat");
        file<<*failure_unif_distribution;
        file.close();
    }

    if (failure_exponential_distribution != nullptr)
    {
        file.open(checkpoint_dir + "/failure_exponential_distribution.dat");
        file<<*failure_exponential_distribution;
        file.close();
    }

    if (machine_unif_distribution != nullptr)
    {
        file.open(checkpoint_dir + "/machine_unif_distribution.dat");
        file<<*machine_unif_distribution;
        file.close();
    }

    if (repair_time_exponential_distribution != nullptr)
    {
        file.open(checkpoint_dir + "/repair_time_exponential_distribution.dat");
        file<<*repair_time_exponential_distribution;
        file.close();
    }
    std::ofstream f(checkpoint_dir+"/batsched_variables.chkpt",std::ios_base::out);
    if (f.is_open())
    {
        f<<std::fixed<<std::setprecision(15)<<std::boolalpha
        <<"{\n"
        <<"\t\"base\":{\n"
        <<"\t\t\"_consumed_joules\":"                                     <<  batsched_tools::to_json_string(_consumed_joules)                <<","<<std::endl
        <<"\t\t\"_machines_that_became_available_recently\":\""           <<  _machines_that_became_available_recently.to_string_hyphen()     <<"\","<<std::endl
        <<"\t\t\"_machines_that_became_unavailable_recently\":\""         <<  _machines_that_became_unavailable_recently.to_string_hyphen()   <<"\","<<std::endl
        <<"\t\t\"_machines_whose_pstate_changed_recently\":"              <<  batsched_tools::map_to_json_string(_machines_whose_pstate_changed_recently)  <<","<<std::endl
        <<"\t\t\"_jobs_whose_waiting_time_estimation_has_been_requested_recently\":"  <<  batsched_tools::vector_to_json_string(_jobs_whose_waiting_time_estimation_has_been_requested_recently)  <<","<<std::endl
        <<"\t\t\"_jobs_killed_recently\":"                                <<  batsched_tools::unordered_map_to_json_string(_jobs_killed_recently) <<","<<std::endl
        <<"\t\t\"_jobs_ended_recently\":"                                 <<  batsched_tools::vector_to_json_string(_jobs_ended_recently)     <<","<<std::endl
        <<"\t\t\"_jobs_released_recently\":"                              <<  batsched_tools::vector_to_json_string(_jobs_released_recently)  <<","<<std::endl
        <<"\t\t\"_nb_call_me_laters\":"                                   <<  batsched_tools::to_json_string(_nb_call_me_laters)              <<std::endl
        <<"\t}"<<std::endl; //closes base brace, leaves a brace open
        f.close();


    }


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
