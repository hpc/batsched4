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
void ISchedulingAlgorithm::normal_start(double date, const rapidjson::Value & batsim_event)
{
    CLOG_F(INFO,"on simulation start");
    pid_t pid = batsched_tools::get_batsched_pid();
    _decision->add_generic_notification("PID",std::to_string(pid),date);
    const rapidjson::Value & batsim_config = batsim_event["config"];

    // @note LH: set output folder for logging
    _output_folder=batsim_config["output-folder"].GetString();
    _output_folder.replace(_output_folder.rfind("/out"), std::string("/out").size(), "");
    CLOG_F(CCU_DEBUG_FIN,"output folder %s",_output_folder.c_str());
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
void ISchedulingAlgorithm::schedule_start(double date, const rapidjson::Value & batsim_event)
{
    const rapidjson::Value & batsim_config = batsim_event["config"];
    _output_svg=batsim_config["output-svg"].GetString();
    _output_svg_method = batsim_config["output-svg-method"].GetString();
    _svg_frame_start = batsim_config["svg-frame-start"].GetInt64();
    _svg_frame_end = batsim_config["svg-frame-end"].GetInt64();
    _svg_output_start = batsim_config["svg-output-start"].GetInt64();
    _svg_output_end = batsim_config["svg-output-end"].GetInt64();
    Schedule::convert_policy(batsim_config["reschedule-policy"].GetString(),_reschedule_policy);
    Schedule::convert_policy(batsim_config["impact-policy"].GetString(),_impact_policy);
    CLOG_F(CCU_DEBUG_FIN,"output svg: %s, method: %s",_output_svg.c_str(),_output_svg_method.c_str());
    //was there
    _schedule = Schedule(_nb_machines, date);
    //added
    _schedule.set_output_svg(_output_svg);
    _schedule.set_output_svg_method(_output_svg_method);
    _schedule.set_svg_frame_and_output_start_and_end(_svg_frame_start,_svg_frame_end,_svg_output_start,_svg_output_end);
    _schedule.set_svg_prefix(_output_folder + "/svg/");
    _schedule.set_policies(_reschedule_policy,_impact_policy);
}
void ISchedulingAlgorithm::set_compute_resources(const rapidjson::Value & batsim_event)
{
    const rapidjson::Value& resources = batsim_event["compute_resources"];
    for ( auto & itr : resources.GetArray())
    {
	machine* a_machine = new machine();
        if (itr.HasMember("id"))
            a_machine->id = (itr)["id"].GetInt();
        if (itr.HasMember("core_count"))
        {
            a_machine->core_count = (itr)["core_count"].GetInt();
            a_machine->cores_available = int(a_machine->core_count * _core_percent);
        }
        if (itr.HasMember("speed"))
            a_machine->speed = (itr)["speed"].GetDouble();
        if (itr.HasMember("name"))
            a_machine->name = (itr)["name"].GetString();
        machines_by_int[a_machine->id] = a_machine;
        machines_by_name[a_machine->name] = a_machine;
        //LOG_F(INFO,"machine id = %d, core_count= %d , cores_available= %d",a_machine->id,a_machine->core_count,a_machine->cores_available);
   }
}
void ISchedulingAlgorithm::requested_failure_call(double date, batsched_tools::CALL_ME_LATERS cml_in)
{
    double number = failure_exponential_distribution->operator()(generator_failure);
    batsched_tools::KILL_TYPES killType;
    switch(cml_in.forWhat){
    
        case batsched_tools::call_me_later_types::SMTBF:
            //Log the failure
            BLOG_F(blog_types::FAILURES,"%s,%s",blog_failure_event::FAILURE.c_str(),"SMTBF");
            CLOG_F(CCU_DEBUG,"SMTBF Failure");
            killType=batsched_tools::KILL_TYPES::SMTBF;
        break;
        case batsched_tools::call_me_later_types::MTBF:
            BLOG_F(blog_types::FAILURES, "%s,%s",blog_failure_event::FAILURE.c_str(),"MTBF");
            CLOG_F(CCU_DEBUG,"MTBF Failure");
            killType=batsched_tools::KILL_TYPES::MTBF;
        break;
        case batsched_tools::call_me_later_types::FIXED_FAILURE:
            BLOG_F(blog_types::FAILURES,"%s,%s", blog_failure_event::FAILURE.c_str(),"FIXED_FAILURE");
            CLOG_F(CCU_DEBUG,"Fixed Failure");
            killType=batsched_tools::KILL_TYPES::FIXED_FAILURE;
        break;
        case batsched_tools::call_me_later_types::REPAIR_DONE:
        {
            rapidjson::Document doc;
            doc.Parse(cml_in.extra_data.c_str());
            PPK_ASSERT(doc.HasMember("machine"),"Error, repair done but no 'machine' field in extra_data");
            int machine_number = doc["machine"].GetInt();
            BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::REPAIR_DONE.c_str() ,machine_number);
            CLOG_F(CCU_DEBUG,"Repair Done On Machine: %d",machine_number);
            //a repair is done, all that needs to happen is add the machines to available
            //and remove them from repair machines and add one to the number of available
            IntervalSet machine = machine_number;
            _available_machines += machine;
            _unavailable_machines -= machine;
            _repair_machines -= machine;
            _nb_available_machines=_available_machines.size();
            _machines_that_became_available_recently += machine;
            _need_to_backfill = true;
            if (_reject_possible)
                _repairs_done++;
        }
        break;
    }
    if (_workload->_repair_time == -1.0  && _workload->_MTTR == -1.0)
        _on_machine_instant_down_ups.push_back(killType);
    else
        _on_machine_down_for_repairs.push_back(killType);
    batsched_tools::CALL_ME_LATERS cml;
    cml.forWhat = cml_in.forWhat;
    cml.id = _nb_call_me_laters;
    _decision->add_call_me_later(date,number+date,cml);
    
}
IntervalSet ISchedulingAlgorithm::normal_repair(double date)
{
     //get a random number of a machine to kill
    int number = machine_unif_distribution->operator()(generator_machine);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = number;
    //if the machine is already down for repairs ignore it.
    BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::MACHINE_REPAIR.c_str(),number);
    if ((machine & _repair_machines).is_empty())
    {
        CLOG_F(CCU_DEBUG,"here, machine going down for repair %d",number);
        //ok the machine is not down for repairs
        //it will be going down for repairs now
        _available_machines-=machine;
        _unavailable_machines+=machine;
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
        return machine;
    }
    else{
        BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::MACHINE_ALREADY_DOWN.c_str(),number);
        return IntervalSet::empty_interval_set();
    }
}
IntervalSet ISchedulingAlgorithm::normal_downUp(double date)
{
    //get a random number of a machine to kill
    int number = machine_unif_distribution->operator()(generator_machine);
    //make it an intervalset so we can find the intersection of it with current allocations
    IntervalSet machine = number;
    BLOG_F(blog_types::FAILURES,"%s,%d",blog_failure_event::MACHINE_INSTANT_DOWN_UP.c_str(), number);
    CLOG_F(CCU_DEBUG,"here, machine going down for repair %d",number);
    return machine;
}
void ISchedulingAlgorithm::set_failure_map(std::map<double,batsched_tools::failure_tuple> failure_map)
{
 _file_failures = failure_map;
 for (auto myPair: failure_map)
 {
    batsched_tools::CALL_ME_LATERS cml;
    cml.id = _nb_call_me_laters;
    cml.forWhat = myPair.second.type;
    _decision->add_call_me_later(0,myPair.first,cml);
 }   
}
void ISchedulingAlgorithm::set_nb_machines(int nb_machines)
{
    CLOG_F(CCU_DEBUG_FIN,"_nb_machines: %d",_nb_machines);
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

void ISchedulingAlgorithm::on_requested_call(double date,batsched_tools::CALL_ME_LATERS cml)
{
    if (cml.forWhat == batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED)
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
    unsigned time_seed = std::chrono::system_clock::now().time_since_epoch().count();
    generator_failure_seed = _workload->_seed_failures;
    generator_machine_seed = _workload->_seed_failure_machine;
    generator_repair_time_seed = _workload->_seed_repair_time;
    if (_workload->_seed_failures == -1)
        generator_failure_seed = time_seed;
    if (_workload->_seed_failure_machine == -1)
        generator_machine_seed = time_seed;
    if (_workload->_seed_repair_time == -1)
        generator_repair_time_seed = time_seed;
        
    generator_failure.seed(generator_failure_seed);
    generator_machine.seed(generator_machine_seed);
    generator_repair_time.seed(generator_repair_time_seed);
    if (_workload->_fixed_failures != -1.0)
     {
        if (machine_unif_distribution == nullptr)
            machine_unif_distribution = new std::uniform_int_distribution<int>(0,_nb_machines-1);
        double number = _workload->_fixed_failures;
        batsched_tools::CALL_ME_LATERS cml;
        cml.forWhat = batsched_tools::call_me_later_types::FIXED_FAILURE;
        cml.id = _nb_call_me_laters;
        _decision->add_call_me_later(date,number+date,cml); 
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
        batsched_tools::CALL_ME_LATERS cml;
        cml.forWhat = batsched_tools::call_me_later_types::SMTBF;
        cml.id = _nb_call_me_laters;
        _decision->add_call_me_later(date,number+date,cml);
    }
    else if (_workload->_MTBF!=-1.0)
    {
        failure_exponential_distribution = new std::exponential_distribution<double>(1.0/_workload->_MTBF);
        std::exponential_distribution<double>::param_type new_lambda(1.0/_workload->_MTBF);
        failure_exponential_distribution->param(new_lambda);
        double number;         
        number = failure_exponential_distribution->operator()(generator_failure);
        nb_failure_exponential_distribution++;
        batsched_tools::CALL_ME_LATERS cml;
        cml.forWhat = batsched_tools::call_me_later_types::MTBF;
        cml.id = _nb_call_me_laters;
        _decision->add_call_me_later(date,number+date,cml);
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
    double epsilon = 1e-4;
    if (_batsim_checkpoint_interval_type == "simulated")
    {
        LOG_F(INFO,"_nb_batsim_checkpoints: %d",_nb_batsim_checkpoints);
        if ((date >= _batsim_checkpoint_interval_seconds*(_nb_batsim_checkpoints + 1)) && (date > _start_from_checkpoint_time+epsilon))
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
    if (((_batsim_checkpoint_interval_type != "False" && check_checkpoint_time(date))||_need_to_send_checkpoint) && !_block_checkpoint)
    {
       LOG_F(INFO,"here");
        _decision->add_generic_notification("checkpoint","",date);
        LOG_F(INFO,"here");
        if (!_need_to_send_checkpoint)//check that this is a scheduled checkpoint
            _nb_batsim_checkpoints +=1;
        _need_to_send_checkpoint=false;
        LOG_F(INFO,"here");
        return true;
    }
    else
        return false;
}
void ISchedulingAlgorithm::ingest_variables(double date)
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
    ISchedulingAlgorithm::on_ingest_variables(variablesDoc,date);
    LOG_F(INFO,"here");
    if (variablesDoc.HasMember("derived"))
        on_ingest_variables(variablesDoc,date);

}
void ISchedulingAlgorithm::on_ingest_variables(const rapidjson::Document & doc,double date)
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
    _nb_batsim_checkpoints = base["_nb_batsim_checkpoints"].GetInt();
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
        <<"\t\t\"_nb_call_me_laters\":"                                   <<  batsched_tools::to_json_string(_nb_call_me_laters)              <<","<<std::endl
        <<"\t\t\"_nb_batsim_checkpoints\":"                               <<  batsched_tools::to_json_string(_nb_batsim_checkpoints)          <<std::endl
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
