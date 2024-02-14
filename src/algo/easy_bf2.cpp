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
    _myBLOG = new b_log();
    _myBLOG->add_log_file(_output_folder+"/log/Soft_Errors.log",blog_types::SOFT_ERRORS);
    _myBLOG->add_log_file(_output_folder+"/log/simulated_failures.log",blog_types::FAILURES);
    (void) batsim_config;
  

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
     //lets do all the normal stuff first
    std::set<batsched_tools::call_me_later_types> blocked_cmls;
    _block_checkpoint = true;
    blocked_cmls.insert(batsched_tools::call_me_later_types::FIXED_FAILURE);
    blocked_cmls.insert(batsched_tools::call_me_later_types::MTBF);
    blocked_cmls.insert(batsched_tools::call_me_later_types::SMTBF);
    blocked_cmls.insert(batsched_tools::call_me_later_types::REPAIR_DONE);
    blocked_cmls.insert(batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED);
    _decision->set_blocked_call_me_laters(blocked_cmls);
    pid_t pid = batsched_tools::get_batsched_pid();
    _decision->add_generic_notification("PID",std::to_string(pid),date);
    const rapidjson::Value & batsim_config = batsim_event["config"];
    LOG_F(INFO,"***** on_start_from_checkpoint ******");
    _output_svg=batsim_config["output-svg"].GetString();
    std::string output_svg_method = batsim_config["output-svg-method"].GetString();
    //output_svg_method = "text";
    _svg_frame_start = batsim_config["svg-frame-start"].GetInt64();
    _svg_frame_end = batsim_config["svg-frame-end"].GetInt64();
    _svg_output_start = batsim_config["svg-output-start"].GetInt64();
    _svg_output_end = batsim_config["svg-output-end"].GetInt64();
    LOG_F(INFO,"output svg %s",_output_svg.c_str());
    _output_folder=batsim_config["output-folder"].GetString();
    _output_folder.replace(_output_folder.rfind("/out"), std::string("/out").size(), "");
    
    Schedule::convert_policy(batsim_config["reschedule-policy"].GetString(),_reschedule_policy);
    Schedule::convert_policy(batsim_config["impact-policy"].GetString(),_impact_policy);

    _schedule = Schedule(_nb_machines, date);
    _schedule.set_output_svg(_output_svg);
    _schedule.set_output_svg_method(output_svg_method);
    _schedule.set_svg_frame_and_output_start_and_end(_svg_frame_start,_svg_frame_end,_svg_output_start,_svg_output_end);
    _schedule.set_svg_prefix(_output_folder + "/svg/");
    _schedule.set_policies(_reschedule_policy,_impact_policy);


    std::string schedule_filename = _output_folder + "/start_from_checkpoint/batsched_schedule.chkpt";
    ifstream ifile(schedule_filename);
    PPK_ASSERT_ERROR(ifile.is_open(), "Cannot read schedule file '%s'", schedule_filename.c_str());
    std::string content;

    ifile.seekg(0, ios::end);
    content.reserve(static_cast<unsigned long>(ifile.tellg()));
    ifile.seekg(0, ios::beg);

    content.assign((std::istreambuf_iterator<char>(ifile)),
                std::istreambuf_iterator<char>());
    ifile.close();
    rapidjson::Document scheduleDoc;
    scheduleDoc.Parse(content.c_str());
    LOG_F(INFO,"here");
    _start_from_checkpoint.jobs_that_should_have_been_submitted_already = _schedule.get_jobs_that_should_have_been_submitted_already(scheduleDoc);
    LOG_F(INFO,"here");
    //we need to set our generators even though they will be overwritten, so that distributions aren't null
    ISchedulingAlgorithm::set_generators(date);

    //ok now we need to modify things but it would be best to wait until jobs are being submitted
    //when the first jobs come in, they should be the "previous currently running" jobs
    //so just get our checkpoint directory
    
    //we will need to look-up jobs in the schedule so set the workload over there
    _schedule.set_workload(_workload);
    _schedule.set_start_from_checkpoint(&_start_from_checkpoint);
    _recently_under_repair_machines = IntervalSet::empty_interval_set();
    _recover_from_checkpoint = true;
    _myBLOG = new b_log();
    _myBLOG->add_log_file(_output_folder+"/log/Soft_Errors.log",blog_types::SOFT_ERRORS);
    _myBLOG->add_log_file(_output_folder+"/log/simulated_failures.log",blog_types::FAILURES);
    
   //we are going to wait on setting any generators
}
// @note Leslie added on_checkpoint_batsched(double date)
void EasyBackfilling2::on_checkpoint_batsched(double date)
{
    LOG_F(INFO,"here");
    std::string checkpoint_dir = _output_folder+"/checkpoint_latest";
    LOG_F(INFO,"here");
    std::ofstream f(checkpoint_dir+"/batsched_schedule.chkpt",std::ios_base::out);
    if (f.is_open())
    {
        f<<_schedule.to_json_string()<<std::endl;
        f.close();
    }
    LOG_F(INFO,"here");
    f.open(checkpoint_dir+"/batsched_queues.chkpt",std::ios_base::out);
    if (f.is_open())
    {
        f<<"{\n"
            <<"\t\"_queue\":"<<_queue->to_json_string()<<std::endl;
        // @note Leslie commented out 
        // <<"\t\"_reservation_queue\":"<<_reservation_queue->to_json_string()<<std::endl;
        f<<"}";
        f.close();
    }
    LOG_F(INFO,"here");
    f.open(checkpoint_dir+"/batsched_variables.chkpt",std::ios_base::app);
    if (f.is_open())
    {
        f<<std::fixed<<std::setprecision(15)<<std::boolalpha
        <<"\t,\"derived\":{\n" //after base we need a comma // @note Leslie changed syntax 
        <<"\t\t\"_output_svg\":"<<batsched_tools::to_json_string(_output_svg)<<","<<std::endl
        <<"\t\t\"_svg_frame_start\":"<<_svg_frame_start<<","<<std::endl
        <<"\t\t\"_svg_frame_end\":"<<_svg_frame_end<<","<<std::endl
        <<"\t\t\"_svg_output_start\":"<<_svg_output_start<<","<<std::endl
        <<"\t\t\"_svg_output_end\":"<<_svg_output_end<<","<<std::endl
        <<"\t\t\"_reschedule_policy\":"<<int(_reschedule_policy)<<","<<std::endl
        <<"\t\t\"_impact_policy\":"<<int(_impact_policy)<<","<<std::endl
        <<"\t\t\"_previous_date\":"<<_previous_date<<","<<std::endl
        // @note Leslie commented out 
        // <<"\t\"_saved_reservations\":"<<_schedule.vector_to_json_string(&_saved_reservations)<<","<<std::endl
        <<"\t\t\"_killed_jobs\":"<<_killed_jobs<<","<<std::endl
        <<"\t\t\"_need_to_send_finished_submitting_jobs\":"<<_need_to_send_finished_submitting_jobs<<","<<std::endl
        <<"\t\t\"_saved_recently_queued_jobs\":"<<batsched_tools::vector_to_json_string(&_saved_recently_queued_jobs)<<","<<std::endl
        <<"\t\t\"_saved_recently_ended_jobs\":"<<batsched_tools::vector_to_json_string(&_saved_recently_ended_jobs)<<","<<std::endl
        <<"\t\t\"_recently_under_repair_machines\":\""<<_recently_under_repair_machines.to_string_hyphen()<<"\","<<std::endl
        <<"\t\t\"_need_to_compress\":"<<_need_to_compress<<","<<std::endl
        <<"\t\t\"_checkpointing_on\":"<<_checkpointing_on<<","<<std::endl
        // @note Leslie commented out
        //<<"\t\"_start_a_reservation\":"<<_start_a_reservation<<","<<std::endl
        <<"\t\t\"_resubmitted_jobs\":"<<batsched_tools::map_to_json_string(&_resubmitted_jobs)<<","<<std::endl
        <<"\t\t\"_resubmitted_jobs_released\":"<<batsched_tools::vector_pair_to_json_string(&_resubmitted_jobs_released)<<","<<std::endl
        <<"\t\t\"_on_machine_instant_down_ups\":"<<batsched_tools::vector_to_json_string(&_on_machine_instant_down_ups)<<","<<std::endl
        <<"\t\t\"_on_machine_down_for_repairs\":"<<batsched_tools::vector_to_json_string(&_on_machine_down_for_repairs)<<","<<std::endl
        <<"\t\t\"_call_me_laters\":"<<batsched_tools::map_to_json_string(_decision->get_call_me_laters())<<","<<std::endl
        <<"\t\t\"SIMULATED_CHECKPOINT_TIME\":"<<batsched_tools::to_json_string(date)<<","<<std::endl
        <<"\t\t\"REAL_CHECKPOINT_TIME\":"<<batsched_tools::to_json_string(_real_time)<<std::endl
        <<"\t}";//closes derived, still a brace open // @note Leslie changed syntax 
        f.close();
    }

    _need_to_checkpoint=false;
}
// @note Leslie added on_ingest_variables(const rapidjson::Document & doc,double date)
void EasyBackfilling2::on_ingest_variables(const rapidjson::Document & doc,double date)
{
    using namespace rapidjson;
    const Value & derived = doc["derived"];
    _output_svg = derived["_output_svg"].GetString();
    _svg_frame_start = derived["_svg_frame_start"].GetInt();
    _svg_frame_end = derived["_svg_frame_end"].GetInt();
    _svg_output_start = derived["_svg_output_start"].GetInt();
    _svg_output_end = derived["_svg_output_end"].GetInt();
    _reschedule_policy = static_cast<Schedule::RESCHEDULE_POLICY>(derived["_reschedule_policy"].GetInt());
    _impact_policy = static_cast<Schedule::IMPACT_POLICY>(derived["_impact_policy"].GetInt());
    _previous_date = derived["_previous_date"].GetDouble();
    /* @note Leslie commented out 
    _saved_reservations.clear();
    const Value & Vsr = derived["_saved_reservations"].GetArray();
    for (SizeType i = 0;i<Vsr.Size();i++)
    {
        _saved_reservations.push_back(_schedule.ReservedTimeSlice_from_json(Vsr[i]));
    }
    */
    _killed_jobs = derived["_killed_jobs"].GetBool();
    _need_to_send_finished_submitting_jobs = derived["_need_to_send_finished_submitting_jobs"].GetBool();
    //not going to get _saved_recently_queued_jobs or _saved_recently_ended_jobs
    std::string rurm = derived["_recently_under_repair_machines"].GetString();
    if (rurm == "")
        _recently_under_repair_machines = IntervalSet::empty_interval_set();
    else
        _recently_under_repair_machines = IntervalSet::from_string_hyphen(rurm);
    _need_to_compress = derived["_need_to_compress"].GetBool();
    _checkpointing_on = derived["_checkpointing_on"].GetBool();
    //@note Leslie commented out 
    // _start_a_reservation = derived["_start_a_reservation"].GetBool();
    //not going to get _resubmitted_jobs _resubmitted_jobs_released
    const Value & Vomidu = derived["_on_machine_instant_down_ups"].GetArray();
    for (SizeType i=0;i<Vomidu.Size();i++)
    {
        _on_machine_instant_down_ups.push_back(static_cast<batsched_tools::KILL_TYPES>(Vomidu[i].GetInt()));
    }
    const Value & Vomdfr = derived["_on_machine_down_for_repairs"].GetArray();
    for (SizeType i=0;i<Vomdfr.Size();i++)
    {
        _on_machine_down_for_repairs.push_back(static_cast<batsched_tools::KILL_TYPES>(Vomdfr[i].GetInt()));
    }
    const Value & Vcml = derived["_call_me_laters"].GetArray();
    LOG_F(INFO,"here");
    std::map<int,batsched_tools::CALL_ME_LATERS> cmls;
    LOG_F(INFO,"here");
    for (SizeType i=0;i<Vcml.Size();i++)
    {
        batsched_tools::CALL_ME_LATERS cml;
        LOG_F(INFO,"here");
        cml.time = Vcml[i]["value"]["time"].GetDouble();
        LOG_F(INFO,"here");
        cml.forWhat = static_cast<batsched_tools::call_me_later_types>(Vcml[i]["value"]["forWhat"].GetInt());
        LOG_F(INFO,"here");
        cml.extra_data = Vcml[i]["value"]["extra_data"].GetString();
        LOG_F(INFO,"here");
        cml.id = Vcml[i]["value"]["id"].GetInt();
        LOG_F(INFO,"here");
        cmls[cml.id]=cml;
    }
    _decision->set_call_me_laters(cmls,date,true);
    _decision->remove_blocked_call_me_later(batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED);
    LOG_F(INFO,"here");
}
bool EasyBackfilling2::all_submitted_jobs_check_passed()
{
    for (auto job_id :_jobs_released_recently)
        _start_from_checkpoint.jobs_that_have_been_submitted_already.insert(job_id);
    for (auto job_id :_start_from_checkpoint.jobs_that_should_have_been_submitted_already)
    {
        if (!(_start_from_checkpoint.jobs_that_have_been_submitted_already.count(job_id)==1))
        {
            LOG_F(INFO,"job_id: %s",job_id.c_str());
            return false;
        }
    }
    return true;
}
// @note Leslie added on_first_jobs_submitted(double date)
void EasyBackfilling2::on_first_jobs_submitted(double date)
{
    //ok we need to update things now
    //workload should have our jobs now
    //lets ingest our schedule
    _decision->clear_blocked_call_me_laters();
    _decision->add_blocked_call_me_later(batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED);
    _block_checkpoint = false;
    std::string schedule_filename = _output_folder + "/start_from_checkpoint/batsched_schedule.chkpt";
    ifstream ifile(schedule_filename);
    PPK_ASSERT_ERROR(ifile.is_open(), "Cannot read schedule file '%s'", schedule_filename.c_str());
    std::string content;

    ifile.seekg(0, ios::end);
    content.reserve(static_cast<unsigned long>(ifile.tellg()));
    ifile.seekg(0, ios::beg);

    content.assign((std::istreambuf_iterator<char>(ifile)),
                std::istreambuf_iterator<char>());
    ifile.close();
    rapidjson::Document scheduleDoc;
    scheduleDoc.Parse(content.c_str());
    LOG_F(INFO,"here");
    
    _schedule.ingest_schedule(scheduleDoc);
    LOG_F(INFO,"here");
    ingest_variables(date);
    LOG_F(INFO,"here");
    content = "";
    std::string batsim_filename = _output_folder + "/start_from_checkpoint/batsim_variables.chkpt";
    ifile.open(batsim_filename);
    PPK_ASSERT_ERROR(ifile.is_open(), "Cannot read batsim_variables.chkpt file '%s'", batsim_filename.c_str());

    ifile.seekg(0, ios::end);
    content.reserve(static_cast<unsigned long>(ifile.tellg()));
    ifile.seekg(0, ios::beg);

    content.assign((std::istreambuf_iterator<char>(ifile)),
                std::istreambuf_iterator<char>());
    ifile.close();
    rapidjson::Document batVarDoc;
    batVarDoc.Parse(content.c_str());
    LOG_F(INFO,"here");
    /*  This was thought to be needed.  It may be in the future, but at this time
        it simply results in a duplication of the call_me_later at the target time
        This is because we are starting the simulation effectively at time 0 and going through all the call_me_laters.
        This may be wrong.  It may be advisable to set a variable when on_first_jobs_submitted() is run and only do call me laters
        when this has been set*/
    /*
    rapidjson::Value & Vcml = batVarDoc["call_me_laters"];
    LOG_F(INFO,"here");
    for (rapidjson::Value::ConstMemberIterator it = Vcml.MemberBegin(); it != Vcml.MemberEnd(); ++it)
    {
        const rapidjson::Value & value = it->value;
        batsched_tools::call_me_later_types forWhat = static_cast<batsched_tools::call_me_later_types>(value["forWhat"].GetInt());
        int id = value["id"].GetInt();
        double target_time = value["target_time"].GetDouble();
        _decision->add_call_me_later(forWhat,id,target_time,date);
    }
    */
    LOG_F(INFO,"here");
    //now get the first time slice jobs to execute on the same machines they exectued on before
    for (auto kv_pair:_schedule.begin()->allocated_jobs)
    {
        
        _decision->add_execute_job(kv_pair.first->id,kv_pair.second,date);
        //we don't remove the job from queue because we are going to make the queue back to how it was in a second
    }
    LOG_F(INFO,"here");
    content = "";
    std::string queues_filename = _output_folder + "/start_from_checkpoint/batsched_queues.chkpt";
    ifile.open(queues_filename);
    PPK_ASSERT_ERROR(ifile.is_open(), "Cannot read batsched_queues.chkpt file '%s'", queues_filename.c_str());
    LOG_F(INFO,"here");
    ifile.seekg(0, ios::end);
    LOG_F(INFO,"here");
    content.reserve(static_cast<unsigned long>(ifile.tellg()));
    ifile.seekg(0, ios::beg);
    LOG_F(INFO,"here");
    content.assign((std::istreambuf_iterator<char>(ifile)),
                std::istreambuf_iterator<char>());

    ifile.close();
    LOG_F(INFO,"here");
    rapidjson::Document queueDoc;
    queueDoc.Parse(content.c_str());
    LOG_F(INFO,"content: %s",content.c_str());
    LOG_F(INFO,"here");
    if(!_queue->is_empty()) _queue->clear();
    LOG_F(INFO,"here");
    //@note !!! Error - restarting from checkpoint breaks here
    //@Leslie Not anymore, fixed.  took comma out of the queue file.
    rapidjson::Value & Vqueue = queueDoc["_queue"].GetArray();
    LOG_F(INFO,"here");
    SortableJobOrder::UpdateInformation update_info(date);
    LOG_F(INFO,"here");
    for (rapidjson::SizeType i=0;i<Vqueue.Size();i++)
    {
        std::string job_id = Vqueue[i].GetString();
        auto parts = batsched_tools::get_job_parts(job_id);
        const Job * new_job = (*_workload)[parts.next_checkpoint];
        
        _queue->append_job(new_job,&update_info);
    }
    LOG_F(INFO,"here");
    /* @note Leslie commented out 
    _reservation_queue->clear();
    rapidjson::Value & Vrqueue = queueDoc["_reservation_queue"].GetArray();
    for (rapidjson::SizeType i=0;i<Vrqueue.Size();i++)
    {
        std::string job_id = Vrqueue[i].GetString();
        auto parts = batsched_tools::get_job_parts(job_id);
        const Job * new_job = (*_workload)[parts.next_checkpoint];
        
        _reservation_queue->append_job(new_job,&update_info);
    }
    */
    //now touch back with batsim just to get things solidified
    _decision->add_generic_notification("recover_from_checkpoint","",date);
    _start_from_checkpoint_time = date;
}

/*********************************************************
 *            SIMULATED CHECKPOINTING FUNCTIONS          *
**********************************************************/

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
    BLOG_F(blog_types::FAILURES,"On_Machine_Down_For_Repair: %d",number);
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
        std::string extra_data = batsched_tools::string_format("{\"machine\":%d}",number);
        batsched_tools::CALL_ME_LATERS cml;
        cml.forWhat = batsched_tools::call_me_later_types::REPAIR_DONE;
        cml.id = _nb_call_me_laters;
        cml.extra_data = extra_data;
        _decision->add_call_me_later(date,date+repair_time,cml);

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
                    BLOG_F(blog_types::FAILURES,"On_Machine_Down_For_Repair_Kill: %s",job_id.c_str());
                    LOG_F(INFO,"killing job %s",job_id.c_str());
                    auto msg = new batsched_tools::Job_Message;
                    msg->id = job_id;
                    msg->forWhat = forWhat;
                    msgs.push_back(msg);
                }


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
    BLOG_F(blog_types::FAILURES,"Instant_Down_Up: %d",number);
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
       
        std::vector<std::string> jobs_to_kill;
        _schedule.get_jobs_running_on_machines(machine,jobs_to_kill);
                 
       
        if (!jobs_to_kill.empty())
        {
            
            std::vector<batsched_tools::Job_Message *> msgs;
            for (auto job_id : jobs_to_kill){
                BLOG_F(blog_types::FAILURES,"Instant_Down_Up_Kill: %s",job_id.c_str());
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

// @note Leslie modified on_requested_call(double date,int id,batsched_tools::call_me_later_types forWhat)
void EasyBackfilling2::on_requested_call(double date,batsched_tools::CALL_ME_LATERS cml_in)
{
        if (_output_svg != "none")
            _schedule.set_now((Rational)date);
        switch (cml_in.forWhat){

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
                            batsched_tools::CALL_ME_LATERS cml;
                            cml.forWhat = batsched_tools::call_me_later_types::SMTBF;
                            cml.id = _nb_call_me_laters;
                            _decision->add_call_me_later(date,number+date,cml);
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
                            batsched_tools::CALL_ME_LATERS cml;
                            cml.forWhat = batsched_tools::call_me_later_types::FIXED_FAILURE;
                            cml.id = _nb_call_me_laters;
                            _decision->add_call_me_later(date,number+date,cml);
                        }
                }
                break;
                
            case batsched_tools::call_me_later_types::REPAIR_DONE:
                {
                    //BLOG_F(b_log::FAILURES,"REPAIR_DONE");
                    rapidjson::Document doc;
                    doc.Parse(cml_in.extra_data.c_str());
                    PPK_ASSERT(doc.HasMember("machine"),"Error, repair done but no 'machine' field in extra_data");
                    int machine_number = doc["machine"].GetInt();
                    //a repair is done, all that needs to happen is add the machines to available
                    //and remove them from repair machines and add one to the number of available
                    if (_output_svg == "all")
                        _schedule.output_to_svg("top Repair Done  Machine #: "+std::to_string(machine_number));
                    IntervalSet machine = machine_number;
                    _schedule.remove_repair_machines(machine);
                    _schedule.remove_svg_highlight_machines(machine);
                    if (_output_svg == "all")
                        _schedule.output_to_svg("bottom Repair Done  Machine #: "+std::to_string(machine_number));
                    _need_to_compress=true; // @note Leslie added 
                    //LOG_F(INFO,"in repair_machines.size(): %d nb_avail: %d avail: %d  running_jobs: %d",_repair_machines.size(),_nb_available_machines,_available_machines.size(),_running_jobs.size());
                }
                break;

            /*    @note Leslie commented out 
            case batsched_tools::call_me_later_types::RESERVATION_START:
                {
                    _start_a_reservation = true;
                    //SortableJobOrder::UpdateInformation update_info(date);
                    //make_decisions(date,&update_info,nullptr);
                    
                }
            break;
            */
            // @note Leslie added 
            case batsched_tools::call_me_later_types::CHECKPOINT_BATSCHED:
                {
                    _need_to_checkpoint = true;
                }
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
    LOG_F(INFO,"batsim_checkpoint_seconds: %d",_batsim_checkpoint_interval_seconds);
    send_batsim_checkpoint_if_ready(date);
    LOG_F(INFO,"here");
    if (_need_to_checkpoint){
        checkpoint_batsched(date);
    }
        
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


    const Job * priority_job_before = _queue->first_job_or_nullptr();

    // Let's remove finished jobs from the schedule
    for (const string & ended_job_id : _jobs_ended_recently)
        _schedule.remove_job_if_exists((*_workload)[ended_job_id]);

    // Let's handle recently released jobs
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
    // @note Leslie added 
    if( _recover_from_checkpoint && _start_from_checkpoint.received_submitted_jobs)
    {
        double epsilon = 1e-6;
        PPK_ASSERT(date - _start_from_checkpoint.first_submitted_time <= epsilon,"Error, waiting on all submitted jobs to come back resulted in simulated time moving too far ahead.");
        if (all_submitted_jobs_check_passed())
        {
            on_first_jobs_submitted(date);
            _recover_from_checkpoint = false;
        }
        
        return;
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
    _decision->handle_resubmission(_jobs_killed_recently,_workload,date);


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

            if (_schedule.contains_job(job))
                _schedule.remove_job_if_exists(job);

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
                    _schedule.remove_job_if_exists(job);
                    ++job_it;
                }
            }
        }
    }

    if (!_killed_jobs && _jobs_killed_recently.empty() && _queue->is_empty()  && _schedule.size() == 0 &&
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
