#include "schedule.hpp"
#include <cstdlib>
#include <vector>

#include <boost/algorithm/string/join.hpp>
#include <fstream>
#include <stdio.h>

#include <loguru.hpp>

#include "pempek_assert.hpp"
#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

using namespace std;
bool JobComparator::operator()(const Job *j1, const Job *j2) const
{
    return j1->id < j2->id;
}

Schedule::Schedule(int nb_machines,Rational initial_time)
{
    PPK_ASSERT_ERROR(nb_machines > 0);
    _nb_machines = nb_machines;

    TimeSlice slice;
    slice.begin = initial_time;
    slice.end = 1e19; // greater than the number of seconds elapsed since the big bang
    slice.length = slice.end - slice.begin;
    slice.available_machines.insert(IntervalSet::ClosedInterval(0, nb_machines - 1));
    slice.allocated_machines = IntervalSet::empty_interval_set();
    slice.nb_available_machines = nb_machines;
    PPK_ASSERT_ERROR(slice.available_machines.size() == (unsigned int)nb_machines);

    _profile.push_back(slice);

    generate_colors();
    _svg_highlight_machines = IntervalSet::empty_interval_set();
    _repair_machines.empty_interval_set();
}


Schedule::Schedule(const Schedule &other)
{
    *this = other;
}

void Schedule::set_workload(Workload * workload)
{
    _workload = workload;
}
void Schedule::set_start_from_checkpoint(batsched_tools::start_from_chkpt * sfc)
{
    _start_from_checkpoint = sfc;
}
void Schedule::set_svg_prefix(std::string svg_prefix){
    _svg_prefix = svg_prefix;
    fs::create_directories(_svg_prefix);
    
}
void Schedule::set_now(Rational now){
    if (_now != now)
    {
        ofstream f(_svg_prefix + "/time_frames.txt",std::ios_base::app);

    if (f.is_open())
        f << setw(10)<<now <<"\t\t"<<setw(10)<<_output_number<< "\n";

    f.close();
    
    _now = now;
    }
}
void Schedule::incorrect_call_me_later(double difference)
{
    //ok, it is a little after the time that a reservation should start
    //the reservation will be ending a little later as well
    //we increased the walltime to 1 second larger than a reservation's run time so there should be enough padding for when the reservation ends
    //but we need to take care of the first time slice's end and also the next time slice's begin
    auto slice = _profile.begin();
    slice->end+=difference;
    slice->length+=difference;
    slice++;
    slice->begin+=difference;
    slice->length-=difference;

}
void Schedule::ingest_schedule(rapidjson::Document & doc)
{
       
    using namespace rapidjson;
    //let's clear the schedule
    _profile.clear();
    
    PPK_ASSERT_ERROR(doc.HasMember("Schedule"),"Trying to ingest schedule from checkpoint, but there is no 'Schedule' key");
    const Value & schedule = doc["Schedule"];
    
    const Value & timeSlices = schedule["TimeSlices"].GetArray();
    PPK_ASSERT_ERROR(timeSlices.IsArray(), "Trying to ingest schedule from checkpoint, but Schedule is not an array");
    
    for (SizeType i = 0;i<timeSlices.Size();i++)
    {
        _profile.push_back(TimeSlice_from_json(timeSlices[i]));
    }
      
    const Value & colors = schedule["_colors"].GetArray();
    std::vector<std::string> temp;
    for(SizeType i = 0;i<colors.Size();i++)
    {
        temp.push_back(colors[i].GetString());
    }
    
    _colors = temp;
    _frame_number = schedule["_frame_number"].GetInt();
    _largest_time_slice_length = Rational(schedule["_largest_time_slice_length"].GetDouble());
    _nb_jobs_size = schedule["_nb_jobs_size"].GetInt();
    _nb_reservations_size = schedule["_nb_reservations_size"].GetInt();
    _output_number = schedule["_output_number"].GetInt();
    _previous_time_end = Rational(schedule["_previous_time_end"].GetDouble());
    
    std::string repair_machines = schedule["_repair_machines"].GetString();
    if (repair_machines == "")
        _repair_machines = IntervalSet::empty_interval_set();
    else
        _repair_machines = IntervalSet::from_string_hyphen(repair_machines);
    const Value & resv_colors = schedule["_reservation_colors"].GetArray();
    temp.clear();
    for(SizeType i = 0;i<resv_colors.Size();i++)
    {
        temp.push_back(resv_colors[i].GetString());
    }
    _reservation_colors = temp;
    _size = schedule["_size"].GetInt();
    _smallest_time_slice_length = Rational(schedule["_smallest_time_slice_length"].GetDouble());
    
    std::string svg_highlight_machines = schedule["_svg_highlight_machines"].GetString();
    if (svg_highlight_machines == "")
        _svg_highlight_machines = IntervalSet::empty_interval_set();
    else
        _svg_highlight_machines = IntervalSet::from_string_hyphen(svg_highlight_machines);
    
    _svg_prefix = schedule["_svg_prefix"].GetString();
    
    const Value & svg_resvs = schedule["_svg_reservations"].GetArray();
    std::list<Schedule::ReservedTimeSlice> tempL;
    for(SizeType i = 0;i<svg_resvs.Size();i++)
    {
        const Value & Vslice = svg_resvs[i];
        tempL.push_back(ReservedTimeSlice_from_json(Vslice));
    }
    _svg_reservations = tempL;

}
std::vector<const Job *> Schedule::JobVector_from_json(const rapidjson::Value & Varray)
{
    using namespace rapidjson;
    std::vector<const Job *> ourVector;
    for(SizeType i = 0;i<Varray.Size();i++)
    {
        ourVector.push_back((*_workload)[Varray[i].GetString()]);
    }
    return ourVector;
}
JobAlloc * Schedule::JobAlloc_from_json(const rapidjson::Value & Valloc)
{
    JobAlloc *alloc = new JobAlloc;
    
    if (Valloc.HasMember("begin"))
    {
    
        alloc->begin = Rational(std::stod(Valloc["begin"].GetString()));
        
        alloc->end = Rational(std::stod(Valloc["end"].GetString()));
        
        alloc->has_been_inserted = Valloc["has_been_inserted"].GetBool();
        
        alloc->started_in_first_slice = Valloc["started_in_first_slice"].GetBool();
    }
   // std::string job_id_str = Valloc["job"].GetString();
    
    
    //a job has to have used_machines in a schedule
    std::string used_machines = Valloc["used_machines"].GetString();
    //PPK_ASSERT(used_machines != "","Error, job: %s has an empty string for used_machines in batsched_schedule.chkpt",job_id_str.c_str() );
    alloc->used_machines = IntervalSet::from_string_hyphen(used_machines);
    
    return alloc;
}
Schedule::ReservedTimeSlice Schedule::ReservedTimeSlice_from_json(const rapidjson::Value & Vslice)
{
    using namespace rapidjson;
    //first get the individual parts
    const Value & Valloc = Vslice["alloc"];
    std::string job_str = Vslice["job"].GetString();
    const Value & Vjobs_affected = Vslice["jobs_affected"].GetArray();
    const Value & Vjobs_needed_to_be_killed = Vslice["jobs_to_be_killed"].GetArray();
    const Value & Vjobs_to_reschedule = Vslice["jobs_to_reschedule"].GetArray();
    bool success = Vslice["success"].GetBool();

    ReservedTimeSlice slice;
    // Let's create the job allocation
    JobAlloc * alloc = JobAlloc_from_json(Valloc);
    const Job * job = alloc->job;
    //lets get jobs_affected
    slice.jobs_affected = JobVector_from_json(Vjobs_affected);
    slice.jobs_needed_to_be_killed = JobVector_from_json(Vjobs_needed_to_be_killed);
    slice.jobs_to_reschedule = JobVector_from_json(Vjobs_to_reschedule);
    slice.job = job;
    TimeSlice ts;
    ts.begin = alloc->begin;
    slice.slice_begin = std::find(_profile.begin(),_profile.end(),ts);
    ts.begin = alloc->end;
    slice.slice_end = std::find(_profile.begin(),_profile.end(),ts);
    return slice;
}
Schedule::TimeSlice Schedule::TimeSlice_from_json(const rapidjson::Value & Vslice)
{
    using namespace rapidjson;
    TimeSlice slice;
    const Value & VTimeSlice= Vslice["Time slice"];
    
    slice.begin = Rational(VTimeSlice["begin"].GetDouble());
    //LOG_F(INFO,"here slice_begin: %f",slice.begin.convert_to<double>());
    slice.end = Rational(VTimeSlice["end"].GetDouble());
    
    slice.length = Rational(VTimeSlice["length"].GetDouble());
    std::string available_machines = VTimeSlice["available_machines"].GetString();
    if (available_machines == "")
        slice.available_machines = IntervalSet::empty_interval_set();
    else
        slice.available_machines = IntervalSet::from_string_hyphen(VTimeSlice["available_machines"].GetString());
    //if the length of the slice is above 100 years I think it is safe to assume it is the last slice
    //there will be no allocated jobs and there will be no allocated machines
    if (slice.length > 3153600000)
    {
        std::map<const Job*,IntervalSet,JobComparator> map;
        slice.allocated_jobs = map;
        slice.allocated_machines = IntervalSet::empty_interval_set();
    }
    else
    {
        slice.allocated_jobs = JobMap_from_json(VTimeSlice["allocated_jobs"].GetArray(),slice.begin);
        slice.allocated_machines = IntervalSet::from_string_hyphen(VTimeSlice["allocated_machines"].GetString());
    }
    
    slice.has_reservation = VTimeSlice["has_reservation"].GetBool();
    
    slice.nb_available_machines = VTimeSlice["nb_available_machines"].GetInt();
    
    slice.nb_reservations = VTimeSlice["nb_reservations"].GetInt();

    return slice;
}
std::map<const Job *, IntervalSet, JobComparator>  Schedule::JobMap_from_json(const rapidjson::Value & Vjobs,Rational begin)
{
    using namespace rapidjson;
    std::map<const Job*,IntervalSet,JobComparator> ourMap;
    for(SizeType i = 0;i<Vjobs.Size();i++)
    {
        const Value & Vjob = Vjobs[i];
        
        const Value & Vid = Vjob["job_id"];
       
        const Value & Valloc = Vjob["alloc"];
        JobAlloc * alloc = JobAlloc_from_json(Valloc);
        batsched_tools::job_parts parts = batsched_tools::get_job_parts(Vid.GetString()); 
        std::string next_checkpoint = parts.next_checkpoint;
    
        const Job * job = (*_workload)[next_checkpoint];
        //LOG_F(INFO,"here job:%s ",job->id.c_str());
        ourMap[job]=alloc->used_machines;
       
        alloc->job = job;
      
        if (Valloc.HasMember("begin"))
            alloc->job->allocations[alloc->begin]=alloc;
       
        

    }
    return ourMap;
}






void Schedule::set_smallest_and_largest_time_slice_length(Rational length){
    //first set the smallest and largest to init values

    if (_smallest_time_slice_length == 0 && _largest_time_slice_length == 1e19)
    {        
        _smallest_time_slice_length = length;
        _largest_time_slice_length = length;
        return;
    }
    else
    {
        //ok _smallest is no longer 0 and _largest is no longer 1e19
        if (length < _smallest_time_slice_length && length != 0)
            _smallest_time_slice_length = length;
        if (length > _largest_time_slice_length && length < 3.1536e7) // length must be less than a year's seconds, ie not the last time slice
            _largest_time_slice_length = length;
        return;
            
    }
    
    
}
Rational Schedule::get_smallest_time_slice_length(){
    return _smallest_time_slice_length;
}
Rational Schedule::get_largest_time_slice_length()
{
    return _largest_time_slice_length;
}
void Schedule::set_output_svg(std::string output_svg){
    _output_svg = output_svg;
    if(_output_svg == "none")
        _debug = _short_debug = false;
    else if(_output_svg == "all"){
        LOG_F(INFO,"setting debug to true svgs");
        _debug = true;
        _short_debug = false;
    }
    else if(_output_svg == "short"){
        _debug = false;
        _short_debug = true;
    }
}
void Schedule::set_output_svg_method(std::string output_svg_method)
{
    _output_svg_method = output_svg_method;

}
void Schedule::set_svg_frame_and_output_start_and_end(long frame_start, long frame_end,long output_start,long output_end){
    _svg_frame_start = frame_start;
    _svg_frame_end = frame_end;
    _svg_output_start = output_start;
    _svg_output_end = output_end;
}
void Schedule::set_policies(RESCHEDULE_POLICY r_policy,IMPACT_POLICY i_policy){
    _reschedule_policy = r_policy;
    _impact_policy = i_policy;
}

void Schedule::convert_policy(std::string policy, RESCHEDULE_POLICY & variable){
    if (policy == "RESCHEDULE_AFFECTED")
        variable = RESCHEDULE_POLICY::AFFECTED ;
    else if (policy == "RESCHEDULE_ALL")
        variable = RESCHEDULE_POLICY::ALL;
    else
        variable = RESCHEDULE_POLICY::NONE;
    return;
}
void Schedule::convert_policy(std::string policy, IMPACT_POLICY & variable){
    if (policy == "LEAST_KILLING_LARGEST_FIRST")
        variable = IMPACT_POLICY::LEAST_KILLING_LARGEST_FIRST;
    else if (policy == "LEAST_KILLING_SMALLEST_FIRST")
        variable = IMPACT_POLICY::LEAST_KILLING_SMALLEST_FIRST;
    else if (policy == "LEAST_RESCHEDULING")
        variable = IMPACT_POLICY::LEAST_RESCHEDULING;
    else
        variable = IMPACT_POLICY::NONE;
    return;
}
void Schedule::add_svg_highlight_machines(IntervalSet machines)
{
    _svg_highlight_machines += machines;
}
bool Schedule::remove_svg_highlight_machines(IntervalSet machines)
{
    if (machines.is_subset_of(_svg_highlight_machines))
    {
        _svg_highlight_machines -= machines;
        return true;
    }
    else
        return false;
}
IntervalSet Schedule::add_repair_machine(IntervalSet machine,double duration){
    //first add the repair machines to our IntervalSet
    IntervalSet added = machine - _repair_machines;
    int number_added = added.size();
    _repair_machines+=machine;
    if(!added.is_empty())
    {
        
        //we used to just take the machines away from the timeslices
        //now we are going to add reservations to the schedule with purpose="repair" //TODO

        
        //ok so we added some machines
        //lets take them away from the time_slices
        for (auto slice_it = _profile.begin();slice_it!=_profile.end();++slice_it)
        {
            slice_it->nb_available_machines-=number_added;
            slice_it->available_machines -=added;
        }
        
       /*
        rapidjson::Document jobDoc;
        std::string ourJobString;
        ourJobString = batsched_tools::string_format(
        R"({ 
            "id": "repair_%s",
            "subtime": %f,
            "res": %d,
            "profile": "1",
            "walltime": %f,
            "purpose":"repair",
            "alloc":"%s"
        })",machine.to_string_elements(),0,machine.size(),duration,machine.to_string_hyphen());
        jobDoc.Parse(ourJobString.c_str());
        Job * job = _workload->job_from_json_object(jobDoc);
       */
    }
    return added;

}

/*
IntervalSet Schedule::add_repair_machines_as_job(IntervalSet machines,double duration){
    //first add the repair machines to our IntervalSet
    IntervalSet added = machines - _repair_machines;
    int number_added = added.size();
    _repair_machines+=machines;
    if(!added.is_empty())
    {
        //ok so we added some machines
        //instead of taking them away from slices let's treat the repair time as a job duration
        //first get affected jobs
        std::map<const Job *,IntervalSet> jobs_affected_on_machines;
        get_jobs_affected_on_machines(machines,jobs_affected_on_machines,true);
        
        add_repair_job(job);

    }
    return added;

}
*/

//This function will remove the given machines that are in _repair_machines
//It will also add back those machines that are not allocated to each time slice
IntervalSet Schedule::remove_repair_machines(IntervalSet machines){
    IntervalSet removed = _repair_machines & machines;
    _repair_machines-=machines;
    int number_removed = removed.size();
    if (!removed.is_empty())
    {
        for (auto slice_it = _profile.begin();slice_it!=_profile.end();++slice_it)
        {
            //we don't want to add machines that are already being used
            //normally they shouldn't be in _repair_machines if there are
            //jobs using them but if the _repair_machine was added and the jobs using
            //that machine were not removed then this can be the case
            //originally we used which_machines_are_allocated_in_time_slice
                //Intervalset allocated = which_machines_are_allocated_in_time_slice(slice_it,removed)
            //added machines_allocated to time slice, so no need to waste time traversing all jobs in time slice
            IntervalSet removed_and_allocated = slice_it->allocated_machines & removed;          
            slice_it->nb_available_machines+=(number_removed-removed_and_allocated.size());
            slice_it->available_machines +=(removed-removed_and_allocated);
            
        }
    }
    return removed;

}
JobAlloc Schedule::add_repair_job(Job *job)
{
    //TODO
    
    JobAlloc alloc;
    return alloc;
}
//This function will return the intersection of machines that are allocated
//in the time slice with the machines that you give it.  Pass it all machines to find out
//all machines used in time slice
IntervalSet Schedule::which_machines_are_allocated_in_time_slice(TimeSliceIterator slice, IntervalSet machines)
{
    IntervalSet machines_allocated = IntervalSet::empty_interval_set();
    for(auto job_interval_pair : slice->allocated_jobs)
    {
        machines_allocated +=(job_interval_pair.second & machines);
    }
    return machines_allocated;
}
int Schedule::get_number_of_running_jobs(){
    return _profile.begin()->allocated_jobs.size();
}
/* This function takes the difference between total number of machines and the first timeslice's nb_available_machines
*  This may be inaccurate for some uses as repair machines are not "running" but are also not available. 
*/
int Schedule::get_number_of_running_machines(){
    return int(_nb_machines - _profile.begin()->nb_available_machines );
}
double Schedule::get_utilization(){
    return double(get_number_of_running_machines()) / double(_nb_machines);
}
double Schedule::get_utilization_no_resv(){
    //if first slice has reservations take those machines out of the equation
    if (_profile.begin()->has_reservation){
        int resv_machines = get_machines_running_reservations().size();
        return double(get_number_of_running_machines()-resv_machines)/double(_nb_machines);
    }
    else
        return -1;
}

void Schedule::get_jobs_running_on_machines(IntervalSet machines,std::vector<std::string>& jobs_running_on_machines){
    for (auto job_interval_pair : _profile.begin()->allocated_jobs)
    {
        //is there an intersection between this job in the first slice and the machines in question?
        if (!(job_interval_pair.second & machines).is_empty())
        {
            //yes there is an intersection, add the job id
            if (job_interval_pair.first->purpose != "reservation")
                jobs_running_on_machines.push_back(job_interval_pair.first->id);

        }
    }
    return;
}

void Schedule::get_jobs_running_on_machines(IntervalSet machines,std::map< const Job *,IntervalSet>& jobs_running_on_machines){

    for (auto job_interval_pair : _profile.begin()->allocated_jobs)
    {
        //is there an intersection between this job in the first slice and the machines in question?
        if (!(job_interval_pair.second & machines).is_empty())
        {
            //yes there is an intersection, add the job id
            if (job_interval_pair.first->purpose != "reservation")
                jobs_running_on_machines[job_interval_pair.first]=job_interval_pair.second;

        }
    }
    return;
}

void Schedule::get_jobs_affected_on_machines(IntervalSet machines, std::vector<std::string>& jobs_affected_on_machines,bool reservations_too){
    std::map<const Job*,IntervalSet> jobs_running_on_machines;
    std::map<const Job *,IntervalSet> jobs_affected;
    get_jobs_running_on_machines(machines,jobs_running_on_machines);
    auto slice_it = _profile.begin();
    ++slice_it;
    //go through all time slices after the first to see what is affected by machines
    for (;slice_it!=_profile.end();++slice_it){
        //go through each {job,allocation} pair to see what is affected
        for( auto job_interval_pair : slice_it->allocated_jobs)
        {   
            //first check that it's not a reservation
            if (job_interval_pair.first->purpose != "reservation" || reservations_too)
            {
                //ok it's not a reservation
                //check if there is an intersection of machines and the job's machines
                //then make sure it is not running
                if (!(job_interval_pair.second & machines).is_empty() && jobs_running_on_machines.count(job_interval_pair.first)==0)
                {
                    //yes there is an intersection and no it is not running
                    //add the job to a map first so we don't add it more than once
                    jobs_affected[job_interval_pair.first]=job_interval_pair.second;
                }
            }
        }
    }
    //now convert the map to a simple vector of strings
    for(auto job_interval_pair : jobs_affected)
    {
        jobs_affected_on_machines.push_back(job_interval_pair.first->id);
    }
    
}
void Schedule::get_jobs_affected_on_machines(IntervalSet machines, std::map<const Job *,IntervalSet>& jobs_affected_on_machines,bool reservations_too){
    std::map<const Job*,IntervalSet> jobs_running_on_machines;
    get_jobs_running_on_machines(machines,jobs_running_on_machines);
    auto slice_it = _profile.begin();
    ++slice_it;
    //go through all time slices after the first to see what is affected by machines
    for (;slice_it!=_profile.end();++slice_it){
        //go through each {job,allocation} pair to see what is affected
        for( auto job_interval_pair : slice_it->allocated_jobs)
        {   
            //first check that it's not a reservation
            if (job_interval_pair.first->purpose != "reservation" || reservations_too)
            {
                //ok it's not a reservation
                //check if there is an intersection of machines and the job's machines
                //then make sure it is not running
                if (!(job_interval_pair.second & machines).is_empty() && jobs_running_on_machines.count(job_interval_pair.first)==0)
                {
                    //yes there is an intersection and no it is not running
                    //add the job to the map
                    jobs_affected_on_machines[job_interval_pair.first]=job_interval_pair.second;
                }
            }
        }
    }
    
}
std::vector<std::string> Schedule::get_reservations_running_on_machines(IntervalSet machines){
    std::vector<std::string> reservations_running_on_machines;
    for (auto job_interval_pair : _profile.begin()->allocated_jobs)
    {
        //is there an intersection between this job in the first slice and the machines in question?
        if (!(job_interval_pair.second & machines).is_empty())
        {
            //yes there is an intersection, add the job id
            if (job_interval_pair.first->purpose == "reservation")
                reservations_running_on_machines.push_back(job_interval_pair.first->id);

        }
    }
    return reservations_running_on_machines;
}
IntervalSet Schedule::get_machines_running_reservations(){
    return get_machines_running_reservations_on_slice(_profile.begin());
}
IntervalSet Schedule::get_machines_running_reservations_on_slice(TimeSliceIterator slice){
    IntervalSet machines;
    for (auto job_interval_pair : slice->allocated_jobs)
    {
            if (job_interval_pair.first->purpose == "reservation")
                machines+=job_interval_pair.second;
    }
    return machines;
}
Schedule &Schedule::operator=(const Schedule &other)
{
    _profile = other._profile;
    _nb_machines = other._nb_machines;
    _output_number = other._output_number;
    _colors = other._colors;

    return *this;
}

void Schedule::update_first_slice(Rational current_time)
{
    
    
    double epsilon = 1e-4;
    auto slice = _profile.begin();
 
    PPK_ASSERT_ERROR(
        (current_time + epsilon)>= slice->begin, "current_time=%g, slice->begin=%g", (double)current_time+epsilon, (double)slice->begin);
    PPK_ASSERT_ERROR(
        current_time <= (slice->end+epsilon), "current_time=%g, slice->end=%g", (double)current_time, (double)slice->end+epsilon);

    Rational old_time = slice->begin;
    slice->begin = current_time;
    slice->length = slice->end - slice->begin;
    //LOG_F(INFO,"allocated_jobs.size: %d, old time: %.15f",slice->allocated_jobs.size(),old_time.convert_to<double>());
    for (auto it = slice->allocated_jobs.begin(); it != slice->allocated_jobs.end(); ++it)
    {
        const Job *job_ref = (it->first);
        //LOG_F(INFO,"update: job_id: %s",job_ref->id.c_str());
        //for (auto myAlloc:job_ref->allocations)
        //    LOG_F(INFO,"update: job_id: %s, myAlloc: %.15f",job_ref->id.c_str(),myAlloc.first.convert_to<double>());
        auto alloc_it = job_ref->allocations.find(old_time);

        if (alloc_it != job_ref->allocations.end() && old_time != current_time)
        {
            //LOG_F(INFO,"update: job_id: %s,current_time: %.15f",job_ref->id.c_str(),current_time.convert_to<double>());
            job_ref->allocations[current_time] = alloc_it->second;
            job_ref->allocations.erase(alloc_it);
        }
    }
}

void Schedule::update_first_slice_removing_remaining_jobs(Rational current_time)
{
    PPK_ASSERT_ERROR(current_time < infinite_horizon());

    auto slice = _profile.begin();
    PPK_ASSERT_ERROR(
        current_time >= slice->begin, "current_time=%g, slice->begin=%g", (double)current_time, (double)slice->begin);

    while (current_time >= slice->end)
        slice = _profile.erase(slice);

    slice->begin = current_time;
    slice->length = slice->end - slice->begin;
}

int Schedule::size(){
    return _size;
}
int Schedule::nb_jobs_size(){
    return _nb_jobs_size;
}
int Schedule::nb_reservations_size(){
    return _nb_reservations_size;
}

void Schedule::remove_job(const Job *job)
{
    remove_job_first_occurence(job);
}

bool Schedule::remove_job_if_exists(const Job *job)
{
    auto job_first_slice = find_first_occurence_of_job(job, _profile.begin());

    if (job_first_slice != _profile.end())
    {
        remove_job_internal(job, job_first_slice);
        return true;
    }

    return false;
}

void Schedule::remove_job_all_occurences(const Job *job)
{
    auto job_first_slice = find_first_occurence_of_job(job, _profile.begin());

    while (job_first_slice != _profile.end())
    {
        remove_job_internal(job, job_first_slice);
        job_first_slice = find_first_occurence_of_job(job, job_first_slice);
    }
}

void Schedule::remove_job_first_occurence(const Job *job)
{
    auto job_first_slice = find_first_occurence_of_job(job, _profile.begin());
    PPK_ASSERT_ERROR(job_first_slice != _profile.end(),
        "Cannot remove job '%s' from the schedule since it is not in it", job->id.c_str());

    remove_job_internal(job, job_first_slice);
}

void Schedule::remove_job_last_occurence(const Job *job)
{
    auto job_first_slice = find_last_occurence_of_job(job, _profile.begin());
    PPK_ASSERT_ERROR(job_first_slice != _profile.end(),
        "Cannot remove job '%s' from the schedule since it is not in it", job->id.c_str());

    remove_job_internal(job, job_first_slice);
}

JobAlloc Schedule::add_job_first_fit(
    const Job *job, ResourceSelector *selector, bool assert_insertion_successful)
{
    PPK_ASSERT_ERROR(!contains_job(job),
        "Invalid Schedule::add_job_first_fit call: Cannot add "
        "job '%s' because it is already in the schedule. %s",
        job->id.c_str(), to_string().c_str());

    return add_job_first_fit_after_time_slice(job, _profile.begin(), selector, assert_insertion_successful);
}
void Schedule::find_least_impactful_fit(JobAlloc* alloc,TimeSliceIterator begin_slice, TimeSliceIterator end_slice, IMPACT_POLICY policy){
    
    LOG_F(INFO,"find_least_impactful_fit");
    const Job * job = alloc->job;
    //we will want to use at least these machines
    IntervalSet amdp = available_machines_during_period(begin_slice->begin,end_slice->end);
    LOG_F(INFO,"amdp %s",amdp.to_string_hyphen().c_str());
    //if the available machines encompass all the requested resources then take a chunk from that and return
    if(amdp.size() >= job->nb_requested_resources){
        alloc->used_machines = amdp.left(job->nb_requested_resources);
        return;
    }
    //get all the machines and subtract those that we are definitely going to use
    IntervalSet all_machines = IntervalSet::ClosedInterval(0, _nb_machines - 1);
    all_machines -= amdp;

    int still_needed = job->nb_requested_resources - amdp.size();
    if(policy==IMPACT_POLICY::LEAST_KILLING_LARGEST_FIRST || policy == IMPACT_POLICY::LEAST_KILLING_SMALLEST_FIRST)
    {
        //get the jobs that are in the begin_slice
        std::vector<const Job *> jobs;
        for(std::pair<const Job*,IntervalSet> job_map : begin_slice->allocated_jobs)
        {
            jobs.push_back(job_map.first);
        }
        //get the jobs that are in the first time slice and begin_slice, ie running
        //also get their allocated machines
        std::vector<const Job *> jobs_running;
        IntervalSet running_machines = IntervalSet::empty_interval_set();
        for ( auto job : jobs)
        {

            if (_profile.begin()->contains_job(job))
            {
                jobs_running.push_back(job);
                running_machines+= begin_slice->allocated_jobs[job];
            }
        }
        //now check for reservation conflicts
        auto slice_after_next = end_slice;
        slice_after_next++;
        IntervalSet reservation_machines = IntervalSet::empty_interval_set();
        for (auto it = begin_slice;it!=slice_after_next;++it)
        {
            for (auto job_map : it->allocated_jobs)
            {
                if(job_map.first->purpose == "reservation"){
                    LOG_F(INFO,"job reservation %s",job_map.first->id.c_str());
                    reservation_machines += it->allocated_jobs[job_map.first];
                }
            }
        }
        //can we reserve based on all_machines - running_machines - reservation_machines?
        all_machines -=running_machines;
        all_machines -=reservation_machines;
        if (all_machines.size() >= still_needed )
        {
            alloc->used_machines = all_machines.left(still_needed) + amdp;
            return;
        }
        //ok, it looks like some jobs are going to have to be killed
        //do we want smallest first or largest first to be killed?
        
        if (policy==IMPACT_POLICY::LEAST_KILLING_LARGEST_FIRST)
        {
            auto sortRuleLambda = [] (const Job * j1, const Job * j2) -> bool
            {
                //we want them sorted highest to lowest so j1 should be greater than j2
                return j1->nb_requested_resources > j2->nb_requested_resources;

            };
            std::sort(jobs_running.begin(), jobs_running.end(), sortRuleLambda);

        }
        if (policy==IMPACT_POLICY::LEAST_KILLING_SMALLEST_FIRST)
        {
            auto sortRuleLambda = [] (const Job * j1, const Job * j2) -> bool
            {
                //we want them sorted lowest to highest so j1 should be less than j2
                return j1->nb_requested_resources < j2->nb_requested_resources;

            };
            std::sort(jobs_running.begin(), jobs_running.end(), sortRuleLambda);

        }        
        
        //now incrementally add the resources from running jobs
        for (auto job : jobs_running)
        {
            if(job->purpose != "reservation")
                all_machines+=begin_slice->allocated_jobs[job];
            if (all_machines.size() >= still_needed)
            {
                alloc->used_machines = all_machines.left(still_needed) + amdp;
                return;
            }
                
        }
        return;

    }
    else if(policy==IMPACT_POLICY::LEAST_RESCHEDULING)
    {
        //TODO
    }
}
Schedule::ReservedTimeSlice Schedule::reserve_time_slice(const Job* job){
    if (_debug)
        output_to_svg("top reserve_time_slice " + job->id);
    
    // Let's create the job allocation
    JobAlloc *alloc = new JobAlloc;
         
        //find insertion slice
        auto slice_begin = _profile.begin();
        bool first_slice_is_suspect = false;
        if (_profile.begin()->begin == _profile.begin()->end)
        {
            first_slice_is_suspect = true;
            slice_begin++;
        }
        for(;slice_begin!=_profile.end();slice_begin++)
        {
            //we are at the right slice if it's beginning = the reservation start or the slice ends at a greater point than the reservation start
            if (slice_begin->begin == job->start || slice_begin->end > job->start)
                break;
            

        }
        
        //PPK_ASSERT_ERROR(slice_begin == _profile.end(), "When inserting reservation '%s', the beginning time slice hit the end of the profile",job->id.c_str());
        
        
        LOG_F(INFO,"DEBUG line 298");
        //slice_begin should point to the correct time slice to insert the job
        //do we need to slice it to start with?
        //if so second_slice_after_split will point to the new slice
        TimeSliceIterator first_slice_after_split;
        TimeSliceIterator second_slice_after_split;
        Rational split_date = job->start;
        if(_debug)
            output_to_svg("Before split slice " + job->id);
        split_slice(slice_begin,split_date,first_slice_after_split,second_slice_after_split);
        if (_debug)
            output_to_svg("After split slice " + job->id);
        LOG_F(INFO,"DEBUG line 306");
        slice_begin=second_slice_after_split;

        //now make an allocation
        Rational beginning = job->start;
        alloc->begin = beginning;
        alloc->end = alloc->begin + job->walltime;
        alloc->started_in_first_slice = (slice_begin == _profile.begin()) ? true : false;
        alloc->job = job;
        alloc->used_machines = job->future_allocations;

        //now find the end slice
        auto slice_end = second_slice_after_split;
        Rational end_time = job->start + job->walltime;
        //find ending slice
        for (;slice_end!=_profile.end();slice_end++)
        {
            if (end_time <= slice_end->end)
                break;
        }
        //PPK_ASSERT_ERROR(slice_end == _profile.end(), "When inserting reservation '%s', the ending time slice hit the end of the profile",job->id.c_str());
        
        //now split the slice if needed based on the reservation's end
        split_date = job->start+job->walltime;
        if (_debug)
            output_to_svg("Before split slice "+job->id);
        split_slice(slice_end,split_date,first_slice_after_split,second_slice_after_split);
        if(_debug)
            output_to_svg("After split slice "+job->id);
        LOG_F(INFO,"DEBUG line 322");
        slice_end = first_slice_after_split;
        auto slice_end_next = first_slice_after_split;
        slice_end_next++;
        //now we know what slices will be affected, namely from slice_begin to slice_end
        //if this reservation does not already know what nodes to use then alloc->used_machines is empty
        //we need to get an allocation
        if(alloc->used_machines.is_empty())
            find_least_impactful_fit(alloc, slice_begin, slice_end, IMPACT_POLICY::LEAST_KILLING_LARGEST_FIRST); // ok it is empty, get the least_impactful fit
        
LOG_F(INFO,"DEBUG line 331 %s",alloc->used_machines.to_string_hyphen().c_str());
        //now gather jobs affected
        //first iterate through all the time slices this reservation is temporarily taking up
        std::vector<const Job *> jobs_affected;
        for(auto slice_it = slice_begin;slice_it !=slice_end_next;slice_it++)
        {
            //for each allocated job:intervalset mapping
            for(auto job_map : slice_it->allocated_jobs)
            {
                //if there is an intersection of any job's used machines and the reservation's used machines
                //then add it to affected jobs
                if(!(job_map.second & alloc->used_machines).is_empty())
                {
                     if (std::find(jobs_affected.begin(), jobs_affected.end(), job_map.first) == jobs_affected.end())
                     {
                         //ok it did not find it, push it
                        jobs_affected.push_back(job_map.first);
                     }
                }
            }
        }
        Schedule::ReservedTimeSlice * reserved = new Schedule::ReservedTimeSlice;
        //affected_jobs now holds all affected_jobs
        //find if any affected_jobs are running
        //right now just looking if any are in the first slice
        std::vector<const Job *> jobs_needed_to_be_killed;
        std::vector<const Job *> jobs_to_reschedule;
        for (auto job : jobs_affected)
        {
                if (_profile.begin()->contains_job(job))
                {
                    if (job->purpose == "reservation")
                    {
                        if (!first_slice_is_suspect || (job->walltime != _profile.begin()->end))
                        {
                            reserved->success = false;
                            return *reserved;
                        }
                        else
                            continue;
                    }
                    //this needs to be looked over.  Batsim doesn't always send the completed jobs
                    //in the same message as submitted jobs 
                    jobs_needed_to_be_killed.push_back(job);
                    
                }
                else
                    jobs_to_reschedule.push_back(job);
            
        }
    //LOG_F(INFO,"DEBUG line 367");
        reserved->alloc = alloc;
    //LOG_F(INFO,"DEBUG line 369");
        reserved->jobs_affected = jobs_affected;
        reserved->jobs_needed_to_be_killed = jobs_needed_to_be_killed;
        reserved->jobs_to_reschedule = jobs_to_reschedule;
      //  LOG_F(INFO,"DEBUG line 373");
        reserved->slice_begin = slice_begin;
        reserved->slice_end = slice_end;
        reserved->success = true;
        if (alloc->used_machines.is_empty())
            reserved->success = false;
        
        reserved->job = job;

        //now return the reserved time slice
        //will need to act on the object to make it part of the schedule
        //LOG_F(INFO,"DEBUG line 380");
        if (_debug)
            output_to_svg("bottom reserve_time_slice "+job->id);
        return *reserved;
}
void Schedule::add_reservation(ReservedTimeSlice reservation){
    
        if (_debug)
            output_to_svg("top add_reservation "+reservation.job->id);
        
        _size++;
        _nb_reservations_size++;
        //LOG_F(INFO,"sched_size++ %d",_size);
        const Job * job = reservation.alloc->job;
        //now update all slices between slice_begin and slice_end
        auto slice_end_next = reservation.slice_end;
        ++slice_end_next;
        //LOG_F(INFO,"DEBUG line 395 job id %s",job->id.c_str());
        for (auto slice_it = reservation.slice_begin; slice_it != slice_end_next; ++slice_it)
        {
          //  LOG_F(INFO,"DEBUG line 398");
            //LOG_F(INFO,"used machines %s",reservation.alloc->used_machines.to_string_hyphen().c_str());
            slice_it->available_machines -= reservation.alloc->used_machines;
            slice_it->allocated_machines += reservation.alloc->used_machines;
            //LOG_F(INFO,"DEBUG line 399+1");
            slice_it->nb_available_machines -= job->nb_requested_resources;
            slice_it->allocated_jobs[job] = reservation.alloc->used_machines;
            //LOG_F(INFO,"DEBUG line 402+1");
            slice_it->has_reservation = true;
            //LOG_F(INFO,"DEBUG line 404+1");
            (slice_it->nb_reservations)++;
            //LOG_F(INFO,"DEBUG line 406+1");
        }
          if (_debug)
            output_to_svg("bottom add_reservation "+reservation.job->id);
}

/* old code for adding reservation
        if (job->future_allocations.is_subset_of(pit->available_machines))
        {   //first make new time slice
            TimeSliceIterator first_slice_after_split;
            TimeSliceIterator second_slice_after_split;
            Rational split_date = pit->begin + job->start;
            LOG_F(INFO,"Split date: %g",(double)split_date);
            split_slice(pit,split_date,first_slice_after_split,second_slice_after_split);
            pit=second_slice_after_split;
            
            //now make an allocation
            Rational beginning = job->start;
            alloc->begin = beginning;
            alloc->end = alloc->begin + job->walltime;
            alloc->started_in_first_slice = (pit == _profile.begin()) ? true : false;
            alloc->job = job;
            alloc->used_machines = job->future_allocations;
            
            split_date = pit->begin + job->walltime;
            LOG_F(INFO,"Split date: %g",(double)split_date);
            LOG_F(INFO,"pit->begin: %g  job->walltime: %g",(double) pit->begin, (double) job->walltime);
            LOG_F(INFO,"pit==begin:%d ,pit==next:%d pit==end:%d",pit==_profile.begin(),pit==(_profile.begin()++),pit==_profile.end());
            split_slice(pit, split_date, first_slice_after_split, second_slice_after_split);
            // Let's remove the allocated machines from the available machines of the time slice
            first_slice_after_split->available_machines.remove(alloc->used_machines);
            first_slice_after_split->nb_available_machines -= job->nb_requested_resources;
            first_slice_after_split->allocated_jobs[job] = alloc->used_machines;
            if (first_slice_after_split->has_reservation == true)
                first_slice_after_split->nb_reservations +=1;
            else
            {
                first_slice_after_split->has_reservation = true;
                first_slice_after_split->nb_reservations = 1;
            }
            
            return *alloc;

        }
        if (_debug)
            output_to_svg();
}

*/
JobAlloc Schedule::add_current_reservation(const Job * job, ResourceSelector * selector,bool assert_insertion_successful)
{
     PPK_ASSERT_ERROR(!contains_job(job),
        "Invalid Schedule::add_current_reservation call: Cannot add "
        "job '%s' because it is already in the schedule. %s",
        job->id.c_str(), to_string().c_str());
    return add_current_reservation_after_time_slice(job, _profile.begin(),selector,assert_insertion_successful);
}

JobAlloc Schedule::add_current_reservation_after_time_slice(const Job *job,
    std::list<TimeSlice>::iterator first_time_slice, ResourceSelector *selector, bool assert_insertion_successful)
{
    _size++;
    //LOG_F(INFO,"sched_size++ %d",_size);
    PPK_ASSERT_ERROR(job->purpose=="reservation","You tried to add a non reservation job, job: '%s' via add_current_reservation, consider add_job_first_fit",job->id.c_str());
    
    if (_debug)
    {
      //  LOG_F(1, "Adding job '%s' (size=%d, walltime=%g). Output number %d. %s",
       //     job->id.c_str(), job->nb_requested_resources, (double)job->walltime,
        //    _output_number, to_string().c_str());
        output_to_svg("top add_current_reservation_after_time_slice "+job->id);
    }

    // Let's scan the profile for an anchor point.
    // An anchor point is a point where enough processors are available to run this job
    for (auto pit = _profile.begin(); pit != _profile.end(); ++pit)
    {
        // If the current time slice is an anchor point
        
        IntervalSet available_machines = pit->available_machines;
        //we add the _repair_machines back because they don't factor in with a reservation
        available_machines+=_repair_machines;
        if ((int)available_machines.size() >= job->nb_requested_resources)
        {
            // Let's continue to scan the profile to ascertain that
            // the machines remain available until the job's expected termination

            // If the job has no walltime, its size will be "infinite"
            if (!job->has_walltime)
            {
                // TODO: remove this ugly const_cast?
                const_cast<Job *>(job)->walltime = infinite_horizon() - pit->begin;
            }

            int availableMachinesCount = available_machines.size();
            Rational totalTime = pit->length;

            // If the job fits in the current time slice (temporarily speaking)
            if (totalTime >= job->walltime)
            {
                // Let's create the job allocation
                JobAlloc *alloc = new JobAlloc;

                // If the job fits in the current time slice (according to the fitting function)
                if (selector->fit_reservation(job, available_machines, alloc->used_machines))
                {
                    Rational beginning = pit->begin;
                    alloc->begin = beginning;
                    alloc->end = alloc->begin + job->walltime;
                    alloc->started_in_first_slice = (pit == _profile.begin()) ? true : false;
                    alloc->job = job;
                    job->allocations[beginning] = alloc;

                    // Let's split the current time slice if needed
                    TimeSliceIterator first_slice_after_split;
                    TimeSliceIterator second_slice_after_split;
                    Rational split_date = pit->begin + job->walltime;
                    split_slice(pit, split_date, first_slice_after_split, second_slice_after_split);
                    
                    // Let's remove the allocated machines from the available machines of the time slice
                    first_slice_after_split->available_machines.remove(alloc->used_machines);
                    first_slice_after_split->allocated_machines.insert(alloc->used_machines);
                    first_slice_after_split->nb_available_machines -= job->nb_requested_resources;
                    first_slice_after_split->allocated_jobs[job] = alloc->used_machines;
                    first_slice_after_split->nb_reservations++;
                    if (first_slice_after_split->nb_reservations == 1) 
                        first_slice_after_split->has_reservation = true; //should only need to do this when first adding a reservation(nb_reservations == 1)
                    if (_debug)
                    {
        //                LOG_F(1, "Added job '%s' (size=%d, walltime=%g). Output number %d. %s", job->id.c_str(),
         //                   job->nb_requested_resources, (double)job->walltime, _output_number, to_string().c_str());
                        output_to_svg("bottom add_current_reservation_after_time_slice "+job->id);
                    }

                    // The job has been placed, we can leave this function
                    return *alloc;
                }
            }
            else
            {
                // TODO : merge this big else with its if, as the "else" is a more general case of the "if"
                // The job does not fit in the current time slice (temporarily speaking)
                auto availableMachines = pit->available_machines;
                availableMachines += _repair_machines;

                auto pit2 = pit;
                ++pit2;

                for (; (pit2 != _profile.end()) && ((int)pit2->nb_available_machines >= job->nb_requested_resources);
                     ++pit2)
                {
                    availableMachines &= pit2->available_machines;
                    availableMachinesCount = (int)availableMachines.size();
                    totalTime += pit2->length;

                    if (availableMachinesCount < job->nb_requested_resources) // We don't have enough machines to run the job
                        break;
                    else if (totalTime >= job->walltime) // The job fits in the slices [pit, pit2[ (temporarily speaking)
                    {
                        // Let's create the job allocation
                        JobAlloc *alloc = new JobAlloc;

                        // If the job fits in the current time slice (according to the fitting function)
                        if (selector->fit_reservation(job, availableMachines, alloc->used_machines))
                        {
                            alloc->begin = pit->begin;
                            alloc->end = alloc->begin + job->walltime;
                            alloc->started_in_first_slice = (pit == _profile.begin()) ? true : false;
                            alloc->job = job;
                            job->allocations[alloc->begin] = alloc;

                            // Let's remove the used machines from the slices before pit2
                            auto pit3 = pit;
                            for (; pit3 != pit2; ++pit3)
                            {
                                //if some of the machines are repair machines then they won't be in available machines
                                //the intersection of available_machines and used machines tells us how many machines to subtract from nb_available_machines
                                int subtract = (pit3->available_machines & alloc->used_machines).size();
                                pit3->available_machines -= alloc->used_machines;
                                pit3->allocated_machines += alloc->used_machines;
                                pit3->nb_available_machines -= subtract;
                                pit3->allocated_jobs[job] = alloc->used_machines;
                                pit3->nb_reservations++;
                            if (pit3->nb_reservations == 1)//means a reservation has been added
                                pit3->has_reservation = true;
                            }

                            // Let's split the current time slice if needed
                            TimeSliceIterator first_slice_after_split;
                            TimeSliceIterator second_slice_after_split;
                            Rational split_date = pit->begin + job->walltime;
                            split_slice(pit2, split_date, first_slice_after_split, second_slice_after_split);

                            // Let's remove the allocated machines from the available machines of the time slice
                            int subtract = (first_slice_after_split->available_machines & alloc->used_machines).size();
                            first_slice_after_split->available_machines -= alloc->used_machines;
                            first_slice_after_split->allocated_machines += alloc->used_machines;
                            first_slice_after_split->nb_available_machines -= subtract;
                            first_slice_after_split->allocated_jobs[job] = alloc->used_machines;

                            if (_debug)
                            {
          //                      LOG_F(1, "Added job '%s' (size=%d, walltime=%g). Output number %d. %s", job->id.c_str(),
          //                          job->nb_requested_resources, (double)job->walltime, _output_number,
            //                        to_string().c_str());
                                output_to_svg("bottom else add_current_reservation_after_time_slice " +job->id);
                            }

                            // The job has been placed, we can leave this function
                            return *alloc;
                        }
                    }
                }
            }
        }
    }

    if (assert_insertion_successful)
        PPK_ASSERT_ERROR(false, "Error in Schedule::add_job_first_fit: could not add job '%s' into %s", job->id.c_str(),
            to_string().c_str());

    JobAlloc failed_alloc;
    failed_alloc.has_been_inserted = false;
    return failed_alloc;

}
bool Schedule::remove_reservations_if_ready(std::vector<const Job *>& jobs_removed)
{
    //first check if next timeslice has a reservation
    auto slice = _profile.begin();
    ++slice;
    if (slice == _profile.end())
        return false; //it does not have a reservation...return false
    
    bool ready = false;
    if (slice->has_reservation)
    {
        //ok the next timeslice does have a reservation
        //check if it is ready to copy over to the first slice
        //by this we mean if the next timeslice starts at an equal time as the first time slice
        //within an epsilon
        Rational epsilon =1e-4;
        if (abs(_profile.begin()->begin - slice->begin)<=epsilon)
        {
            ready = true;
            //ok they are equal, remove the reservations

            for (auto it = slice->allocated_jobs.begin();it != slice->allocated_jobs.end();++it)
            {
                
                const Job* job = it->first;
                //first make sure it's a reservation and not currently running (ie not in the first slice)
                if (job->purpose == "reservation" && !_profile.begin()->contains_job(job))
                    {
                        //ok it is a reservation and not currently running
                        //now make sure the resources are available
                        IntervalSet available_machines = _profile.begin()->available_machines;
                        //we add repair_machines since a reservation takes precedence over such things
                        available_machines+=_repair_machines;
                        //we return false if we can't run all of the reservations meant for this timeslice
                        //we simply check if the IntervalSet scheduled is part of the available machines
                        //making sure reservations aren't using the same allocations is up to scheduling them
                        //in the first place, not checked here.
                        if ( !(it->second.is_subset_of(available_machines)))
                            return false;
                        //we are removing the reservation from this next time slice so the next
                        //time slice has its number of reservations decreased
                        if (slice->nb_reservations > 0)
                            slice->nb_reservations--;
                        //reservations will be removed, push this reservation.
                        jobs_removed.push_back(job);
           
                                              
                    }

            }
            //we remove all the reservations from the schedule and return true
            //jobs_removed will hold all the reservations that were removed from next time slice
            for (auto job : jobs_removed)
                remove_job_if_exists(job);
            return true;   
        }

    }
    //its not time to remove reservations.  return false
    return false;
}
    
JobAlloc Schedule::add_job_first_fit_after_time_slice(const Job *job,
    std::list<TimeSlice>::iterator first_time_slice, ResourceSelector *selector, bool assert_insertion_successful)
{

    if (_debug)
    {
        //LOG_F(1, "Adding job '%s' (size=%d, walltime=%g). Output number %d. %s",
        //    job->id.c_str(), job->nb_requested_resources, (double)job->walltime,
        //    _output_number, to_string().c_str());
        output_to_svg("top add_job_first_fit_after_time_slice "+job->id);
    }
    LOG_F(INFO,"add_job_first_fit_after_time_slice %s",job->id.c_str());
    _size++;
    _nb_jobs_size++;
    //LOG_F(INFO,"sched_size++ %d",_size);
    /*
    if (!_repair_machines.is_empty())
    {
        _profile.begin()->nb_available_machines -= _repair_machines.size();
        _profile.begin()->available_machines -= _repair_machines;
    }
    */

    // Let's scan the profile for an anchor point.
    // An anchor point is a point where enough processors are available to run this job
    for (auto pit = first_time_slice; pit != _profile.end(); ++pit)
    {
        // If the current time slice is an anchor point
        if ((int)pit->nb_available_machines >= job->nb_requested_resources)
        {
            // Let's continue to scan the profile to ascertain that
            // the machines remain available until the job's expected termination

            // If the job has no walltime, its size will be "infinite"
            if (!job->has_walltime)
            {
                // TODO: remove this ugly const_cast?
                const_cast<Job *>(job)->walltime = infinite_horizon() - pit->begin;
            }

            int availableMachinesCount = pit->nb_available_machines;
            Rational totalTime = pit->length;

            // If the job fits in the current time slice (temporarily speaking)
            if (totalTime >= job->walltime)
            {
                // Let's create the job allocation
                JobAlloc *alloc = new JobAlloc;

                // If the job fits in the current time slice (according to the fitting function)
                if (selector->fit(job, pit->available_machines, alloc->used_machines))
                {
                    Rational beginning = pit->begin;
                    alloc->begin = beginning;
                    alloc->started_in_first_slice = (pit == _profile.begin()) ? true : false;
                    alloc->job = job;
                    LOG_F(INFO,"job id: %s, beginning: %.15f, alloc: %s",job->id.c_str(),beginning.convert_to<double>(),alloc->used_machines.to_string_hyphen().c_str());
                    job->allocations[beginning] = alloc;
                    
    

                    // Let's split the current time slice if needed
                    TimeSliceIterator first_slice_after_split;
                    TimeSliceIterator second_slice_after_split;
                    Rational split_date = pit->begin + job->walltime;
                    split_slice(pit, split_date, first_slice_after_split, second_slice_after_split);

                    // Let's remove the allocated machines from the available machines of the time slice
                    first_slice_after_split->available_machines.remove(alloc->used_machines);
                    first_slice_after_split->allocated_machines.insert(alloc->used_machines);
                    first_slice_after_split->nb_available_machines -= job->nb_requested_resources;
                    first_slice_after_split->allocated_jobs[job] = alloc->used_machines;

                    if (_debug)
                    {
      //                  LOG_F(1, "Added job '%s' (size=%d, walltime=%g). Output number %d. %s", job->id.c_str(),
      //                      job->nb_requested_resources, (double)job->walltime, _output_number, to_string().c_str());
                        output_to_svg("bottom add_job_first_fit_after_time_slice "+job->id);
                    }

                    // The job has been placed, we can leave this function
                    /*
                    if (!_repair_machines.is_empty())
                    {
                        _profile.begin()->nb_available_machines += _repair_machines.size();
                        _profile.begin()->available_machines += _repair_machines;
                    }
                    */
                    return *alloc;
                }
            }
            else
            {
                // TODO : merge this big else with its if, as the "else" is a more general case of the "if"
                // The job does not fit in the current time slice (temporarily speaking)
                auto availableMachines = pit->available_machines;

                auto pit2 = pit;
                ++pit2;

                for (; (pit2 != _profile.end()) && ((int)pit2->nb_available_machines >= job->nb_requested_resources);
                     ++pit2)
                {
                    availableMachines &= pit2->available_machines;
                    availableMachinesCount = (int)availableMachines.size();
                    totalTime += pit2->length;

                    if (availableMachinesCount
                        < job->nb_requested_resources) // We don't have enough machines to run the job
                        break;
                    else if (totalTime
                        >= job->walltime) // The job fits in the slices [pit, pit2[ (temporarily speaking)
                    {
                        // Let's create the job allocation
                        JobAlloc *alloc = new JobAlloc;

                        // If the job fits in the current time slice (according to the fitting function)
                        if (selector->fit(job, availableMachines, alloc->used_machines))
                        {
                            alloc->begin = pit->begin;
                            alloc->end = alloc->begin + job->walltime;
                            alloc->started_in_first_slice = (pit == _profile.begin()) ? true : false;
                            alloc->job = job;
                            job->allocations[alloc->begin] = alloc;

                            // Let's remove the used machines from the slices before pit2
                            auto pit3 = pit;
                            for (; pit3 != pit2; ++pit3)
                            {
                                pit3->available_machines -= alloc->used_machines;
                                pit3->allocated_machines += alloc->used_machines;
                                pit3->nb_available_machines -= job->nb_requested_resources;
                                pit3->allocated_jobs[job] = alloc->used_machines;
                            }

                            // Let's split the current time slice if needed
                            TimeSliceIterator first_slice_after_split;
                            TimeSliceIterator second_slice_after_split;
                            Rational split_date = pit->begin + job->walltime;
                            split_slice(pit2, split_date, first_slice_after_split, second_slice_after_split);

                            // Let's remove the allocated machines from the available machines of the time slice
                            first_slice_after_split->available_machines -= alloc->used_machines;
                            first_slice_after_split->allocated_machines += alloc->used_machines;
                            first_slice_after_split->nb_available_machines -= job->nb_requested_resources;
                            first_slice_after_split->allocated_jobs[job] = alloc->used_machines;

                            if (_debug)
                            {
              //                  LOG_F(1, "Added job '%s' (size=%d, walltime=%g). Output number %d. %s", job->id.c_str(),
                //                    job->nb_requested_resources, (double)job->walltime, _output_number,
                  //                  to_string().c_str());
                                output_to_svg("bottom else add_job_first_fit_after_time_slice "+job->id);
                            }

                            // The job has been placed, we can leave this function
                            /*
                            if (!_repair_machines.is_empty())
                            {
                                _profile.begin()->nb_available_machines += _repair_machines.size();
                                _profile.begin()->available_machines += _repair_machines;
                            }
                            */
                            return *alloc;
                        }
                    }
                }
            }
        }
    }

    if (assert_insertion_successful)
        PPK_ASSERT_ERROR(false, "Error in Schedule::add_job_first_fit: could not add job '%s' into %s", job->id.c_str(),
            to_string().c_str());

    JobAlloc failed_alloc;
    failed_alloc.has_been_inserted = false;
    _size--;
    _nb_jobs_size--;
    return failed_alloc;
}

JobAlloc Schedule::add_job_first_fit_after_time(
    const Job *job, Rational date, ResourceSelector *selector, bool assert_insertion_successful)
{
    if (_debug)
    {
        LOG_F(1, "Adding job '%s' (size=%d, walltime=%g) after date %g. Output number %d. %s", job->id.c_str(),
            job->nb_requested_resources, (double)job->walltime, (double)date, _output_number, to_string().c_str());
        output_to_svg("top add_job_first_fit_after_time "+job->id);
    }

    // Let's first search at each time slice the job should be added
    auto insertion_slice_it = _profile.begin();
    bool insertion_slice_found = false;

    while (insertion_slice_it != _profile.end() && !insertion_slice_found)
    {
        if ((date >= insertion_slice_it->begin) && (date < insertion_slice_it->end))
        {
            insertion_slice_found = true;
        }
        else
            ++insertion_slice_it;
    }

    PPK_ASSERT_ERROR(insertion_slice_found, "Cannot find the insertion slice of date %g. Schedule : %s\n", (double)date,
        to_string().c_str());

    // Let's split the insertion slice in two parts if needed
    TimeSliceIterator first_slice_after_split;
    TimeSliceIterator second_slice_after_split;
    split_slice(insertion_slice_it, date, first_slice_after_split, second_slice_after_split);

    // In both cases (whether a split occured or not), we can simply call add_job_first_fit_after_time_slice on the
    // second slice now
    return add_job_first_fit_after_time_slice(job, second_slice_after_split, selector, assert_insertion_successful);
}

double Schedule::query_wait(int size, Rational time, ResourceSelector *selector)
{
    // very similar to job insertions...

    Job fake_job;
    fake_job.id = "fake";
    fake_job.unique_number = -1;
    fake_job.nb_requested_resources = size;
    fake_job.walltime = time;

    // Let's scan the profile for an anchor point.
    // An anchor point is a point where enough processors are available to run this job
    for (auto pit = _profile.begin(); pit != _profile.end(); ++pit)
    {
        // If the current time slice is an anchor point
        if ((int)pit->nb_available_machines >= size)
        {
            // Let's continue to scan the profile to ascertain that
            // the machines remain available until the job's expected termination

            int availableMachinesCount = pit->nb_available_machines;
            Rational totalTime = pit->length;

            // If the job fits in the current time slice (temporarily speaking)
            if (totalTime >= time)
            {
                IntervalSet used_machines;

                // If the job fits in the current time slice (according to the fitting function)
                if (selector->fit(&fake_job, pit->available_machines, used_machines))
                {
                    return static_cast<double>(pit->begin);
                }
            }
            else
            {
                // TODO : merge this big else with its if, as the "else" is a more general case of the "if"
                // The job does not fit in the current time slice (temporarily speaking)
                auto availableMachines = pit->available_machines;

                auto pit2 = pit;
                ++pit2;

                for (; (pit2 != _profile.end()) && ((int)pit2->nb_available_machines >= size); ++pit2)
                {
                    availableMachines &= pit2->available_machines;
                    availableMachinesCount = (int)availableMachines.size();
                    totalTime += pit2->length;

                    if (availableMachinesCount < size) // We don't have enough machines to run the job
                        break;
                    else if (totalTime >= time) // The job fits in the slices [pit, pit2[ (temporarily speaking)
                    {

                        IntervalSet used_machines;

                        // If the job fits in the current time slice (according to the fitting function)
                        if (selector->fit(&fake_job, availableMachines, used_machines))
                        {
                            return static_cast<double>(pit->begin);
                        }
                    }
                }
            }
        }
    }

    return -1;
}


Rational Schedule::first_slice_begin() const
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto it = _profile.begin();
    return it->begin;
}

Rational Schedule::finite_horizon() const
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto it = _profile.end();
    --it;
    return it->begin;
}

Rational Schedule::infinite_horizon() const
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto it = _profile.end();
    --it;
    return it->end;
}

std::multimap<std::string, JobAlloc> Schedule::jobs_allocations() const
{
    multimap<std::string, JobAlloc> res;

    map<const Job *, Rational> jobs_starting_times;
    map<const Job *, Rational> jobs_ending_times;
    set<const Job *> current_jobs;
    for (auto mit : _profile.begin()->allocated_jobs)
    {
        const Job *allocated_job = mit.first;
        current_jobs.insert(allocated_job);
        jobs_starting_times[allocated_job] = _profile.begin()->begin;
    }

    // Let's traverse the profile to find the beginning of each job
    for (auto slice_it = _profile.begin(); slice_it != _profile.end(); ++slice_it)
    {
        const TimeSlice &slice = *slice_it;
        set<const Job *> allocated_jobs;
        for (auto mit : slice.allocated_jobs)
        {
            const Job *job = mit.first;
            allocated_jobs.insert(job);
        }

        set<const Job *> finished_jobs;
        set_difference(current_jobs.begin(), current_jobs.end(), allocated_jobs.begin(), allocated_jobs.end(),
            std::inserter(finished_jobs, finished_jobs.end()));

        for (const Job *job : finished_jobs)
        {
            jobs_ending_times[job] = slice_it->begin;

            // Let's find where the job has been allocated
            PPK_ASSERT_ERROR(slice_it != _profile.begin());
            auto previous_slice_it = slice_it;
            --previous_slice_it;

            JobAlloc alloc;
            alloc.job = job;
            alloc.begin = jobs_starting_times[job];
            alloc.end = jobs_ending_times[job];
            alloc.started_in_first_slice = (alloc.begin == first_slice_begin());
            alloc.used_machines = previous_slice_it->allocated_jobs.at(job);

            res.insert({ job->id, alloc });
        }

        set<const Job *> new_jobs;
        set_difference(allocated_jobs.begin(), allocated_jobs.end(), current_jobs.begin(), current_jobs.end(),
            std::inserter(new_jobs, new_jobs.end()));

        for (const Job *job : new_jobs)
        {
            jobs_starting_times[job] = slice.begin;
        }

        // Update current_jobs
        for (const Job *job : finished_jobs)
            current_jobs.erase(job);
        for (const Job *job : new_jobs)
            current_jobs.insert(job);
    }

    return res;
}

bool Schedule::contains_job(const Job *job) const
{
    return find_first_occurence_of_job(job, _profile.begin()) != _profile.end();
}

bool Schedule::split_slice(Schedule::TimeSliceIterator slice_to_split, Rational date,
    Schedule::TimeSliceIterator &first_slice_after_split, Schedule::TimeSliceIterator &second_slice_after_split)
{
    if ((date > slice_to_split->begin) && (date < slice_to_split->end))
    {
        // The split must be done.
        // Let's create the new slice
        TimeSlice new_slice = *slice_to_split;

        new_slice.begin = date;
        new_slice.length = new_slice.end - new_slice.begin;
        set_smallest_and_largest_time_slice_length(new_slice.length);
        PPK_ASSERT_ERROR(new_slice.length > 0);

        // Let's reduce the existing slice length
        slice_to_split->end = date;
        slice_to_split->length = slice_to_split->end - slice_to_split->begin;
        set_smallest_and_largest_time_slice_length(slice_to_split->length);
        PPK_ASSERT_ERROR(slice_to_split->length > 0);

        // Let's insert the new_slice just after slice_to_split
        // To do so, since list::insert inserts BEFORE the given iterator, we must point after slice 1.
        auto list_insert_it = slice_to_split;
        ++list_insert_it;

        // Let's update returned iterators
        second_slice_after_split = _profile.insert(list_insert_it, new_slice);
        first_slice_after_split = second_slice_after_split;
        --first_slice_after_split;
        LOG_F(INFO,"slice split, first:%f   second:%f",first_slice_after_split->end.convert_to<double>(),second_slice_after_split->end.convert_to<double>());

        return true;
    }
    else
    {
        first_slice_after_split = slice_to_split;
        second_slice_after_split = slice_to_split;
        return false;
    }
}

Schedule::TimeSliceIterator Schedule::find_first_occurence_of_job(
    const Job *job, Schedule::TimeSliceIterator starting_point)
{
    PPK_ASSERT_ERROR(_profile.size() > 0);
    for (auto slice_it = starting_point; slice_it != _profile.end(); ++slice_it)
    {
        const TimeSlice &slice = *slice_it;
        if (slice.allocated_jobs.count(job) == 1)
            return slice_it;
    }

    return _profile.end();
}

Schedule::TimeSliceIterator Schedule::find_last_occurence_of_job(
    const Job *job, Schedule::TimeSliceIterator starting_point)
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto slice_it = _profile.end();
    bool found = false;

    do
    {
        --slice_it;

        const TimeSlice &slice = *slice_it;

        if (slice.allocated_jobs.count(job) == 1)
            found = true;
        else if (found) // If the job is no longer found, its starting point is just after the current slice
            return ++slice_it;

    } while (slice_it != starting_point);

    if (found)
        return starting_point;
    else
        return _profile.end();
}

Schedule::TimeSliceConstIterator Schedule::find_first_occurence_of_job(
    const Job *job, Schedule::TimeSliceConstIterator starting_point) const
{
    PPK_ASSERT_ERROR(_profile.size() > 0);
    for (auto slice_it = starting_point; slice_it != _profile.end(); ++slice_it)
    {
        const TimeSlice &slice = *slice_it;
        if (slice.allocated_jobs.count(job) == 1)
            return slice_it;
    }

    return _profile.end();
}

Schedule::TimeSliceConstIterator Schedule::find_last_occurence_of_job(
    const Job *job, Schedule::TimeSliceConstIterator starting_point) const
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto slice_it = _profile.end();
    bool found = false;

    do
    {
        --slice_it;

        const TimeSlice &slice = *slice_it;

        if (slice.allocated_jobs.count(job) == 1)
            found = true;
        else if (found) // If the job is no longer found, its starting point is just after the current slice
            return ++slice_it;

    } while (slice_it != starting_point);

    if (found)
        return starting_point;
    else
        return _profile.end();
}

Schedule::TimeSliceIterator Schedule::find_last_time_slice_before_date(Rational date, bool assert_not_found)
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto slice_it = _profile.end();

    do
    {
        --slice_it;

        const TimeSlice &slice = *slice_it;

        if (slice.begin <= date)
            return slice_it;
 IntervalSet _repair_machines;
    } while (slice_it != _profile.begin());

    if (assert_not_found)
        PPK_ASSERT_ERROR(false, "No time slice beginning before date %g could be found. Schedule: %s\n", (double)date,
            to_string().c_str());
    return _profile.begin();
}

Schedule::TimeSliceConstIterator Schedule::find_last_time_slice_before_date(Rational date, bool assert_not_found) const
{
    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto slice_it = _profile.end();

    do
    {
        --slice_it;

        const TimeSlice &slice = *slice_it;

        if (slice.begin <= date)
            return slice_it;

    } while (slice_it != _profile.begin());

    if (assert_not_found)
        PPK_ASSERT_ERROR(false, "No time slice beginning before date %g could be found. Schedule: %s\n", (double)date,
            to_string().c_str());
    return slice_it;
}

Schedule::TimeSliceIterator Schedule::find_first_time_slice_after_date(Rational date, bool assert_not_found)
{
    auto slice_it = _profile.begin();

    while (slice_it != _profile.end())
    {
        if (slice_it->begin >= date)
            return slice_it;
        ++slice_it;
    }

    if (assert_not_found)
        PPK_ASSERT_ERROR(false, "No time slice starting after date %g could be found. Schedule: %s\n", (double)date,
            to_string().c_str());

    return _profile.end();
}

Schedule::TimeSliceConstIterator Schedule::find_first_time_slice_after_date(Rational date, bool assert_not_found) const
{
    auto slice_it = _profile.begin();

    while (slice_it != _profile.end())
    {
        if (slice_it->begin >= date)
            return slice_it;
        ++slice_it;
    }

    if (assert_not_found)
        PPK_ASSERT_ERROR(false, "No time slice starting after date %g could be found. Schedule: %s\n", (double)date,
            to_string().c_str());

    return _profile.end();
}

IntervalSet Schedule::available_machines_during_period(Rational begin, Rational end) const
{
    PPK_ASSERT_ERROR(
        begin >= first_slice_begin(), "begin=%f, first_slice_begin()=%f", (double)begin, (double)first_slice_begin());
    PPK_ASSERT_ERROR(
        end <= infinite_horizon(), "end=%f, infinite_horizon()=%f", (double)end, (double)infinite_horizon());

    auto slice_it = find_first_time_slice_after_date(begin);
    IntervalSet available_machines = slice_it->available_machines;

    while (slice_it != _profile.end() && slice_it->begin < end)
    {
        available_machines &= slice_it->available_machines;

        ++slice_it;
    }

    return available_machines;
}

std::list<Schedule::TimeSlice>::iterator Schedule::begin()
{
    return _profile.begin();
}

std::list<Schedule::TimeSlice>::iterator Schedule::end()
{
    return _profile.end();
}

std::list<Schedule::TimeSlice>::const_iterator Schedule::begin() const
{
    return _profile.cbegin();
}

std::list<Schedule::TimeSlice>::const_iterator Schedule::end() const
{
    return _profile.cend();
}

int Schedule::nb_slices() const
{
    return (int) _profile.size();
}

string Schedule::to_string() const
{
    string res = "Schedule:\n";

    for (const TimeSlice &slice : _profile)
        res += slice.to_string(2, 2);

    return res;
}
string Schedule::to_json_string() const
{
    string res;
    res = "{\n";
        res = res + "\t\"Schedule\":\n\t{\n";
        
            res = res + "\t\t\"_colors\":" + batsched_tools::vector_to_json_string(_colors) + ",\n";
            
            res = res + "\t\t\"_frame_number\":" + batsched_tools::to_json_string(_frame_number) + ",\n";
            
            res = res + "\t\t\"_largest_time_slice_length\":" + batsched_tools::to_json_string(_largest_time_slice_length) + ",\n";
            
            res = res + "\t\t\"_nb_jobs_size\":" + batsched_tools::to_json_string(_nb_jobs_size) + ",\n";
            
            res = res + "\t\t\"_nb_reservations_size\":" + batsched_tools::to_json_string(_nb_reservations_size) + ",\n";
            
            res = res + "\t\t\"_output_number\":" + batsched_tools::to_json_string(_output_number) + ",\n";
            
            res = res + "\t\t\"_output_svg\":" + batsched_tools::to_json_string(_output_svg) + ",\n";
            res = res + "\t\t\"_previous_time_end\":" + batsched_tools::to_json_string(_previous_time_end) + ",\n";
            
            res = res + "\t\t\"_repair_machines\":" + batsched_tools::to_json_string(_repair_machines.to_string_hyphen()) + ",\n";
            res = res + "\t\t\"_reservation_colors\":" + batsched_tools::vector_to_json_string(_reservation_colors) + ",\n";
            
            res = res + "\t\t\"_size\":" + batsched_tools::to_json_string(_size) + ",\n";
            res = res + "\t\t\"_smallest_time_slice_length\":" + batsched_tools::to_json_string(_smallest_time_slice_length) + ",\n";
            res = res + "\t\t\"_svg_frame_end\":" + batsched_tools::to_json_string(_svg_frame_end) + ",\n";
            res = res + "\t\t\"_svg_frame_start\":" + batsched_tools::to_json_string(_svg_frame_start) + ",\n";
            
            res = res + "\t\t\"_svg_highlight_machines\":" + batsched_tools::to_json_string(_svg_highlight_machines.to_string_hyphen()) + ",\n";
            
            res = res + "\t\t\"_svg_output_end\":" + batsched_tools::to_json_string(_svg_output_end) + ",\n";
            res = res + "\t\t\"_svg_output_start\":" + batsched_tools::to_json_string(_svg_output_start) + ",\n";
            
            res = res + "\t\t\"_svg_prefix\":" + batsched_tools::to_json_string(_svg_prefix) + ",\n";
            
            res = res + "\t\t\"_svg_reservations\":" + list_to_json_string(&_svg_reservations) + ",\n";
            
            res = res + "\t\t\"TimeSlices\":\n\t\t[\n";
            
                int count = _profile.size();
                
                for (const TimeSlice &slice : _profile)
                {
                    res +=slice.to_json_string(3,1);
                    if (count > 1)
                    {
                        res +=",\n";
                        count--;
                    }
                    else
                        res +="\n";
                }
                
            res = res + "\t\t]\n";
        res = res + "\t}\n";
    res = res +"}";
    return res;
}
void Schedule::add_reservation_for_svg_outline(const ReservedTimeSlice & reservation_to_be ){
    _svg_reservations.push_back(reservation_to_be);

}
bool Schedule::TimeSlice::operator==(const TimeSlice & t)
{
    return begin == t.begin;
}
bool Schedule::ReservedTimeSlice::operator==(const ReservedTimeSlice & r)const {
    return job->id == r.job->id;
}
std::string Schedule::ReservedTimeSlice::to_string()const{
    std::string rts_string="{";
            rts_string+="\"alloc\":\""+batsched_tools::to_string(alloc)+"\"";
            rts_string+=",\"job\":\""+batsched_tools::to_string(job)+"\"";
            rts_string+=",\"jobs_affected\":"+batsched_tools::vector_to_string(jobs_affected)+"\"";
            rts_string+=",\"jobs_needed_to_be_killed\":\"" +batsched_tools::vector_to_string(jobs_needed_to_be_killed)+"\"";
            rts_string+=",\"jobs_to_reschedule\":"+batsched_tools::vector_to_string(jobs_to_reschedule)+"\"";
            rts_string+=",\"success:\":"+std::string((success ? "true":"false"));
        rts_string+="}";
        return rts_string;
}
std::string Schedule::ReservedTimeSlice::to_json_string()const{
            std::string rts_string="{";
            rts_string+="\"alloc\":"+batsched_tools::to_json_string(alloc);
            rts_string+=",\"job\":"+batsched_tools::to_json_string(job);
            rts_string+=",\"jobs_affected\":"+batsched_tools::vector_to_json_string(jobs_affected);
            rts_string+=",\"jobs_needed_to_be_killed\":" +batsched_tools::vector_to_json_string(jobs_needed_to_be_killed);
            rts_string+=",\"jobs_to_reschedule\":"+batsched_tools::vector_to_json_string(jobs_to_reschedule);
            rts_string+=",\"success\":"+std::string((success ? "true":"false"));
        rts_string+="}";
        return rts_string;
}
std::string Schedule::vector_to_json_string(const std::vector<Schedule::ReservedTimeSlice> * vec)const
{
        std::string ourString="[";
        bool first = true;
        for (ReservedTimeSlice value:(*vec))
        {
            if (!first)
                ourString + ", "; first = false;
            ourString = ourString + value.to_json_string();
        }
        ourString = ourString + "]";
        return ourString;
}
std::string Schedule::list_to_json_string(const std::list<Schedule::ReservedTimeSlice> * lst)const
{
        std::string ourString="[";
        bool first = true;
        for (ReservedTimeSlice value:(*lst))
        {
            if (!first)
                ourString + ", "; first = false;
            ourString = ourString + value.to_json_string();
        }
        ourString = ourString + "]";
        return ourString;
}
void Schedule::remove_reservation_for_svg_outline(const ReservedTimeSlice & reservation_to_be){
    _svg_reservations.remove(reservation_to_be);
}

string Schedule::to_svg(const std::string& message, const std::list<ReservedTimeSlice> & svg_reservations) const
{
    //LOG_F(INFO,"line 1311");
    Rational x0, x1, y0, y1;
    x0 = y0 = std::numeric_limits<double>::max();
    x1 = y1 = std::numeric_limits<double>::min();

    PPK_ASSERT_ERROR(_profile.size() > 0);

    auto last_finite_slice = _profile.end();
    --last_finite_slice;

    Rational second_width = 8;
    const Rational smallest_length = _smallest_time_slice_length;
    const Rational total_seconds = last_finite_slice->begin - _profile.begin()->begin;
    if ((total_seconds* second_width) > 4000)
        second_width = 8.0/smallest_length;
    if ((total_seconds * second_width) > 10000)
        second_width = 10000.0/total_seconds;
    Rational machine_height = 10;
    if ((_nb_machines * machine_height) > 4000){
        machine_height = 3;
    }
    const Rational space_between_machines_ratio(1, 8);
    PPK_ASSERT_ERROR(space_between_machines_ratio >= 0 && space_between_machines_ratio <= 1);

    x0 = _profile.begin()->begin * second_width;
    x1 = last_finite_slice->begin * second_width;

    y0 = 0 * machine_height;
    y1 = _nb_machines * machine_height;
    const Rational width = x1 - x0 + 10;
    const Rational height = y1 - y0;

    const int buf_size = 4096;
    char *buf = new char[buf_size];
    Rational img_width = width;
    if (img_width < 240)
        img_width = 240;
    // header
    Rational sim_time = _now;
    //if(_profile.size() == 1 && _profile.begin()->allocated_jobs.empty() )
    //    sim_time=_previous_time_end;
    snprintf(buf, buf_size,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"%g\" height=\"%g\">\n"
        "<title>Schedule</title>\n"
        "<!-- %g-->\n"
        "<text x=\"5\" y=\"5\" font-size=\"1pt\" fill=\"black\">Frame: %d</text>\n"
        "<text x=\"50\" y=\"5\" font-size=\"1pt\" fill=\"black\">Output: %d</text>\n"
        "<text x=\"100\" y=\"5\" font-size=\"1pt\" fill=\"black\">Sim Time: %g seconds</text>\n"
        "<text x=\"150\" y=\"5\" font-size=\"1pt\" fill=\"black\">%s</text>\n",
        (double)img_width, (double)height+20,(double)smallest_length,_frame_number,_output_number,(double)sim_time,message.c_str());


    string res = buf;
    for (auto slice_it = _profile.begin(); slice_it != _profile.end(); ++slice_it){
        Rational line_x0 = slice_it->begin * second_width -x0;
        snprintf(buf,buf_size,
        "<line x1=\"%g\" y1=\"8\" x2=\"%g\" y2=\"13\" stroke=\"black\" stroke-width=\".1\"/>\n"
        "<text x=\"%g\" y=\"11\" font-size=\"1pt\" fill=\"black\">%.2f</text>\n",
        (double)line_x0,(double)line_x0,((double)line_x0)+1,(double)slice_it->begin);

        res+=buf;
        
    }
    res+="<g  transform=\"translate(0,13)\">";
    // machines background color
    for (int i = 0; i < _nb_machines; ++i)
    {
        string machine_color;

        if (i % 2 == 0)
            machine_color = "#EEEEEE";
        else
            machine_color = "#DDDDDD";

        snprintf(buf, buf_size,
            "  <rect x=\"%g\" y=\"%g\" width=\"%g\" height=\"%g\" stroke=\"none\" fill=\"%s\"/>\n" , (double)0,
            (double)(i * machine_height), (double)width, (double)machine_height, machine_color.c_str());
        res += buf;
    }

    map<const Job *, Rational> jobs_starting_times;
    set<const Job *> current_jobs;
    //first slice are current jobs
    for (auto mit : _profile.begin()->allocated_jobs)
    {
        const Job *allocated_job = mit.first;
        current_jobs.insert(allocated_job);
        jobs_starting_times[allocated_job] = _profile.begin()->begin;
    }

    // Let's traverse the profile to find the beginning of each job
    for (auto slice_it = _profile.begin(); slice_it != _profile.end(); ++slice_it)
    {
        
        const TimeSlice &slice = *slice_it;
        set<const Job *> allocated_jobs;
        for (auto mit : slice.allocated_jobs)
        {
            const Job *job = mit.first;
            allocated_jobs.insert(job);
        }

        set<const Job *> finished_jobs;
        set_difference(current_jobs.begin(), current_jobs.end(), allocated_jobs.begin(), allocated_jobs.end(),
            std::inserter(finished_jobs, finished_jobs.end()));

        for (const Job *job : finished_jobs)
        {
            Rational rect_x0 = jobs_starting_times[job] * second_width - x0;
            Rational rect_x1 = slice.begin * second_width - x0;
            Rational rect_width = rect_x1 - rect_x0;
            string rect_color = _colors[job->unique_number % (int)_colors.size()];

            // Let's find where the job has been allocated
            PPK_ASSERT_ERROR(slice_it != _profile.begin());
            auto previous_slice_it = slice_it;
            --previous_slice_it;
            std::string job_id = job->id;
            job_id = job_id.substr(job_id.find("!")+1,job_id.size());
             if (job->purpose=="reservation")
                {
                    job_id+=" R";
                    rect_color = _reservation_colors[job->unique_number %(int)_reservation_colors.size()];
                }
            IntervalSet job_machines = previous_slice_it->allocated_jobs.at(job);

            // Let's create a rectangle for each contiguous part of the allocation
            for (auto it = job_machines.intervals_begin(); it != job_machines.intervals_end(); ++it)
            {
                PPK_ASSERT_ERROR(it->lower() <= it->upper());
                Rational rect_y0 = it->lower() * machine_height - y0;
                Rational rect_y1 = ((it->upper() + Rational(1)) * machine_height)
                    - (space_between_machines_ratio * machine_height) - y0;
                Rational rect_height = rect_y1 - rect_y0;
                
                double stroke_width = (double)std::min(std::max((Rational)(std::min(second_width, machine_height) / 10),(Rational)0.1),(Rational)0.5);//nothing less than 0.1, nothing greater than 0.5
                    
                snprintf(buf, buf_size,
                    "  <rect x=\"%g\" y=\"%g\" width=\"%g\" height=\"%g\" stroke=\"black\" stroke-width=\"%g\" "
                    "fill=\"%s\"/>\n"
                    " <text x=\"%g\" y=\"%g\" font-size=\"%dpx\">%s</text>\n",
                    (double)rect_x0, (double)rect_y0, (double)rect_width, (double)rect_height,
                    stroke_width, rect_color.c_str(),(double)(rect_x0+1),(double)(rect_y0+2),(int)2,job_id.c_str());

                res += buf;
            }
        }
        


        set<const Job *> new_jobs;
        set_difference(allocated_jobs.begin(), allocated_jobs.end(), current_jobs.begin(), current_jobs.end(),
            std::inserter(new_jobs, new_jobs.end()));

        for (const Job *job : new_jobs)
        {
            jobs_starting_times[job] = slice.begin;
        }

        // Update current_jobs
        for (const Job *job : finished_jobs)
            current_jobs.erase(job);
        for (const Job *job : new_jobs)
            current_jobs.insert(job);
    }
    //LOG_F(INFO,"line 1461");
    for (const ReservedTimeSlice reservation : svg_reservations)
    {   
        
        const Job * job = reservation.job;
        const double start = job->start;
        const Rational walltime = job->walltime;
        Rational rect_x0 = start * second_width - x0;
        Rational rect_x1 = (start+walltime) * second_width - x0;
        Rational rect_width = rect_x1 - rect_x0;
        std::string rect_color = _reservation_colors[job->unique_number % (int)_reservation_colors.size()];
        for (auto it = reservation.alloc->used_machines.intervals_begin(); it != reservation.alloc->used_machines.intervals_end(); ++it)
        {
            PPK_ASSERT_ERROR(it->lower() <= it->upper());
            Rational rect_y0 = it->lower() * machine_height - y0;
            Rational rect_y1 = ((it->upper() + Rational(1)) * machine_height)
                - (space_between_machines_ratio * machine_height) - y0;
            Rational rect_height = rect_y1 - rect_y0;
            std::string job_id = job->id;
            job_id = job_id.substr(job_id.find("!")+1);
            
            if (job->purpose=="reservation")
                job_id+=" R";
            snprintf(buf, buf_size,
                "  <rect x=\"%g\" y=\"%g\" width=\"%g\" height=\"%g\" stroke-width=\".3\" stroke-dasharray=\"3,3\" " 
                "stroke=\"black\" fill=\"%s\" fill-opacity=\"0.4\"/>\n"
                " <text x=\"%g\" y=\"%g\" font-size=\"%dpx\">%s</text>\n",
                (double)rect_x0, (double)rect_y0, (double)rect_width, (double)rect_height,
                rect_color.c_str(),(double)(rect_x0+1),(double)(rect_y0+2),(int)2,job_id.c_str()); 
            res += buf;
        }

    }
    for (auto it = _svg_highlight_machines.elements_begin(); it != _svg_highlight_machines.elements_end(); ++it)
    {
      //  LOG_F(INFO,"DEBUG");
        // Use operator* to retrieve the element value
        
        int i = *it;
//        LOG_F(INFO,"DEBUG");
        snprintf(buf, buf_size,
            "<rect x=\"%g\" y=\"%g\" width=\"%g\" height=\"%g\" stroke-width=\".3\" stroke-dasharray=\"10,10,5,5,5,10\" " 
                "stroke=\"black\" fill=\"%s\" fill-opacity=\"0.6\"/>\n"
                " <text x=\"%g\" y=\"%g\" font-size=\"%dpx\">%s</text>\n", (double)0,
            (double)(i * machine_height), (double)width, (double)machine_height, "#ce7000",
            ((double)width)/2 - 10,(double)(i*machine_height)+(double)((machine_height/2.0)+(machine_height/4.0)),(int)(machine_height/2),("m "+std::to_string(i)).c_str());
        res += buf;
  //      LOG_F(INFO,"DEBUG");

    }
    res += "</g></svg>";

    delete[] buf;
    return res;
}

void Schedule::write_svg_to_file(const string &filename,
                                const std::string& message,
                                const std::list<ReservedTimeSlice> & svg_reservations) const
{
    ofstream f(filename);

    if (f.is_open())
        f << to_svg(message,svg_reservations) << "\n";

    f.close();
}

void Schedule::output_to_svg(const std::string &message,bool json)
{
    if (_frame_number >= _svg_frame_start && (_frame_number <= _svg_frame_end || _svg_frame_end == -1)){
        if (_output_number >= _svg_output_start && (_output_number < _svg_output_end || _svg_output_end == -1)){
            const int bufsize = 4096;
            char *buf = new char[bufsize];
            char *buf2 = new char[bufsize];
            

            snprintf(buf, bufsize, "%s%06d.svg", _svg_prefix.c_str(), _output_number);
            snprintf(buf2,bufsize, "%s%06d.txt",_svg_prefix.c_str(),_output_number);
            
            ofstream f(buf2);
            auto first_slice = _profile.begin();
            if (f.is_open())
                f << first_slice->begin;
            f.close();
            
            const std::list<ReservedTimeSlice> svg_reservations = _svg_reservations;
            if (!json)
            {
                if (_output_svg_method == "text" || _output_svg_method == "both")
                    LOG_F(INFO,"Frame: %06d Output: %06d Sec: %.1f Msg: %s \n %s",_frame_number, _output_number,(double)_profile.begin()->begin,message.c_str(),to_string().c_str());
            }
            else
            {
                if (_output_svg_method == "text" || _output_svg_method == "both")
                    LOG_F(INFO,"Frame: %06d Output: %06d Sec: %.1f Msg: %s \n %s",_frame_number, _output_number,(double)_profile.begin()->begin,message.c_str(),to_json_string().c_str());
            }
            if (_output_svg_method == "svg" || _output_svg_method == "both")
                write_svg_to_file(buf,message,svg_reservations);
            if (_profile.size()>1)
            {
                auto slice = _profile.end();
                slice--;
                slice--;
                Rational end = slice->end;
                if (end > 0 && end != 1e19)
                    _previous_time_end = end;
            }
            

            delete[] buf;
        }
    }
    if (message=="make_decisions")
        ++_frame_number %=1000000000;
    ++_output_number %= 1000000000;

}

void Schedule::dump_to_batsim_jobs_file(const string &filename) const
{
    ofstream f(filename);
    if (f.is_open())
    {
        f << "job_id,submission_time,requested_number_of_resources,requested_time,starting_time,finish_time,allocated_resources\n";

        PPK_ASSERT_ERROR(_profile.size() > 0);

        const int buf_size = 4096;
        char *buf = new char[buf_size];

        map<const Job *, Rational> jobs_starting_times;
        set<const Job *> current_jobs;
        for (auto mit : _profile.begin()->allocated_jobs)
        {
            const Job *allocated_job = mit.first;
            current_jobs.insert(allocated_job);
            jobs_starting_times[allocated_job] = _profile.begin()->begin;
        }

        // Let's traverse the profile to find the beginning of each job
        for (auto slice_it = _profile.begin(); slice_it != _profile.end(); ++slice_it)
        {
            const TimeSlice &slice = *slice_it;
            set<const Job *> allocated_jobs;
            for (auto mit : slice.allocated_jobs)
            {
                const Job *job = mit.first;
                allocated_jobs.insert(job);
            }

            set<const Job *> finished_jobs;
            set_difference(current_jobs.begin(), current_jobs.end(), allocated_jobs.begin(), allocated_jobs.end(),
                std::inserter(finished_jobs, finished_jobs.end()));

            for (const Job *job : finished_jobs)
            {
                // Find where the job has been allocated
                PPK_ASSERT_ERROR(slice_it != _profile.begin());
                auto previous_slice_it = slice_it;
                --previous_slice_it;
                IntervalSet job_machines = previous_slice_it->allocated_jobs.at(job);

                snprintf(buf, buf_size, "%s,%g,%d,%g,%g,%g,%s\n",
                         job->id.c_str(),
                         job->submission_time,
                         job->nb_requested_resources,
                         (double)job->walltime,
                         (double)jobs_starting_times[job],
                         (double)slice_it->begin,
                         job_machines.to_string_hyphen(" ", "-").c_str());
                f << buf;
            }

            set<const Job *> new_jobs;
            set_difference(allocated_jobs.begin(), allocated_jobs.end(), current_jobs.begin(), current_jobs.end(),
                std::inserter(new_jobs, new_jobs.end()));

            for (const Job *job : new_jobs)
            {
                jobs_starting_times[job] = slice.begin;
            }

            // Update current_jobs
            for (const Job *job : finished_jobs)
                current_jobs.erase(job);
            for (const Job *job : new_jobs)
                current_jobs.insert(job);
        }

        delete[] buf;
    }

    f.close();
}

void Schedule::incremental_dump_as_batsim_jobs_file(const string &filename_prefix)
{
    const int bufsize = 4096;
    char *buf = new char[bufsize];

    snprintf(buf, bufsize, "%s%06d.csv", filename_prefix.c_str(), _output_number);
    _output_number = (_output_number + 1) % 10000000;

    dump_to_batsim_jobs_file(buf);

    delete[] buf;
}

int Schedule::nb_machines() const
{
    return _nb_machines;
}

void Schedule::generate_colors(int nb_colors)
{
    PPK_ASSERT_ERROR(nb_colors > 0);
    _colors.reserve(nb_colors);
    _reservation_colors.reserve(nb_colors);

    double h, s = 1, v = 1, r, g, b;
    double h2,s2 =1,v2 =1,r2,g2,b2;
    const int color_bufsize = 16;
    char color_buf[color_bufsize];

    double hue_fraction = 360.0 / nb_colors;
    for (int i = 0; i < nb_colors; ++i)
    {
        h = i * hue_fraction;
        hsvToRgb(h, s, v, r, g, b);

        unsigned int red = std::max(50, std::min((int)(floor(256 * r)), 255));
        unsigned int green = std::max(20, std::min((int)(floor(256 * g)), 255));
        unsigned int blue = std::max(20, std::min((int)(floor(256 * g)), 255));

        snprintf(color_buf, color_bufsize, "#%02x%02x%02x", red, green, blue);
        _colors.push_back(color_buf);
    }
    double value_fraction =  1 / (double)(nb_colors); //keep the value higher so we can see it
    double saturation_fraction = 1 /(double)(nb_colors);
    h2 = 270; //purple hue
    for (int i = 0; i < nb_colors; ++i)
    {
        v2 = i * value_fraction;
        LOG_F(INFO,"nb_colors: %d i: %d top: %d value_frac: %g v2: %g",nb_colors,i,(nb_colors -1 +i),value_fraction,v2);
        s2 = (double)(nb_colors-i) * saturation_fraction;
        if (s2 < .2)
            s2=.2;
        hsvToRgb(h2, s2, v2, r2, g2, b2);
        
        unsigned int red = std::max(20, std::min((int)(floor(256 * r2)), 255));
        unsigned int green = std::max(20, std::min((int)(floor(256 * g2)), 255));
        unsigned int blue = std::max(20, std::min((int)(floor(256 * b2)), 255));

        snprintf(color_buf, color_bufsize, "#%02x%02x%02x", red, green, blue);
        _reservation_colors.push_back(color_buf);
    }

    
    random_shuffle(_reservation_colors.begin(),_reservation_colors.end());
    random_shuffle(_colors.begin(), _colors.end());
}

void Schedule::remove_job_internal(const Job *job, Schedule::TimeSliceIterator removal_point)
{
    // Let's retrieve the machines used by the job
    PPK_ASSERT_ERROR(removal_point->allocated_jobs.count(job) == 1);
    IntervalSet job_machines = removal_point->allocated_jobs.at(job);
    //LOG_F(INFO," current allocated machines %s \n current repair machines inside remove_job_internal %s",
      //      job_machines.to_string_hyphen().c_str(),
        //    _repair_machines.to_string_hyphen().c_str());
    job_machines-=_repair_machines;
    //LOG_F(INFO,"removing job %s",job->id.c_str());
    //LOG_F(INFO,"adding back %s",job_machines.to_string_hyphen().c_str());
    
    _size--;
    if (job->purpose!="reservation")
        _nb_jobs_size--;
    else
        _nb_reservations_size--;
    //LOG_F(INFO,"sched_size-- %d",_size);
    /*
    //Doing this in the add_repair_machines() function so no longer needed
    if (!_repair_machines.is_empty())
    {
        _profile.begin()->nb_available_machines -= _repair_machines.size();
        _profile.begin()->available_machines -= _repair_machines;
    }
    */
    if (_debug)
    {
      //  LOG_F(INFO, "Removing job '%s'. Output number %d. %s", job->id.c_str(), _output_number, to_string().c_str());
        output_to_svg("top remove_job_internal "+job->id);
    }

    // Let's iterate the time slices until the job is found
    for (auto pit = removal_point; pit != _profile.end(); ++pit)
    {
        // If the job was succesfully erased from the current slice (the job was in it)
        if (pit->allocated_jobs.erase(job) == 1)
        {
            pit->available_machines.insert(job_machines);
            pit->allocated_machines.remove(job_machines);
            pit->nb_available_machines += job_machines.size();
            if (job->purpose == "reservation")
            {
                pit->nb_reservations--;
                if (pit->nb_reservations == 0)
                    pit->has_reservation = false;
            }

            // If the slice is not the first one, let's try to merge it with its preceding slice
            if (pit != _profile.begin())
            {
                auto previous = pit;
                previous--;
                LOG_F(INFO,"remove_job_internal: %s \nbegin:%f  end:%f\nbegin:%f  end:%f",job->id.c_str(),previous->begin.convert_to<double>(),previous->end.convert_to<double>(),pit->begin.convert_to<double>(),pit->end.convert_to<double>());
                // The slices are merged if they have the same jobs
                std::vector<std::string> jobsV;
                for (auto pair : previous->allocated_jobs)
                    jobsV.push_back(pair.first->id);
                std::string previousString = boost::algorithm::join(jobsV, ",");
                jobsV.clear();
                for (auto pair : pit->allocated_jobs)
                    jobsV.push_back(pair.first->id);
                std::string pitString = boost::algorithm::join(jobsV, ",");
                LOG_F(INFO,"previous alloc: %s\npit alloc: %s",previousString.c_str(),pitString.c_str());

                
                if ((previous->allocated_jobs == pit->allocated_jobs) || (previousString == pitString ))
                {
                    LOG_F(INFO,"TRUE");
                    PPK_ASSERT_ERROR(previous->available_machines == pit->available_machines,
                        "Two consecutive time slices, do NOT use the same resources "
                        "whereas they contain the same jobs. Slices:\n%s%s",
                        previous->to_string(2).c_str(), pit->to_string(2).c_str());

                    pit->begin = previous->begin;
                    pit->length = pit->end - pit->begin;
                    set_smallest_and_largest_time_slice_length(pit->length);
                    

                    // pit is updated to ensure --pit points to a valid location after erasure
                    pit = _profile.erase(previous);
                }
            }

            // Let's iterate the slices while the job is in it, and erase it
            for (++pit; pit != _profile.end() && pit->allocated_jobs.erase(job) == 1; ++pit)
            {
                pit->available_machines.insert(job_machines);
                pit->allocated_machines.remove(job_machines);
                pit->nb_available_machines += job_machines.size();
                if (job->purpose == "reservation")
                {
                    pit->nb_reservations--;
                    if (pit->nb_reservations == 0)
                        pit->has_reservation = false;
                }

                // If the slice is not the first one, let's try to merge it with its preceding slice
                if (pit != _profile.begin())
                {
                    auto previous = pit;
                    previous--;
                    LOG_F(INFO,"remove_job_internal: %s \nbegin:%f  end:%f\nbegin:%f  end:%f",job->id.c_str(),previous->begin.convert_to<double>(),previous->end.convert_to<double>(),pit->begin.convert_to<double>(),pit->end.convert_to<double>());
                    std::vector<std::string> jobsV;
                for (auto pair : previous->allocated_jobs)
                    jobsV.push_back(pair.first->id);
                std::string previousString = boost::algorithm::join(jobsV, ",");
                jobsV.clear();
                for (auto pair : pit->allocated_jobs)
                    jobsV.push_back(pair.first->id);
                std::string pitString = boost::algorithm::join(jobsV, ",");
                LOG_F(INFO,"previous alloc: %s\npit alloc: %s",previousString.c_str(),pitString.c_str());

                
                if ((previous->allocated_jobs == pit->allocated_jobs) || (previousString == pitString ))
                    {
                        LOG_F(INFO,"TRUE");
                        PPK_ASSERT_ERROR(previous->available_machines == pit->available_machines,
                            "Two consecutive time slices, do NOT use the same resources "
                            "whereas they contain the same jobs. Slices:\n%s%s",
                            previous->to_string(2).c_str(), pit->to_string(2).c_str());

                        pit->begin = previous->begin;
                        pit->length = pit->end - pit->begin;
                        set_smallest_and_largest_time_slice_length(pit->length);

                        // pit is updated to ensure --pit points to a valid location after erasure
                        pit = _profile.erase(previous);
                    }
                }
            }

            // pit is either profile.end() or does NOT contain the job
            // Let's try to merge it with its previous slice
            if (pit != _profile.end())
            {
                if (pit != _profile.begin())
                {
                    auto previous = pit;
                    previous--;

                    // The slices are merged if they have the same jobs
                    LOG_F(INFO,"remove_job_internal: %s \nbegin:%f  end:%f\nbegin:%f  end:%f",job->id.c_str(),previous->begin.convert_to<double>(),previous->end.convert_to<double>(),pit->begin.convert_to<double>(),pit->end.convert_to<double>());
                    std::vector<std::string> jobsV;
                for (auto pair : previous->allocated_jobs)
                    jobsV.push_back(pair.first->id);
                std::string previousString = boost::algorithm::join(jobsV, ",");
                jobsV.clear();
                for (auto pair : pit->allocated_jobs)
                    jobsV.push_back(pair.first->id);
                std::string pitString = boost::algorithm::join(jobsV, ",");
                LOG_F(INFO,"previous alloc: %s\npit alloc: %s",previousString.c_str(),pitString.c_str());

                
                if ((previous->allocated_jobs == pit->allocated_jobs) || (previousString == pitString ))
                    {
                        LOG_F(INFO,"TRUE");
                        PPK_ASSERT_ERROR(previous->available_machines == pit->available_machines,
                            "Two consecutive time slices, do NOT use the same resources "
                            "whereas they contain the same jobs. Slices:\n%s%s",
                            previous->to_string(2).c_str(), pit->to_string(2).c_str());

                        pit->begin = previous->begin;
                        pit->length = pit->end - pit->begin;
                        set_smallest_and_largest_time_slice_length(pit->length);

                        // pit is updated to ensure --pit points to a valid location after erasure
                        pit = _profile.erase(previous);
                    }
                }
            }

            if (_debug)
            {
                LOG_F(1, "Removed job '%s'. Output number %d. %s", job->id.c_str(), _output_number, to_string().c_str());
                output_to_svg("bottom remove_job_internal "+job->id);
            }
            /*
            //no longer needed
            if (!_repair_machines.is_empty())
            {
                _profile.begin()->nb_available_machines -= _repair_machines.size();
                _profile.begin()->available_machines -= _repair_machines;
            }
            */

            return;
        }
    }
}

bool Schedule::TimeSlice::contains_job(const Job *job) const
{
    return allocated_jobs.count(job);
}

bool Schedule::TimeSlice::contains_matching_job(std::function<bool(const Job *)> matching_function) const
{
    for (auto mit : allocated_jobs)
    {
        const Job *job = mit.first;
        if (matching_function(job))
            return true;
    }

    return false;
}

const Job *Schedule::TimeSlice::job_from_job_id(string job_id) const
{
    for (auto mit : allocated_jobs)
    {
        const Job *job = mit.first;
        if (job->id == job_id)
            return job;
    }

    return nullptr;
}

string Schedule::TimeSlice::to_string_interval() const
{
    double ibegin = begin.convert_to<double>();
    double iend = end.convert_to<double>();
    double ilength = length.convert_to<double>();

    char buf[256];
    snprintf(buf, 256, "[%.15f,%.15f] (length=%.15f)", ibegin, iend, ilength);

    return string(buf);
}
string Schedule::TimeSlice::to_json_string_interval() const
{
    double ibegin = begin.convert_to<double>();
    double iend = end.convert_to<double>();
    double ilength =length.convert_to<double>();
    return batsched_tools::string_format(  "\"begin\" : %.15f , \"end\" : %.15f, \"length\" : %.15f ,\n",
                                           ibegin,iend,ilength);
    


}

string Schedule::TimeSlice::to_string_allocated_jobs() const
{
    vector<string> jobs_str;
    jobs_str.reserve(allocated_jobs.size());
    for (auto mit : allocated_jobs)
    {
        const Job *job = mit.first;
        
        //LOG_F(INFO,"here %s",job->id.c_str());
        
        //LOG_F(INFO,"job_alloc_size: %d",job->allocations.size());
        //for (auto kv_pair:job->allocations)
        //    LOG_F(INFO,"%.15f, ",kv_pair.first.convert_to<double>());
        //LOG_F(INFO,"here %.15f",this->begin.convert_to<double>());
        if (job->allocations.find(this->begin)!= job->allocations.end())
            jobs_str.push_back("{\"job_id\":\"" + job->id + "\", \"alloc\":" +
                              batsched_tools::to_json_string(job->allocations[this->begin]) +"}");
        else
            jobs_str.push_back("{\"job_id\":\"" + job->id + "\", \"alloc\":{ \"used_machines\":\""+mit.second.to_string_hyphen()+"\" }}");

        
    }
    

    return boost::algorithm::join(jobs_str, ",");
}

string Schedule::TimeSlice::to_json_string(int initial_indent, int indent) const
{
    string res;

    string iistr, istr;

    for (int i = 0; i < initial_indent; ++i)
        iistr += "\t";

    for (int i = 0; i < indent; ++i)
        istr += "\t";

    res += iistr + "{\"Time slice\": {";
    
    res += to_json_string_interval();
    res += iistr + istr + "\"available_machines\": \"" + available_machines.to_string_hyphen() + "\",\n";
    
    res += iistr + istr + "\"allocated_jobs\": [" + to_string_allocated_jobs() + "],\n";
    
    res += iistr + istr + "\"allocated_machines\":\"" +allocated_machines.to_string_hyphen()+"\",\n";
    
    res += iistr + istr + "\"has_reservation\":"+(has_reservation ? "true" : "false")+",\n";
    
    res += iistr + istr + "\"nb_available_machines\":" + std::to_string(nb_available_machines) + ",\n";
    
    res += iistr + istr + "\"nb_reservations\":" + std::to_string(nb_reservations)+"\n";
    
    res += iistr + "}}";
    
    

    return res;
}

string Schedule::TimeSlice::to_string(int initial_indent, int indent) const
{
    string res;

    string iistr, istr;

    for (int i = 0; i < initial_indent; ++i)
        iistr += " ";

    for (int i = 0; i < indent; ++i)
        istr += " ";

    res += iistr + "Time slice: ";

    res += to_string_interval() + "\n";
    res += iistr + istr + "available machines: " + available_machines.to_string_hyphen() + "\n";
    res += iistr + istr + "allocated jobs: {" + to_string_allocated_jobs() + "}\n";
    

    return res;
}

void hsvToRgb(double h, double s, double v, double &r, double &g, double &b)
{
    if (s == 0) // Achromatic (grey)
    {
        r = g = b = v;
        return;
    }

    h /= 60; // sector 0 to 5
    int i = floor(h);
    float f = h - i; // factorial part of h
    float p = v * (1 - s);
    float q = v * (1 - s * f);
    float t = v * (1 - s * (1 - f));

    switch (i)
    {
    case 0:
        r = v;
        g = t;
        b = p;
        break;
    case 1:
        r = q;
        g = v;
        b = p;
        break;
    case 2:
        r = p;
        g = v;
        b = t;
        break;
    case 3:
        r = p;
        g = q;
        b = v;
        break;
    case 4:
        r = t;
        g = p;
        b = v;
        break;
    default: // case 5:
        r = v;
        g = p;
        b = q;
        break;
    }
}
