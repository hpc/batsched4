#include "machine.hpp"
#include "pempek_assert.hpp"
#include <loguru.hpp>
#include <regex>
#include <string>
#include "batsched_tools.hpp"

std::string Machine::to_json_string()
{
    using namespace std;
    std::string machine_str= "{";
    machine_str += batsched_tools::pair_to_simple_json_string(std::pair<string,int>("id",id));
    machine_str +=",";
    machine_str += batsched_tools::pair_to_simple_json_string(std::pair<string,string>("name",name));
    machine_str +=",";
    machine_str += batsched_tools::pair_to_simple_json_string(std::pair<string,string>("prefix",prefix));
    machine_str +=",";
    machine_str += batsched_tools::pair_to_simple_json_string(std::pair<string,double>("speed",speed));
    machine_str +=",";
    machine_str += batsched_tools::pair_to_simple_json_string(std::pair<string,double>("repair_time",repair_time));
    machine_str +=",";
    machine_str += batsched_tools::pair_to_simple_json_string(std::pair<string,int>("int_name",int_name));
    machine_str +=",";
    machine_str += batsched_tools::pair_to_simple_json_string(std::pair<string,string>("role",role));
    machine_str +=",";
    machine_str += batsched_tools::pair_to_simple_json_string(std::pair<string,int>("core_count",core_count));
    machine_str +=",";
    machine_str += batsched_tools::pair_to_simple_json_string(std::pair<string,int>("cores_available",cores_available));
    machine_str += "}";
    return machine_str;
}
void Machines::ingest(const rapidjson::Value &json)
{
  //first find the machine
  PPK_ASSERT_ERROR(json.HasMember("id"), "ingesting machines failed, no 'id' in json");
  int id = json["id"].GetInt();
  Machine * machine = this->_machinesById[id];
  //now we have the machine, time to update the things that change or could potentially change in the future
  //first check we have the right information
  PPK_ASSERT_ERROR(json.HasMember("speed"),"ingesting machines failed, no 'speed' in json");
  PPK_ASSERT_ERROR(json.HasMember("repair_time"),"ingesting machines failed, no 'repair_time' in json");
  PPK_ASSERT_ERROR(json.HasMember("role"),"ingesting machines failed, no 'role' in json");
  PPK_ASSERT_ERROR(json.HasMember("core_count"),"ingesting machines failed, no 'core_count' in json");
  PPK_ASSERT_ERROR(json.HasMember("cores_available"),"ingesting machines failed, no 'cores_available' in json");
  //now set the machines fields
  machine->speed = json["speed"].GetDouble();
  machine->repair_time = json["repair_time"].GetDouble();
  machine->role = json["role"].GetString();
  machine->core_count = json["core_count"].GetInt();
  machine->cores_available = json["cores_available"].GetInt();
  //that should be it
}
Machines::~Machines(){

}
Machine * Machines::operator[](std::string machine_name){
    PPK_ASSERT_ERROR(_machinesM.count(machine_name) == 1, "Machine '%s' does not exist", machine_name.c_str());
    return _machinesM.at(machine_name);
}
Machine * Machines::operator[](int machine_number){
    PPK_ASSERT_ERROR(_machinesById.count(machine_number) == 1,"Machine with id '%d' does not exist",machine_number);
    return _machinesById[machine_number];
}
Machine * Machines::operator()(std::string prefix_name, int machine_number)
{
    PPK_ASSERT_ERROR(_machinesByPrefix.count(prefix_name) == 1,"Machine with prefix '%s' does not exist",prefix_name.c_str());
    Prefix * prefix=_machinesByPrefix.at(prefix_name);
    PPK_ASSERT_ERROR(prefix->machinesInPrefix->count(machine_number) == 1, "There does not exist a Machine number '%d' in the prefix '%s'",machine_number,prefix_name.c_str());
    return (*(prefix->machinesInPrefix))[machine_number];
}
void Machines::set_core_percent(double core_percent)
{
    _core_percent = core_percent;
}
std::string Machines::to_json_string()
{
    std::string machines="{\n\t";
    
    std::map<int,Machine*>::iterator it;
    for (it=_machinesById.begin();it!=_machinesById.end();)
    {
        machines += it->second->to_json_string();
        it++;
        if (it != _machinesById.end())
            machines+=",\n\t";
    }
    machines+="}";
    return machines;
}
void Machines::add_machine_from_json_object(const rapidjson::Value & object){
    
    //first get the info into the machine
    //**************************************
    Machine* new_machine = new Machine;
    
    PPK_ASSERT_ERROR(object.HasMember("name"),"machine has no name");
    new_machine->name = object["name"].GetString();

    PPK_ASSERT_ERROR(object.HasMember("core_count"),"machine %s has no core_count",new_machine->name.c_str());
    new_machine->core_count = object["core_count"].GetInt();

    PPK_ASSERT_ERROR(object.HasMember("speed"),"machine %s has no speed",new_machine->name.c_str());
    new_machine->speed = object["speed"].GetDouble();

    PPK_ASSERT_ERROR(object.HasMember("repair_time"),"machine %s has no repair_time",new_machine->name.c_str());
    new_machine->repair_time = object["repair-time"].GetDouble();

    PPK_ASSERT_ERROR(object.HasMember("id"),"machine %s has no id",new_machine->name.c_str());
    new_machine->id = object["id"].GetInt();
        
    
    std::regex r(".*[a-zA-Z]+([0-9]+)");//all characters then at least one letter, then capture all numbers
    //matches m[0](m[1])[not matched]  a30b(109)  @d(309)   b(12)   ab(309)[bc] 
    //non-matches [1089]  --no characters
    //non-matches [bbb@], [ccbb]  --no numbers
    
    std::smatch m;
    //std::regex_match(new_machine->name, m, r);
    //new_machine->id = std::stoi(m.str(m.size()));
    //new_machine->prefix = new_machine->name.substr(0,m.position(m.size()));
    for(std::sregex_iterator i = std::sregex_iterator(new_machine->name.begin(), new_machine->name.end(), r);
                            i != std::sregex_iterator();
                            ++i )
    {
        m = *i;
    }
    PPK_ASSERT_ERROR(m.size()==2,"Machine name should be any characters followed by at least one letter followed by numbers.  This should generate a regex match of size 2.  "
        "This is not the case with machine '%s'",new_machine->name.c_str());
    new_machine->int_name = std::stoi(m[1]);
    new_machine->prefix = new_machine->name.substr(0,m.position(1));
    new_machine->cores_available = int(new_machine->core_count * _core_percent);
    


    //ok we got the info, now add to containers
    //******************************************

    _machinesM[new_machine->name] = new_machine; //all machines in a map.  Can get the machine if you know the machine name
    _machinesById[new_machine->id]=new_machine;//all machines in a vector.  can get machines by index.
    //_machinesByPrefix  // all machines separated by prefix name into maps separated by machine id.
    //check if the prefix has already been made
    if (_machinesByPrefix.count(new_machine->prefix) == 1)
    {
        //the prefix has already been made
        //get the Prefix associated with that prefix and add the machine
        Prefix * prefix=_machinesByPrefix.at(new_machine->prefix);
        prefix->machineIds += new_machine->id;
        prefix->machineIdsAvailable += new_machine->id;
        
        (*(prefix->machinesInPrefix))[new_machine->id]=new_machine;

    }
    else
    {
        //the prefix has not already been made
        //make a new map and add the machine to that map
        //then make a new prefix and add the map to the prefix
        std::map<int,Machine *> * aMap = new std::map<int,Machine *>;
        (*aMap)[new_machine->id]=new_machine;
        Prefix * aPrefix = new Prefix;
        aPrefix->machineIds = aPrefix->machineIdsAvailable = aPrefix->machineIdsUnavailable = IntervalSet::empty_interval_set();
        aPrefix->repair_machines = IntervalSet::empty_interval_set();
        aPrefix->machineIds += new_machine->id;
        aPrefix->machineIdsAvailable += new_machine->id;
        
        aPrefix->machinesInPrefix = aMap;
        _machinesByPrefix[new_machine->prefix]=aPrefix;

    }
}
void Machines::update_machine_from_json_object(const rapidjson::Value & object)
{
    PPK_ASSERT_ERROR(object.HasMember("name"),"machine has no name");
    std::string name;
    Machine * new_machine = _machinesM[name];
    PPK_ASSERT_ERROR(object.HasMember("cores_available"),"machine %s has no speed",new_machine->name.c_str());
    new_machine->cores_available = object["cores_available"].GetInt();
}

    
