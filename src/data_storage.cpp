#include "data_storage.hpp"

#include <boost/locale.hpp>

#include <loguru.hpp>

#include "pempek_assert.hpp"

using namespace std;

RedisStorage::RedisStorage()
{
}

RedisStorage::~RedisStorage()
{
    if (_is_connected)
    {
        disconnect();
    }
}

void RedisStorage::set_instance_key_prefix(const std::string & key_prefix)
{
    _instance_key_prefix = key_prefix;
}

void RedisStorage::connect_to_server(const std::string & host,
                                     int port,
                                     std::function<void (int)> connection_callback)
{
    PPK_ASSERT_ERROR(!_is_connected, "Bad RedisStorage::connect_to_server call: "
                                     "Already connected");

    _is_connected = _redox.connect(host, port, connection_callback);
    PPK_ASSERT_ERROR(_is_connected, "Error: could not connect to Redis server "
                                    "(host='%s', port=%d)", host.c_str(), port);
}

void RedisStorage::disconnect()
{
    PPK_ASSERT_ERROR(_is_connected, "Bad RedisStorage::connect_to_server call: "
                                    "Not connected");

    _redox.disconnect();
}

std::string RedisStorage::get(const std::string & key)
{
    PPK_ASSERT_ERROR(_is_connected, "Bad RedisStorage::get call: Not connected");

    string real_key = boost::locale::conv::to_utf<char>(build_key(key), "UTF-8");

    try
    {
        return _redox.get(real_key);
    }
    catch (const std::runtime_error & e)
    {
        PPK_ASSERT_ERROR(false, "Couldn't get the value associated to key '%s' in Redis! "
                         "Message: %s", real_key.c_str(), e.what());
        return "";
    }
}

string RedisStorage::get_job_json_string(const string &job_id)
{
    string job_key = "job_" + job_id;
    return _redox.get(build_key(job_key));
}

int RedisStorage::get_number_of_machines()
{
    std::string nb_machines_str = get("nb_res");
    int nb_res = stoi(nb_machines_str);
    PPK_ASSERT_ERROR(nb_res > 0);

    return nb_res;
}

bool RedisStorage::set(const std::string &key, const std::string &value)
{
    string real_key = boost::locale::conv::to_utf<char>(build_key(key), "UTF-8");
    string real_value = boost::locale::conv::to_utf<char>(value, "UTF-8");

    PPK_ASSERT_ERROR(_is_connected, "Bad RedisStorage::get call: Not connected");
    bool ret = _redox.set(real_key, real_value);
    if (ret)
    {
        LOG_F(1, "Redis: Set '%s'='%s'", real_key.c_str(), real_value.c_str());
        PPK_ASSERT_ERROR(get(key) == value, "Batsim <-> Redis communications are inconsistent!");
    }
    else
       LOG_F(WARNING, "Redis: Couldn't set: '%s'='%s'", real_key.c_str(), real_value.c_str());

    return ret;
}

bool RedisStorage::del(const std::string &key)
{
    PPK_ASSERT_ERROR(_is_connected, "Bad RedisStorage::get call: Not connected");
    return _redox.del(build_key(key));
}

std::string RedisStorage::instance_key_prefix() const
{
    return _instance_key_prefix;
}

std::string RedisStorage::key_subparts_separator() const
{
    return _key_subparts_separator;
}

string RedisStorage::job_key(const string & workload_name,
                             const string & job_id)
{
    string key = "job_" + workload_name + '!' + job_id;
    return key;
}

string RedisStorage::profile_key(const string & workload_name,
                                 const string & profile_name)
{
    string key = "profile_" + workload_name + '!' + profile_name;
    return key;
}


std::string RedisStorage::build_key(const std::string & user_given_key) const
{
    string key = _instance_key_prefix + _key_subparts_separator + user_given_key;

    /*string key_latin1 = boost::locale::conv::to_utf<char>(key, "Latin1");

    return key_latin1;*/
    return boost::locale::conv::utf_to_utf<char>(key);
}
