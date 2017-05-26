#include <stdio.h>
#include <vector>
#include <iostream>
#include <fstream>
#include <set>

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

#include <rapidjson/document.h>

#include "isalgorithm.hpp"
#include "decision.hpp"
#include "network.hpp"
#include "json_workload.hpp"
#include "pempek_assert.hpp"
#include "data_storage.hpp"

#include "algo/conservative_bf.hpp"
#include "algo/easy_bf.hpp"
#include "algo/easy_bf_plot_liquid_load_horizon.hpp"
#include "algo/energy_bf.hpp"
#include "algo/energy_bf_dicho.hpp"
#include "algo/energy_bf_idle_sleeper.hpp"
#include "algo/energy_bf_monitoring_period.hpp"
#include "algo/energy_bf_monitoring_inertial_shutdown.hpp"
#include "algo/energy_bf_machine_subpart_sleeper.hpp"
#include "algo/filler.hpp"
#include "algo/killer.hpp"
#include "algo/killer2.hpp"
#include "algo/sleeper.hpp"
#include "algo/submitter.hpp"

using namespace std;
namespace po = boost::program_options;
namespace n = network;
namespace r = rapidjson;

void run(Network & n, ISchedulingAlgorithm * algo, SchedulingDecision &d,
         Workload &workload, bool call_make_decisions_on_single_nop = true);

int main(int argc, char ** argv)
{
    string socket_endpoint;
    string scheduling_variant;
    string selection_policy;
    string queue_order;
    string variant_options;
    string variant_options_filepath;
    double rjms_delay;
    bool call_make_decisions_on_single_nop;

    const set<string> variants_set = {"conservative_bf", "easy_bf", "easy_bf_plot_liquid_load_horizon",
                                      "energy_bf", "energy_bf_dicho", "energy_bf_idle_sleeper",
                                      "energy_bf_monitoring",
                                      "energy_bf_monitoring_inertial", "energy_bf_subpart_sleeper",
                                      "filler", "killer", "killer2", "sleeper", "submitter"};
    const set<string> policies_set = {"basic", "contiguous"};
    const set<string> queue_orders_set = {"fcfs", "lcfs", "desc_bounded_slowdown", "desc_slowdown",
                                          "asc_size", "desc_size", "asc_walltime", "desc_walltime"};

    const string variants_string = "{" + boost::algorithm::join(variants_set, ", ") + "}";
    const string policies_string = "{" + boost::algorithm::join(policies_set, ", ") + "}";
    const string queue_orders_string = "{" + boost::algorithm::join(queue_orders_set, ", ") + "}";

    ISchedulingAlgorithm * algo = nullptr;
    ResourceSelector * selector = nullptr;
    Queue * queue = nullptr;
    SortableJobOrder * order = nullptr;

    try
    {
        po::options_description desc("Allowed options");
        desc.add_options()
            ("rjms_delay,d", po::value<double>(&rjms_delay)->default_value(5.0), "sets the expected time that the RJMS takes to do some things like killing a job")
            ("help,h", "produce help message")
            ("policy,p", po::value<string>(&selection_policy)->default_value("basic"), string("sets resource selection policy. Available values are " + policies_string).c_str())
            ("socket-endpoint,s", po::value<string>(&socket_endpoint)->default_value("tcp://*:28000"), "sets socket endpoint")
            ("variant,v", po::value<string>(&scheduling_variant)->default_value("filler"), string("sets scheduling variant. Available values are " + variants_string).c_str())
            ("variant_options", po::value<string>(&variant_options)->default_value("{}"), "sets scheduling variant options. Must be formatted as a JSON object.")
            ("variant_options_filepath", po::value<string>(&variant_options_filepath)->default_value(""), "sets scheduling variants options as the content of the given filepath. Overrides the variant_options option.")
            ("queue_order,o", po::value<string>(&queue_order)->default_value("fcfs"), string("sets queue order. Available values are " + queue_orders_string).c_str())
            ("call_make_decisions_on_single_nop", po::value<bool>(&call_make_decisions_on_single_nop)->default_value(true), "If set to true, make_decisions will be called after single NOP messages")
        ;

        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);

        if (vm.count("help"))
        {
            printf("Usage : %s [options]\n", argv[0]);
            cout << desc << "\n";
            return 1;
        }

        po::notify(vm);

        if (rjms_delay < 0)
        {
            printf("Invalid RJMS delay parameter (%g). It must be non-negative.\n", rjms_delay);
            return 1;
        }

        // Workload creation
        Workload w;
        w.set_rjms_delay(rjms_delay);

        // Scheduling parameters
        SchedulingDecision decision;

        // Queue order
        if (queue_order == "fcfs")
            order = new FCFSOrder;
        else if (queue_order == "lcfs")
            order = new LCFSOrder;
        else if (queue_order == "desc_bounded_slowdown")
            order = new DescendingBoundedSlowdownOrder(1);
        else if (queue_order == "desc_slowdown")
            order = new DescendingSlowdownOrder;
        else if (queue_order == "asc_size")
            order = new AscendingSizeOrder;
        else if (queue_order == "desc_size")
            order = new DescendingSizeOrder;
        else if (queue_order == "asc_walltime")
            order = new AscendingWalltimeOrder;
        else if (queue_order == "desc_walltime")
            order = new DescendingWalltimeOrder;
        else
        {
            printf("Invalid queue order '%s'. Available options are %s\n", queue_order.c_str(), queue_orders_string.c_str());
            return 1;
        }

        queue = new Queue(order);

        // Resource selector
        if (selection_policy == "basic")
            selector = new BasicResourceSelector;
        else if (selection_policy == "contiguous")
            selector = new ContiguousResourceSelector;
        else
        {
            printf("Invalid resource selection policy '%s'. Available options are %s\n", selection_policy.c_str(), policies_string.c_str());
            return 1;
        }

        // Scheduling variant options
        if (!variant_options_filepath.empty())
        {
            ifstream variants_options_file(variant_options_filepath);

            if (variants_options_file.is_open())
            {
                // Let's put the whole file content into one string
                variants_options_file.seekg(0, ios::end);
                variant_options.reserve(variants_options_file.tellg());
                variants_options_file.seekg(0, ios::beg);

                variant_options.assign((std::istreambuf_iterator<char>(variants_options_file)),
                                        std::istreambuf_iterator<char>());
            }
            else
            {
                printf("Couldn't open variants options file '%s'. Aborting.\n", variant_options_filepath.c_str());
                return 1;
            }
        }

        rapidjson::Document json_doc_variant_options;
        json_doc_variant_options.Parse(variant_options.c_str());
        if (!json_doc_variant_options.IsObject())
        {
            printf("Invalid variant options: Not a JSON object. variant_options='%s'\n", variant_options.c_str());
            return 1;
        }
        printf("variant_options = '%s'\n", variant_options.c_str());

        // Scheduling variant
        if (scheduling_variant == "filler")
            algo = new Filler(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "conservative_bf")
            algo = new ConservativeBackfilling(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "easy_bf")
            algo = new EasyBackfilling(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "easy_bf_plot_liquid_load_horizon")
            algo = new EasyBackfillingPlotLiquidLoadHorizon(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "energy_bf")
            algo = new EnergyBackfilling(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "energy_bf_dicho")
            algo = new EnergyBackfillingDichotomy(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "energy_bf_idle_sleeper")
            algo = new EnergyBackfillingIdleSleeper(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "energy_bf_monitoring")
            algo = new EnergyBackfillingMonitoringPeriod(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "energy_bf_monitoring_inertial")
            algo = new EnergyBackfillingMonitoringInertialShutdown(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "energy_bf_subpart_sleeper")
            algo = new EnergyBackfillingMachineSubpartSleeper(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "killer")
            algo = new Killer(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "killer2")
            algo = new Killer2(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "sleeper")
            algo = new Sleeper(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else if (scheduling_variant == "submitter")
            algo = new Submitter(&w, &decision, queue, selector, rjms_delay, &json_doc_variant_options);
        else
        {
            printf("Invalid scheduling variant '%s'. Available variants are %s\n", scheduling_variant.c_str(), variants_string.c_str());
            return 1;
        }

        // Network
        Network n;
        n.bind(socket_endpoint);

        // Run the simulation
        run(n, algo, decision, w, call_make_decisions_on_single_nop);
    }
    catch(const std::exception & e)
    {
        string what = e.what();

        if (what == "Connection lost")
        {
            cout << what << endl;
        }
        else
        {
            cout << "Error: " << e.what() << endl;

            delete queue;
            delete order;

            delete algo;
            delete selector;

            throw;
        }
    }

    delete queue;
    delete order;

    delete algo;
    delete selector;

    return 0;
}

void run(Network & n, ISchedulingAlgorithm * algo, SchedulingDecision & d,
         Workload & workload, bool call_make_decisions_on_single_nop)
{
    bool simulation_finished = false;

    // Redis creation
    RedisStorage redis;
    bool redis_enabled = false;
    algo->set_redis(&redis);

    while (!simulation_finished)
    {
        string received_message;
        n.read(received_message);

        if (boost::trim_copy(received_message).empty())
            throw runtime_error("Empty message received (connection lost ?)");

        d.clear();

        r::Document doc;
        doc.Parse(received_message.c_str());

        double message_date = doc["now"].GetDouble();
        double current_date = message_date;
        bool requested_callback_received = false;

        // Let's handle all received events
        const r::Value & events_array = doc["events"];

        for (unsigned int event_i = 0; event_i < events_array.Size(); ++event_i)
        {
            const r::Value & event_object = events_array[event_i];
            const std::string event_type = event_object["type"].GetString();
            current_date = event_object["timestamp"].GetDouble();
            const r::Value & event_data = event_object["data"];

            if (event_type == "SIMULATION_BEGINS")
            {
                int nb_resources = event_data["nb_resources"].GetInt();
                redis_enabled = event_data["config"]["redis"]["enabled"].GetBool();

                if (redis_enabled)
                {
                    string redis_hostname = event_data["config"]["redis"]["hostname"].GetString();
                    int redis_port = event_data["config"]["redis"]["port"].GetInt();
                    string redis_prefix = event_data["config"]["redis"]["prefix"].GetString();

                    redis.connect_to_server(redis_hostname, redis_port, nullptr);
                    redis.set_instance_key_prefix(redis_prefix);
                }

                d.set_redis(redis_enabled, &redis);

                algo->set_nb_machines(nb_resources);
                algo->on_simulation_start(current_date, event_data["config"]);
            }
            else if (event_type == "SIMULATION_ENDS")
            {
                algo->on_simulation_end(current_date);
                simulation_finished = true;
            }
            else if (event_type == "JOB_SUBMITTED")
            {
                string job_id = event_data["job_id"].GetString();

                if (redis_enabled)
                    workload.add_job_from_redis(redis, job_id, current_date);
                else
                    workload.add_job_from_json_object(event_data["job"], job_id, current_date);

                algo->on_job_release(current_date, {job_id});
            }
            else if (event_type == "JOB_COMPLETED")
            {
                string job_id = event_data["job_id"].GetString();
                workload[job_id]->completion_time = current_date;
                algo->on_job_end(current_date, {job_id});
            }
            else if (event_type == "RESOURCE_STATE_CHANGED")
            {
                MachineRange resources = MachineRange::from_string_hyphen(event_data["resources"].GetString(), " ");
                string new_state = event_data["state"].GetString();
                algo->on_machine_state_changed(current_date, resources, std::stoi(new_state));
            }
            else if (event_type == "JOB_KILLED")
            {
                const r::Value & job_ids_array = event_data["job_ids"];

                vector<string> job_ids;
                job_ids.reserve(job_ids_array.Size());

                for (int i = 0; i < (int)job_ids_array.Size(); ++i)
                    job_ids.push_back(job_ids_array[i].GetString());

                algo->on_job_killed(current_date, job_ids);
            }
            else if (event_type == "REQUESTED_CALL")
            {
                requested_callback_received = true;
                algo->on_requested_call(current_date);
            }
            else
            {
                throw runtime_error("Unknown event received. Type = " + event_type);
            }
        }

        bool requested_callback_only = requested_callback_received && (events_array.Size() == 1);

        // make_decisions is not called if (!call_make_decisions_on_single_nop && single_nop_received)
        if (!(!call_make_decisions_on_single_nop && requested_callback_only))
        {
            SortableJobOrder::UpdateInformation update_info(current_date);
            algo->make_decisions(message_date, &update_info, nullptr);
            algo->clear_recent_data_structures();
        }

        message_date = max(message_date, d.last_date());

        const string & message_to_send = d.content(message_date);
        n.write(message_to_send);
    }
}
