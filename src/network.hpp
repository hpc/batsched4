#pragma once
#include "decision.hpp"

namespace network
{
    const char separator0 = '|';
    const char separator1 = ':';
    const char separator2 = ';';
    const char separator3 = '=';
    const char separator4 = ',';

    const char nop = 'N';
    const char nopMeLater = 'n';
    const char job_submission = 'S';
    const char job_completion = 'C';
    const char jobAllocation = 'J';
    const char jobRejection = 'R';
    const char machinePStateChangeRequest = 'P';
    const char machine_pstate_changed = 'p';
    const char simulation_begin = 'A';
    const char simulation_end = 'Z';
    const char failure = 'F';
    const char failure_finished = 'f';
}

class Network
{
public:
    void connect(const std::string & socketFilename);
    void write(const std::string & content);
    void read(std::string & received_content);

private:
    int _clientSocket = -1;
};

std::string absolute_filename(const std::string & filename);
