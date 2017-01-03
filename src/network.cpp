#include "network.hpp"
#include "isalgorithm.hpp"

#include <boost/locale.hpp>

#include <stdexcept>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "pempek_assert.hpp"

using namespace std;

void Network::connect(const std::string &socketFilename)
{
    sockaddr_un address;
    address.sun_family = AF_UNIX;
    strncpy(address.sun_path, socketFilename.c_str(), sizeof(address.sun_path)-1);

    _clientSocket = socket(PF_UNIX, SOCK_STREAM, 0);
    PPK_ASSERT_ERROR(_clientSocket != -1, "Unable to create socket");

    int ret = ::connect(_clientSocket, (struct sockaddr*)&address, sizeof(address));
    PPK_ASSERT_ERROR(ret != -1, "Impossible to connect to '%s'", socketFilename.c_str());
}

void Network::write(const string &content)
{
    // Let's make sure the sent message is in UTF-8
    string msg_utf8 = boost::locale::conv::to_utf<char>(content, "UTF-8");

    int32_t lg = msg_utf8.size();
    printf("Sending '%s'\n", msg_utf8.c_str());
    fflush(stdout);
    ::write(_clientSocket, &lg, 4);
    ::write(_clientSocket, msg_utf8.c_str(), lg);
}

void Network::read(string &received_content)
{
    int32_t lg;

    int ret = ::read(_clientSocket, &lg, 4);
    if (ret < 4)
        throw runtime_error("Connection lost");

    received_content.resize(lg);

    ret = ::read(_clientSocket, &received_content[0], lg);
    if (ret < lg)
        throw runtime_error("Connection lost");

    printf("Received '%s'\n", received_content.c_str());

    // Let's convert the received string from UTF-8
    string received_utf8 = boost::locale::conv::from_utf(received_content, "UTF-8");
    received_content = received_utf8;

    fflush(stdout);
}

std::string absolute_filename(const std::string & filename)
{
    PPK_ASSERT_ERROR(filename.length() > 0);

    // Let's assume filenames starting by "/" are absolute.
    if (filename[0] == '/')
        return filename;

    char cwd_buf[PATH_MAX];
    char * getcwd_ret = getcwd(cwd_buf, PATH_MAX);
    PPK_ASSERT_ERROR(getcwd_ret == cwd_buf, "getcwd failed");

    return string(getcwd_ret) + '/' + filename;
}
