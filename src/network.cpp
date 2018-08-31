#include "network.hpp"
#include "isalgorithm.hpp"

#include <boost/locale.hpp>

#include <stdexcept>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <loguru.hpp>

#include "pempek_assert.hpp"

using namespace std;

Network::~Network()
{
    if (_socket != nullptr)
    {
        delete _socket;
        _socket = nullptr;
    }
}

void Network::bind(const std::string &socket_endpoint)
{
    _socket = new zmq::socket_t(_context, ZMQ_REP);
    _socket->bind(socket_endpoint);
}

void Network::write(const string &content)
{
    // Let's make sure the sent message is in UTF-8
    string msg_utf8 = boost::locale::conv::to_utf<char>(content, "UTF-8");

    LOG_F(INFO, "Sending '%s'", msg_utf8.c_str());
    _socket->send(msg_utf8.data(), msg_utf8.size());
}

void Network::read(string &received_content)
{
    zmq::message_t message;
    _socket->recv(&message);

    received_content = string((char*)message.data(), message.size());

    // Let's convert the received string from UTF-8
    string received_utf8 = boost::locale::conv::from_utf(received_content, "UTF-8");
    received_content = received_utf8;

    LOG_F(INFO, "Received '%s'", received_content.c_str());
}
