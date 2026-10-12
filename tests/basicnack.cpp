/**
 *  BasicNack.cpp
 *
 *  Test program for basic.nack, which has no reply in AMQP 0-9-1: frames that the
 *  channel sends after it must not wait for one. The program plays the server for a
 *  real AMQP::TcpConnection on a loopback socket, lets the client nack and ack
 *  messages, and checks that every frame arrives on the socket in order.
 *
 *  Build and run from the root of the repository, against the sources in the working tree
 *  (add -I and -L for OpenSSL if it is not in the default paths):
 *
 *      c++ -std=c++17 -g -fsanitize=address,undefined -Iinclude $(find src -name '*.cpp') \
 *          tests/basicnack.cpp -lssl -lcrypto -ldl -lpthread -o basicnack && ./basicnack
 */

/**
 *  Dependencies
 */
#include <amqpcpp.h>
#include <amqpcpp/linux_tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstdint>
#include <cstdio>
#include <map>
#include <string>
#include <vector>

/**
 *  Handler that keeps track of the filedescriptors
 */
class Handler : public AMQP::TcpHandler
{
public:
    std::map<int, int> fds;
    bool detached = false;

    virtual void monitor(AMQP::TcpConnection *connection, int fd, int flags) override
    {
        if (flags) fds[fd] = flags; else fds.erase(fd);
    }
    virtual void onDetached(AMQP::TcpConnection *connection) override { detached = true; }
};

/**
 *  Let the connection process the events that come in within the timeout
 *  @param  connection
 *  @param  handler
 *  @param  timeout     in milliseconds
 */
static void pump(AMQP::TcpConnection &connection, Handler &handler, int timeout)
{
    std::vector<pollfd> polls;
    for (auto &fd : handler.fds) polls.push_back({ fd.first, (short)((fd.second & AMQP::readable ? POLLIN : 0) | (fd.second & AMQP::writable ? POLLOUT : 0)), 0 });
    if (poll(polls.data(), polls.size(), timeout) <= 0) return;
    for (auto &p : polls)
    {
        int flags = (p.revents & (POLLIN | POLLHUP | POLLERR) ? AMQP::readable : 0) | (p.revents & POLLOUT ? AMQP::writable : 0);
        if (flags) connection.process(p.fd, flags);
    }
}

/**
 *  Helpers to write and read big-endian numbers
 */
static void add(std::string &s, uint64_t value, int bytes) { while (bytes--) s += (char)(value >> (8 * bytes)); }
static uint64_t get(const std::string &s, size_t pos, int bytes)
{
    uint64_t value = 0;
    while (bytes--) value = value << 8 | (uint8_t)s[pos++];
    return value;
}

/**
 *  Add a method frame
 */
static void method(std::string &data, uint16_t channel, uint16_t cls, uint16_t id, const std::string &args)
{
    add(data, 1, 1); add(data, channel, 2); add(data, args.size() + 4, 4);
    add(data, cls, 2); add(data, id, 2); data += args; data += (char)0xCE;
}

/**
 *  The basic.ack, basic.nack and basic.reject frames that the client sent, as "name tag bits"
 *  @param  data        what the client sent, starting with the protocol header
 *  @return std::string
 */
static std::string acks(const std::string &data)
{
    std::string result;
    for (size_t pos = 8; pos + 12 <= data.size(); pos += 8 + get(data, pos + 3, 4))
    {
        if (data[pos] != 1 || get(data, pos + 7, 2) != 60) continue;
        uint64_t id = get(data, pos + 9, 2);
        const char *name = id == 80 ? "ack" : id == 90 ? "reject" : id == 120 ? "nack" : nullptr;
        if (name) result += std::string(name) + " " + std::to_string(get(data, pos + 11, 8)) + " " + std::to_string(get(data, pos + 19, 1)) + "\n";
    }
    return result;
}

/**
 *  Main procedure
 *  @return int
 */
int main()
{
    // the client may write to a socket that the server closed
    signal(SIGPIPE, SIG_IGN);

    // listening socket on an ephemeral loopback port
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof(address);
    if (bind(listener, (sockaddr *)&address, length) != 0 || listen(listener, 1) != 0) { perror("listen"); return 1; }
    getsockname(listener, (sockaddr *)&address, &length);

    // once the channel is open: nack, ack, reject, nack with requeue, ack
    Handler handler;
    AMQP::TcpConnection connection(&handler, AMQP::Address("amqp://guest:guest@127.0.0.1:" + std::to_string(ntohs(address.sin_port)) + "/"));
    AMQP::TcpChannel channel(&connection);
    channel.onReady([&]() {
        channel.reject(2, AMQP::multiple);
        channel.ack(3);
        channel.reject(4);
        channel.reject(6, AMQP::multiple | AMQP::requeue);
        channel.ack(7, AMQP::multiple);
    });

    // the server side of the handshake: connection.start, connection.tune, connection.open-ok, channel.open-ok
    std::string handshake, args;
    add(args, 0, 1); add(args, 9, 1); add(args, 0, 4); add(args, 5, 4); args += "PLAIN"; add(args, 5, 4); args += "en_US";
    method(handshake, 0, 10, 10, args);
    args.clear(); add(args, 0, 2); add(args, 4096, 4); add(args, 0, 2);
    method(handshake, 0, 10, 30, args);
    method(handshake, 0, 10, 41, std::string(1, 0));
    method(handshake, 1, 20, 11, std::string(4, 0));

    int server = accept(listener, nullptr, nullptr);
    fcntl(server, F_SETFL, O_NONBLOCK);
    if (write(server, handshake.data(), handshake.size()) != (ssize_t)handshake.size()) { perror("write"); return 1; }

    // collect what the client sends until all five frames are there, or nothing comes in for a second
    const std::string expected = "nack 2 1\nack 3 0\nreject 4 0\nnack 6 3\nack 7 1\n";
    std::string received;
    char buffer[4096];
    for (int idle = 0; idle < 100 && acks(received) != expected; )
    {
        pump(connection, handler, 10);
        auto result = read(server, buffer, sizeof(buffer));
        if (result > 0) { received.append(buffer, result); idle = 0; } else idle++;
    }

    // close, the client should detach
    shutdown(server, SHUT_WR);
    for (int i = 0; i < 100 && !handler.detached; ++i) pump(connection, handler, 10);
    close(server);
    close(listener);

    bool ok = acks(received) == expected && handler.detached;
    if (!ok) fprintf(stderr, "expected:\n%sreceived:\n%sdetached: %d\n", expected.c_str(), acks(received).c_str(), handler.detached);
    printf("%s\n", ok ? "OK" : "FAILED");
    return ok ? 0 : 1;
}
