//-< SOCKETIO.H >----------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Abstract socket interface
//-------------------------------------------------------------------*--------*

#ifndef __SOCKETIO_H__
#define __SOCKETIO_H__

#include <stddef.h>
#include <time.h>

const int DEFAULT_SOCK_CONNECT_MAX_ATTEMPTS = 100;
const int DEFAULT_SOCK_RECONNECT_TIMEOUT    = 1;
const int DEFAULT_SOCK_LISTEN_QUEUE_SIZE    = 100;
const int SOCK_LINGER_TIME                  = 10;
const time_t SOCK_WAIT_FOREVER              = (time_t)-1;
const int MAX_SOCKET_ERROR_LENGTH           = 256;

struct TrafficInfo { 
    size_t inputTraffic;
    size_t inputMsgCount;
    size_t outputTraffic;
    size_t outputMsgCount;
    size_t sendQueueSize;
    size_t sendQueueLength;

    void operator += (TrafficInfo const& t) { 
        inputTraffic += t.inputTraffic;
        inputMsgCount += t.inputMsgCount;
        outputTraffic += t.outputTraffic;
        outputMsgCount += t.outputMsgCount;
        sendQueueSize += t.sendQueueSize;
        sendQueueLength += t.sendQueueLength;
    }

    TrafficInfo() { 
        inputTraffic = 0;
        inputMsgCount = 0;
        outputTraffic = 0;
        outputMsgCount = 0;
        sendQueueSize = 0;
        sendQueueLength = 0;
    }
};


//
// Abstract socket interface
//
class Socket : public TrafficInfo {
  public:
    bool              read(void* buf, size_t size) {
        return read(buf, size, size) == (int)size;
    }
    virtual int       read(void* buf, size_t min_size, size_t max_size,
                           time_t timeout = SOCK_WAIT_FOREVER) = 0;
    virtual bool      write(void const* buf, size_t size) = 0;
    virtual int       write(void const* buf, size_t size, time_t timeout) = 0;

    virtual bool      is_ok() = 0;
    virtual void      get_error_text(char* buf, size_t buf_size) = 0;

    //
    // This method is called by server to accept client connection
    //
    virtual Socket*   accept() = 0;

    //
    // Cancel accept operation and close socket
    //
    virtual bool      cancel_accept() = 0;

    //
    // Shutdown socket: prohibit write and read operations on socket
    //
    virtual bool      shutdown() = 0;

    //
    // Close socket
    //
    virtual bool      close() = 0;

    //
    // Get socket peer IP address
    //
    virtual unsigned  get_peer() = 0;

    //
    // Get socket handle
    //
    virtual int       get_handle() = 0;

    //
    // Create client socket connected to local or global server socket
    //
    enum socket_domain {
        sock_any_domain,   // domain is chosen automatically
        sock_local_domain, // local domain (i.e. Unix domain socket)
        sock_global_domain // global domain (i.e. INET sockets)
    };

    static Socket*    connect(char const* address,
                              socket_domain domain = sock_any_domain,
                              int max_attempts = DEFAULT_SOCK_CONNECT_MAX_ATTEMPTS,
                              time_t timeout = DEFAULT_SOCK_RECONNECT_TIMEOUT);

    //
    // Create local domain socket
    //
    static Socket*    create_local(char const* address,
                                   int listen_queue_size =
                                   DEFAULT_SOCK_LISTEN_QUEUE_SIZE);

    //
    // Create global domain socket
    //
    static Socket*    create_global(char const* address,
                                    int listen_queue_size =
                                    DEFAULT_SOCK_LISTEN_QUEUE_SIZE);

    virtual ~Socket() {}
    Socket() { 
        state = ss_close; 
        used = 0;
    }
    enum error_codes {
        ok = 0,
        not_opened = -1,
        bad_address = -2,
        connection_failed = -3,
        broken_pipe = -4,
        invalid_access_mode = -5
    };

  public:
    int   errcode;     // error code of last failed operation
    int   used;        // socket access counter
    char* address;     // host address

  protected:
    enum { ss_open, ss_shutdown, ss_close } state;
};

#endif



