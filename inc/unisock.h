//-< UNISOCK.H >-----------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Unix socket implementation
//-------------------------------------------------------------------*--------*

#ifndef __UNISOCKET_H__
#define __UNISOCKET_H__

#include "socketio.h"

class UnixSocket : public Socket {
  protected:
    int           fd;
    socket_domain domain;      // Unix domain or INET socket
    bool          create_file; // Unix domain sockets use files for connection

  public:
    //
    // Directory for Unix Domain socket files. This directory should be
    // either empty or be terminated with "/". Default value is "/tmp/"
    //
    static char* unix_socket_dir;

    bool      open(int listen_queue_size);
    bool      connect(int max_attempts, time_t timeout);

    int       read(void* buf, size_t min_size, size_t max_size,
                   time_t timeout);
    bool      write(void const* buf, size_t size);
    int       write(void const* buf, size_t size, time_t timeout);

    bool      is_ok();

    virtual bool shutdown();
    virtual bool close();

    unsigned  get_peer();
    void      get_error_text(char* buf, size_t buf_size);

    int       get_handle();

    Socket*   accept();
    bool      cancel_accept();

    UnixSocket(const char* address, socket_domain domain);
    UnixSocket(int new_fd);

    ~UnixSocket();

  protected:
    virtual int receive(void* data, size_t size);
    virtual int send(void const* data, size_t size);
};

#endif





