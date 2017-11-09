//-< UNISOCK.CPP >---------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Unix socket implementation
//-------------------------------------------------------------------*--------*

#include "unisock.h"

#include <sys/ioctl.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/utsname.h>
#include <poll.h>
#ifndef HPUX11
#include <sys/select.h>
#endif
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#if defined(_AIX)
#include <strings.h>
#endif  /* _AIX */
#include <stddef.h>
#include "debug.h"
#include <errno.h>

extern "C" {
#include <netdb.h>
}


#include <signal.h>

const int MAX_HOST_NAME = 256;
const int GETHOSTBYNAME_BUF_SIZE = 1024;
const int SOCK_BUF_SIZE = 8*1024*1024;
const int MSEC = 1000;

#define USE_LINGER_TIME
#define SELECT_BEFORE_ACCEPT
#define NON_BLOCKING_ACCEPT
#define USE_NODELAY
#define HAVE_GETHOSTBYNAME_R

char* UnixSocket::unix_socket_dir = "/tmp/";

class UnixSocketLibrary {
  public:
    UnixSocketLibrary() {
        static struct sigaction sigpipe_ignore;
        sigpipe_ignore.sa_handler = SIG_IGN;
        sigaction(SIGPIPE, &sigpipe_ignore, NULL);
    }
};

static UnixSocketLibrary unisock_lib;

bool UnixSocket::open(int listen_queue_size)
{
    char hostname[MAX_HOST_NAME];
    unsigned short port;
    char* p;

    assert(address != NULL);

    if ((p = strchr(address, ':')) == NULL
        || unsigned(p - address) >= sizeof(hostname)
        || sscanf(p+1, "%hu", &port) != 1)
    {
        errcode = bad_address;
        return false;
    }
    memcpy(hostname, address, p - address);
    hostname[p - address] = '\0';

    create_file = false;
    union {
        sockaddr    sock;
        sockaddr_in sock_inet;
        char        name[MAX_HOST_NAME];
    } u;
    int len;

    if (domain == sock_local_domain) {
        u.sock.sa_family = AF_UNIX;

        assert(strlen(unix_socket_dir) + strlen(address)
               < MAX_HOST_NAME - offsetof(sockaddr,sa_data));

        len = offsetof(sockaddr,sa_data) +
            sprintf(u.sock.sa_data, "%s%s.%u", unix_socket_dir, hostname, port);

        unlink(u.sock.sa_data); // remove file if existed
        create_file = true;
    } else {
        u.sock_inet.sin_family = AF_INET;
        struct utsname local_host;
        uname(&local_host);

//        if (*hostname && strcmp(hostname, "localhost") != 0) 
        if (*hostname && strcmp(hostname, "localhost") != 0 && strcmp(hostname, local_host.nodename) != 0) 
        {
            struct hostent* hp;
#if defined(HAVE_GETHOSTBYNAME_R)
            struct hostent ent;  // entry in hosts table
            char buf[GETHOSTBYNAME_BUF_SIZE];
            int h_err;
#if defined(__sun)
            if ((hp = gethostbyname_r(hostname, &ent, buf, sizeof buf, &h_err)) == NULL
#else
            if (gethostbyname_r(hostname, &ent, buf, sizeof buf, &hp, &h_err) != 0
                || hp == NULL
#endif
                || hp->h_addrtype != AF_INET)
#else
            if ((hp = gethostbyname(hostname)) == NULL || hp->h_addrtype != AF_INET) 
#endif
            {
                errcode = bad_address;
                return false;
            }
            memcpy(&u.sock_inet.sin_addr, hp->h_addr,
                   sizeof u.sock_inet.sin_addr);
        } else {
            u.sock_inet.sin_addr.s_addr = htonl(INADDR_ANY);
        }
        u.sock_inet.sin_port = htons(port);
        len = sizeof(sockaddr_in);
    }
    if ((fd = socket(u.sock.sa_family, SOCK_STREAM, 0)) < 0) {
        errcode = errno;
        return false;
    }
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&on, sizeof on);

    if (bind(fd, &u.sock, len) < 0) {
        errcode = errno;
        ::close(fd);
        return false;
    }
    if (listen(fd, listen_queue_size) < 0) {
        errcode = errno;
        ::close(fd);
        return false;
    }
#ifdef NON_BLOCKING_ACCEPT
    if (fcntl(fd, F_SETFL, O_NONBLOCK) != 0) { 
        errcode = errno;
        ::close(fd);
        return false; 
    }
#endif
    errcode = ok;
    state = ss_open;
    return true;
}

unsigned UnixSocket::get_peer()
{
    if (state != ss_open) {
        errcode = not_opened;
        return 0;
    }
    struct sockaddr_in insock;
#if defined(__linux__) || (defined(__FreeBSD__) && __FreeBSD__ > 3) || defined(_AIX43) || defined(__OpenBSD__) || defined(HPUX11) || defined(_SOCKLEN_T)
    socklen_t len = sizeof(insock);
#elif defined(_AIX41)
    size_t len = sizeof(insock);
#else
    int len = sizeof(insock);
#endif
    if (getpeername(fd, (struct sockaddr*)&insock, &len) != 0) {
        errcode = errno;
        return 0;
    }
    return htonl(insock.sin_addr.s_addr);
}

bool  UnixSocket::is_ok()
{
    return errcode == ok;
}

void UnixSocket::get_error_text(char* buf, size_t buf_size)
{
    char* msg;
    switch(errcode) {
      case ok:
        msg = "ok";
        break;
      case not_opened:
        msg = "socket not opened";
        break;
      case bad_address:
        msg = "bad address";
        break;
      case connection_failed:
        msg = "exceed limit of attempts of connection to server";
        break;
      case broken_pipe:
        msg = "connection is broken";
        break;
      case invalid_access_mode:
        msg = "invalid access mode";
        break;
      default:
        msg = strerror(errcode);
        break;
    }
    strncpy(buf, msg, buf_size-1);
    buf[buf_size-1] = '\0';
}

Socket* UnixSocket::accept()
{
    int rc, s;

    while (true) { 
        if (state != ss_open) {
            errcode = not_opened;
            return NULL;
        }
    
#ifdef SELECT_BEFORE_ACCEPT
        fd_set events;
        FD_ZERO(&events);
        FD_SET(fd, &events);
        while ((rc = select(fd+1, &events, NULL, NULL, NULL)) < 0 && errno == EINTR);
        if (rc < 0) {
            return NULL;
        }
#endif
        while((s = ::accept(fd, NULL, NULL )) < 0 && errno == EINTR);

        if (s < 0) {
            if (errno == EAGAIN) { 
                continue;
            }
            errcode = errno;
            return NULL;
        } else if (state != ss_open) {
            errcode = not_opened;
            return NULL;
        } else {
#ifdef USE_NODELAY
            if (domain == sock_global_domain) {
                int enabled = 1;
                if (setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (char*)&enabled,
                               sizeof enabled) != 0)
                {
                    errcode = errno;
                    ::close(s);
                    return NULL;
                }
            }
#endif
#ifdef USE_LINGER_TIME
            static struct linger l = {1, SOCK_LINGER_TIME};
            if (setsockopt(s, SOL_SOCKET, SO_LINGER, (char*)&l, sizeof l) != 0) {
                errcode = invalid_access_mode;
                ::close(s);
                return NULL;
            }
#endif
            if (SOCK_BUF_SIZE > 0) { 
                setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char*)&SOCK_BUF_SIZE, sizeof(int));
                setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char*)&SOCK_BUF_SIZE, sizeof(int));
            }
            errcode = ok;
            return new UnixSocket(s);
        }
    }
}

bool UnixSocket::cancel_accept()
{
    bool result = close();
    // Wakeup listener
    delete Socket::connect(address, domain, 1, 0);
    return result;
}


bool UnixSocket::connect(int max_attempts, time_t timeout)
{
    int   rc;
    char* p;
    struct utsname local_host;
    char hostname[MAX_HOST_NAME];
    unsigned short port;

    assert(address != NULL);

    if ((p = strchr(address, ':')) == NULL
        || unsigned(p - address) >= sizeof(hostname)
        || sscanf(p+1, "%hu", &port) != 1)
    {
        errcode = bad_address;
        return false;
    }
    memcpy(hostname, address, p - address);
    hostname[p - address] = '\0';

    create_file = false;
    uname(&local_host);

    if (domain == sock_local_domain || (domain == sock_any_domain &&
        strcmp(hostname, "localhost") == 0))
    {
        // connect UNIX socket
        union {
            sockaddr sock;
            char     name[MAX_HOST_NAME];
        } u;
        u.sock.sa_family = AF_UNIX;

        assert(strlen(unix_socket_dir) + strlen(address)
               < MAX_HOST_NAME - offsetof(sockaddr,sa_data));

        int len = offsetof(sockaddr,sa_data) +
            sprintf(u.sock.sa_data, "%s%s.%u", unix_socket_dir, hostname, port);

        while (true) {
            if ((fd = socket(u.sock.sa_family, SOCK_STREAM, 0)) < 0) {
                errcode = errno;
                return false;
            }
            do {
                rc = ::connect(fd, &u.sock, len);
            } while (rc < 0 && errno == EINTR);
            if (rc < 0) {
                errcode = errno;
                ::close(fd);
                if (errcode == ENOENT || errcode == ECONNREFUSED) {
                    if (--max_attempts > 0) {
                        sleep(timeout);
                    } else {
                        break;
                    }
                } else {
                    return false;
                }
            } else {
                errcode = ok;
                state = ss_open;
                return true;
            }
        }
    } else {
        sockaddr_in sock_inet;
        struct hostent* hp;
#if defined(HAVE_GETHOSTBYNAME_R)
        struct hostent ent;  // entry in hosts table
        char buf[GETHOSTBYNAME_BUF_SIZE];
        int h_err;
#if defined(__sun)
        if ((hp = gethostbyname_r(hostname, &ent, buf, sizeof buf, &h_err)) == NULL
#else
        if (gethostbyname_r(hostname, &ent, buf, sizeof buf, &hp, &h_err) != 0
            || hp == NULL
#endif
            || hp->h_addrtype != AF_INET)
#else
        if ((hp = gethostbyname(hostname)) == NULL || hp->h_addrtype != AF_INET) 
#endif
        {
            errcode = bad_address;
            return false;
        }
        sock_inet.sin_family = AF_INET;
        sock_inet.sin_port = htons(port);

        while (true) {
            for (int i = 0; hp->h_addr_list[i] != NULL; i++) {
                memcpy(&sock_inet.sin_addr, hp->h_addr_list[i],
                       sizeof sock_inet.sin_addr);
                if ((fd = socket(sock_inet.sin_family, SOCK_STREAM, 0)) < 0) {
                    errcode = errno;
                    return false;
                }
                do {
                    rc = ::connect(fd,(sockaddr*)&sock_inet,sizeof(sock_inet));
                } while (rc < 0 && errno == EINTR);

                if (rc < 0) {
                    errcode = errno;
                    ::close(fd);
                    if (errcode != ENOENT && errcode != ECONNREFUSED) {
                        return false;
                    }
                } else {
#ifdef USE_NODELAY
                    int enabled = 1;
                    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
                                   (char*)&enabled, sizeof enabled) != 0)
                    {
                        errcode = errno;
                        ::close(fd);
                        return false;
                    }
#endif
                    if (SOCK_BUF_SIZE > 0) { 
                        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char*)&SOCK_BUF_SIZE, sizeof(int));
                        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char*)&SOCK_BUF_SIZE, sizeof(int));
                    }
                    errcode = ok;
                    state = ss_open;
                    return true;
                }
            }
            if (--max_attempts > 0) {
                sleep(timeout);
            } else {
                break;
            }
        }
    }
    errcode = connection_failed;
    return false;
}

int UnixSocket::receive(void* data, size_t size)
{
    int rc = ::read(fd, data, size);
    if (rc > 0) { 
        inputTraffic += size;
        inputMsgCount += 1;
    }
    return rc;
}

int UnixSocket::send(void const* data, size_t size)
{
    int rc = ::write(fd, data, size);
    if (rc > 0) { 
        outputTraffic += size;
        outputMsgCount += 1;
    }
    return rc;
}

int UnixSocket::read(void* buf, size_t min_size, size_t max_size,
                      time_t timeout)
{
    size_t size = 0;
    time_t start = 0;
    if (state != ss_open) {
        errcode = not_opened;
        return -1;
    }
    if (timeout != SOCK_WAIT_FOREVER) {
        start = time(NULL);
    }
    do {
        ssize_t rc;
        if (timeout != SOCK_WAIT_FOREVER) {
            struct pollfd fds;
            fds.events = POLLIN | POLLRDNORM | POLLRDBAND;
            fds.fd = fd;
            while ((rc = poll(&fds, 1, timeout*MSEC)) < 0 && errno == EINTR);
            if (rc < 0) {
                errcode = errno;
                return -1;
            }
            if (rc == 0 || (fds.revents & (POLLIN | POLLRDNORM | POLLRDBAND)) == 0) {
                return size;
            }
            time_t now = time(NULL);
            timeout = start + timeout >= now ? start + timeout - now : 0;
            start = now;
        }
        while ((rc = receive((char*)buf + size, max_size - size)) < 0
               && errno == EINTR);
        if (rc < 0) {
            errcode = errno;
            return -1;
        } else if (rc == 0) {
            errcode = broken_pipe;
            return -1;
        } else {
            size += rc;
        }
    } while (size < min_size);

    return (int)size;
}


int UnixSocket::write(void const* buf, size_t size, time_t timeout)
{
    size_t written = 0;
    time_t start = 0;
    if (state != ss_open) {
        errcode = not_opened;
        return -1;
    }
    if (timeout != SOCK_WAIT_FOREVER) {
        start = time(NULL);
    }
    do {
        ssize_t rc;
        if (timeout != SOCK_WAIT_FOREVER) {
            struct pollfd fds;
            fds.events = POLLOUT | POLLWRNORM | POLLWRBAND;
            fds.fd = fd;
            while ((rc = poll(&fds, 1, timeout*MSEC)) < 0 && errno == EINTR);
            if (rc < 0) {
                errcode = errno;
                return -1;
            }
            if (rc == 0 || (fds.revents & (POLLOUT | POLLWRNORM | POLLWRBAND)) == 0) {
                return written;
            }
            time_t now = time(NULL);
            timeout = start + timeout >= now ? start + timeout - now : 0;
            start = now;
        }
        while ((rc = send((char*)buf + written, size - written)) < 0
               && errno == EINTR);
        if (rc < 0) {
            errcode = errno;
            return -1;
        } else if (rc == 0) {
            errcode = broken_pipe;
            return -1;
        } else {
            written += rc;
        }
    } while (written < size);

    return written;
}

bool UnixSocket::write(void const* buf, size_t size)
{
    if (state != ss_open) {
        errcode = not_opened;
        return false;
    }
    if (size == 0) {
    	return true;
    }
    do {
        ssize_t rc;
        while ((rc = send(buf, size)) < 0 && errno == EINTR);
        if (rc < 0) {
            errcode = errno;
            return false;
        } else if (rc == 0) {
            errcode = broken_pipe;
            return false;
        } else {
            buf = (char*)buf + rc;
            size -= rc;
        }
    } while (size != 0);

    //
    // errcode is not assigned 'ok' value because write function
    // can be called in parallel with other socket operations, so
    // we want to preserve old error code here.
    //
    return true;
}

bool UnixSocket::close()
{
    if (state != ss_close) {
        state = ss_close;
        if (::close(fd) == 0) {
            errcode = ok;
            return true;
        } else {
            errcode = errno;
            return false;
        }
    }
    errcode = ok;
    return true;
}

bool UnixSocket::shutdown()
{
    if (state == ss_open) {
        state = ss_shutdown;
        int rc = ::shutdown(fd, 2);
        if (rc != 0) {
            errcode = errno;
            return false;
        }
    }
    return true;
}

UnixSocket::~UnixSocket()
{
    close();
    if (create_file) {
        char name[MAX_HOST_NAME];
        char* p = strrchr(address, ':');
        sprintf(name, "%s%.*s.%s", unix_socket_dir, (int)(p - address), address, p+1);
        unlink(name);
    }
    delete[] address;
}

UnixSocket::UnixSocket(const char* addr, socket_domain domain)
{
    address = new char[strlen(addr)+1];
    strcpy(address, addr);
    this->domain = domain;
    create_file = false;
    errcode = ok;
}

UnixSocket::UnixSocket(int new_fd)
{
    fd = new_fd;
    address = NULL;
    create_file = false;
    state = ss_open;
    errcode = ok;
}

int UnixSocket::get_handle() 
{
    return fd;
}

Socket* Socket::create_local(char const* address, int listen_queue_size)
{
    UnixSocket* sock = new UnixSocket(address, sock_local_domain);
    sock->open(listen_queue_size);
    return sock;
}

Socket* Socket::create_global(char const* address, int listen_queue_size)
{
    UnixSocket* sock = new UnixSocket(address, sock_global_domain);
    sock->open(listen_queue_size);
    return sock;
}

Socket* Socket::connect(char const* address,
                            socket_domain domain,
                            int max_attempts,
                            time_t timeout)
{
    UnixSocket* sock = new UnixSocket(address, domain);
    sock->connect(max_attempts, timeout);
    return sock;
}

