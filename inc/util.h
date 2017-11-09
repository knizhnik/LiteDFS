//-< UTIL.H >--------------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed Dile System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Varyous support stuff
//-------------------------------------------------------------------*--------*

#ifndef __UTIL_H__
#define __UTIL_H__

#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <pthread.h>

#include "debug.h"
#include "types.h"

class VFileHandle;
class Socket;

const size_t MAX_CFG_FILE_LINE_LENGTH = 256;
const size_t MAX_TIME_LENGTH = 128;
const time_t SOCKET_WRITE_TIMEOUT = 600; /* ten minutes */


#define ALIGN(x,y) (((x) + (y) - 1) & ~((y) - 1))

const char*  const DATE_FORMAT = "%c";

extern int strtrim(char* str);

extern size_t ignoreCaseStringHashFunction(char const* str); 
extern size_t ignoreCaseStringHashFunction(char const* str, size_t size); 

inline size_t stringHashFunction(char const* str) 
{ 
    size_t h;
    for (h = 0; *str != 0; h = h*31 + (unsigned char)*str++);
    return h;
}

extern crc_t calculateCRC(char const* content, size_t contentLength);

inline size_t stringHashFunction(char const* str, size_t size) 
{ 
    size_t h;
    for (h = 0; size != 0; size -= 1, h = h*31 + (unsigned char)*str++);
    return h;
}

extern char* getSize(size_t size, char* buf, size_t bufSize);

extern char* getTime(char* buf, size_t bufSize, char const* format = DATE_FORMAT);

extern char* getTime(time_t time, char* buf, size_t bufSize, char const* format = DATE_FORMAT, bool useGmt = false);

extern bool loadConfigurationFile(char const* cfgFilePath, int nParams, char const* format, ...);

extern double getCurrentTime();

extern void diskCopy(int dstFd, fsize_t dstOffs, int srcFd, fsize_t srcOffs, fsize_t size);

extern void diskWrite(int fd, void* data, size_t size, fsize_t offs, bool align = true);

extern void diskRead(int fd, void* data, size_t size, fsize_t offs);

extern bool socketWrite(Socket* s, void* data, size_t size, time_t timeout = SOCKET_WRITE_TIMEOUT);

extern bool socketWriteFromDisk(Socket* s, VFileHandle* fh, fsize_t offs, size_t size, time_t timeout = SOCKET_WRITE_TIMEOUT);

extern bool socketReadToDisk(Socket* s, VFileHandle* fh, fsize_t offs);
extern bool socketReadToDisk(Socket* s, VFileHandle* fh, fsize_t offs, size_t& size);

extern int  forceRead(void* ptr, size_t size, bool lock = false);

extern void getMemoryStatistic(void* addr, size_t size, size_t& ram, size_t& disk, size_t& regions);

extern void getAbsoluteTime(struct timespec& abs_ts, time_t deltaMsec);
     
class DnmBuffer { 
    char* body;

  public:
    operator char*() { 
        return body;
    }

    DnmBuffer(size_t size) { 
        body = new char[size];
    }
    ~DnmBuffer() {
        delete[] body;
    }
};

class PerformanceCounter { 
  public:
    size_t total;
    size_t current;
    time_t begin;
    
    size_t average(time_t now) { 
        update(now);
        return now == begin ? 0 : total*period/(now - begin);
    }

    PerformanceCounter(time_t period = 0) {
        start(period);
    }
        
    void start(time_t period) { 
        this->period = period;
        measure = begin = time(NULL);
        total = current = counter = 0;
    }
        
    void inc(time_t now) { 
        update(now);
        total += 1;
        counter += 1;
    }

  protected:

    void update(time_t now) { 
        if (measure + period < now) { 
            current = counter;
            counter = 0;
            measure = begin + (now - begin)/period*period;
        }
    }

    time_t measure;
    time_t period;
    size_t counter;
};

class PerformanceProfileCounter : public PerformanceCounter 
{
  public:
    double currMaxTime;
    double maxTime;
    double totalTime;
    double time;

    double totalAveTime() { 
        return total == 0 ? 0 : totalTime / total;
    }

    double currAveTime() { 
        return current == 0 ? 0 : time / current;
    }
    
    void inc(double now, double delta) { 
        if (measure + period < now) { 
            currMaxTime = currMax;
            if (currMax > maxTime) { 
                maxTime = currMax;
            }
            time = currTime;
            currMax = 0;
            currTime = 0;
        }
        PerformanceCounter::inc((time_t)now);
        currTime += delta;
        totalTime += delta;
        if (delta > currMax) { 
            currMax = delta;
        }
    }
    PerformanceProfileCounter(time_t period) 
    : PerformanceCounter(period)
    {
        currTime = 0;
        currMax = 0;
        currMaxTime = 0;
        maxTime = 0;
        totalTime = 0;
        time = 0;
    }

  private:
    double currTime;
    double currMax;
};

template<class T>
struct array {    
    T*     body;
    size_t size;
    bool   deleteBody;

    T& operator[](int index) {
        assert((size_t)index < size);
        return body[index];
    }

    void operator=(array<T> const& src) { 
        body = src.body;
        size = src.size;
        deleteBody = src.deleteBody;
        ((array<T>*)&src)->deleteBody = false;
    }

    array(T* ptr, size_t len, bool del = true) : body(ptr), size(len), deleteBody(del) {}

    array() : body(NULL), size(0), deleteBody(false) {}

    ~array() { 
        if (deleteBody) {
            delete[] body;
        }
    }
};


template<class T>
class stack : public array<T> {    
  private:
    size_t reserved;

  public:
    size_t allocated() { 
        return reserved;
    }

    void push(T const& elem) { 
        assert(array<T>::deleteBody);
        if (array<T>::size == reserved) { 
            T* newBody = new T[reserved*2];
            for (size_t i = 0, n = reserved; i < n; i++) {
                newBody[i] = array<T>::body[i];
            }
            delete[] array<T>::body;
            array<T>::body = newBody;
            reserved *= 2;
        }
        array<T>::body[array<T>::size++] = elem;
    }

    T& pop() { 
        assert(array<T>::size > 0);
        return array<T>::body[--array<T>::size];
    }

    T& top() { 
        assert(array<T>::size > 0);
        return array<T>::body[array<T>::size-1];
    }

    void reset() {
        if (array<T>::deleteBody) { 
            delete[] array<T>::body;
        }
        reserved = 16;
        array<T>::deleteBody = true;
        array<T>::body = new T[reserved];
        array<T>::size = 0;
    }

    stack() {
        reset();
    }
};
    
extern FILE* openLog(char const* fileName, char const* format = "w");
extern FILE* openLocalConfig(char const* fileName, bool optional = false);
extern FILE* openGlobalConfig(char const* fileName, bool optional = false);

typedef void* (*thread_proc_t)(void*);
void createThread(pthread_t& thread, thread_proc_t proc, void* arg);

#endif
