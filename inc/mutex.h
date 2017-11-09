//-< MUTEX.H >-------------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Inter-thread synchronization mutex primitive
//-------------------------------------------------------------------*--------*

#ifndef __MUTEX_H__
#define __MUTEX_H__

#include <pthread.h>
#include "debug.h"

class Event;

class Mutex { 
    friend class Event;
    friend class Semaphorex;
  protected:
    pthread_mutex_t mutex;

  public:
    Mutex() {
        int rc = pthread_mutex_init(&mutex, NULL);
        assert(rc == 0);
    }

    ~Mutex() {
        pthread_mutex_destroy(&mutex);
    }

    void lock() {
        int rc = pthread_mutex_lock(&mutex);
        assert(rc == 0);
    }

    void unlock() {
        int rc = pthread_mutex_unlock(&mutex);
        assert(rc == 0);
    }

    void* operator new(size_t size, void* addr) { 
        return addr;
    }
};
    
class CriticalSection {
    Mutex& mutex;

  public:
    CriticalSection(Mutex& m) : mutex(m) { 
        m.lock();
    }

    ~CriticalSection() {
        mutex.unlock();
    }   
};

#endif
