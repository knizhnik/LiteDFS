//-< EVENT.H >-------------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Inter-thread synchronization event primitive
//-------------------------------------------------------------------*--------*

#ifndef __EVENT_H__
#define __EVENT_H__

#include <pthread.h>
#include "debug.h"
#include <errno.h>
#include "mutex.h"
#include "util.h"

class Event { 
  public:
    Event(Mutex& mutex) : guard(mutex) {
        int rc = pthread_cond_init(&cond, NULL);
        assert(rc == 0);
    }        
        
    void signal() { 
        int rc = pthread_cond_broadcast(&cond);        
        assert(rc == 0);
    }
    
    void wait() { 
        int rc = pthread_cond_wait(&cond, &guard.mutex);
        assert(rc == 0);   
    }

    bool wait(struct timespec& expiration) { 
        int rc = pthread_cond_timedwait(&cond, &guard.mutex, &expiration);
        if (rc == ETIMEDOUT) {
            return false;
        }
        assert(rc == 0);
        return true;
    }

    bool wait(time_t timeout) { 
        struct timespec expiration; 
        getAbsoluteTime(expiration, timeout);
        return wait(expiration);
    }

  private:
    Mutex&         guard;
    pthread_cond_t cond;
};

#endif
