//-< THREADPOOL.H >--------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Pool of threads
//-------------------------------------------------------------------*--------*

#ifndef __THREAD_POOL_H__
#define __THREAD_POOL_H__

#include <pthread.h>

class Thread;

class ThreadPool 
{ 
    friend class Thread;
    Thread*  idle;
    int      nActiveThreads;
    int      nIdleThreads;
    pthread_mutex_t mutex;
    pthread_cond_t  event;

    void done(Thread* thread);

  protected:
    virtual void run(void* param, void* threadSpecificData) = 0;
    virtual void* getThreadSpecificData() { 
        return NULL;
    }
    virtual void releaseThreadSpecificData(void*) {}


  public:
    ThreadPool();
    virtual ~ThreadPool();

    int getNumberOfActiveThreads() const { 
        return nActiveThreads;
    }

    void start(int nThreads);
    void stop();

    void schedule(void* param);
    void suspend();
};

#endif    
 
