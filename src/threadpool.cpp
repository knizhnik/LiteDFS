//-< THREADPOOL.CPP >------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Pool of threads
//-------------------------------------------------------------------*--------*

#include "debug.h"
#include "util.h"
#include "threadpool.h"

class Thread 
{ 
  public:
    pthread_t       thread;
    pthread_mutex_t mutex;
    pthread_cond_t  event;
    Thread*         next;
    void*           param;
    int             go;
    int             closed;
    void*           specificData;
    ThreadPool*     pool;

    static void* threadFunc(void* arg) 
    { 
        ((Thread*)arg)->run();
        return NULL;  
    }

    void start(void* param) 
    { 
        int rc = pthread_mutex_lock(&mutex);
        assert(rc == 0);
        this->param = param;
        go = true;
        rc = pthread_cond_signal(&event);        
        assert(rc == 0);
        rc = pthread_mutex_unlock(&mutex);
        assert(rc == 0);
    }

    void run() 
    { 
        int rc = pthread_mutex_lock(&mutex);
        assert(rc == 0);
        while (true) { 
            while (!go) { 
                rc = pthread_cond_wait(&event, &mutex);
                assert(rc == 0);
            }
            go = false;
            if (closed) { 
                break;
            }
            pool->run(param, specificData);
            pool->done(this);
        }
        rc = pthread_mutex_unlock(&mutex);
        assert(rc == 0);
    }
    
    Thread(ThreadPool* pool) 
    {
        this->pool = pool;
        go = false;
        closed = false;
        specificData = pool->getThreadSpecificData();
        int rc = pthread_mutex_init(&mutex, NULL); 
        assert(rc == 0);
        rc = pthread_cond_init(&event, NULL);
        assert(rc == 0);
        
        createThread(thread, &threadFunc, this);
    }
    
    void stop() 
    { 
        int rc = pthread_mutex_lock(&mutex);
        assert(rc == 0);
        closed = true;
        go = true;
        rc = pthread_cond_signal(&event);        
        assert(rc == 0);
        rc = pthread_mutex_unlock(&mutex);
        assert(rc == 0);
        void* result;
        rc = pthread_join(thread, &result);
        assert(rc == 0);
    }

    ~Thread() 
    { 
        int rc = pthread_mutex_destroy(&mutex);
        assert(rc == 0);
        rc = pthread_cond_destroy(&event);
        assert(rc == 0);
        pool->releaseThreadSpecificData(specificData);
    }
};



void ThreadPool::done(Thread* thread)
{
    int rc = pthread_mutex_lock(&mutex);
    assert(rc == 0);
    nActiveThreads -= 1;
    nIdleThreads += 1;    
    rc = pthread_cond_signal(&event);        
    assert(rc == 0);
    thread->next = idle;
    idle = thread;
    rc = pthread_mutex_unlock(&mutex);
    assert(rc == 0);
}

ThreadPool::ThreadPool()
{
    nIdleThreads = 0;
    nActiveThreads = 0;
    idle = NULL;
    int rc = pthread_mutex_init(&mutex, NULL); 
    assert(rc == 0);
    rc = pthread_cond_init(&event, NULL);
    assert(rc == 0);
}

void ThreadPool::start(int nThreads)
{
    nIdleThreads = nThreads;
    nActiveThreads = 0;
    idle = NULL;
    for (int i = nIdleThreads; --i >= 0;) { 
        Thread* thread = new Thread(this);
        thread->next = idle;
        idle = thread;
    }
}


void ThreadPool::stop() 
{
    suspend();
    Thread* t = idle, *next;
    while (t != NULL) { 
        next = t->next;
        t->stop();
        delete t;
        t = next;
    }
    idle = NULL;
}

ThreadPool::~ThreadPool()
{    
    stop();
}
    
void ThreadPool::schedule(void* param)
{
    int rc = pthread_mutex_lock(&mutex);
    assert(rc == 0);

    while (nIdleThreads == 0) { 
        rc = pthread_cond_wait(&event, &mutex);
        assert(rc == 0);   
    }        
    nIdleThreads -= 1;
    nActiveThreads += 1;

    Thread* t = idle;
    idle = t->next;

    rc = pthread_mutex_unlock(&mutex);
    assert(rc == 0);   

    t->start(param);
}
    
void ThreadPool::suspend()
{
    int rc = pthread_mutex_lock(&mutex);
    assert(rc == 0);
    while (nActiveThreads != 0) { 
        rc = pthread_cond_wait(&event, &mutex);
        assert(rc == 0);   
    }        
    rc = pthread_mutex_unlock(&mutex);
    assert(rc == 0);   
}
