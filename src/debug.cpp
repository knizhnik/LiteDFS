//-< DEBUG.CPP >-----------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Debugging utilities
//-------------------------------------------------------------------*--------*

#include "util.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>

bool sigtermReceived;
bool signalCatched;
bool ignoreAssertFailure;

static void terminationHandler(int)
{
    sigtermReceived = true;
}

int assertFailure(char const* file, int line, char const* expr)
{
    FILE* f = openLog("crash.log", "a");
    char buf[64];
    fprintf(f, "%s:%d: assert %s failure in thread %ld of process %d at %s: \n", 
            file, line, expr, pthread_self(), getpid(), getTime(buf, sizeof buf));
    fclose(f);
    fprintf(stderr, "%s:%d: assert %s failure in thread %ld of process %d at %s: \n", 
            file, line, expr, pthread_self(), getpid(), getTime(buf, sizeof buf));
    signalCatched = true;
    while (!ignoreAssertFailure) { 
        sleep(10);
    }
    signalCatched = false;
    ignoreAssertFailure = false;
    return 1;
}


static void signalHandler(int sig) 
{
    static bool inside;
    if (!inside) { 
        inside = true;
        signalCatched = true;
        FILE* f = openLog("crash.log", "a");
        char buf[64];
        fprintf(f, "Thread %ld of process %d catch signal %d at %s\n", 
                pthread_self(), getpid(), sig, getTime(buf, sizeof buf));
        fclose(f);
    }
    while (true) { 
        pause();
    }
}

static void exitHandler(void)
{
    FILE* f = openLog("crash.log", "a");
    char buf[64];
    fprintf(f, "Process %d is terminated at %s\n", getpid(), getTime(buf, sizeof buf));
    fclose(f);
/*
    while (true) { 
        pause();
    }    
*/
}

void installSignalHandlers() 
{
    static struct sigaction sa;
    sa.sa_flags = SA_NOMASK;
    sa.sa_handler = &terminationHandler;
    sigaction(SIGTERM, &sa, NULL); 

    sa.sa_handler = &signalHandler;
    if (sigaction(SIGSEGV, &sa, NULL) < 0
        || sigaction(SIGBUS, &sa, NULL) < 0
        || sigaction(SIGABRT, &sa, NULL) < 0
        || sigaction(SIGFPE, &sa, NULL) < 0
        || sigaction(SIGILL, &sa, NULL) < 0)
    {
        fprintf(stderr, "Failed to install signal handlers: errno=%d\n", errno);
    }
    sa.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &sa, NULL) < 0) {
        fprintf(stderr, "Failed to install signal handler for SIGPIPE: errno=%d\n", errno);
    }
    if (atexit(&exitHandler) < 0) { 
        fprintf(stderr, "Failed to install exit handler: errno=%d\n", errno);
    }
}
