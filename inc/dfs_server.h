//-< DFS_SERVER.H >--------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// DFS server implementation
//-------------------------------------------------------------------*--------*

#ifndef __DFS_SERVER_H__
#define __DFS_SERVER_H__

#include "socketio.h"
#include "event.h"
#include "mutex.h"
#include "dfs_client.h"

#include <stdio.h>
#include <pthread.h>

struct DFSDiskHeader {
    fsize_t currPos;
};

/**
 * Class representing DFS server node
 */
class DFSServer 
{ 
  protected:
    fsize_t currPos;
    fsize_t endPos;
    fsize_t allocated;
    size_t  headerSize;
    int     fd;
    
    Socket* coordinator;
    char*   coordinatorAddress;
    int     nRecoveredClients;
    int     nClients;
    Mutex   mutex;
    Event   recoveryFinishedEvent;
    Event   recoveryStartedEvent;

    FILE*   log;
    int     id;
    bool    recovery;

    DFSDiskHeader* hdr;
    UnixFileHandle vfh;
    stack<char*>   clients;

#ifdef PROFILE
    FILE*  prof;
    Mutex  profMutex;
    size_t totalWritten;
    size_t writtenPerPeriod;
    double maxOutput;
    double lastUpdate;
    double measureStart;
#endif

    virtual void start();
    virtual void readClients(char const* file);
    void disconnect(Socket* s, char* host);
    fsize_t allocate();
    void reconnect();
    
    static void* serveThread(void*);
    void serve(char* host);

  public:
    static DFSServer instance;

    DFSServer() : recoveryFinishedEvent(mutex), recoveryStartedEvent(mutex) {}

    /**
     * Open DFS node
     * @param id identifier of this node [0..NumberOfNodes)
     * @param clique clique to which this node belongs
     * @param partition path to the raw partition where data will be stored
     * @param clientsList path to the file with list of DFS client addresses
     * @param recovery if node has to be recovered (is set to true after disk corruption)
     * @param rewind skip specified number of gigabytes from the current location in the partition
     */
    void open(int id, int clique, char const* partition, char const* clientsList, bool recovery = false, fsize_t rewind = 0);

    /** 
     * Close DFS node
     */
    void close();
};

#endif
