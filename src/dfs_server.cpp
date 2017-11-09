//-< DFS_SERVER.CPP >------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// DFS server implementation
//-------------------------------------------------------------------*--------*

#include "dfs_server.h"
#include "socketio.h"
#include "util.h"
#include "debug.h"
#include "system.h"

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "debug.h"
#include <fcntl.h>
#include <ctype.h>

const fsize_t DFS_REPORT_PERIOD = 10LL*1024*1024*1024;
const fsize_t DFS_METADATA_UPDATE_PERIOD = 1L*1024*1024*1024;
const int RECOVERY_FAILED_EXIT_STATUS = 1;
const int INVALID_PARAMETERS_EXIT_STATUS = 2;
const int DFS_WAIT_REQUEST_TIMEOUT = DFS_PING_INTERVAL*2; 
const int RECONNECT_TIMEOUT = 120;

const double PROFILE_OPERATION_TIME_THRESHOLD = 1;
const double PROFILE_UPDATE_INTERVAL = 3600;
const double PROFILE_MEASURE_PERIOD = 1;

DFSServer DFSServer::instance;

void DFSServer::start()
{
    nClients = clients.size-1; // excluding coordinator
    coordinatorAddress = clients.body[0];
    coordinator = NULL;
    nRecoveredClients = 0;
    pthread_t t;

    for (int i = 0; i <= nClients; i++) { 
        createThread(t, &serveThread,  clients.body[i]);
    }
}

void* DFSServer::serveThread(void* arg)
{
    DFSServer::instance.serve((char*)arg);
    return NULL;
}

void DFSServer::open(int id, int clique, char const* partition, char const* clientsList, bool recovery, fsize_t rewind)
{
    char fileName[64];
    
    sprintf(fileName, "dfs-%d.log", id);
    log = openLog(fileName, "a");

#ifdef PROFILE
    sprintf(fileName, "profile-%d.log", id);
    prof = openLog(fileName, "a");
    lastUpdate = measureStart = getCurrentTime();
    totalWritten = 0;
    writtenPerPeriod = 0;
    maxOutput = 0;
#endif

    fd = ::open(partition, O_RDWR|O_CREAT, 0777);
    assert(fd >= 0);
    vfh.assign(fd);
    headerSize = vfh.blockSize();

    allocated = 0;
    endPos = lseek(fd, 0, SEEK_END);

    hdr = (DFSDiskHeader*)new char[headerSize];
    vfh.pread(hdr, headerSize, 0);
    if (hdr->currPos < DFS_CHUNK_SIZE + rewind*GB) { 
        hdr->currPos += endPos - rewind*GB - DFS_CHUNK_SIZE;
    } else { 
        hdr->currPos -= rewind*GB;
    }
    hdr->currPos = ALIGN(hdr->currPos, DFS_CHUNK_SIZE);
    if (hdr->currPos >= endPos) { 
        hdr->currPos = 0;
    }
    currPos = hdr->currPos;
    if (currPos == 0) { 
        currPos = DFS_CHUNK_SIZE;
    }
    hdr->currPos = (hdr->currPos + DFS_METADATA_UPDATE_PERIOD) & ~(DFS_METADATA_UPDATE_PERIOD-1);
    vfh.pwrite(hdr, headerSize, 0); 

    this->recovery = recovery;
    this->id = id | (clique << DFS_CLIQUE_OFFSET) | DFS_NODE_MARKER;

    readClients(clientsList);
    start();
}

void DFSServer::close()
{
#ifdef PROFILE
    fclose(prof);
#endif
    ::close(fd);
    fclose(log);
    delete[] (char*)hdr;
}
    
void DFSServer::disconnect(Socket* s, char* host)
{
    CriticalSection cs(mutex);
    char timeBuf[MAX_TIME_LENGTH];
    char errBuf[MAX_SOCKET_ERROR_LENGTH];
    if (!recovery) { 
        s->get_error_text(errBuf, sizeof(errBuf));
        fprintf(log, "%s: Connection with node %s is lost: %s\n", getTime(timeBuf, sizeof(timeBuf)), host, errBuf);
        fflush(log);
    } else { 
        if (s == coordinator) { 
            s->get_error_text(errBuf, sizeof(errBuf));
            fprintf(log, "%s: Recovery is not possible because connection to coordinator is lost: %s\n", getTime(timeBuf, sizeof(timeBuf)), errBuf);       
            exit(RECOVERY_FAILED_EXIT_STATUS);
        }
    }
    delete s;
}

fsize_t DFSServer::allocate()
{
    CriticalSection cs(mutex);
    if (currPos + DFS_CHUNK_SIZE > endPos) { 
        hdr->currPos = currPos = DFS_CHUNK_SIZE;
    }
    if ((allocated + DFS_CHUNK_SIZE) / DFS_REPORT_PERIOD != allocated / DFS_REPORT_PERIOD) { 
        char buf[MAX_TIME_LENGTH];
        fprintf(log, "%s: %uGb allocated, current position %lld\n", 
                getTime(buf, sizeof(buf)), (unsigned)((allocated + DFS_CHUNK_SIZE) >> 30), (long long)hdr->currPos);
        fflush(log);
    }
    allocated += DFS_CHUNK_SIZE;
    fsize_t pos = currPos;
    currPos += DFS_CHUNK_SIZE;
    if (currPos > hdr->currPos) { 
        hdr->currPos = ALIGN(currPos, DFS_METADATA_UPDATE_PERIOD);
        vfh.pwrite(hdr, headerSize, 0); 
    }
    return pos;
}

void DFSServer::serve(char* host)
{
    char timeBuf[MAX_TIME_LENGTH];
    time_t connectTimeout = 0;
#ifdef PROFILE
    double start, intermediate, stop;
#endif
    while (true) {
        if (connectTimeout != 0) { 
            sleep(connectTimeout);
        }

        Socket* s = Socket::connect(host);
        if (!s->is_ok()) { 
            { 
                CriticalSection cs(mutex); 
                fprintf(log, "%s: Failed to establish connections with node %s\n", getTime(timeBuf, sizeof(timeBuf)), host);
                fflush(log);
            }
            delete s;
            connectTimeout = RECONNECT_TIMEOUT;
            continue;
        } else { 
            CriticalSection cs(mutex); 
            fprintf(log, "%s: Establish connection with node %s\n", getTime(timeBuf, sizeof(timeBuf)), host);
            fflush(log);
            int id = this->id;
            if (recovery) { 
                if (host == coordinatorAddress) { 
                    id |= DFS_RECOVERY_MARKER;
                    coordinator = s;
                    recoveryStartedEvent.signal();
                } else { 
                    id |= DFS_PREPARE_RECOVERY_MARKER;
                    while (coordinator == NULL) { 
                        recoveryStartedEvent.wait();
                    }
                }
            }            
            if (!s->write(&id, sizeof(id))) { 
                disconnect(s, host);
                continue;
            }
        }
        while (true) { 
            DFSRequest req;
            DFSState state;
            size_t received = s->read(&req, sizeof(req), sizeof(req), DFS_WAIT_REQUEST_TIMEOUT);
            if (received == sizeof(req)) { 
                DnmBuffer buf(DFS_CHUNK_SIZE);
                switch (req.op) {            
                  case DFSRequest::DFS_GET_STATE:
                    state.available = endPos;
                    state.allocated = allocated;
                    state.currPos = currPos;
                    if (s->write(&state, sizeof state)) { 
                        continue;
                    }
                    break;

                  case DFSRequest::DFS_PING:
                    continue;

                  case DFSRequest::DFS_WRITE:
                    if ((size_t)s->read(buf, DFS_CHUNK_SIZE, DFS_CHUNK_SIZE, DFS_READ_TIMEOUT) == DFS_CHUNK_SIZE) { 
                        fsize_t pos = allocate();
                        offs32_t chunkNo = pos / DFS_CHUNK_SIZE;
                        if (s->write(&chunkNo, sizeof chunkNo)) { 
                            diskWrite(fd, buf, DFS_CHUNK_SIZE, pos);
                            continue;
                        }
                    }
                    break;
                    
                  case DFSRequest::DFS_READ:
                    diskRead(fd, buf, DFS_CHUNK_SIZE, (fsize_t)req.chunk*DFS_CHUNK_SIZE);
                    if (s->write(buf, DFS_CHUNK_SIZE)) { 
                        continue;
                    }
                    break;
                
                  case DFSRequest::DFS_READ_BLOCK:
#ifdef PROFILE
                    start = getCurrentTime();
#endif
#ifdef DFS_SERVER_CHECK_CRC
                    diskRead(fd, buf, DFS_CHUNK_SIZE, (fsize_t)req.chunk*DFS_CHUNK_SIZE);
#ifdef PROFILE
                    intermediate = getCurrentTime();
#endif
                    if (calculateCRC(buf, DFS_CHUNK_SIZE) != req.crc || !s->write(buf + req.offs, req.size)) { 
                        break;
                    }
#else 
                    diskRead(fd, buf, req.size, (fsize_t)req.chunk*DFS_CHUNK_SIZE + req.offs);
#ifdef PROFILE
                    intermediate = getCurrentTime();
#endif
                    if (!s->write(buf, req.size)) { 
                        break;
                    }                    
#endif
#ifdef PROFILE
                    stop = getCurrentTime();
                    { 
                        CriticalSection cs(profMutex);
                        char timeBuf[MAX_TIME_LENGTH];
                        totalWritten += req.size;
                        writtenPerPeriod += req.size;
                        if (stop >= measureStart + PROFILE_MEASURE_PERIOD) { 
                            double output = writtenPerPeriod / (stop - measureStart) / MB;
                            if (output > maxOutput) { 
                                maxOutput = output;
                            }
                            writtenPerPeriod = 0;
                            measureStart = stop;
                        }
                        if (stop >= lastUpdate + PROFILE_UPDATE_INTERVAL) { 
                            fprintf(prof, "%s: output network traffic: %.2fMb/sec (top %.2fMb/sec)\n",
                                    getTime(timeBuf, sizeof timeBuf), 
                                    totalWritten / (stop - lastUpdate) / MB, 
                                    maxOutput);
                            lastUpdate = stop;
                            totalWritten = 0;
                            maxOutput = 0;
                            fflush(prof);
                        }
                        if (stop - start > PROFILE_OPERATION_TIME_THRESHOLD) { 
                            fprintf(prof, 
                                    "%s: disk read: %.2f sec, socket write to %s: %.2f sec\n",
                                    getTime(timeBuf, sizeof timeBuf), 
                                    intermediate - start, host, stop - intermediate);
                            fflush(prof);
                        }                    
                    }    
#endif
                    continue;
                
                  case DFSRequest::DFS_UPDATE:
                    if ((size_t)s->read(buf, DFS_CHUNK_SIZE, DFS_CHUNK_SIZE, DFS_READ_TIMEOUT) == DFS_CHUNK_SIZE) { 
                        diskWrite(fd, buf, DFS_CHUNK_SIZE, (fsize_t)req.chunk*DFS_CHUNK_SIZE);
                        continue;
                    }
                    break;
            
                  case DFSRequest::DFS_PREPARED:
                  { 
                      CriticalSection cs(mutex);
                      if (++nRecoveredClients < nClients || coordinator->write(&req, sizeof req)) { 
                          continue;
                      }
                      disconnect(coordinator, coordinatorAddress);
                      break;
                  }

                  case DFSRequest::DFS_PREPARE_RECOVERY:
                  {
                      int size = req.size;
                      if (s->read(buf, size, size, DFS_READ_TIMEOUT) == size) { 
                          CriticalSection cs(mutex);
                          if (coordinator->write(&req, sizeof req)
                              && coordinator->write(buf, size))
                          {
                              continue;
                          }
                          disconnect(coordinator, coordinatorAddress);
                      }
                      break;
                  }
                  case DFSRequest::DFS_RECOVERY_SUCCESS:
                  { 
                      CriticalSection cs(mutex);
                      fprintf(log, "%s: Recovery successfully completed\n", getTime(timeBuf, sizeof(timeBuf)));       
                      fflush(log);
                      recovery = false;
                      recoveryFinishedEvent.signal();
                      continue;
                  }
                    
                  case DFSRequest::DFS_RECOVERY_FAILURE:
                    fprintf(log, "%s: Coordinator failed to perform recovery\n", getTime(timeBuf, sizeof(timeBuf)));
                    exit(RECOVERY_FAILED_EXIT_STATUS);
                    
                  default:
                    assert(false);
                }                
#ifdef DO_NOT_HANGUP_TIMED_OUT_CONNECTION
            } else if (received == 0) { 
                continue;
#endif
            }
            disconnect(s, host);                 
            break;
        }
                
        if (recovery) { 
            CriticalSection cs(mutex);
            while (recovery) { 
                recoveryFinishedEvent.wait();
            }
        }
    }
}

void DFSServer::readClients(char const* file) 
{ 
    FILE* f = openLocalConfig(file);
    char buf[MAX_CFG_FILE_LINE_LENGTH];
    while (fgets(buf, sizeof buf, f)) { 
        size_t len = strtrim(buf);
        char* name = new char[len+1];
        memcpy(name, buf, len);
        name[len] = '\0';
        clients.push(name);
    }
    fclose(f);
}
    
int nDays;                 
 
int main(int argc, char* argv[])
{
    if (argc < 3) { 
        fprintf(stderr, "Usage: dfs_server NODE-ID PARTITION [CLIQUE] [-clients FILE] [-rewind N] [-recovery]\n");       
        return INVALID_PARAMETERS_EXIT_STATUS;
    }
    installSignalHandlers(); 

    bool recovery = false;
    char* clients = NULL;
    int id = atoi(argv[1]);
    int clique = 0;
    char* partition = argv[2];
    fsize_t rewind = 0;
    for (int i = 3; i < argc; i++) { 
        if (strcmp(argv[i], "-recovery") == 0) { 
            recovery = true;
        } else if (strcmp(argv[i], "-clients") == 0) { 
            clients = argv[++i];
        } else if (strcmp(argv[i], "-rewind") == 0) { 
            rewind = atol(argv[++i]);
        } else if (isdigit(*argv[i] & 0xFF)) { 
            clique =  atoi(argv[i]);
        } else { 
            fprintf(stderr, "Usage: dfs_server NODE-ID PARTITION [CLIQUE] [-clients FILE] [-rewind N] [-recovery]\n");
            return INVALID_PARAMETERS_EXIT_STATUS;
        }
    }        
    if (clients == NULL) { 
        fprintf(stderr, "Clients list is not specified\n");
        return INVALID_PARAMETERS_EXIT_STATUS;
    }
        
    DFSServer::instance.open(id, clique, partition, clients, recovery, rewind);
    while (true) { 
        sleep(SECONDS_IN_DAY);
        nDays += 1;
    }
    DFSServer::instance.close();

    return 0;
}
