//-< DFS_CLIENT.H >--------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Client interface to DFS
//-------------------------------------------------------------------*--------*

#ifndef __DFS_CLIENT_H__
#define __DFS_CLIENT_H__

#include "threadpool.h"
#include "mutex.h"
#include "event.h"
#include "socketio.h"

#include <unistd.h>
#include <pthread.h>

const size_t DFS_CHUNK_SIZE = 4*1024*1024;
const int    DFS_DEFAULT_REDUNDANCY = 1;
const int    DFS_INDEX_REDUNDANCY = 2;
const size_t DFS_CACHE_BLOCK_SIZE = 256*1024; 
const int    DFS_NODE_BITS = 10;
const size_t DFS_MAX_NODES = 1 << DFS_NODE_BITS;
const int    DFS_NODE_MARKER = 0x10000000;
const int    DFS_PREPARE_RECOVERY_MARKER = 0x20000000;
const int    DFS_RECOVERY_MARKER = 0x40000000;
const int    DFS_CLIQUE_OFFSET = 16;
const int    DFS_CLIQUE_BITS = 3;
const int    DFS_MAX_CLIQUES = 1 << DFS_CLIQUE_BITS;
const int    DFS_NODE_ID_MASK = (1 << DFS_CLIQUE_OFFSET) - 1;
const int    DFS_MARKERS = DFS_NODE_MARKER | DFS_PREPARE_RECOVERY_MARKER | DFS_RECOVERY_MARKER;
const int    DFS_RECOVERY_BLOCK_SIZE = 256*1024;
const time_t DFS_READ_TIMEOUT = 10;
const time_t DFS_WRITE_TIMEOUT = 2;
const int    DFS_MAX_CACHE_DISKS = 16;
const int    DFS_MAX_PREFETCH_THREADS = 1; // result of prefetch seems to be not so efficient as expected
const time_t DFS_PING_INTERVAL = 300;

#define DFS_INODE_SIZE(redundancy)           (sizeof(DFSINode) + sizeof(DFSAddress) * ((redundancy) - DFS_DEFAULT_REDUNDANCY))

struct DFSAddress 
{
    offs32_t nodeId : DFS_NODE_BITS;
    offs32_t chunk  : 32 - DFS_NODE_BITS;
};

struct DFSINode 
{
    offs32_t   crc;
    DFSAddress addr[DFS_DEFAULT_REDUNDANCY];
};

struct DFSRequest
{
    enum { 
        DFS_PING,
        DFS_READ,
        DFS_READ_BLOCK,
        DFS_WRITE,
        DFS_UPDATE,
        DFS_PREPARE_RECOVERY,
        DFS_PREPARED,
        DFS_RECOVERY_SUCCESS,
        DFS_RECOVERY_FAILURE,
        DFS_GET_STATE
    };
    int      op;
    int      redundancy;
    offs32_t chunk;
    offs32_t offs;
    offs32_t size;
    crc_t    crc;
};


/**
 * In-memory DFS client cache
 */
class DFSCache 
{
  private:
    struct EventItem : Event { 
        EventItem* next;
        
        EventItem(Mutex& mutex) : Event(mutex) {}
    };
        
    struct Entry { 
        Entry*     next;
        Entry*     prev;
        Entry*     collision;
        fsize_t    addr;
        unsigned   accessCount;
        bool       isRaw;
        bool       waiting;
        EventItem* event;

        void unlink() { 
            next->prev = prev;
            prev->next = next;
        }

        void link(Entry* e) { 
            e->next = next;
            e->prev = this;
            next = next->prev = e;
        }
        
        bool isEmpty() { 
            return next == this;
        }
    };
    EventItem* freeEvents;
    Entry** hashTable;
    Entry*  entries;    
    char*   data;
    Mutex   mutex;
    Event   full;
    Entry   lru;
    Entry*  free;
    size_t  size;
    bool    waiting;
    size_t  hits;
    size_t  misses;
    size_t  used;

  public:
    DFSCache() : full(mutex) { freeEvents = NULL; }

    void  open(size_t cacheSize);
    void  close();
    char* get(fsize_t addr, bool& isRaw);
    void  release(char* chunk);
    void  update(fsize_t addr, char* buf);
};


/**
 * Cache of DFS client on local disk
 */
class DFSFileCache 
{
  private:
    struct Entry { 
        Entry*  next;
        Entry*  prev;
        Entry*  collision;
        fsize_t addr;
        fsize_t pos;
        int     disk;

        void unlink() { 
            next->prev = prev;
            prev->next = next;
        }

        void link(Entry* e) { 
            e->next = next;
            e->prev = this;
            next = next->prev = e;
        }
        
        bool isEmpty() { 
            return next == this;
        }
    };

    Entry** hashTable;
    Entry*  entries;    
    Mutex   mutex;
    Entry   lru;
    Entry*  free;
    int     fd[DFS_MAX_CACHE_DISKS];
    int     nDisks;
    fsize_t size;
    size_t  hits;
    size_t  misses;
    size_t  used;


  public:
    DFSFileCache();

    void  open();
    void  close();
    bool  get(fsize_t addr, char* buf);
    void  put(fsize_t addr, char* buf);
    void  update(fsize_t addr, char* buf);
};


struct DFSChannel 
{ 
    Socket*     s;
    unsigned    peer;
    bool        isDead;
    int         clique;
    Mutex       mutex;
    TrafficInfo traffic;

    DFSChannel() { 
        s = NULL;
        isDead = true;
    }

    ~DFSChannel() { 
        delete s;
    }
};

class DFSRecoveryBuffer 
{
    struct { 
        DFSRequest hdr;
        char       inodes[DFS_RECOVERY_BLOCK_SIZE];
    } msg;
    char*   currInode;
    int     failedNodeId;
    Socket* s;

  public:

    bool add(DFSINode* inode);
    bool flush();

    DFSRecoveryBuffer(Socket* s, int failedNodeId, int redundancy);
};

class DFSRecoveryPipe 
{
  public:
    struct PipeElem { 
        PipeElem* next;
        int       size;
        int       redundancy;
        char      inodes[DFS_RECOVERY_BLOCK_SIZE];
    };

  private:
    PipeElem* first;
    PipeElem* last;
    Mutex     mutex;
    Event     event;
    bool      waiting;

  public:
    DFSChannel* channel;

    void put(PipeElem* elem); 
    PipeElem* get();
    
    DFSRecoveryPipe(DFSChannel* channel) : event(mutex) {
        this->channel = channel;
        first = last = NULL;
    }
};
        
class DFS;

/**
 * DFS file implementation
 */
class DFSFile : private ThreadPool 
{
    friend class DFS;
  private:
    DFSINode*    inodes;
    DFSCache     cache;
    DFSFileCache fileCache;
    DFSFile*     next;
    size_t       inodeTableSize;
    int          redundancy;
    int          handle;
#ifdef PROFILE
    FILE*        prof;
    Mutex        profMutex;
    double       lastUpdate;
    size_t       totalRead;
#endif

    virtual void run(void* param, void* threadSpecificData);
    bool prepareRecovery(Socket* s, int failedNodeId);

    size_t getInodeSize();

    DFSINode* getInodeTable() { 
        return inodes;
    }

  public:
    /**
     * Fetch content of inode table in memory
     */
    void reread();
    
    /**
     * Write data to the DFS
     * @param offs offset in the file
     * @param chunk pointer to the written chunk data
     * @param clique to which DFS clique data should be written
     */
    void write(fsize_t offs, void* chunk, int clique = 0);

    /**
     * Prefetch data in cache
     * @param offs offset in the file
     * @param size size of prefetched data
     */
    void prefetch(fsize_t offs, size_t size);

    /**
     * Read specified number of bytes from specified location in the DFS file
     * @param offs offset in the file
     * @param buf destination buffer
     * @param size number of bytes to read
     * @param flags combination of V_DIRECT and V_NOWAIT flags
     * @return true if specified number of bytes is successfully read, false otherwise (last one is possible only in V_NOWAIT mode)
     */
    bool read(fsize_t offs, void* buf, size_t size, int flags = 0) {
        return read(inodes, 0, offs, buf, size, flags);
    }

    /**
     * Read data using provided INODE table. This method allows to access DFS from the process having not own DFS INODE table 
     * @param inodeTab pointer to INODE table elements describing requested segment of the DFS file
     * @param inodeTabBase offset in INODE table
     * @param offs offset in the file
     * @param buf destination buffer
     * @param size number of bytes to read
     * @param flags combination of V_DIRECT and V_NOWAIT flags
     * @return true if specified number of bytes is successfully read, false otherwise (last one is possible only in V_NOWAIT mode)
     */     
    bool read(DFSINode* inodeTab, size_t inodeTabBase, fsize_t offs, void* buf, size_t size, int flags);

    enum OpenFlags { 
        ReadOnly = 1,
        Truncate = 2
    };

    /**
     * Open DFS file
     * @param fd file descriptor containing INODE table (if -1, then there is no INODE table associated with file and it can be 
     * accessed only using read(DFSINode* inodeTab, size_t inodeTabBase,...) method
     * @param inodeTableOffset offset of INODE table in the file
     * @param inodeTableSize size (in bytes) of inode table
     * @param cacheSize in-memory cache size (in-bytes)
     * @param flags combination of ReadOnly and Truncate flags
     * @param redundancy DFS redundancy (how much time each chunk is replicated)
     */
    void open(int fd, size_t inodeTableOffset, size_t inodeTableSize, size_t cacheSize, int flags, int redundancy = DFS_DEFAULT_REDUNDANCY);
    
    /**
     * Open DFS file
     * @param name name of the file INODE table 
     * @param inodeTableOffset offset of INODE table in the file
     * @param inodeTableSize size (in bytes) of inode table
     * @param cacheSize in-memory cache size (in-bytes)
     * @param flags combination of ReadOnly and Truncate flags
     * @param redundancy DFS redundancy (how much time each chunk is replicated)
     */
    void open(char const* name, size_t inodeTableOffset, size_t inodeTableSize, size_t cacheSize, int flags, int redundancy = DFS_DEFAULT_REDUNDANCY);
    
    /**
     * Flush all changes
     */
    void flush();

    /**
     * Close DFS file
     */
    void close();    

    DFSFile();
};

struct DFSState { 
    fsize_t allocated;
    fsize_t used;
    fsize_t available;
    fsize_t currPos;
};

/**
 * Main DFS control class
 */
class DFS 
{ 
    friend class DFSFile;
    friend class DFSRecoveryBuffer;
  private:
    DFSChannel channels[DFS_MAX_NODES];
    int        cliqueFirstNode[DFS_MAX_CLIQUES];
    int        cliqueLastNode[DFS_MAX_CLIQUES];
    int        nOnlineChannels;
    int        maxChannelNo;
    Mutex      mutex;
    Mutex      logMutex;
    Event      event;
    FILE*      log;
    DFSFile*   files;
    pthread_t  pingThread;
    pthread_t  listenThread;

    void prepareRecovery(Socket* s, int nodeId);
    void initiateRecovery(DFSRecoveryPipe* pipe, int nodeId);
 
    static void* readPreparedDataThread(void* arg);
    void readPreparedData(DFSRecoveryPipe* pipe);
    
    static void* recoveryThread(void* arg);
    void recovery(DFSRecoveryPipe* pipe);
    
    void ping();
    static void* pingProc(void* arg);

    void accept(char const* address);
    void printSocketError(char const* operation, Socket* s, int node);
    void trace(char const* msg, int nodeId);

    void attachFile(DFSFile* file);
    void detachFile(DFSFile* file);

    void addChannel(Socket* s, int nodeId);
    DFSChannel* getRandomNode(DFSINode& node, int i, int clique);
    DFSChannel* getChannel(int nodeId) { 
        assert(nodeId > 0);
        return &channels[nodeId-1];
    }
    void waitNode(int nodeId);
    void disconnect(DFSChannel* channel, char* msg = NULL);

    static void* listenProc(void* arg);

  public:
    DFS();
    ~DFS();

    static DFS instance;
    bool       verify;

    /**
     * Start thread listening connections of DFS servers
     * @param address accept socket address in form NAME:PORT 
     */
    void listen(char const* address); 
    
    /**
     * Get DFS traffic statistic
     * @param info structure collection information about network traffic
     */
    void getTrafficStatistic(TrafficInfo& info);

    /**
     * Get information about status of DFS nodes
     * @param state buffer to receive information of DFS node state. It should be large enough to collect information about all nodes
     * @return number of registered DFS nodes (online or offline)
     */
    size_t poll(DFSState* state);
};    

/**
 * Write operations flags
 */
enum VFHFlags 
{ 
    V_DIRECT = 1, /** do not use cache */
    V_NOWAIT = 2  /** do not wait for DFS node restart if operation can no be completed immediately */
};

/**
 * Virtual file handle allowing to work in the same way either with DFS file, either with local file system file
 */
class VFileHandle {
  public:
    /**
     * Read data from the specified location
     * @param buf destination buffer
     * @param size amount of bytes to read
     * @param pos position in the file
     * @param flags combination of V_DIRECT and V_NOWAIT flags
     * @return true if specified number of bytes is successfully read, false otherwise (last one is possible only in V_NOWAIT mode)
     */
    virtual bool   pread(void* buf, size_t size, fsize_t pos, int flags = 0) = 0;

    /**
     * Write data to the specified location
     * @param buf source buffer
     * @param size amount of bytes to be written
     * @param pos position in the file
     * @param clique to which DFS clique data should be written (ignored in case of OS file)
     */
    virtual void   pwrite(void* buf, size_t size, fsize_t pos, int clique = 0) = 0;

    /**
     * Read data from the current position in the file
     * @param buf destination buffer
     * @param size amount of bytes to read
     * @param flags combination of V_DIRECT and V_NOWAIT flags
     * @return true if specified number of bytes is successfully read, false otherwise (last one is possible only in V_NOWAIT mode)
     */
    virtual bool   read(void* buf, size_t size, int flags = 0) = 0;

    /**
     * Write data to the current position in the file
     * @param buf source buffer
     * @param size amount of bytes to be written
     * @param clique to which DFS clique data should be written (ignored in case of OS file)
     */
    virtual void   write(void* buf, size_t size, int clique = 0) = 0;
    
    /**
     * Set current position in the file
     * @param offs new position in the file
     * @param whence SEEK_SET or SEEK_CUR
     * @return absolute position in the file
     */
    virtual off_t  seek(off_t offs, int whence) = 0;

    /**
     * Prefetch data in the cache
     * @param offs offset inn the file
     * @param size size of data to be prefetched
     */
    virtual void   prefetch(off_t offs, size_t size) = 0;

    /**
     * Get block size for the underlying device (chunk size for DFS)
     * @return block size in bytes
     */
    virtual size_t blockSize() = 0;
    virtual ~VFileHandle();
};

class DFSFileHandle : public VFileHandle
{
  private:
    fsize_t  currPos;
    DFSFile* file;
  public:
    DFSFile* getFile() const { return file; }
    
    virtual bool   pread(void* buf, size_t bufSize, fsize_t pos, int flags = 0);
    virtual void   pwrite(void* buf, size_t bufSize, fsize_t pos, int clique = 0);
    virtual bool   read(void* buf, size_t bufSize, int flags = 0);
    virtual void   write(void* buf, size_t bufSize, int clique = 0);
    virtual off_t  seek(off_t offs, int whence);
    virtual void   prefetch(off_t offs, size_t size);
    virtual size_t blockSize();
    
    size_t  getCurrent() { 
        return currPos;
    }
    
    /**
     * Assign DFS file to the handle
     * @param f DFS file
     * @param initPos initial position of handle in DFS file
     */
    void assign(DFSFile& f, fsize_t initPos = 0) { 
        file = &f;
        currPos = initPos;
    }
};
   
class UnixFileHandle : public VFileHandle
{
  private:
    int fd;
  public:
    virtual bool   pread(void* buf, size_t bufSize, fsize_t pos, int flags = 0);
    virtual void   pwrite(void* buf, size_t bufSize, fsize_t pos, int clique = 0);
    virtual bool   read(void* buf, size_t bufSize, int flags = 0);
    virtual void   write(void* buf, size_t bufSize, int clique = 0);
    virtual off_t  seek(off_t offs, int whence);
    virtual void   prefetch(off_t offs, size_t size);
    virtual size_t blockSize();
    
    /**
     * Assign Unix file descriptor to the handle
     */
    void assign(int f) { 
        fd = f;
    }
};
   
/**
 * Class performing buffered output 
 */
class BufferedOutput { 
    char*  buf;
    size_t size;
    size_t used;
    size_t blockSize;
    VFileHandle* vfh;
    int clique;

  public:
    /**
     * Constructor of buffered output stream
     * @param size buffer size
     * @param clique DFS clique
     */
    BufferedOutput(size_t size, int clique = 0);
    ~BufferedOutput();

    /**
     * Get device block size (chunk size for DFS)
     */
    size_t getBlockSize() { 
        return blockSize;
    }

    /** 
     * Get pointer inside buffer
     * @param offs offset within buffer
     * @return pointer to the location if buffer
     */
    char* get(size_t offs) { return buf + offs; }

    /**
     * Open file handle
     * vfh virtual file handle
     */
    void open(VFileHandle* vfh); 
    
    /**
     * Append data to the buffer
     * @param src written data
     * @param len amount of bytes to be written
     */
    void write(void const* src, size_t len);
    
    /** 
     * Flush buffer to the file. This method always round amount of data available in the buffer on device block size.
     * @param align if true than just clear buffer otherwise return current position to the beginning written block.
     * In the last case it is possible to continue appending data to the stream without any gap.
     */
    void flush(bool align);
};

#endif
