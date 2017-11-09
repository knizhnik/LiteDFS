//-< DFS_CLIENT.CPP >------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Client interface to DFS
//-------------------------------------------------------------------*--------*

#include "types.h"
#include "dfs_client.h"
#include "util.h"
#include "system.h"
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

const offs32_t DFS_INVALID_CHUNK = 0;
const size_t LOG_NAME_SIZE = 256;
const int MAX_DISK_PATH_LENGHT = 256;
const int MAX_MSG_LENGTH = 1024;
const int MAX_ERR_LENGTH = 256;
const double PROFILE_REQUEST_TIME_THRESHOLD = 1.0;
const double PROFILE_UPDATE_INTERVAL = 3600;
const time_t GATE_RECONNECT_INTERVAL = 60; // one minute


const time_t DFS_WAIT_FOR_ONLINE_NODES_TIMEOUT = 10000;
const int DFS_MIN_ONLINE_NODES = 4;
const size_t MAX_IO_BUF_SIZE = DFS_CHUNK_SIZE;

#define DFS_INODE(inodes, index, redundancy) (*(DFSINode*)((char*)(inodes) + DFS_INODE_SIZE(redundancy)*(index)))

DFSFileCache::DFSFileCache()
{
    entries = NULL;
    hashTable = NULL;
    nDisks = 0;
}

void DFSFileCache::open()
{
    fsize_t fileSize[DFS_MAX_CACHE_DISKS];
    fsize_t filePos[DFS_MAX_CACHE_DISKS];
    char path[MAX_DISK_PATH_LENGHT];
    char* cacheDesc = System::getDfsFileCacheDescriptor();
    if (cacheDesc == NULL) {
        return;
    }
    FILE* f = fopen(cacheDesc, "r");    
    assert (f != NULL);
    nDisks = 0;
    size = 0;
    misses = hits = used = 0;
    while (fgets(path, sizeof(path), f) != NULL) {
        int len = strtrim(path);
        if (len != 0) { 
            path[len] = '\0';
            assert(nDisks < DFS_MAX_CACHE_DISKS);
            fd[nDisks] = ::open(path, O_RDWR, 0);
            assert(fd[nDisks] >= 0);
            size += fileSize[nDisks] = lseek(fd[nDisks], 0, SEEK_END) / DFS_CACHE_BLOCK_SIZE;
            filePos[nDisks] = 0;
            nDisks += 1;
        }
    }
    fclose(f);
    entries = new Entry[size];
    
    for (size_t i = 0, j = 0; i < size; i++) { 
        entries[i].next = &entries[i+1];
        do { 
            j = (j + 1) % nDisks;
        } while (filePos[j] == fileSize[j]);
        entries[i].disk = j;
        entries[i].pos = filePos[j]++ * DFS_CACHE_BLOCK_SIZE;
    }        
    entries[size-1].next = NULL;
    free = entries;
    hashTable = new Entry*[size];
    memset(hashTable, 0, sizeof(Entry*)*size);
    lru.next = lru.prev = &lru;
}

void DFSFileCache::close()
{
    for (int i = 0; i < nDisks; i++) { 
        ::close(fd[i]);
    }
    delete[] hashTable;
    delete[] entries;
    entries = NULL;
    hashTable = NULL;
    nDisks = 0;
}

bool DFSFileCache::get(fsize_t addr, char* buf)
{
    if (entries == NULL) {
        return false;
    }
    mutex.lock();
    for (Entry* entry = hashTable[addr % size]; entry != NULL; entry = entry->collision) { 
        if (entry->addr == addr) { 
            entry->unlink();
            hits += 1;
            mutex.unlock();
            diskRead(fd[entry->disk], buf, DFS_CACHE_BLOCK_SIZE, entry->pos);
            mutex.lock();
            lru.link(entry);
            mutex.unlock();
            return true;
        }
    }
    misses += 1;
    mutex.unlock();
    return false;
}
    
void DFSFileCache::update(fsize_t addr, char* buf)
{
    if (entries == NULL) {
        return;
    }
    CriticalSection cs(mutex);
    for (Entry* entry = hashTable[addr % size]; entry != NULL; entry = entry->collision) { 
        if (entry->addr == addr) { 
            diskWrite(fd[entry->disk], buf, DFS_CACHE_BLOCK_SIZE, entry->pos);
            break;
        }
    }
}

void DFSFileCache::put(fsize_t addr, char* buf)
{
    if (entries == NULL) {
        return;
    }
    CriticalSection cs(mutex);
    Entry* entry = free;
    size_t h = addr % size;
    if (entry == NULL) { 
        assert (!lru.isEmpty()); // size of cache should be larger than number of active threads
        entry = lru.prev;
        entry->unlink();    
        Entry** epp = &hashTable[entry->addr % size]; 
        while (*epp != entry) {
            epp = &(*epp)->collision;
        }
        *epp = entry->collision;
    } else { 
        free = entry->next;
        used += 1;
    }
    entry->addr = addr;
    entry->collision = hashTable[h];
    hashTable[h] = entry;
    lru.link(entry);    
    diskWrite(fd[entry->disk], buf, DFS_CACHE_BLOCK_SIZE, entry->pos);
}



void DFSCache::open(size_t cacheSize)
{
    size = cacheSize;
    entries = new Entry[cacheSize];
    data = new char[cacheSize*DFS_CACHE_BLOCK_SIZE];
    hashTable = new Entry*[cacheSize];
    memset(hashTable, 0, sizeof(Entry*)*cacheSize);
    misses = hits = used = 0;

    if (cacheSize > 0) { 
        for (size_t i = 0; i < cacheSize; i++) { 
            entries[i].next = &entries[i+1];
            entries[i].event = NULL;
        }
        free = entries;
        entries[cacheSize-1].next = NULL;
    } else {
        free = NULL;
    }
    waiting = false;
    lru.next = lru.prev = &lru;
    lru.event = NULL;
}

void DFSCache::close()
{
    delete[] entries;
    delete[] data;
    delete[] hashTable;
}

void DFSCache::update(fsize_t addr, char* buf)
{
    CriticalSection cs(mutex);
    for (Entry* entry = hashTable[addr % size]; entry != NULL; entry = entry->collision) { 
        if (entry->addr == addr) { 
            memcpy(data + (entry - entries) * DFS_CACHE_BLOCK_SIZE, buf, DFS_CACHE_BLOCK_SIZE);
            break;
        }
    }
}
                    
char* DFSCache::get(fsize_t addr, bool& isRaw)
{
    CriticalSection cs(mutex);
    while (true) { 
        Entry* entry;
        size_t h = addr % size;
        for (entry = hashTable[h]; entry != NULL; entry = entry->collision) { 
            if (entry->addr == addr) { 
                hits += 1;
                if (entry->accessCount++ == 0) { 
                    entry->unlink();
                }
                while (entry->isRaw) { 
                    entry->waiting = true;
                    EventItem* e = entry->event;
                    if (e == NULL) { 
                        e = freeEvents;
                        if (e == NULL) { 
                            e = new EventItem(mutex);
                        }
                        entry->event = e;
                    }
                    e->wait();
                }
                isRaw = false;
                return data + (entry - entries) * DFS_CACHE_BLOCK_SIZE;
            }
        }
        entry = free;
        if (entry == NULL) { 
            if (lru.isEmpty()) { 
                waiting = true;
                full.wait();
                continue;
            }            
            entry = lru.prev;
            assert(entry->accessCount == 0);
            entry->unlink();    
            Entry** epp = &hashTable[entry->addr % size]; 
            while (*epp != entry) {
                epp = &(*epp)->collision;
            }
            *epp = entry->collision;
        } else { 
            free = entry->next;
            used += 1;
        }
        misses += 1;
        entry->addr = addr;
        entry->collision = hashTable[h];
        hashTable[h] = entry;
        entry->waiting = false;
        entry->isRaw = true;
        entry->accessCount = 1;
        isRaw = true;
        return data + (entry - entries) * DFS_CACHE_BLOCK_SIZE;
    }
}
 
void DFSCache::release(char* chunk)
{
    CriticalSection cs(mutex);
    offs32_t i = (chunk - data) / DFS_CACHE_BLOCK_SIZE;
    assert(i < size);
    Entry* entry = &entries[i];
    if (entry->isRaw) { 
        entry->isRaw = false;
        if (entry->waiting) { 
            entry->waiting = false;
            entry->event->signal();
        }
    }
    if (--entry->accessCount == 0) {
        if (entry->event != NULL) { 
            entry->event->next = freeEvents;
            freeEvents = entry->event;
            entry->event = NULL;
        }
        lru.link(entry);
        if (waiting) { 
            waiting = false;
            full.signal();
        }
    }
}

DFSFile::DFSFile()
{
    handle = -1;
    inodes = NULL;
}
            
void DFSFile::open(char const* name, size_t inodeTableOffset, size_t inodeTableSize, size_t cacheSize, int flags, int redundancy)
{
    handle = ::open(name, (flags & ReadOnly) ? O_RDONLY : (O_RDWR|O_CREAT), 0777);
    assert(handle >= 0);
    open(handle, inodeTableOffset, inodeTableSize, cacheSize, flags, redundancy);
}

void DFSFile::open(int fd, size_t inodeTableOffset, size_t inodeTableSize, size_t cacheSize, int flags, int redundancy)
{
    this->redundancy = redundancy;
    cache.open(cacheSize);
    if (flags & ReadOnly) {
        fileCache.open();
    }
    start(DFS_MAX_PREFETCH_THREADS);
    this->inodeTableSize = inodeTableSize;
    if (inodeTableSize != 0) { 
		ftruncate(fd, inodeTableSize);
        inodes = (DFSINode*)mmap(NULL, inodeTableSize, (flags & ReadOnly) ? PROT_READ : PROT_READ|PROT_WRITE, 
                                 fd < 0 ? (MAP_ANONYMOUS|MAP_SHARED) : MAP_SHARED, 
                                 fd, inodeTableOffset);            
        assert(inodes != (DFSINode*)-1);  
        if (flags & Truncate) { 
            memset(inodes, 0, inodeTableSize);
        }
    }
    DFS::instance.attachFile(this);
#ifdef PROFILE
    char logName[LOG_NAME_SIZE];
    sprintf(logName, "profile_%s.log", BulletinBoard::instance.getNodeName());
    prof = openLog(logName, "a");
    lastUpdate = getCurrentTime();
    totalRead = 0;
#endif
}

void DFSFile::flush()
{
    if (inodeTableSize != 0) { 
        int rc = msync(inodes, inodeTableSize, MS_SYNC);
        assert(rc == 0);
    }
}

void DFSFile::close()
{
    stop();
    DFS::instance.detachFile(this);
    if (handle >= 0) { 
        ::close(handle);
        handle = -1;
    }
    cache.close();
    fileCache.close();
    if (inodeTableSize != 0) { 
        int rc = munmap(inodes, inodeTableSize);
        assert(rc == 0);
    }
#ifdef PROFILE
    fclose(prof);
#endif
}

void DFSFile::write(fsize_t offs, void* chunk, int clique)
{
    assert((offs & (DFS_CHUNK_SIZE-1)) == 0);
    offs32_t chunkNo = (offs32_t)(offs / DFS_CHUNK_SIZE);
    DFSRequest req; 
    DFSINode& inode = DFS_INODE(inodes, chunkNo, redundancy);
    inode.crc = calculateCRC((char*)chunk, DFS_CHUNK_SIZE);

    fsize_t blockAddr = offs / DFS_CACHE_BLOCK_SIZE;
    cache.update(blockAddr, (char*)chunk);
    fileCache.update(blockAddr, (char*)chunk);

    for (int i = 0; i < redundancy; i++) { 
        if (inode.addr[i].nodeId != 0) {
            // overwrite existed block
            DFSChannel* chan = DFS::instance.getChannel(inode.addr[i].nodeId);
            CriticalSection cs(chan->mutex);
            if (!chan->isDead) { 
                req.op = DFSRequest::DFS_UPDATE; 
                req.chunk = inode.addr[i].chunk;
                if (chan->s->write(&req, sizeof(req)) && chan->s->write(chunk, DFS_CHUNK_SIZE)) { 
                    continue;
                }
                DFS::instance.disconnect(chan);
            }
        }
        while (true) { 
            DFSChannel* chan = DFS::instance.getRandomNode(inode, i, clique);
            CriticalSection cs(chan->mutex);
            if (!chan->isDead) { 
                offs32_t chunkNo;
                req.op = DFSRequest::DFS_WRITE;
                req.chunk = chunkNo;
                if (chan->s->write(&req, sizeof(req))
                    && chan->s->write(chunk, DFS_CHUNK_SIZE)
                    && (size_t)chan->s->read(&chunkNo, sizeof(chunkNo), sizeof(chunkNo), DFS_READ_TIMEOUT) == sizeof(chunkNo))
                {
                    inode.addr[i].chunk = chunkNo;
                    break;
                }            
                DFS::instance.disconnect(chan);
            }
        }
    }
}

void DFSFile::prefetch(fsize_t pos, size_t size)
{
    fsize_t end = pos + size;
    while (pos < end) {         
        schedule((void*)(size_t)pos);
        pos += DFS_CACHE_BLOCK_SIZE;
    }
}

size_t DFSFile::getInodeSize()
{
    return DFS_INODE_SIZE(redundancy);
}


void DFSFile::run(void* pos, void*)
{
    bool rc = read((size_t)pos, NULL, DFS_CACHE_BLOCK_SIZE, 0);
    assert(rc);
}

bool DFSFile::read(DFSINode* inodeTab, size_t inodeTabBase, fsize_t pos, void* buf, size_t bufSize, int flags)
{
    char* dst = (char*)buf;
    if (((pos | bufSize) & (DFS_CHUNK_SIZE-1)) == 0 || (flags & V_DIRECT)) { 
        char* dst = (char*)buf;
        while (bufSize != 0) {
            unsigned j = rand();
            size_t chunkNo = pos / DFS_CHUNK_SIZE;
            size_t offs = pos & (DFS_CHUNK_SIZE-1);
            size_t size = DFS_CHUNK_SIZE - offs < bufSize ? DFS_CHUNK_SIZE - offs : bufSize; 
            DFSINode& inode = DFS_INODE(inodeTab, chunkNo - inodeTabBase, redundancy);            
            while (true) { 
                int nodeId = -1;
                for (int i = 0; i < redundancy; i++) { 
                    int n = j++ % redundancy;
                    nodeId = inode.addr[n].nodeId;
                    assert(nodeId != 0);
                    DFSChannel* chan = DFS::instance.getChannel(nodeId);
                    CriticalSection cs(chan->mutex);
                    if (!chan->isDead) { 
                        DFSRequest req;
                        req.op = DFSRequest::DFS_READ_BLOCK;
                        req.chunk = inode.addr[n].chunk;
                        req.crc = inode.crc;
                        req.offs = offs;
                        req.size = size;
#ifdef PROFILE
                        double start = getCurrentTime();
                        if (chan->s->write(&req, sizeof(req))) { 
                            double intermediate = getCurrentTime();
                            if ((size_t)chan->s->read(dst, size, size, DFS_READ_TIMEOUT) == size
                                && (size != DFS_CHUNK_SIZE || calculateCRC(dst, DFS_CHUNK_SIZE) == inode.crc))
                            {
                                double stop = getCurrentTime();
                                CriticalSection cs(profMutex);
                                char timeBuf[MAX_TIME_LENGTH];
                                totalRead += size;
                                if (stop >= lastUpdate + PROFILE_UPDATE_INTERVAL) { 
                                    fprintf(prof, "%s: input network traffic: %.2fMb/sec\n",
                                            getTime(timeBuf, sizeof timeBuf), 
                                            totalRead / (stop - lastUpdate) / MB);
                                    lastUpdate = stop;
                                    totalRead = 0;
                                    fflush(prof);
                                }
                                if (stop - start >= PROFILE_REQUEST_TIME_THRESHOLD) { 
                                    fprintf(prof, 
                                            "%s: send request to node %d: %.2f sec, receive response: %.2f sec\n",
                                            getTime(timeBuf, sizeof timeBuf), 
                                            nodeId-1, 
                                            intermediate - start, stop - intermediate);
                                    fflush(prof);
                                }                        
                                dst += size;
                                bufSize -= size;
                                pos += size; 
                                goto NextChunk;
                            }
                        }
#else
                        if (chan->s->write(&req, sizeof(req)) 
                            && (size_t)chan->s->read(dst, size, size, DFS_READ_TIMEOUT) == size
                            && (size != DFS_CHUNK_SIZE || calculateCRC(dst, DFS_CHUNK_SIZE) == inode.crc))
                        {                             
                            dst += size;
                            bufSize -= size;
                            pos += size; 
                            goto NextChunk;
                        }
#endif
                        DFS::instance.disconnect(chan);
                    }
                }
                if (redundancy == 1 || (flags & V_NOWAIT)) { 
                    char msgBuf[256];
                    sprintf(msgBuf, "Failed to load page with address %lld from node %u chunk %u",
                            (long long)pos, inode.addr[0].nodeId, inode.addr[0].chunk);
                    DFS::instance.trace(msgBuf, inode.addr[0].nodeId-1);
                    return false;
                }
                DFS::instance.waitNode(nodeId);
            }
          NextChunk:;
        } 
    } 
    else 
    {
        while (bufSize != 0) { 
            bool isRaw;
            size_t chunkNo = pos / DFS_CHUNK_SIZE;
            fsize_t blockAddr = pos / DFS_CACHE_BLOCK_SIZE;
            size_t offs = pos & ((DFS_CHUNK_SIZE-1) & ~(DFS_CACHE_BLOCK_SIZE-1));
            char* page = cache.get(blockAddr, isRaw);
            if (DFS::instance.verify) { 
                char* block = new char[DFS_CACHE_BLOCK_SIZE];
                DFSINode& inode = DFS_INODE(inodeTab, chunkNo - inodeTabBase, redundancy);                
                int nAliveNodes = 0;  
                while (true) {
                    int nodeId = -1;
                    for (int i = 0; i < redundancy; i++) { 
                        nodeId = inode.addr[i].nodeId;
                        assert(nodeId != 0);
                        DFSChannel* chan = DFS::instance.getChannel(nodeId);
                        CriticalSection cs(chan->mutex);
                        if (!chan->isDead) { 
                            DFSRequest req;
                            req.op = DFSRequest::DFS_READ_BLOCK;
                            req.chunk = inode.addr[i].chunk;
                            req.offs = offs;
                            req.size = DFS_CACHE_BLOCK_SIZE;
                            req.crc = inode.crc;
                            if (chan->s->write(&req, sizeof(req)) 
                                && (size_t)chan->s->read(block, DFS_CACHE_BLOCK_SIZE, DFS_CACHE_BLOCK_SIZE, DFS_READ_TIMEOUT) == DFS_CACHE_BLOCK_SIZE)
                            { 
                                if (isRaw && nAliveNodes == 0) { 
                                    fileCache.put(blockAddr, page);
                                    memcpy(page, block, DFS_CACHE_BLOCK_SIZE);
                                } else { 
                                    assert(memcmp(page, block, DFS_CACHE_BLOCK_SIZE) == 0); 
                                }
                                nAliveNodes += 1;
                            }
                            DFS::instance.disconnect(chan);
                        }
                    }
                    if (nAliveNodes == 0) { 
                        DFS::instance.waitNode(nodeId);
                    } else { 
                        break;
                    }
                }
            } else if (isRaw) { 
                if (!fileCache.get(blockAddr, page)) {
                    unsigned j = rand();
                    DFSINode& inode = DFS_INODE(inodeTab, chunkNo - inodeTabBase, redundancy);
                    while (true) { 
                        int nodeId = -1;
                        for (int i = 0; i < redundancy; i++) { 
                            int n = j++ % redundancy;
                            nodeId = inode.addr[n].nodeId;
                            assert(nodeId != 0);
                            DFSChannel* chan = DFS::instance.getChannel(nodeId);
                            CriticalSection cs(chan->mutex);
                            if (!chan->isDead) { 
                                DFSRequest req;
                                req.op = DFSRequest::DFS_READ_BLOCK;
                                req.chunk = inode.addr[n].chunk;
                                req.offs = offs;
                                req.size = DFS_CACHE_BLOCK_SIZE;
                                req.crc = inode.crc;
#ifdef PROFILE
                                double start = getCurrentTime();
                                if (chan->s->write(&req, sizeof(req))) { 
                                    double intermediate = getCurrentTime();
                                    if ((size_t)chan->s->read(page, DFS_CACHE_BLOCK_SIZE, DFS_CACHE_BLOCK_SIZE, DFS_READ_TIMEOUT) == DFS_CACHE_BLOCK_SIZE) {
                                        double stop = getCurrentTime();
                                        CriticalSection cs(profMutex);
                                        char timeBuf[MAX_TIME_LENGTH];
                                        totalRead += DFS_CACHE_BLOCK_SIZE;
                                        if (stop >= lastUpdate + PROFILE_UPDATE_INTERVAL) { 
                                            fprintf(prof, "%s: input network traffic: %.2fMb/sec\n",
                                                    getTime(timeBuf, sizeof timeBuf), 
                                                    totalRead / (stop - lastUpdate) / MB);
                                            lastUpdate = stop;
                                            totalRead = 0;
                                            fflush(prof);
                                        }
                                        if (stop - start >= PROFILE_REQUEST_TIME_THRESHOLD) { 
                                            fprintf(prof, 
                                                    "%s: send request to node %d: %.2f sec, receive response: %.2f sec\n",
                                                    getTime(timeBuf, sizeof timeBuf), 
                                                    nodeId-1, 
                                                    intermediate - start, stop - intermediate);
                                            fflush(prof);
                                        }                        
                                        fileCache.put(blockAddr, page);
                                        goto CopyFromChunk;
                                    }
                                }
#else
                                if (chan->s->write(&req, sizeof(req)) 
                                    && (size_t)chan->s->read(page, DFS_CACHE_BLOCK_SIZE, DFS_CACHE_BLOCK_SIZE, DFS_READ_TIMEOUT) == DFS_CACHE_BLOCK_SIZE)
                                { 
                                    fileCache.put(blockAddr, page);
                                    goto CopyFromChunk;
                                }
#endif
                                DFS::instance.disconnect(chan);
                            }
                        }
                        if (redundancy == 1 || (flags & V_NOWAIT)) { 
                            char msgBuf[256];
                            sprintf(msgBuf, "Failed to load page with address %lld from node %u chunk %u",
                                    (long long)pos, inode.addr[0].nodeId, inode.addr[0].chunk);
                            DFS::instance.trace(msgBuf, inode.addr[0].nodeId-1);
                            cache.release(page);
                            return false;
                        }
                        DFS::instance.waitNode(nodeId);
                    }
                }
            } 
          CopyFromChunk:
            offs = pos & (DFS_CACHE_BLOCK_SIZE-1);
            size_t size = bufSize > DFS_CACHE_BLOCK_SIZE - offs ? DFS_CACHE_BLOCK_SIZE - offs : bufSize;
            if (dst != NULL) {
                memcpy(dst, page + offs, size);
                dst += size;
            }
            bufSize -= size;
            pos += size;
            cache.release(page);
        }
    }
    return true;
}    

void DFSFile::reread()
{
    forceRead(inodes, inodeTableSize, true);
}

bool DFSFile::prepareRecovery(Socket* s, int failedNodeId)
{    
    if (redundancy > 1) { 
        DFSRecoveryBuffer buf(s, failedNodeId, redundancy);
        size_t inodeSize = DFS_INODE_SIZE(redundancy);
        for (char *inodesBeg = (char*)inodes, *inodesEnd = inodesBeg + inodeTableSize; inodesBeg < inodesEnd; inodesBeg += inodeSize) {
            for (int j = 0; j < redundancy; j++) { 
                DFSINode* inode = (DFSINode*)inodesBeg;
                if ((int)inode->addr[j].nodeId == failedNodeId) { 
                    if (!buf.add(inode)) { 
                        return false;
                    }
                }
            }
        }
        buf.flush();
    }
    return true;
}

DFSRecoveryBuffer::DFSRecoveryBuffer(Socket* s, int failedNodeId, int redundancy)
{
    this->s = s;
    this->failedNodeId = failedNodeId;
    msg.hdr.op = DFSRequest::DFS_PREPARE_RECOVERY; 
    msg.hdr.redundancy = redundancy;
    currInode = msg.inodes;
}

bool DFSRecoveryBuffer::add(DFSINode* inode)
{
    size_t inodeSize = DFS_INODE_SIZE(msg.hdr.redundancy);
    if (currInode + inodeSize > msg.inodes + DFS_RECOVERY_BLOCK_SIZE) {
        if (!flush()) { 
            return false;
        }
    }
    memcpy(currInode, inode, inodeSize);
    currInode += inodeSize;
    return true;
}

bool DFSRecoveryBuffer::flush()
{
    if (currInode != msg.inodes) { 
        msg.hdr.size = currInode - msg.inodes;
        if (!s->write(&msg, sizeof(DFSRequest) + msg.hdr.size)) {
            DFS::instance.trace("Recovery failed", failedNodeId-1);
            return false;
        }
        currInode = msg.inodes;
    }
    return true;
}
    
void DFSRecoveryPipe::put(DFSRecoveryPipe::PipeElem* elem) 
{ 
    CriticalSection cs(mutex);
    if (first == NULL) { 
        first = elem;
    } else { 
        last->next = elem;
    }
    last = elem;
    elem->next = NULL;
    if (waiting) { 
        waiting = false;
        event.signal();
    }
}

DFSRecoveryPipe::PipeElem* DFSRecoveryPipe::get() 
{ 
    CriticalSection cs(mutex);
    while (first == NULL) { 
        waiting = true;
        event.wait();
    }
    PipeElem* elem = first;
    first = first->next;
    return elem;
}


DFS DFS::instance;

void DFS::getTrafficStatistic(TrafficInfo& info)
{
    int n = maxChannelNo;
    for (int i = 0; i < n; i++) { 
        DFSChannel& chan = channels[i];
        Socket* s = chan.s;
        info += chan.traffic;
        if (!chan.isDead && s != NULL) { 
            info += *s;
        }
    }
}

size_t DFS::poll(DFSState* state)
{
    DFSRequest req;
    int n = maxChannelNo;
    memset(state, 0, sizeof(DFSState)*n);
    for (int i = 0; i < n; i++) { 
        DFSChannel* chan = &channels[i];
        CriticalSection cs(chan->mutex);
        if (!chan->isDead) { 
            req.op = DFSRequest::DFS_GET_STATE;
            if ((size_t)chan->s->write(&req, sizeof(req), DFS_WRITE_TIMEOUT) != sizeof(req)
                || (size_t)chan->s->read(&state[i], sizeof(DFSState), sizeof(DFSState), DFS_READ_TIMEOUT) != sizeof(DFSState))
            {
                disconnect(chan);
            } 
        }
    }
    return n;
}
void* DFS::pingProc(void* arg)
{
    ((DFS*)arg)->ping();
    return NULL;
}

DFS::DFS()
: event(mutex)
{
    nOnlineChannels = 0;
    maxChannelNo = 0;
    log = NULL;
    verify = false;
    for (int i = 0; i < DFS_MAX_CLIQUES; i++) { 
        cliqueFirstNode[i] = DFS_MAX_NODES;
        cliqueLastNode[i] = 0;
    }
    createThread(pingThread, &pingProc, this);    
}

void DFS::ping()
{
    DFSRequest req;

    pthread_detach(pthread_self()); 
    while (true) { 
        sleep(DFS_PING_INTERVAL);
        for (int i = 0; i < maxChannelNo; i++) { 
            DFSChannel* chan = &channels[i];
            CriticalSection cs(chan->mutex);
            if (!chan->isDead) { 
                req.op = DFSRequest::DFS_PING;
                if ((size_t)chan->s->write(&req, sizeof(req), DFS_WRITE_TIMEOUT) != sizeof(req)) {
                    disconnect(chan);
                }
            }
        }
    }
}

void DFS::attachFile(DFSFile* file)
{
    CriticalSection cs(mutex);    
    file->next = files;
    files = file;
}

void DFS::detachFile(DFSFile* file)
{
    CriticalSection cs(mutex);    
    DFSFile** fpp = &files;
    while (*fpp != file) { 
        fpp = &(*fpp)->next;
    }
    *fpp = file->next;
}

void DFS::initiateRecovery(DFSRecoveryPipe* pipe, int failedNodeId)
{
    CriticalSection cs(mutex);
    for (DFSFile* file = files; file != NULL; file = file->next) {
		DFSRecoveryPipe::PipeElem* elem = new DFSRecoveryPipe::PipeElem();
		int redundancy = file->redundancy;
		if (redundancy > 1) {
			size_t inodeSize = DFS_INODE_SIZE(redundancy);
			size_t used = 0;
			elem->redundancy = redundancy;
			for (char *inodesBeg = (char*)file->inodes, *inodesEnd = inodesBeg + file->inodeTableSize; inodesBeg < inodesEnd; inodesBeg += inodeSize) {
				for (int j = 0; j < redundancy; j++) {
					DFSINode* inode = (DFSINode*)inodesBeg;
					if ((int)inode->addr[j].nodeId == failedNodeId) {
						if (used + inodeSize > DFS_RECOVERY_BLOCK_SIZE) {
							elem->size = used;
							pipe->put(elem);
							used = 0;
							elem = new DFSRecoveryPipe::PipeElem();
							elem->redundancy = redundancy;
						}
						memcpy(elem->inodes + used, inode, inodeSize);
						used += inodeSize;
					}
				}
			}
			if (used != 0) {
				elem->size = used;
				pipe->put(elem);
			} else {
				delete elem;
			}
		}
	}
}

void DFS::prepareRecovery(Socket* s, int failedNodeId)
{
    CriticalSection cs(mutex);    
    for (DFSFile* file = files; file != NULL; file = file->next) { 
        if (!file->prepareRecovery(s, failedNodeId)) { 
            delete s;
            return;
        }
    }
    DFSRequest req;
    req.op = DFSRequest::DFS_PREPARED;
    req.size = 0;
    if (!s->write(&req, sizeof(req))) {
        trace("Recovery failed", failedNodeId-1);
    }
    delete s;
}

void* DFS::readPreparedDataThread(void* arg)
{
    DFS::instance.readPreparedData((DFSRecoveryPipe*)arg);
    return NULL;
}

void* DFS::recoveryThread(void* arg)
{
    DFS::instance.recovery((DFSRecoveryPipe*)arg);
    return NULL;
}

void DFS::readPreparedData(DFSRecoveryPipe* pipe)
{
    DFSChannel* channel = pipe->channel;
    int nodeId = channel - channels;
    Socket* s = channel->s;
    DFSRequest req;

    while (true) { 
        DFSRecoveryPipe::PipeElem* elem = new DFSRecoveryPipe::PipeElem();
        if ((size_t)s->read(&req, sizeof(req), sizeof(req), DFS_READ_TIMEOUT) != sizeof(req)) { 
            trace("Failed to receive request with prepared recovery data", nodeId);        
            elem->size = -1;
            pipe->put(elem);
            break;
        }
        if (req.op == DFSRequest::DFS_PREPARED) { 
            elem->size = 0;
            pipe->put(elem);
            break;
        } else { 
            assert(req.op == DFSRequest::DFS_PREPARE_RECOVERY);
            elem->size = req.size;
            elem->redundancy = req.redundancy;
            if (s->read(&elem->inodes, req.size, req.size, DFS_READ_TIMEOUT) != (int)req.size) { 
                trace("Failed to receive prepared recovery data", nodeId);        
                break;
            }
            pipe->put(elem);
        }
    }
}
           
                
void DFS::recovery(DFSRecoveryPipe* pipe)
{
    DFSChannel* channel = pipe->channel;
    CriticalSection cs(channel->mutex);    
    offs32_t nodeId = channel - channels;
    Socket* s = channel->s;
    DFSRequest req;
    char* buf = new char[DFS_CHUNK_SIZE];
    req.size = 0;

    while (true) { 
        DFSRecoveryPipe::PipeElem* elem = pipe->get();
        int size = elem->size;
        if (size < 0) { 
            req.op = DFSRequest::DFS_RECOVERY_FAILURE;
            s->write(&req, sizeof(req));
            delete elem;
            break;
        } else if (size == 0) { 
            CriticalSection cs(mutex);           
            req.op = DFSRequest::DFS_RECOVERY_SUCCESS;
            delete elem;
            if (s->write(&req, sizeof(req))) {
                trace("Node successfully recovered", nodeId);
                channel->isDead = false;   
                delete[] buf;
                delete pipe;
                return;
            } else { 
                trace("Failed to send RECOVERED status", nodeId);        
            }
            break;
        }
        int redundancy = elem->redundancy;
        assert(redundancy > 1);
        size_t inodeSize = DFS_INODE_SIZE(redundancy);
        for (char *inodesBeg = elem->inodes, *inodesEnd = inodesBeg + size; inodesBeg < inodesEnd; inodesBeg += inodeSize) {
            DFSINode& inode = *(DFSINode*)inodesBeg;
            bool srcChunkAvailable = false;
            offs32_t dstChunkNo = DFS_INVALID_CHUNK;
            for (int j = 0; j < redundancy; j++) {
                if (inode.addr[j].nodeId != nodeId+1) {
                    if (!srcChunkAvailable) { 
                        DFSChannel* rc = DFS::instance.getChannel(inode.addr[j].nodeId);
                        CriticalSection cs(rc->mutex);
                        if (!rc->isDead) { 
                            req.op = DFSRequest::DFS_READ;
                            req.chunk = inode.addr[j].chunk;
                            if (rc->s->write(&req, sizeof(req)) 
                                && (size_t)rc->s->read(buf, DFS_CHUNK_SIZE, DFS_CHUNK_SIZE, DFS_READ_TIMEOUT) == DFS_CHUNK_SIZE
                                && calculateCRC(buf, DFS_CHUNK_SIZE) == inode.crc)
                            {
                                srcChunkAvailable = true;
                            } else { 
                                disconnect(rc);
                            }
                        }
                    } 
                } else { 
                    dstChunkNo = inode.addr[j].chunk;
                } 
            }
            assert(dstChunkNo != DFS_INVALID_CHUNK);
            if (srcChunkAvailable) { 
                req.op = DFSRequest::DFS_UPDATE;
                req.chunk = dstChunkNo;
                if (s->write(&req, sizeof(req)) && s->write(buf, DFS_CHUNK_SIZE)) {
                    continue;
                } else { 
                    trace("Node recovery failed: failed to write chunk of data", nodeId);
                }
            } else {
                trace("Node recovery failed: failed to read chunk of data", nodeId);
            }
            req.op = DFSRequest::DFS_RECOVERY_FAILURE;
            s->write(&req, sizeof(req));
            delete elem;
            goto epilogue;                           
        }
        delete elem;            
    }
  epilogue:
    channel->traffic += *s;
    delete s;
    channel->s = NULL;
    delete[] buf;
    delete pipe;
}


void DFS::addChannel(Socket* s, int id)
{
    int nodeId = id & DFS_NODE_ID_MASK;
    assert(nodeId < (int)DFS_MAX_NODES);
    int clique = (id & ~DFS_MARKERS) >> DFS_CLIQUE_OFFSET;
    DFSChannel* chan = &channels[nodeId];
    CriticalSection ccs(chan->mutex);    
    CriticalSection gcs(mutex);    
    if (!chan->isDead) { 
        trace("Node is reconnected", nodeId); 
        nOnlineChannels -= 1;
        chan->traffic += *chan->s;
        delete chan->s; 
        chan->s = NULL; 
        chan->isDead = true;
    }
    if (id & DFS_PREPARE_RECOVERY_MARKER) { 
        trace("Prepare recovery", nodeId); 
        prepareRecovery(s, nodeId+1);
        trace("Node is recovered", nodeId); 
    } else if (id & DFS_RECOVERY_MARKER) { 
        pthread_t t[2];
        chan->s = s;
        chan->peer = s->get_peer();
        chan->clique = clique;
        trace("Start recovery", nodeId); 
        DFSRecoveryPipe* pipe = new DFSRecoveryPipe(chan);
		initiateRecovery(pipe, nodeId+1);
        createThread(t[0], readPreparedDataThread, pipe);
        createThread(t[1], recoveryThread, pipe);
    } else { 
        chan->s = s;
        chan->peer = s->get_peer();
        chan->clique = clique;
        chan->isDead = false;
        event.signal();
        if (cliqueFirstNode[clique] > nodeId) { 
            cliqueFirstNode[clique] = nodeId;
        }
        if (cliqueLastNode[clique] < nodeId) { 
            cliqueLastNode[clique] = nodeId;
        }
        nOnlineChannels += 1;
        if (nodeId >= maxChannelNo) { 
            maxChannelNo = nodeId+1;
        }
        trace("New node attached", nodeId);    
    }
}

DFSChannel* DFS::getRandomNode(DFSINode& inode, int i, int clique)
{
    CriticalSection cs(mutex);
    while (cliqueFirstNode[clique] > cliqueLastNode[clique]) { 
        trace("Wait for online nodes", nOnlineChannels);
        event.wait(DFS_WAIT_FOR_ONLINE_NODES_TIMEOUT);
    }
    for (int nRetries = 0; ; nRetries++) { 
        int first = cliqueFirstNode[clique];
        int last = cliqueLastNode[clique];
        int nodeId = first + (unsigned)rand() % (last - first + 1); 
        if (!channels[nodeId].isDead && channels[nodeId].clique == clique) { 
#ifndef DO_NOT_SCATTER_NODES
            int j = i;
            unsigned peer = channels[nodeId].peer;
            while (--j >= 0 && peer != channels[inode.addr[j].nodeId-1].peer); 
            if (j < 0) 
#endif
            { 
                inode.addr[i].nodeId = nodeId+1;
                return &channels[nodeId];
            }
        }
        if (nRetries >= maxChannelNo*2) { 
            int nOnlineNodesForClique = 0;
            unsigned peer = -1;
            for (int j = first; j <= last; j++) { 
                if (!channels[j].isDead && channels[j].clique == clique && channels[j].peer != peer) { 
                    nOnlineNodesForClique += 1;
                    peer = channels[j].peer;
                }
            }
            if (nOnlineNodesForClique <= i) { 
                trace("Wait for online nodes", nOnlineChannels);
                event.wait(DFS_WAIT_FOR_ONLINE_NODES_TIMEOUT);
            }
            nRetries  = 0;
        }            
    }
}

void DFS::waitNode(int nodeId)
{
    CriticalSection cs(mutex);
    trace("Wait for node restart", nodeId-1);
    event.wait();    
}



void DFS::disconnect(DFSChannel* chan, char* msg)
{
    CriticalSection cs(mutex);
    if (!chan->isDead) { 
        char msgBuf[MAX_MSG_LENGTH];
        char errBuf[MAX_ERR_LENGTH];
        if (msg == NULL) { 
            chan->s->get_error_text(errBuf, sizeof(errBuf));
            sprintf(msgBuf, "Connection failed: %s", errBuf);
            msg = msgBuf;
        }
        trace(msg, (int)(chan - channels));
        chan->traffic += *chan->s;
        delete chan->s;
        chan->s = NULL;
        nOnlineChannels -= 1;
        chan->isDead = true;
    }
}

void DFS::trace(char const* msg, int nodeId)
{
    CriticalSection cs(logMutex);    
    if (log == NULL) { 
        char logName[LOG_NAME_SIZE];
        sprintf(logName, "dfs_%d.log", getpid());
        log = openLog(logName, "w");
    }
    char timeBuf[MAX_TIME_LENGTH];
    fprintf(log, "%s [%d] %s\n", getTime(timeBuf, sizeof timeBuf), nodeId, msg);
    fflush(log);
}

DFS::~DFS()
{
    if (log != NULL) { 
        fclose(log);
    }
}


void* DFS::listenProc(void* arg)
{
    DFS::instance.accept((char const*)arg);
    return NULL;
}

void DFS::listen(char const* address)
{
    createThread(listenThread, &listenProc, (void*)address);
}

void DFS::printSocketError(char const* operation, Socket* s, int node) 
{ 
    if (s != NULL) { 
        char errBuf[MAX_SOCKET_ERROR_LENGTH];
        s->get_error_text(errBuf, sizeof(errBuf));
        char msgBuf[MAX_SOCKET_ERROR_LENGTH*2];
        sprintf(msgBuf, "%s: %s", operation, errBuf);
        trace(msgBuf, node);
    } else { 
        trace(operation, node);
    }
}

void DFS::accept(char const* address) 
{
    Socket* gate = NULL;
    while (true) { 
        while (true) { 
            delete gate;
            if (strncmp(address, "localhost", 9) == 0) { 
                gate = Socket::create_local(address);
            } else { 
                gate = Socket::create_global(address);
            }
            assert(gate != NULL);
            if (gate->is_ok()) { 
                break;
            }
            printSocketError("DFS::accept::create", gate, -1);
            sleep(GATE_RECONNECT_INTERVAL);
        }
        while (true) {
            Socket* s = gate->accept();
            if (s == NULL || !s->is_ok()) { 
                printSocketError("DFS::accept::accept", s, -1);
                break;
            }
            int id;
            if (!s->read(&id, sizeof id)) { 
                printSocketError("DFS::accept::read", s, -1);
            } else { 
                assert((id & DFS_NODE_MARKER) != 0); 
                DFS::instance.addChannel(s, id);
            }
        }
    }
}     

VFileHandle::~VFileHandle() {}

bool DFSFileHandle::pread(void* buf, size_t bufSize, fsize_t pos, int flags)
{
    return file->read(pos, buf, bufSize, flags); 
}

void DFSFileHandle::pwrite(void* buf, size_t bufSize, fsize_t pos, int clique)
{
    assert(bufSize == DFS_CHUNK_SIZE);
    file->write(pos, buf, clique); 
}

bool DFSFileHandle::read(void* buf, size_t bufSize, int flags)
{
    if (file->read(currPos, buf, bufSize, flags)) { 
        currPos += bufSize;
        return true;
    }
    return false;
}

void DFSFileHandle::write(void* buf, size_t bufSize, int clique)
{
    assert(bufSize == DFS_CHUNK_SIZE);
    file->write(currPos, buf, clique); 
    currPos += bufSize;
}

off_t DFSFileHandle::seek(off_t offs, int whence)
{
    assert(whence == SEEK_SET || whence == SEEK_CUR);
    return currPos = ((whence == SEEK_SET) ? offs : currPos + offs);
}

void DFSFileHandle::prefetch(off_t offs, size_t size)
{
    file->prefetch(offs, size);
}

size_t DFSFileHandle::blockSize()
{
    return DFS_CHUNK_SIZE;
}

bool UnixFileHandle::pread(void* buf, size_t bufSize, fsize_t pos, int)
{
    diskRead(fd, buf, bufSize, pos);
    return true;
}

void UnixFileHandle::pwrite(void* buf, size_t bufSize, fsize_t pos, int)
{
    diskWrite(fd, buf, bufSize, pos, false);
}

bool UnixFileHandle::read(void* buf, size_t bufSize, int)
{
    char* dst = (char*)buf;
    while (bufSize != 0) {
        size_t size = bufSize > MAX_IO_BUF_SIZE ? MAX_IO_BUF_SIZE : bufSize;
        size_t rc = ::read(fd, dst, size);
        assert(rc == size);
        dst += rc;
        bufSize -= rc;
    }
    return true;
}

void UnixFileHandle::write(void* buf, size_t bufSize, int)
{
    char* src = (char*)buf;
    while (bufSize != 0) {
        size_t size = bufSize > MAX_IO_BUF_SIZE ? MAX_IO_BUF_SIZE : bufSize;
        size_t rc = ::write(fd, src, size);
        assert(rc == size);
        src += rc;
        bufSize -= rc;
    }
}

void UnixFileHandle::prefetch(off_t offs, size_t size)
{    
}

off_t UnixFileHandle::seek(off_t offs, int whence)
{
    offs = lseek(fd, offs, whence);
    assert(offs >= 0);
    return offs;
}

const int MAX_BLOCK_SIZE = 0x10000;

size_t UnixFileHandle::blockSize()
{
    struct stat st;
    int rc = fstat(fd, &st);
    assert(rc == 0);
    return st.st_blksize < MAX_BLOCK_SIZE ? st.st_blksize : MAX_BLOCK_SIZE;
}


BufferedOutput::BufferedOutput(size_t size, int clique) 
{ 
    buf = new char[size];
    this->size = size;
    this->clique = clique;
    used = 0;
}

void BufferedOutput::open(VFileHandle* vfh) 
{ 
    this->vfh = vfh;
    used = 0;
    blockSize = vfh->blockSize();
	assert((size & (blockSize-1)) == 0);
}

BufferedOutput::~BufferedOutput()
{
    delete[] buf;
}    

void BufferedOutput::write(void const* data, size_t len)
{
    char* src = (char*)data;
    if (used != 0) { 
        if (used + len < size) { 
            memcpy(buf + used, src, len);
            used += len;
            return;
        } else { 
            size_t n = size - used;
            memcpy(buf + used, src, n);
            vfh->write(buf, size, clique);
            src += n;
            len -= n;
            used = 0;
        }
    }
    while (len >= size) { 
        vfh->write(src, size, clique);
        src += size;
        len -= size;
    }
    used = len;
    memcpy(buf, src, len);
}

void BufferedOutput::flush(bool align)
{
    if (used > 0) { 
        size_t alignedSize = ALIGN(used, blockSize);
        vfh->write(buf, alignedSize, clique);
        if (align) { 
            used = 0;
        } else { 
            vfh->seek(-(off_t)alignedSize, SEEK_CUR);
        }
    }
}
