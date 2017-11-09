//-< UTIL.CPP >------------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Various support stuff
//-------------------------------------------------------------------*--------*

#include "util.h"
#include "dfs_client.h"
#include "debug.h"
#include "system.h"

#include <time.h>
#include <ctype.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#define FORCE_MAPPED_FILE_READ true

bool loadConfigurationFile(char const* cfgFileName, int nParams, char const* format,...)
{
    bool result = false;

    FILE* f = openLocalConfig(cfgFileName);    

    va_list ap;
    va_start(ap, format);
    size_t rc = fseek(f, 0, SEEK_END);
    assert(rc == 0);
    size_t size = ftell(f);
    rc = fseek(f, 0, SEEK_SET);
    assert(rc == 0);
    
    char* text = new char[size+1];
    rc = fread(text, 1, size, f);
    assert(rc == size);
    fclose(f);
    
    char* dst = text;
    for (size_t i = 0; i < size; i++) { 
        char ch = text[i];
        if (ch == '#') { 
            while (++i < size && text[i] != '\n');
            *dst++ = ' ';
            continue;
        }
        *dst++ = ch;
    }
    *dst = '\0';
    if (vsscanf(text, format, ap) != nParams) {
        fprintf(stderr, "Failed to read configuration file '%s'\n", cfgFileName);
    } else { 
        result = true;
    }
    delete[] text;
    va_end(ap);

    return result;
}

int strtrim(char* buf)
{
    int len = strlen(buf);
    while (len > 0 && (buf[len-1] == ' ' || buf[len-1] == '\n' || buf[len-1] == '\r')) { 
        len -= 1;
    }
    return len;
}

char* getTime(char* buf, size_t bufSize, char const* format)
{
    return getTime(time(NULL), buf, bufSize, format);
}
    
char* getTime(time_t time, char* buf, size_t bufSize, char const* format, bool useGmt)
{
    struct tm t;
    strftime(buf, bufSize-1, format, (useGmt) ? gmtime_r(&time, &t) : localtime_r(&time, &t));
    buf[bufSize-1] = '\0';
    return buf;
}

double getCurrentTime()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + (double)tv.tv_usec / 1000000;
}


FILE* openGlobalConfig(char const* fileName, bool optional)
{
    char const* filePath;
    char* buf = NULL;
    if (*fileName != '/' && *fileName != '.') { 
        char* configDir = System::getGlobalConfigDirectory();
        buf = new char[strlen(configDir) + strlen(fileName) + 1];
        strcat(strcpy(buf, configDir), fileName);
        filePath = buf;
    } else { 
        filePath = fileName;
    }
    FILE* f = fopen(filePath, "r");
    if (f == NULL) { 
        fprintf(stderr, "Failed to open file %s\n", filePath);
        assert(optional);
    }
    delete[] buf;
    return f;
}

FILE* openLocalConfig(char const* fileName, bool optional)
{
    char const* filePath;
    char* buf = NULL;
    if (*fileName != '/' && *fileName != '.') { 
        char* configDir = System::getLocalConfigDirectory();
        buf = new char[strlen(configDir) + strlen(fileName) + 1];
        strcat(strcpy(buf, configDir), fileName);
        filePath = buf;
    } else { 
        filePath = fileName;
    }
    FILE* f = fopen(filePath, "r");
    if (f == NULL) { 
        fprintf(stderr, "Failed to open file %s\n", filePath);
        assert(optional);
    }
    delete[] buf;
    return f;
}

FILE* openLog(char const* fileName, char const* mode)
{
    char* logDir = System::getLogDirectory();
    char* filePath = new char[strlen(logDir) + strlen(fileName) + 1];
    strcat(strcpy(filePath, logDir), fileName);
    FILE* f = fopen(filePath, mode);
    assert(f != NULL);
    delete[] filePath;
    return f;
}

const size_t WRITE_BUFFER_SIZE = DFS_CHUNK_SIZE;

void diskCopy(int dstFd, fsize_t dstOffs, int srcFd, fsize_t srcOffs, fsize_t size)
{
    void* buf = mmap(NULL, WRITE_BUFFER_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0);
    assert(buf != (void*)-1);
    while (size > 0) { 
        fsize_t count = size > WRITE_BUFFER_SIZE ? WRITE_BUFFER_SIZE : size;
        size_t rc = pread(srcFd, buf, count, srcOffs);
        assert(rc == count);
        rc = pwrite(dstFd, buf, count, dstOffs);
        assert(rc == count);
        srcOffs += count;
        dstOffs += count;
        size -= count;
    } 
    munmap(buf, WRITE_BUFFER_SIZE);
}

void diskRead(int fd, void* data, size_t size, fsize_t offs)
{
    char* dst = (char*)data;
    while (size > 0) { 
        long rc = pread(fd, dst, size, offs);
        assert(rc > 0);
        size -= rc;
        offs += rc;
        dst += rc;
    }
}

void diskWrite(int fd, void* data, size_t size, fsize_t offs, bool align)
{
    char* src = (char*)data;
    if (align) { 
        size = ALIGN(size, System::getPageSize());
    }
    while (size > 0) { 
        size_t blockSize = size > WRITE_BUFFER_SIZE ? WRITE_BUFFER_SIZE : size;
        long rc = pwrite(fd, src, blockSize, offs);
        assert(rc > 0);
        offs += rc;
        size -= rc;
        src += rc;
    }
}
 
bool socketWrite(Socket* s, void* data, size_t size, time_t timeout)
{
    if (s->write(&size, sizeof size)) {         
        char* src = (char*)data;
        while (size != 0) { 
            size_t chunk = size > WRITE_BUFFER_SIZE ? WRITE_BUFFER_SIZE : size;
            size_t written = s->write(src, chunk, timeout);
            if (written != chunk) { 
                return false;
            }
            size -= chunk;
            src += chunk;
        }
        return true;
    }
    return false;
}
        
bool socketWriteFromDisk(Socket* s, VFileHandle* fh, fsize_t offs, size_t size, time_t timeout)
{
    fh->seek(offs, SEEK_SET);
    if (s->write(&size, sizeof size)) {         
        char* buf = new char[WRITE_BUFFER_SIZE];
        while (size != 0) {
            size_t chunk = size > WRITE_BUFFER_SIZE ? WRITE_BUFFER_SIZE : size;
            fh->read(buf, chunk);
            if (!s->write(buf, chunk, timeout)) { 
                delete[] buf;
                return false;
            }
            size -= chunk;
        }
        delete[] buf;
        return true;
    }
    return false;
}
            
    
bool socketReadToDisk(Socket* s, VFileHandle* fh, fsize_t offs)
{
    size_t blockSize = fh->blockSize();
    size_t size; 
    fh->seek(offs, SEEK_SET);
    if (s->read(&size, sizeof size)) {         
        char* buf = new char[WRITE_BUFFER_SIZE];
        while (size != 0) {
            size_t chunk = size > WRITE_BUFFER_SIZE ? WRITE_BUFFER_SIZE : size;
            if (!s->read(buf, chunk)) { 
                delete[] buf;
                return false;
            }
            fh->write(buf, ALIGN(chunk, blockSize));
            size -= chunk;
        }
        delete[] buf;
        return true;
    }
    return false;
}
            
bool socketReadToDisk(Socket* s, VFileHandle* fh, fsize_t offs, size_t& size)
{
    size_t blockSize = fh->blockSize();
    fh->seek(offs, SEEK_SET);
    if (s->read(&size, sizeof size)) {         
        fh->write(&size, sizeof(size));
        char* buf = new char[WRITE_BUFFER_SIZE];
        size_t len = size;
        while (len != 0) {
            size_t chunk = len > WRITE_BUFFER_SIZE ? WRITE_BUFFER_SIZE : len;
            if (!s->read(buf, chunk)) { 
                delete[] buf;
                return false;
            }
            fh->write(buf, ALIGN(chunk, blockSize));
            len -= chunk;
        }
        delete[] buf;
        return true;
    }
    return false;
}
     
void getMemoryStatistic(void* addr, size_t size, size_t& ram, size_t& disk, size_t& regions)
{
    size_t ps = getpagesize();
    size_t nPages = (size + ps - 1) / ps;
    unsigned char* map = new unsigned char[nPages];
    int rc = mincore(addr, size, map);
    assert(rc == 0);
    size_t nResidents = 0;
    size_t nRegions = 0;
    bool swapped = true;
    for (size_t i = 0; i < nPages; i++) { 
        if (map[i] & 1) { 
            nRegions += swapped;
            swapped = false;
            nResidents += 1;
        } 
    }
    ram = nResidents;
    disk = nPages - nResidents;
    regions = nRegions;
    delete[] map;
}

int forceRead(void* ptr, size_t size, bool lock)
{
    int sum = 0;
    #if 0
    if (lock) { 
        int rc = mlock(ptr, size);
        assert(rc == 0);
    } else 
    #endif
    { 
#if FORCE_MAPPED_FILE_READ
        size_t pageSize = System::getPageSize();
        char* beg = (char*)ptr;
        char* end = beg + size;
        while (beg < end) { 
            sum += *beg;
            beg += pageSize;
        }    
    }
#endif
    return sum;
}

void getAbsoluteTime(struct timespec& abs_ts, time_t deltaMsec)
{
#ifdef PTHREAD_GET_EXPIRATION_NP
    struct timespec rel_ts; 
    rel_ts.tv_sec = deltaMsec/1000; 
    rel_ts.tv_nsec = deltaMsec%1000*1000000;
    pthread_get_expiration_np(&rel_ts, &abs_ts);
#else
    struct timeval cur_tv;
    gettimeofday(&cur_tv, NULL);
    abs_ts.tv_sec = cur_tv.tv_sec + deltaMsec/1000; 
    abs_ts.tv_nsec = cur_tv.tv_usec*1000 + deltaMsec%1000*1000000;
    if (abs_ts.tv_nsec >= 1000000000) { 
        abs_ts.tv_nsec -= 1000000000;
        abs_ts.tv_sec += 1;
    }
#endif    
}

crc_t calculateCRC(char const* content, size_t contentLength)
{
    static const unsigned table [] = {
        0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA,
        0x076DC419, 0x706AF48F, 0xE963A535, 0x9E6495A3,
        0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988,
        0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91,
        
        0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE,
        0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
        0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC,
        0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5,
        
        0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172,
        0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B,
        0x35B5A8FA, 0x42B2986C, 0xDBBBC9D6, 0xACBCF940,
        0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59,
        
        0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116,
        0x21B4F4B5, 0x56B3C423, 0xCFBA9599, 0xB8BDA50F,
        0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924,
        0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D,
        
        0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A,
        0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
        0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818,
        0x7F6A0DBB, 0x086D3D2D, 0x91646C97, 0xE6635C01,
        
        0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E,
        0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457,
        0x65B0D9C6, 0x12B7E950, 0x8BBEB8EA, 0xFCB9887C,
        0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65,
        
        0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2,
        0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB,
        0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0,
        0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9,
        
        0x5005713C, 0x270241AA, 0xBE0B1010, 0xC90C2086,
        0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F,
        0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4,
        0x59B33D17, 0x2EB40D81, 0xB7BD5C3B, 0xC0BA6CAD,
        
        0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A,
        0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683,
        0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8,
        0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1,
        
        0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE,
        0xF762575D, 0x806567CB, 0x196C3671, 0x6E6B06E7,
        0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC,
        0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5,
        
        0xD6D6A3E8, 0xA1D1937E, 0x38D8C2C4, 0x4FDFF252,
        0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B,
        0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60,
        0xDF60EFC3, 0xA867DF55, 0x316E8EEF, 0x4669BE79,
        
        0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236,
        0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F,
        0xC5BA3BBE, 0xB2BD0B28, 0x2BB45A92, 0x5CB36A04,
        0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D,
        
        0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A,
        0x9C0906A9, 0xEB0E363F, 0x72076785, 0x05005713,
        0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38,
        0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21,
        
        0x86D3D2D4, 0xF1D4E242, 0x68DDB3F8, 0x1FDA836E,
        0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
        0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C,
        0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45,
        
        0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2,
        0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB,
        0xAED16A4A, 0xD9D65ADC, 0x40DF0B66, 0x37D83BF0,
        0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9,
        
        0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6,
        0xBAD03605, 0xCDD70693, 0x54DE5729, 0x23D967BF,
        0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94,
        0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D
    };
    
    crc_t crc = 0xffffffff;
    unsigned char* buffer = (unsigned char*)content;

    while (contentLength--) {
          crc = (crc >> 8) ^ table[(crc & 0xFF) ^ *buffer++];
    }
    return crc^0xffffffff;
}

const size_t THREAD_STACK_SIZE = 1024*1024;

void createThread(pthread_t& thread, thread_proc_t proc, void* arg)
{
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    int rc = pthread_attr_setstacksize(&attr, THREAD_STACK_SIZE);
    assert(rc == 0);
    rc = pthread_create(&thread, &attr, proc, arg);
    assert(rc == 0);
}

bool arePartsOfSameFile(char const* url1, char const* url2) 
{ 
    if (strcmp(url1, url2) == 0) { 
        return false;
    }            
    while (true) { 
        while (*url1 == *url2) { 
            if (*url1 == '\0') { 
                return true;
            }
            url1 += 1;
            url2 += 1;
        }
        if (!isdigit(*url1 & 0xFF) || !isdigit(*url2 & 0xFF)) {
            return false;
        }        
        while (isdigit(*++url1 & 0xFF));
        while (isdigit(*++url2 & 0xFF));
    }
}
