//-< SYSTEM.CPP >----------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// System specific information
//-------------------------------------------------------------------*--------*

#include "system.h"
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#ifdef __linux__
#include <sys/mount.h>
#include <sys/sysinfo.h>
#endif

size_t System::getAvailableMemory() 
{

#ifdef __linux__
    struct sysinfo info;
    sysinfo(&info);
#ifdef SYSINFO_HAS_NO_MEM_UNIT
    return info.totalram;
#else
    return info.totalram*info.mem_unit;
#endif
#else 
    return (size_t)1 << 31;
#endif
}

char* System::getWorkDirectory() 
{ 
    return "./";
}
 
size_t System::getPageSize()
{
#ifdef __linux__
    return getpagesize();
#else
    return 4096;
#endif
}

char* System::getGlobalConfigDirectory()
{
    char* root = getenv("GLOBAL_CONFIG_DIRECTORY");
    return root != NULL ? root : (char*)"./";    
}

char* System::getLocalConfigDirectory()
{
    char* root = getenv("LOCAL_CONFIG_DIRECTORY");
    return root != NULL ? root : (char*)"./";    
}

char* System::getVarDirectory()
{
    char* root = getenv("VAR_CONFIG_DIRECTORY");
    return root != NULL ? root : (char*)"./";    
}

char* System::getLogDirectory()
{
    char* root = getenv("LOG_DIRECTORY");
    return root != NULL ? root : (char*)"./";    
}

char* System::getDfsFileCacheDescriptor()
{
    return getenv("DFS_CACHE");
}


void System::resetCache(int fd)
{
#ifdef __linux__
    ioctl(fd, BLKFLSBUF, 0); // reset file read cache
#endif
}
