//-< SYSTEM.H >------------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// System specific information
//-------------------------------------------------------------------*--------*

#ifndef __SYSTEM_H__
#define __SYSTEM_H__

#include "types.h"

class System {
  public:
    /**
     * Get amount of available physical memory in system
     */
    static size_t getAvailableMemory();

    /**
     * Directory for node local files
     */
    static char*  getWorkDirectory();

    /**
     * Get system page size
     */
    static size_t getPageSize();

    /**
     * Get path to the directory where files with generated configuration are located
     */
    static char* getGlobalConfigDirectory();

    /**
     * Get path to the directory where local configuration files are located
     */
    static char* getLocalConfigDirectory();

    /**
     * Get path to the directory where variable files should be placed.
     */
    static char* getVarDirectory();
    
    /**
     * Get path to the directory where log files should be placed.
     */
    static char* getLogDirectory();

    /**
     * Get path to the descriptor of DFS file cache
     */
    static char* getDfsFileCacheDescriptor();

    /**
     * Reset file read cache
     * @param fd file descriptor
     */
    static void resetCache(int fd);
};

#endif
