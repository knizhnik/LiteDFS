//-< TYPES.H >-----------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Some internal type and constant definitions
//-------------------------------------------------------------------*--------*

#ifndef __TYPES_H__
#define __TYPES_H__

#include <time.h>
#include <stddef.h>
#include <inttypes.h>

typedef uint32_t crc_t;
typedef uint32_t offs32_t;
typedef uint64_t fsize_t;

const size_t MB = 1024*1024;
const size_t GB = 1024*MB;
const time_t SECONDS_IN_DAY = 24*3600;


#endif
