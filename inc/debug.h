//-< DEBUG.H >-------------------------------------------------------*--------*
// LiteDFS                   Version 1.0         (c) 2010  GARRET    *     ?  *
// (Lightweight Distributed File System)                             *   /\|  *
//                                                                   *  /  \  *
//                          Created:     17-Aug-2010  K.A. Knizhnik  * / [] \ *
//                          Last update: 17-Aug-2010  K.A. Knizhnik  * GARRET *
//-------------------------------------------------------------------*--------*
// Debugging utilities
//-------------------------------------------------------------------*--------*

#ifndef __DEBUG_H__
#define __DEBUG_H__

extern bool sigtermReceived;
extern bool signalCatched;

extern void installSignalHandlers();
extern int assertFailure(char const* file, int line, char const* expr);

#define assert(x) ((x) ? 0 : assertFailure(__FILE__, __LINE__, #x))
#define paranoid_assert(x) assert(x)
//#define paranoid_assert(x) 

#endif
