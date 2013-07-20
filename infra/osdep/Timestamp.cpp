#include "infra/osdep/Timestamp.hpp"
#if defined(WIN32)||defined(__WIN32__)||defined(_WIN32)
#define CONFIG_WINDOWS
#endif
#ifdef CONFIG_WINDOWS
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <sys/time.h>
#endif
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
Timestamp::Timestamp()
   // Constructor
{
#ifdef CONFIG_WINDOWS
   QueryPerformanceCounter(static_cast<LARGE_INTEGER*>(ptr()));
#else
   gettimeofday(static_cast<timeval*>(ptr()),0);
#endif
}
//---------------------------------------------------------------------------
unsigned Timestamp::operator-(const Timestamp& other) const
   // Difference in ms
{
#ifdef CONFIG_WINDOWS
   LARGE_INTEGER freq;
   QueryPerformanceFrequency(&freq);
   return static_cast<unsigned>(((static_cast<const LARGE_INTEGER*>(ptr())[0].QuadPart-static_cast<const LARGE_INTEGER*>(other.ptr())[0].QuadPart)*1000)/freq.QuadPart);
#else
   long long a=static_cast<long long>(static_cast<const timeval*>(ptr())->tv_sec)*1000+static_cast<const timeval*>(ptr())->tv_usec/1000;
   long long b=static_cast<long long>(static_cast<const timeval*>(other.ptr())->tv_sec)*1000+static_cast<const timeval*>(other.ptr())->tv_usec/1000;
   return a-b;
#endif
}
//---------------------------------------------------------------------------
AvgTime::AvgTime()
   : count(0)
   // Constructor
{
#ifdef CONFIG_WINDOWS
   *static_cast<__int64*>(ptr())=0;
#else
   *static_cast<long long*>(ptr())=0;
#endif
}
//---------------------------------------------------------------------------
void AvgTime::add(const Timestamp& start,const Timestamp& stop)
   // Add an interval
{
#ifdef CONFIG_WINDOWS
   *static_cast<__int64*>(ptr())+=static_cast<const LARGE_INTEGER*>(stop.ptr())[0].QuadPart-static_cast<const LARGE_INTEGER*>(start.ptr())[0].QuadPart;
#else
   long long a=static_cast<long long>(static_cast<const timeval*>(stop.ptr())->tv_sec)*1000000+static_cast<const timeval*>(stop.ptr())->tv_usec;
   long long b=static_cast<long long>(static_cast<const timeval*>(start.ptr())->tv_sec)*1000000+static_cast<const timeval*>(start.ptr())->tv_usec;
   *static_cast<long long*>(ptr())+=a-b;
#endif
   count++;
}
//---------------------------------------------------------------------------
double AvgTime::avg() const
   // Average
{
   if (!count) return 0;
   double val;
#ifdef CONFIG_WINDOWS
   LARGE_INTEGER freq;
   QueryPerformanceFrequency(&freq);
   val=(static_cast<const __int64*>(ptr())[0]*1000)/freq.QuadPart;
#else
   val=(*static_cast<const long long*>(ptr())/1000);
#endif
   val/=count;
   return val;
}
//---------------------------------------------------------------------------
