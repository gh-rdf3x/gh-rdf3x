#include "infra/osdep/Thread.hpp"
#ifdef CONFIG_WINDOWS
# ifndef _WIN32_WINNT
#  define _WIN32_WINNT 0x0500
# endif
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <process.h>
#else
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>
#endif
//---------------------------------------------------------------------------
namespace std {}
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
bool Thread::start(void (*starter)(void*),void* arg,bool boost)
   // Create a new thread
{
#ifdef CONFIG_WINDOWS
   unsigned long handle=_beginthread(starter,4096,arg);
   if (handle==0xFFFFFFFFul) return false;
   if (!boost)
      return true;
   return SetThreadPriority(reinterpret_cast<HANDLE>(handle),THREAD_PRIORITY_ABOVE_NORMAL);
#else
   // Avoid a warning, boost is not supported for Unix currently
   if (boost)
      arg=arg;

   // Initialize some attributes
   pthread_t tid=0;
   pthread_attr_t pattrs;
   pthread_attr_init(&pattrs);
   // Not required for Linux and valgrind does not like it
#ifndef __linux__
   pthread_attr_setscope(&pattrs,PTHREAD_SCOPE_SYSTEM);
#endif
   int result=pthread_create(&tid,&pattrs,reinterpret_cast<void*(*)(void*)>(starter),arg);
   pthread_attr_destroy(&pattrs);
   if (result!=0)
      return false;
   pthread_detach(tid);

   return true;
#endif
}
//---------------------------------------------------------------------------
uint64_t Thread::guessPhysicalAvailable()
   // Guess the physical available memory
{
#ifdef CONFIG_WINDOWS
   MEMORYSTATUSEX status;
   status.dwLength=sizeof(status);
   GlobalMemoryStatusEx(&status);
   // At least 25% of the physical memory should be usable
   if (status.ullAvailPhys<(status.ullTotalPhys>>2))
      return status.ullTotalPhys>>2; else
      return status.ullAvailPhys;
#elif defined(__APPLE__)
   return 8*1024*1024; // Just guess for now
#else
   long long pageCount=sysconf(_SC_PHYS_PAGES);
   long long pageSize=sysconf(_SC_PAGESIZE);

   return (pageCount*pageSize)>>2; // Guess 25%
#endif
}
//---------------------------------------------------------------------------
void Thread::sleep(unsigned time)
   // Wait x ms
{
#ifdef CONFIG_WINDOWS
   Sleep(time);
#else
   if (!time) {
      sched_yield();
   } else {
      struct timespec a,b;
      a.tv_sec=time/1000; a.tv_nsec=(time%1000)*1000000;
      nanosleep(&a,&b);
   }
#endif
}
//---------------------------------------------------------------------------
long Thread::threadID()
   // Threadid
{
#ifdef CONFIG_WINDOWS
   return static_cast<long>(GetCurrentThreadId());
#else
   union { pthread_t a; long b; } c;
   c.b=0;
   c.a=pthread_self();
   return c.b;
#endif
}
//---------------------------------------------------------------------------
void Thread::yield()
   // Activate other threads
{
#ifndef CONFIG_NOTHREADS
#ifdef CONFIG_WINDOWS
   Sleep(0);
#else
   sched_yield();
#endif
#endif
}
//---------------------------------------------------------------------------
uint64_t Thread::getTicks()
   // Get the current time in ms
{
#ifdef CONFIG_WINDOWS
   return GetTickCount();
#else
   timeval t;
   gettimeofday(&t,0);
   return static_cast<uint64_t>(t.tv_sec)*1000+(t.tv_usec/1000);
#endif
}
//---------------------------------------------------------------------------
