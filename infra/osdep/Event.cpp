#include "infra/osdep/Event.hpp"
#include "infra/osdep/Mutex.hpp"
//---------------------------------------------------------------------------
#ifdef CONFIG_WINDOWS
#else
#include <errno.h>
#include <sys/time.h>
#endif
//---------------------------------------------------------------------------
// Win32 Implementation based on an algorithm by Alexander Terekhov
// discussed on win32-pthreads.
//---------------------------------------------------------------------------
Event::Event()
   // Constructor
#ifdef CONFIG_WINDOWS
   : blockLock(CreateEvent(0,false,true,0)),blockQueueSignal(CreateEvent(0,false,false,0)),
     blockQueueBroadcast(CreateEvent(0,true,false,0)),broadcast(NoBroadcast),waitersGone(0),
     waitersBlocked(0),waitersToUnblock(0)
#endif
{
#ifdef CONFIG_WINDOWS
   InitializeCriticalSection(&unblockLock);
#else
   pthread_cond_init(&condVar,0);
#endif
}
//---------------------------------------------------------------------------
Event::~Event()
   // Destructor
{
#ifdef CONFIG_WINDOWS
   DeleteCriticalSection(&unblockLock);
   CloseHandle(blockQueueBroadcast);
   CloseHandle(blockQueueSignal);
   CloseHandle(blockLock);
#else
   pthread_cond_destroy(&condVar);
#endif
}
//---------------------------------------------------------------------------
void Event::wait(Mutex& mutex)
   // Wait for event. The mutex must be locked.
{
#ifdef CONFIG_WINDOWS
   timedWait(mutex,INFINITE);
#else
   pthread_cond_wait(&condVar,&(mutex.mutex));
#endif
}
//---------------------------------------------------------------------------
bool Event::timedWait(Mutex& mutex,unsigned timeoutMilli)
   // Wait up to a certain time. The mutex must be locked.
{
#ifdef CONFIG_WINDOWS
   WaitForSingleObject(blockLock,INFINITE);
   waitersBlocked++;
   SetEvent(blockLock);

   mutex.unlock();
   HANDLE handles[2]={blockQueueSignal,blockQueueBroadcast};
   bool timeout=(WaitForMultipleObjects(2,handles,false,timeoutMilli)==WAIT_TIMEOUT);

   EnterCriticalSection(&unblockLock);
   int signalsWasLeft=waitersToUnblock;
   int waitersWasGone=0;
   bool wasBroadcast=false;
   if (signalsWasLeft) {
      if (timeout) {
         if (waitersBlocked) {
            waitersBlocked--;
            signalsWasLeft=0;
         } else if (broadcast==Broadcast) {
            waitersGone=1;
         }
      }
      if (!--waitersToUnblock) {
         if (!waitersBlocked) {
            SetEvent(blockLock);
            signalsWasLeft=0;
         } else {
            if (broadcast!=NoBroadcast) { wasBroadcast=true; broadcast=NoBroadcast; }
            if ((waitersWasGone=waitersGone)!=0) waitersGone=0;
         }
      }
   } else if ((++waitersGone)==(1<<(8*sizeof(int)-2))) {
      WaitForSingleObject(blockLock,INFINITE);
      waitersBlocked-=waitersGone;
      SetEvent(blockLock);
      waitersGone=0;
   }
   LeaveCriticalSection(&unblockLock);

   if (signalsWasLeft==1) {
      if (wasBroadcast) ResetEvent(blockQueueBroadcast);
      if (waitersWasGone) ResetEvent(blockQueueSignal);
      SetEvent(blockLock);
   } else if (signalsWasLeft) {
      SetEvent(blockQueueSignal);
   }

   mutex.lock();

   return !timeout;
#else
   struct timeval now; gettimeofday(&now,0);
   uint64_t nowT=(static_cast<uint64_t>(now.tv_sec)*1000)+(now.tv_usec/10000);
   uint64_t future=nowT+timeoutMilli;
   struct timespec abstime;
   abstime.tv_sec=future/1000; abstime.tv_nsec=(future%1000)*1000000;
   return pthread_cond_timedwait(&condVar,&(mutex.mutex),&abstime)!=ETIMEDOUT;
#endif
}
//---------------------------------------------------------------------------
void Event::notify(Mutex& /*mutex*/)
   // Notify at least one waiting thread
{
#ifdef CONFIG_WINDOWS
   EnterCriticalSection(&unblockLock);

   if (waitersToUnblock) {
      if (waitersBlocked) {
         waitersToUnblock++;
         waitersBlocked--;
      } else {
         // Nobody is waiting ...
      }
      LeaveCriticalSection(&unblockLock);
      return;
   } else if (waitersBlocked>waitersGone) {
      WaitForSingleObject(blockLock,INFINITE);
      if (waitersGone) {
         waitersBlocked-=waitersGone;
         waitersGone=0;
      }
      waitersToUnblock = 1;
      waitersBlocked--;
   } else {
      LeaveCriticalSection(&unblockLock);
      return;
   }
   LeaveCriticalSection(&unblockLock);
   SetEvent(blockQueueSignal);
#else
   pthread_cond_signal(&condVar);
#endif
}
//---------------------------------------------------------------------------
void Event::notifyAll(Mutex& /*mutex*/)
   // Notify all waiting threads
{
#ifdef CONFIG_WINDOWS
   EnterCriticalSection(&unblockLock);

   if (waitersToUnblock) {
      if (!waitersBlocked) {
         // Nobody is waiting ...
         LeaveCriticalSection(&unblockLock);
         return;
      }
      waitersToUnblock+=waitersBlocked;
      waitersBlocked=0;
      broadcast=BroadcastAfterSignal;
   } else if (waitersBlocked>waitersGone) {
      WaitForSingleObject(blockLock,INFINITE);
      if (waitersGone) {
         waitersBlocked-=waitersGone;
         waitersGone=0;
      }
      waitersToUnblock=waitersBlocked;
      waitersBlocked=0;
      broadcast=Broadcast;
   } else {
      LeaveCriticalSection(&unblockLock);
      return;
   }
   LeaveCriticalSection(&unblockLock);
   SetEvent(blockQueueBroadcast);
#else
   pthread_cond_broadcast(&condVar);
#endif
}
//---------------------------------------------------------------------------
