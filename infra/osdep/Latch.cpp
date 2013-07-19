#include "infra/osdep/Latch.hpp"
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
Latch::Latch()
   // Constructor
#ifdef CONFIG_WINDOWS
   : event(0),state(0),exclusiveWaitCounter(0),sharedWaitCounter(0)
#endif
{
#ifdef CONFIG_WINDOWS
   InitializeCriticalSection(&lock);
#else
   pthread_rwlock_init(&lock,0);
#endif
}
//---------------------------------------------------------------------------
Latch::~Latch()
   // Destructor
{
#ifdef CONFIG_WINDOWS
   if (event) CloseHandle(event);
   DeleteCriticalSection(&lock);
#else
   pthread_rwlock_destroy(&lock);
#endif
}
//---------------------------------------------------------------------------
void Latch::lockExclusive()
   // Lock exclusive
{
#ifdef CONFIG_WINDOWS
   EnterCriticalSection(&lock);
   if (state!=0) {
      ++exclusiveWaitCounter;
      if (!event) event=CreateEvent(0,true,false,0);
      while (true) {
         LeaveCriticalSection(&lock);
         WaitForSingleObject(event,INFINITE);
         EnterCriticalSection(&lock);
         ResetEvent(event);
         if (state==0) break;
      }
      --exclusiveWaitCounter;
   }
   state=-1;
   LeaveCriticalSection(&lock);
#else
   pthread_rwlock_wrlock(&lock);
#endif
}
//---------------------------------------------------------------------------
bool Latch::tryLockExclusive()
   // Try to lock exclusive
{
#ifdef CONFIG_WINDOWS
   EnterCriticalSection(&lock);
   bool result;
   if (state!=0) {
      result=false;
   } else {
      result=true;
      state=-1;
   }
   LeaveCriticalSection(&lock);
   return result;
#else
   return pthread_rwlock_trywrlock(&lock)==0;
#endif
}
//---------------------------------------------------------------------------
void Latch::lockShared()
   // Lock shared
{
#ifdef CONFIG_WINDOWS
   EnterCriticalSection(&lock);
   if ((state<0)||(exclusiveWaitCounter)) {
      ++sharedWaitCounter;
      if (!event) event=CreateEvent(0,true,false,0);
      while (true) {
         LeaveCriticalSection(&lock);
         WaitForSingleObject(event,INFINITE);
         EnterCriticalSection(&lock);
         ResetEvent(event);
         if (state>=0) break;
      }
      --sharedWaitCounter;
   }
   ++state;
   LeaveCriticalSection(&lock);
#else
   pthread_rwlock_rdlock(&lock);
#endif
}
//---------------------------------------------------------------------------
bool Latch::tryLockShared()
   // Try to lock stared
{
#ifdef CONFIG_WINDOWS
   EnterCriticalSection(&lock);
   bool result;
   if ((state<0)||(exclusiveWaitCounter)) {
      result=false;
   } else {
      result=true;
      ++state;
   }
   LeaveCriticalSection(&lock);
   return result;
#else
   return pthread_rwlock_tryrdlock(&lock)==0;
#endif
}
//---------------------------------------------------------------------------
bool Latch::unlock()
   // Release the lock
{
#ifdef CONFIG_WINDOWS
   EnterCriticalSection(&lock);
   bool result=false;
   if (state<0) {
      ++state;
      if (!state) {
         if ((exclusiveWaitCounter)||(sharedWaitCounter))
            SetEvent(event); else
            result=true;
      }
   } else {
      --state;
      if (!state) {
         if ((exclusiveWaitCounter)||(sharedWaitCounter))
            SetEvent(event); else
            result=true;
      }
   }
   LeaveCriticalSection(&lock);
   return result;
#else
   pthread_rwlock_unlock(&lock);
   #ifdef CONFIG_PTHREAD_RWLOCK_READERS
      return !&lock->CONFIG_PTHREAD_RWLOCK_READERS;
   #else
      return true; // Raten. XXX Bessere Ueberpruefung?
   #endif
#endif
}
//---------------------------------------------------------------------------
