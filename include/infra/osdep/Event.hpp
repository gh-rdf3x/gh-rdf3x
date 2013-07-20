#ifndef H_infra_osdep_Event
#define H_infra_osdep_Event
//---------------------------------------------------------------------------
#include "infra/Config.hpp"
//---------------------------------------------------------------------------
#ifdef CONFIG_WINDOWS
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <pthread.h>
#endif
//---------------------------------------------------------------------------
class Mutex;
//---------------------------------------------------------------------------
/// Wait primitive
class Event
{
   private:
#ifdef CONFIG_WINDOWS
   CRITICAL_SECTION unblockLock;

   /// Auto-Reset event
   void* blockLock;
   /// Auto-Reset event
   void* blockQueueSignal;
   /// Manual-Reset event
   void* blockQueueBroadcast;
   //// Broadcast state
   enum { NoBroadcast, Broadcast, BroadcastAfterSignal } broadcast;
   /// Counters
   int waitersGone,waitersBlocked,waitersToUnblock;
#else
   pthread_cond_t condVar;
#endif

   Event(const Event&);
   void operator=(const Event&);

   public:
   /// Constructor
   Event();
   /// Destructor
   ~Event();

   /// Wait for event. The mutex must be locked.
   void wait(Mutex& mutex);
   /// Wait up to a certain time. The mutex must be locked.
   bool timedWait(Mutex& mutex,unsigned timeoutMilli);
   /// Notify at least one waiting thread
   void notify(Mutex& mutex);
   /// Notify all waiting threads
   void notifyAll(Mutex& mutex);
};
//---------------------------------------------------------------------------
#endif
