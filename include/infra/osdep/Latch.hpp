#ifndef H_infra_osdep_Latch
#define H_infra_osdep_Latch
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
#include "infra/Config.hpp"
//---------------------------------------------------------------------------
#ifdef CONFIG_WINDOWS
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <pthread.h>
#endif
//---------------------------------------------------------------------------
/// A read writer lock
class Latch
{
   private:
#ifdef CONFIG_WINDOWS
   /// Protection for the lock
   CRITICAL_SECTION lock;
   /// The event
   void* event;
   /// THe latch state
   int state;
   /// Waiting threads
   int exclusiveWaitCounter,sharedWaitCounter;

   /// Lock exclusive. A conflict occurred
   void doLockExclusive();
   /// Lock shared. A conflict occurred
   void doLockShared();
   /// Notify
   void notify();
#else
   /// The real latch
   pthread_rwlock_t lock;
#endif

   Latch(const Latch&);
   void operator=(const Latch&);

   public:
   /// Constructor
   Latch();
   /// Destructor
   ~Latch();

   /// Lock exclusive
   void lockExclusive();
   /// Try to lock exclusive
   bool tryLockExclusive();
   /// Lock shared
   void lockShared();
   /// Try to lock shared
   bool tryLockShared();
   /// Release the lock. Returns true if the lock seems to be free now (hint only)
   bool unlock();
};
//---------------------------------------------------------------------------
#endif
