#ifndef H_infra_osdep_Mutex
#define H_infra_osdep_Mutex
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
class Event;
//---------------------------------------------------------------------------
/// Mutex
/** System dependent. Under UNIX a thread cannot lock the same mutex swice.
  */
class Mutex
{
   private:
#ifdef CONFIG_WINDOWS
   CRITICAL_SECTION mutex;
#else
   pthread_mutex_t mutex;
#endif

   // Allow Event to access the handle for pthreads
   friend class Event;

   Mutex(const Mutex&);
   void operator=(const Mutex&);

   public:
   /// Constructor
   Mutex();
   /// Destructor
   ~Mutex();

   /// Lock the mutex
   void lock();
   /// Try to lock the mutex
   bool tryLock();
   /// Unlock the mutex
   void unlock();
};
//---------------------------------------------------------------------------
/// Locker object
class auto_lock
{
   private:
   Mutex& lock;

   auto_lock(const auto_lock&);
   void operator=(const auto_lock&);

   public:
   auto_lock(Mutex& l) : lock(l) { l.lock(); }
   ~auto_lock() { lock.unlock(); }
};
//---------------------------------------------------------------------------
#endif
