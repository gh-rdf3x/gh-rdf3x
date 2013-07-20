#ifndef H_infra_osdep_Thread
#define H_infra_osdep_Thread
//---------------------------------------------------------------------------
#include "infra/Config.hpp"
//---------------------------------------------------------------------------
/// Thread functions
class Thread
{
   public:
   /// Create a new thread
   static bool start(void (*starter)(void*),void* arg,bool boost=false);

   /// Available physical memory
   static uint64_t guessPhysicalAvailable();
   /// Wait x ms
   static void sleep(unsigned time);
   /// Get the thread id
   static long threadID();
   /// Activate the next thread
   static void yield();
   /// Get the current time in milliseconds
   static uint64_t getTicks();
};
//---------------------------------------------------------------------------
#endif
