#include "infra/Config.hpp"
#ifdef CONFIG_WINDOWS
#define _WIN32_WINNT 0x400
#endif
#include "infra/osdep/Mutex.hpp"
//---------------------------------------------------------------------------
Mutex::Mutex()
   // Constructor
{
#ifdef CONFIG_WINDOWS
   InitializeCriticalSection(&mutex);
#else
   pthread_mutex_init(&mutex,0);
#endif
}
//---------------------------------------------------------------------------
Mutex::~Mutex()
   /// Destructor
{
#ifdef CONFIG_WINDOWS
   DeleteCriticalSection(&mutex);
#else
   pthread_mutex_destroy(&mutex);
#endif
}
//---------------------------------------------------------------------------
void Mutex::lock()
   // Lock
{
#ifdef CONFIG_WINDOWS
   EnterCriticalSection(&mutex);
#else
   pthread_mutex_lock(&mutex);
#endif
}
//---------------------------------------------------------------------------
bool Mutex::tryLock()
   // Try to lock
{
#ifdef CONFIG_WINDOWS
   return TryEnterCriticalSection(&mutex);
#else
   return pthread_mutex_trylock(&mutex)==0;
#endif
}
//---------------------------------------------------------------------------
void Mutex::unlock()
   // Unlock
{
#ifdef CONFIG_WINDOWS
   LeaveCriticalSection(&mutex);
#else
   pthread_mutex_unlock(&mutex);
#endif
}
//---------------------------------------------------------------------------
