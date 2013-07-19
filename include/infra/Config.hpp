#ifndef H_infra_Config
#define H_infra_Config
//---------------------------------------------------------------------------
#if !defined(__sparc__)
#include <stdint.h>
#else
// stdint.h is C99, not supported by SUN yet
#include <inttypes.h>
#endif
//---------------------------------------------------------------------------
#if defined(__WIN32__)||defined(WIN32)||defined(_WIN32)
#define CONFIG_WINDOWS
#endif
//---------------------------------------------------------------------------
#endif

