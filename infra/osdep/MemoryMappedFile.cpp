#include "infra/osdep/MemoryMappedFile.hpp"
#if defined(WIN32)||defined(__WIN32__)||defined(_WIN32)
#define CONFIG_WINDOWS
#endif
#ifdef CONFIG_WINDOWS
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0500
#endif
#include <windows.h>
#else
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
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
//----------------------------------------------------------------------------
// OS dependent data
struct MemoryMappedFile::Data
{
#ifdef CONFIG_WINDOWS
   /// The file
   HANDLE file;
   /// The mapping
   HANDLE mapping;
#else
   /// The file
   int file;
   /// The mapping
   void* mapping;
#endif
};
//----------------------------------------------------------------------------
MemoryMappedFile::MemoryMappedFile()
   : data(0),begin(0),end(0)
   // Constructor
{
}
//----------------------------------------------------------------------------
MemoryMappedFile::~MemoryMappedFile()
   // Destructor
{
   close();
}
//----------------------------------------------------------------------------
bool MemoryMappedFile::open(const char* name)
   // Open
{
   if (!name) return false;
   close();

   #ifdef CONFIG_WINDOWS
      HANDLE file=CreateFile(name,GENERIC_READ,FILE_SHARE_READ,0,OPEN_EXISTING,0,0);
      if (file==INVALID_HANDLE_VALUE) return false;
      LARGE_INTEGER fullSize;
      if (!GetFileSizeEx(file,&fullSize)) { CloseHandle(file); return false; }
      HANDLE mapping=CreateFileMapping(file,0,PAGE_READONLY,fullSize.HighPart,fullSize.LowPart,0);
      if (mapping==INVALID_HANDLE_VALUE) { CloseHandle(file); return false; }
      begin=static_cast<char*>(MapViewOfFile(mapping,FILE_MAP_READ,0,0,fullSize.QuadPart));
      if (!begin) { CloseHandle(mapping); CloseHandle(file); return false; }
      end=begin+fullSize.QuadPart;
   #else
      int file=::open(name,O_RDONLY);
      if (file<0) return false;
      size_t size=lseek(file,0,SEEK_END);
      if (!(~size)) { ::close(file); return false; }
      void* mapping=mmap(0,size,PROT_READ,MAP_PRIVATE,file,0);
      if (!mapping) { ::close(file); return false; }
      begin=static_cast<char*>(mapping);
      end=begin+size;
   #endif
   data=new Data();
   data->file=file;
   data->mapping=mapping;
   return true;
}
//----------------------------------------------------------------------------
void MemoryMappedFile::close()
   // Close
{
   if (data) {
#ifdef CONFIG_WINDOWS
      UnmapViewOfFile(const_cast<char*>(begin));
      CloseHandle(data->mapping);
      CloseHandle(data->file);
#else
      munmap(data->mapping,end-begin);
      ::close(data->file);
#endif
      delete data;
      data=0;
      begin=end=0;
   }
}
unsigned sumOfItAll;
//----------------------------------------------------------------------------
void MemoryMappedFile::prefetch(const char* start,const char* end)
   // Ask the operating system to prefetch a part of the file
{
   if ((end<start)||(!data))
      return;

#ifdef CONFIG_WINDOWS
   // XXX todo
#elif defined(CONFIG_DARWIN)
   // XXX todo
#else
   posix_fadvise(data->file,start-begin,end-start+1,POSIX_FADV_WILLNEED);
#endif
}
//----------------------------------------------------------------------------
