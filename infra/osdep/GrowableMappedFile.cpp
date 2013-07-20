#include "infra/osdep/GrowableMappedFile.hpp"
#include <vector>
#if defined(WIN32)||defined(__WIN32__)||defined(_WIN32)
#define CONFIG_WINDOWS
#endif
#ifdef CONFIG_WINDOWS
#include <windows.h>
#else
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#endif
#ifdef CONFIG_DARWIN
#include <sys/stat.h>
#endif
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//----------------------------------------------------------------------------
using namespace std;
//----------------------------------------------------------------------------
// OS dependent data
struct GrowableMappedFile::Data
{
#ifdef CONFIG_WINDOWS
   /// The file
   HANDLE file;
   /// The mappings
   vector<pair<HANDLE,char*> > mappings;
#else
   /// The file
   int file;
   /// The mappings
   vector<pair<char*,char*> > mappings;
#endif
   /// The size
   ofs_t size;
   /// The mapped part of the file
   ofs_t mappedSize;
};
//----------------------------------------------------------------------------
GrowableMappedFile::GrowableMappedFile()
   : data(0)
   // Constructor
{
}
//----------------------------------------------------------------------------
GrowableMappedFile::~GrowableMappedFile()
   // Destructor
{
   close();
}
//----------------------------------------------------------------------------
bool GrowableMappedFile::open(const char* name,char*& begin,char*& end,bool readOnly)
   // Open
{
   if (!name) return false;
   close();

   #ifdef CONFIG_WINDOWS
      HANDLE file;
      if (readOnly)
         file=CreateFile(name,GENERIC_READ,FILE_SHARE_READ,0,OPEN_EXISTING,0,0); else
         file=CreateFile(name,GENERIC_READ|GENERIC_WRITE,0,0,OPEN_EXISTING,0,0);
      if (file==INVALID_HANDLE_VALUE) return false;
      DWORD sizeHigh=0;
      DWORD size=GetFileSize(file,&sizeHigh);
      LARGE_INTEGER fullSize; fullSize.HighPart=sizeHigh; fullSize.LowPart=size;
      if (fullSize.QuadPart) {
         HANDLE mapping=CreateFileMapping(file,0,readOnly?PAGE_READONLY:PAGE_READWRITE,sizeHigh,size,0);
         if (mapping==INVALID_HANDLE_VALUE) { CloseHandle(file); return false; }
         begin=static_cast<char*>(MapViewOfFile(mapping,FILE_MAP_READ|(readOnly?0:FILE_MAP_WRITE),0,0,fullSize.QuadPart));
         if (!begin) { CloseHandle(mapping); CloseHandle(file); return false; }
         end=begin+fullSize.QuadPart;

         data=new Data();
         data->mappings.push_back(pair<HANDLE,char*>(mapping,begin));
      } else {
         begin=end=0;

         data=new Data();
         // create no initial mapping for empty files
      }
      data->size=fullSize.QuadPart;
   #else
      int file=::open(name,readOnly?O_RDONLY:O_RDWR);
      if (file<0) return false;
      size_t size=lseek(file,0,SEEK_END);
      if (!(~size)) { ::close(file); return false; }
      if (size) {
         void* mapping=mmap(0,size,PROT_READ|(readOnly?0:PROT_WRITE),MAP_SHARED,file,0);
         if (!mapping) { ::close(file); return false; }
         begin=static_cast<char*>(mapping);
         end=begin+size;

         data=new Data();
         data->mappings.push_back(pair<char*,char*>(begin,end));
      } else {
         begin=end=0;

         data=new Data();
         // create no initial mapping for empty files
      }
      data->size=size;
   #endif

   data->file=file;
   data->mappedSize=data->size;

   return true;
}
//----------------------------------------------------------------------------
bool GrowableMappedFile::create(const char* name)
   // Create a new file
{
   if (!name) return false;
   close();

   #ifdef CONFIG_WINDOWS
      HANDLE file=CreateFile(name,GENERIC_READ|GENERIC_WRITE,0,0,CREATE_ALWAYS,0,0);
      if (file==INVALID_HANDLE_VALUE) return false;
   #else
      int file=::open(name,O_RDWR|O_CREAT|O_TRUNC,00640);
      if (file<0) return false;
   #endif

   // We created an empty file, no initial mapping
   data=new Data();
   data->file=file;
   data->size=0;
   data->mappedSize=0;

   return true;
}
//----------------------------------------------------------------------------
void GrowableMappedFile::close()
   // Close
{
   if (data) {
#ifdef CONFIG_WINDOWS
      for (vector<pair<HANDLE,char*> >::const_reverse_iterator iter=data->mappings.rbegin(),limit=data->mappings.rend();iter!=limit;++iter) {
         UnmapViewOfFile((*iter).second);
         CloseHandle((*iter).first);
      }
      CloseHandle(data->file);
#else
      for (vector<pair<char*,char*> >::const_reverse_iterator iter=data->mappings.rbegin(),limit=data->mappings.rend();iter!=limit;++iter)
         munmap((*iter).first,(*iter).second-(*iter).first);
      ::close(data->file);
#endif
      delete data;
      data=0;
   }
}
//----------------------------------------------------------------------------
bool GrowableMappedFile::flush()
   // Flush the file
{
   if (!data) return false;

#ifdef CONFIG_WINDOWS
   for (vector<pair<HANDLE,char*> >::const_iterator iter=data->mappings.begin(),limit=data->mappings.end();iter!=limit;++iter)
      if (!FlushViewOfFile((*iter).second,0))
         return false;
   return FlushFileBuffers(data->file);
#else
   for (vector<pair<char*,char*> >::const_iterator iter=data->mappings.begin(),limit=data->mappings.end();iter!=limit;++iter)
      if (msync((*iter).first,(*iter).second-(*iter).first,MS_SYNC)!=0)
         return false;
# ifdef CONFIG_DARWIN
   return fsync(data->file)==0;
# else
   return fdatasync(data->file)==0;
# endif
#endif
}
//----------------------------------------------------------------------------
bool GrowableMappedFile::growPhysically(ofs_t increment)
   // Grow the underlying file physically
{
#ifdef CONFIG_WINDOWS
   // We have to write the data manually. SetEndOfFle is not guaranteed to zero
   // the content, and we _want_ physical allocation.
   const unsigned bufferSize = 16*1024*1024;
   char* buffer=new char[bufferSize];
   memset(buffer,0,bufferSize);

   ofs_t pos=data->size,todo=increment;
   while (todo>bufferSize) {
      if (!write(pos,buffer,bufferSize)) {
         delete[] buffer;
         LARGE_INTEGER s; s.QuadPart=data->size;
         if (SetFilePointerEx(data->file,s,0,FILE_BEGIN))
            SetEndOfFile(data->file);
         return false;
      }
      pos+=bufferSize;
      todo-=bufferSize;
   }
   if (!write(data->size,buffer,todo)) {
      delete[] buffer;
      LARGE_INTEGER s; s.QuadPart=data->size;
      if (SetFilePointerEx(data->file,s,0,FILE_BEGIN))
         SetEndOfFile(data->file);
      return false;
   }
   delete[] buffer;
#elif defined(CONFIG_DARWIN)
   fstore_t fst;
   fst.fst_flags = F_ALLOCATECONTIG | F_ALLOCATEALL;
   fst.fst_posmode = F_PEOFPOSMODE;
   fst.fst_offset = 0;//data->size;
   fst.fst_length = increment;
   fst.fst_bytesalloc = 0;
   if (fcntl (data->file, F_PREALLOCATE, &fst)==-1)
      return false;

   struct stat64 stat;
   if (fstat64 (data -> file, &stat)!=0)
      return false;
   if (ftruncate(data->file, stat.st_size + increment)!=0) // NOT fst.fst_bytesAllocated!
      return false;
#else
   // if (posix_fallocate64(data->file,data->size,increment)!=0) { // XXX disabled for now due to an XFS bug
   if (ftruncate64(data->file,data->size+increment)!=0) {
      return false;
   }
#endif

   data->size+=increment;
   return true;
}
//----------------------------------------------------------------------------
bool GrowableMappedFile::growMapping(ofs_t increment,char*& begin,char*& end)
   // Grow the mapping on the underlying file
{
   // Already fully mapped?
   if ((!increment)||(data->mappedSize+increment>data->size))
      return false;

   // Try to extend the existing mapping if possible
#if defined(__linux__)
   if (!data->mappings.empty()) {
      ofs_t oldSize=data->mappings.back().second-data->mappings.back().first;
      ofs_t newSize=oldSize+increment;
      // Can we increase the mapping?
      if (mremap(data->mappings.back().first,oldSize,newSize,0)!=MAP_FAILED) {
         // Yes, update the mapping entry
         data->mappedSize+=increment;
         begin=data->mappings.back().second;
         data->mappings.back().second=data->mappings.back().first+newSize;
         end=data->mappings.back().second;
         return true;
      }
   }
#endif

   // Create a new mapping
#ifdef CONFIG_WINDOWS
   LARGE_INTEGER size; size.QuadPart=increment;
   HANDLE mapping=CreateFileMapping(data->file,0,PAGE_READWRITE,size.HighPart,size.LowPart,0);
   if (mapping==INVALID_HANDLE_VALUE) return false;
   LARGE_INTEGER ofs; ofs.QuadPart=data->mappedSize;
   begin=static_cast<char*>(MapViewOfFile(mapping,FILE_MAP_READ|FILE_MAP_WRITE,ofs.HighPart,ofs.LowPart,increment));
   if (!begin) { CloseHandle(mapping); return false; }
   end=begin+increment;

   data->mappings.push_back(pair<HANDLE,char*>(mapping,begin));
#else
   void* mapping=mmap(0,increment,PROT_READ|PROT_WRITE,MAP_SHARED,data->file,data->mappedSize);
   if (!mapping) return false;
   begin=static_cast<char*>(mapping);
   end=begin+increment;

   data->mappings.push_back(pair<char*,char*>(begin,end));
#endif

   data->mappedSize+=increment;
   return true;
}
//----------------------------------------------------------------------------
bool GrowableMappedFile::read(ofs_t ofs,void* data,unsigned len)
   // Read from the unmapped part of the file
{
#ifdef CONFIG_WINDOWS
   OVERLAPPED info;
   memset(&info,0,sizeof(info));
   LARGE_INTEGER o; o.QuadPart=ofs;
   info.Offset=o.LowPart;
   info.OffsetHigh=o.HighPart;
   DWORD result;
   if (!ReadFile(this->data->file,data,len,&result,&info))
      return false;
   return result==len;
#else
   return static_cast<unsigned>(pread(this->data->file,data,len,ofs))==len;
#endif
}
//----------------------------------------------------------------------------
bool GrowableMappedFile::write(ofs_t ofs,const void* data,unsigned len)
   // Write to the unmapped part of the file
{
#ifdef CONFIG_WINDOWS
   OVERLAPPED info;
   memset(&info,0,sizeof(info));
   LARGE_INTEGER o; o.QuadPart=ofs;
   info.Offset=o.LowPart;
   info.OffsetHigh=o.HighPart;
   DWORD result;
   if (!WriteFile(this->data->file,data,len,&result,&info))
      return false;
   return result==len;
#else
   return static_cast<unsigned>(pwrite(this->data->file,data,len,ofs))==len;
#endif
}
//----------------------------------------------------------------------------
