#include "rts/partition/FilePartition.hpp"
#include "rts/buffer/BufferReference.hpp"
#include <cassert>
#include <cstring>
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
/// Auxiliary data buffer for updates
struct FilePartition::AuxBuffer {
   /// The copied page
   char page[BufferReference::pageSize];
   /// The next buffer
   AuxBuffer* next;
};
//----------------------------------------------------------------------------
FilePartition::FilePartition()
   : size(0),mappedSize(0),auxBuffers(0)
   // Constructor
{
}
//----------------------------------------------------------------------------
FilePartition::~FilePartition()
   // Destructor
{
   close();
}
//----------------------------------------------------------------------------
FilePartition::AuxBuffer* FilePartition::allocAuxBuffer()
   // Allocate a new buffer
{
   AuxBuffer* result;
   if (auxBuffers) {
      result=auxBuffers;
      auxBuffers=result->next;
   } else {
      result=new AuxBuffer();
   }
   result->next=0;
   return result;
}
//----------------------------------------------------------------------------
void FilePartition::freeAuxBuffer(AuxBuffer* buffer)
   // Release a buffer
{
   buffer->next=auxBuffers;
   auxBuffers=buffer;
}
//----------------------------------------------------------------------------
bool FilePartition::open(const char* name,bool readOnly)
   // Open an existing partition
{
   close();

   // Try to open the file
   char* begin,*end;
   if (!file.open(name,begin,end,readOnly))
      return false;

   // Remember the mapping if any
   if (begin!=end) {
      size=mappedSize=(end-begin)/BufferReference::pageSize;
      mappings[0]=begin;
   } else size=mappedSize=0;

   return true;
}
//----------------------------------------------------------------------------
bool FilePartition::create(const char* name)
   // Create a new partition
{
   close();

   // Try to create the file
   if (!file.create(name))
      return false;
   size=mappedSize=0;

   return true;
}
//----------------------------------------------------------------------------
void FilePartition::close()
   // Close the partition
{
   file.close();
   mappings.clear();
   size=mappedSize=0;

   while (auxBuffers) {
      AuxBuffer* b=auxBuffers;
      auxBuffers=auxBuffers->next;
      delete b;
   }
}
//----------------------------------------------------------------------------
const void* FilePartition::readPage(unsigned pageNo,PageInfo& info)
   // Acess a page for reading
{
   AuxBuffer* buffer;
   {
      auto_lock lock(mutex);

      // Do we have it mapped?
      if (pageNo<mappedSize) {
         map<unsigned,void*>::const_iterator iter=mappings.upper_bound(pageNo); --iter;
         BufferReference::PageBuffer* mapPtr=static_cast<BufferReference::PageBuffer*>((*iter).second);
         unsigned ofs=pageNo-(*iter).first;
         info.ptr=mapPtr+ofs;
         info.aux=0;
         info.pageNo=pageNo;
         info.auxInfo=0;
         return info.ptr;
      }

      // Is it worthwhile to increase the mapping?
      if ((size-mappedSize)>=mappingThreshold) {
         char* begin,*end;
         if (!file.growMapping(static_cast<GrowableMappedFile::ofs_t>(size-mappedSize)*BufferReference::pageSize,begin,end))
            assert(false);
         BufferReference::PageBuffer* mapPtr=reinterpret_cast<BufferReference::PageBuffer*>(begin);
         unsigned ofs=pageNo-mappedSize;
         mappings[mappedSize]=begin;
         mappedSize=size;
         info.ptr=mapPtr+ofs;
         info.aux=0;
         info.pageNo=pageNo;
         info.auxInfo=0;
         return info.ptr;
      }

      // No, allocate a buffer
      buffer=allocAuxBuffer();
      info.ptr=buffer->page;
      info.aux=buffer;
      info.pageNo=pageNo;
      info.auxInfo=0;
   }
   // Perform the read explicitly
   if (!file.read(static_cast<GrowableMappedFile::ofs_t>(pageNo)*BufferReference::pageSize,buffer->page,BufferReference::pageSize))
      assert(false);
   return info.ptr;
}
//----------------------------------------------------------------------------
void FilePartition::finishReadPage(PageInfo& info)
   // Finish reading a page
{
   // Buffer used?
   if (info.aux) {
      auto_lock lock(mutex);
      freeAuxBuffer(static_cast<AuxBuffer*>(info.aux));
   }

   // Clean up. Not required, but can help debugging
   info.ptr=0;
   info.aux=0;
   info.pageNo=0;
   info.auxInfo=0;
}
//----------------------------------------------------------------------------
void* FilePartition::writePage(unsigned pageNo,PageInfo& info)
   // Access a page for writing
{
   // First read the page
   readPage(pageNo,info);

   // And mark it for writing
   return writeReadPage(info);
}
//----------------------------------------------------------------------------
void* FilePartition::buildPage(unsigned pageNo,PageInfo& info)
   // Acess a page for writing without reading it first
{
   auto_lock lock(mutex);

   AuxBuffer* buffer=allocAuxBuffer();
   info.ptr=buffer->page;
   info.aux=buffer;
   info.pageNo=pageNo;
   info.auxInfo=0;

   return info.ptr;
}
//----------------------------------------------------------------------------
void* FilePartition::writeReadPage(PageInfo& info)
   // Access an already read page for writing
{
   // Make sure that we have a copy as scratch pad
   if (!info.aux) {
      AuxBuffer* buffer;
      { auto_lock lock(mutex); buffer=allocAuxBuffer(); }
      memcpy(buffer->page,info.ptr,BufferReference::pageSize);
      info.ptr=buffer->page;
      info.aux=buffer;
   }
   return info.ptr;
}
//----------------------------------------------------------------------------
bool FilePartition::flushWrittenPage(PageInfo& info)
   // Write the changes back
{
   void* target=0;
   {
      auto_lock lock(mutex);

      // Do we have it mapped?
      if (info.pageNo<mappedSize) {
         map<unsigned,void*>::const_iterator iter=mappings.upper_bound(info.pageNo); --iter;
         BufferReference::PageBuffer* mapPtr=static_cast<BufferReference::PageBuffer*>((*iter).second);
         unsigned ofs=info.pageNo-(*iter).first;
         target=mapPtr+ofs;
      }
   }

   // Store
   if (target) {
      memcpy(target,info.ptr,BufferReference::pageSize);
   } else {
      if (!file.write(static_cast<GrowableMappedFile::ofs_t>(info.pageNo)*BufferReference::pageSize,info.ptr,BufferReference::pageSize))
         return false;
   }

   // We only store here, do not release memory
   return true;
}
//----------------------------------------------------------------------------
void FilePartition::finishWrittenPage(PageInfo& info)
   // Finish writing a page
{
   // Release the buffer
   if (info.aux) {
      auto_lock lock(mutex);
      freeAuxBuffer(static_cast<AuxBuffer*>(info.aux));
   }

   // Clean up. Not required, but can help debugging
   info.ptr=0;
   info.aux=0;
   info.pageNo=0;
   info.auxInfo=0;
}
//----------------------------------------------------------------------------
bool FilePartition::flush()
   // Flush the parition
{
   return file.flush();
}
//----------------------------------------------------------------------------
bool FilePartition::grow(unsigned minIncrease,unsigned& start,unsigned& len)
   // Grow the partition.
{
   auto_lock lock(mutex);
   
   // Compute a reasonable increase
   unsigned increase=size/8;
   if (increase<minIncrease)
      increase=minIncrease;

   // Try to grow the underlying file
   if (!file.growPhysically(static_cast<GrowableMappedFile::ofs_t>(increase)*BufferReference::pageSize))
      return false;

   // Report success
   start=size;
   len=increase;
   size+=increase;

   return true;
}
//----------------------------------------------------------------------------
unsigned FilePartition::getSize() const
   // The the partition size in pages
{
   return size;
}
//----------------------------------------------------------------------------
