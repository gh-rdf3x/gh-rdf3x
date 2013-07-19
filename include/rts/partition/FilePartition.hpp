#ifndef H_rts_partition_FilePartition
#define H_rts_partition_FilePartition
//---------------------------------------------------------------------------
#include "rts/partition/Partition.hpp"
#include "infra/osdep/Mutex.hpp"
#include "infra/osdep/GrowableMappedFile.hpp"
#include <map>
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
/// A file based parititon
class FilePartition : public Partition
{
   private:
   /// After how many pages to we create a new mapping?
   static const unsigned mappingThreshold = 4096;
   /// Auxiliary data buffer for updates
   struct AuxBuffer;

   /// Locking mutex
   Mutex mutex;
   /// The backing file
   GrowableMappedFile file;
   /// All mapped parts
   std::map<unsigned,void*> mappings;
   /// The total size in pages
   unsigned size;
   /// The mapped size in pages
   unsigned mappedSize;
   /// The buffers
   AuxBuffer* auxBuffers;

   /// Allocate a new buffer
   AuxBuffer* allocAuxBuffer();
   /// Release a buffer
   void freeAuxBuffer(AuxBuffer* buffer);

   public:
   /// Constructor
   FilePartition();
   /// Destructor
   ~FilePartition();

   /// Open an existing partition
   bool open(const char* name,bool readOnly);
   /// Create a new partition
   bool create(const char* name);
   /// Close the partition
   void close();

   /// Acess a page for reading
   const void* readPage(unsigned pageNo,PageInfo& info);
   /// Finish reading a page
   void finishReadPage(PageInfo& info);
   /// Access a page for writing
   void* writePage(unsigned pageNo,PageInfo& info);
   /// Acess a page for writing without reading it first
   void* buildPage(unsigned pageNo,PageInfo& info);
   /// Access an already read page for writing
   void* writeReadPage(PageInfo& info);
   /// Write the changes back
   bool flushWrittenPage(PageInfo& info);
   /// Finish writing a page. Does _not_ write unflushed changes back!
   void finishWrittenPage(PageInfo& info);
   /// Flush the parition
   bool flush();
   /// Grow the partition.
   bool grow(unsigned minIncrease,unsigned& start,unsigned& len);
   /// The the partition size in pages
   unsigned getSize() const;
};
//----------------------------------------------------------------------------
#endif
