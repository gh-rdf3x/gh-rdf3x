#ifndef H_rts_partition_Partition
#define H_rts_partition_Partition
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
/// Base class for all partitions
class Partition
{
   public:
   /// Information about a current page. Interpretation of values is due to the containing partition!
   struct PageInfo {
      /// Pointer to the page
      void* ptr;
      /// Auxiliary pointer, interpretation is due to the partition
      void* aux;
      /// Page number
      unsigned pageNo;
      /// Auxiliary info, interpretation is due to the partition
      unsigned auxInfo;
   };

   public:
   /// Constructor
   Partition();
   /// Destructor
   virtual ~Partition();

   /// Acess a page for reading
   virtual const void* readPage(unsigned pageNo,PageInfo& info) = 0;
   /// Finish reading a page
   virtual void finishReadPage(PageInfo& info) = 0;
   /// Access a page for writing
   virtual void* writePage(unsigned pageNo,PageInfo& info) = 0;
   /// Acess a page for writing without reading it first
   virtual void* buildPage(unsigned pageNo,PageInfo& info) = 0;
   /// Access an already read page for writing
   virtual void* writeReadPage(PageInfo& info) = 0;
   /// Write the changes back
   virtual bool flushWrittenPage(PageInfo& info) = 0;
   /// Finish writing a page
   virtual void finishWrittenPage(PageInfo& info) = 0;
   /// Flush the parition
   virtual bool flush() = 0;
   /// Grow the partition.
   virtual bool grow(unsigned minIncrease,unsigned& start,unsigned& len) = 0;
   /// The the partition size in pages
   virtual unsigned getSize() const = 0;
};
//----------------------------------------------------------------------------
#endif
