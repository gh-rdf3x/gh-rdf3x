#ifndef H_rts_buffer_BufferManager
#define H_rts_buffer_BufferManager
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
#include "rts/partition/Partition.hpp"
#include "infra/osdep/Mutex.hpp"
#include "infra/osdep/Event.hpp"
#include "infra/osdep/Latch.hpp"
#include <map>
//---------------------------------------------------------------------------
class BufferManager;
class Partition;
class LogManager;
//---------------------------------------------------------------------------
/// A buffer frame. References some part of the database
class BufferFrame
{
   private:
   /// Possible states
   enum State { Empty, Read, Write, WriteDirty };

   /// The buffer manager
   BufferManager* buffer;
   /// The lack
   Latch latch;
   /// Does someone currently try to lock the frame?
   unsigned intentionLock;
   /// The associated data
   void* data;
   /// The partition
   Partition* partition;
   /// Page info used by the partition
   Partition::PageInfo pageInfo;
   /// The page
   unsigned pageNo;
   /// The log sequence number
   uint64_t lsn;
   /// The state
   State state;
   /// The next released frame
   BufferFrame* next;

   /// Grant the buffer manager access
   friend class BufferManager;
   /// The transaction has to change the LSN
   friend class Transaction;

   BufferFrame(const BufferFrame&);
   void operator=(const BufferFrame&);

   public:
   /// Constructor
   BufferFrame();

   /// The associated buffer manager
   BufferManager* getBufferManager() const { return buffer; }
   /// The page data
   const void* pageData() const { return data; }
   /// The page data
   void* pageData() { return data; }
   /// The page number
   unsigned getPageNo() const { return pageNo; }
   /// The paritition
   Partition* getPartition() const { return partition; }
   /// Mark as modified
   BufferFrame* update() const;

   /// The LSN
   uint64_t getLSN() const { return lsn; }
};
//---------------------------------------------------------------------------
/// A database buffer backed by a file
class BufferManager
{

   private:
   /// A page ID
   struct PageID {
      /// The partition
      Partition* partition;
      /// The page
      unsigned pageNo;

      /// Constructor
      PageID(Partition* partition,unsigned pageNo) : partition(partition),pageNo(pageNo) {}

      /// Compare
      bool operator==(const PageID& i) const { return (partition==i.partition)&&(pageNo==i.pageNo); }
      /// Compare
      bool operator<(const PageID& i) const { return (partition<i.partition)||((partition==i.partition)&&(pageNo<i.pageNo)); }
   };

   /// Maximum number of pages in buffer (hint)
   const unsigned bufferSize;
   /// Limit when starting to write back (hint)
   const unsigned dirtLimit;

   /// Lock. Should be held as shortly as possible
   Mutex mutex;
   /// The frame directory
   std::map<PageID,BufferFrame*> directory;
   /// All released buffer frames
   BufferFrame* releasedFrames;
   /// Number of dirty pages (estimate)
   unsigned dirtCounter;
   /// Notification for the writer thread
   Event flusherNotify;
   /// Notification when writing is done
   Event flusherDone;
   /// Notification when the writer thread stopped
   Event flusherDeadNotify;
   /// Flags to control the writer
   bool flusherDie,flusherDead;
   /// The log file (if any)
   LogManager* logManager;
   /// Enable checkpoinds
   bool checkpointsEnabled;
   /// Pages since the last checkpoint
   unsigned pagesSinceLastCheckpoint;
   /// Simulate a crash? Only for testing purposes!
   bool doCrash;

   /// Find or create a buffer frame
   BufferFrame* findBufferFrame(Partition* partition,unsigned pageNo,bool exclusive);

   /// Write dirty pages
   bool doFlush();
   /// Start the writer
   static void startFlusher(void* ptr);

   friend class BufferFrame;

   BufferManager(const BufferManager&);
   void operator=(const BufferManager&);

   public:
   /// Constructor
   BufferManager(unsigned bufferSizeHintInBytes);
   /// BufferManager
   ~BufferManager();

   /// Prepare a page for writing without reading it. Page is exclusive but not modifed
   BufferFrame* buildPage(Partition& partition,unsigned pageNo);
   /// Read a page. Page is shared and not modified
   const BufferFrame* readPageShared(Partition& partition,unsigned pageNo);
   /// Read a page. Page is exclusive and not modifed
   const BufferFrame* readPageExclusive(Partition& partition,unsigned pageNo);
   // Release an (unmodified) page
   void unfixPage(const BufferFrame* cframe);
   /// Release a dirty page without recovery information. Recovery is handled by Transaction::unfixDirtyPage
   void unfixDirtyPageWithoutRecovery(BufferFrame* frame);
   /// Mark a dirty page without recovery information. Recovery is handled by Transaction::unfixDirtyPage
   void markDirtyWithoutRecovery(BufferFrame* frame);
};
//---------------------------------------------------------------------------
#endif
