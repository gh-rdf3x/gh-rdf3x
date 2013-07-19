#include "rts/buffer/BufferManager.hpp"
#include "rts/buffer/BufferReference.hpp"
#include "rts/transaction/LogManager.hpp"
#include "infra/osdep/Thread.hpp"
#include <algorithm>
#include <cassert>
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
/// Checkpoint after how many pages?
static const unsigned checkpointLimit = 1024;
//---------------------------------------------------------------------------
BufferFrame::BufferFrame()
   : buffer(0),intentionLock(0),data(0),partition(0),pageNo(0),lsn(0),state(Empty),next(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
BufferFrame* BufferFrame::update() const
   // Prepare for updates
{
   BufferFrame* result=const_cast<BufferFrame*>(this);
   // Mark as dirty if needed
   if (state!=WriteDirty) {
      result->buffer->mutex.lock();
      if (result->state==Read) {
         result->data=partition->writeReadPage(result->pageInfo);
         result->state=BufferFrame::Write;
      }
      result->state=WriteDirty;
      // Trigger the writer if necessary
      if ((++(result->buffer->dirtCounter))>result->buffer->dirtLimit) {
         result->buffer->flusherNotify.notify(result->buffer->mutex);
      }
      result->buffer->mutex.unlock();
   }
   return result;
}
//---------------------------------------------------------------------------
BufferManager::BufferManager(unsigned bufferSizeHintInBytes)
   : bufferSize(bufferSizeHintInBytes/BufferReference::pageSize),dirtLimit(3*bufferSize/4),releasedFrames(0),
     dirtCounter(0),logManager(0),checkpointsEnabled(false),pagesSinceLastCheckpoint(0),doCrash(false)
   // Constructor
{
   // Start the writer thread
   dirtCounter=0;
   flusherDie=flusherDead=false;
   Thread::start(startFlusher,this,true);
}
//---------------------------------------------------------------------------
BufferManager::~BufferManager()
   // BufferManager
{
   // Lock the mutex to synchronize with the writer

   // Stop the writer
   flusherDie=true;
   flusherNotify.notify(mutex);
   while (!flusherDead)
      flusherDeadNotify.wait(mutex);

   // Write the remaining dirty pages
   do {
      if (!doFlush()) {
         assert(false);
         break;
      }
   } while (dirtCounter>0);

   // Release all remaining pages. All pages must be empty unless we simulate a crash!
   for (std::map<PageID,BufferFrame*>::iterator iter=directory.begin(),limit=directory.end();iter!=limit;++iter) {
      BufferFrame& frame=*((*iter).second);
      if (frame.state!=BufferFrame::Empty) {
         if (!doCrash) {
            assert(frame.state==BufferFrame::Empty);
         }
         switch (frame.state) {
            case BufferFrame::Empty: break;
            case BufferFrame::Read: frame.partition->finishReadPage(frame.pageInfo); break;
            case BufferFrame::Write: frame.partition->finishWrittenPage(frame.pageInfo); break;
            case BufferFrame::WriteDirty: frame.partition->finishWrittenPage(frame.pageInfo); break;
         }
      }
      delete &frame;
   }
   directory.clear();

   // Release all released frames
   while (releasedFrames) {
      BufferFrame* frame=releasedFrames;
      releasedFrames=frame->next;
      delete frame;
   }

   // Release the lock again
   mutex.unlock();
}
//---------------------------------------------------------------------------
BufferFrame* BufferManager::findBufferFrame(Partition* partition,unsigned pageNo,bool exclusive)
   // Find or create a buffer frame
{
   // Check the diectory
   PageID pageID(partition,pageNo);

   std::map<PageID,BufferFrame*>::iterator iter=directory.find(pageID);
   if (iter!=directory.end()) {
      // Fond, try to lock it...
      bool success;
      BufferFrame& frame=*((*iter).second);
      if (frame.state==BufferFrame::Empty)
         exclusive=true;
      if (exclusive)
         success=frame.latch.tryLockExclusive(); else
         success=frame.latch.tryLockShared();
      if (success)
         return &frame;
      // Unsuccessful, try again in blocking mode
      frame.intentionLock++;
      mutex.unlock();
      if (exclusive)
         frame.latch.lockExclusive(); else
         frame.latch.lockShared();
      mutex.lock();
      frame.intentionLock--;
      // Success...
      return &frame;
   }
   // Not found, allocate a new frame
   BufferFrame* frame;
   if (releasedFrames) {
      frame=releasedFrames;
      releasedFrames=frame->next;
      frame->next=0;
   } else {
      frame=new BufferFrame();
   }
   directory[pageID]=frame;

   // And initialize it
   BufferFrame& result=*frame;
   result.buffer=this;
   result.latch.lockExclusive();
   result.intentionLock=0;
   result.data=0;
   result.partition=partition;
   result.pageNo=pageNo;
   result.lsn=0;
   result.state=BufferFrame::Empty;

   // Trigger the flusher if needed, otherwise write operations can flood the main memory
   if ((dirtCounter>dirtLimit)&&(directory.size()>bufferSize+dirtLimit)) {
      flusherNotify.notify(mutex);
      flusherDone.wait(mutex);
   }

   return &result;
}
//---------------------------------------------------------------------------
BufferFrame* BufferManager::buildPage(Partition& partition,unsigned pageNo)
   // Prepare a page for writing without reading it. Page is exclusive but not modifed
{
   mutex.lock();
   BufferFrame* frame=findBufferFrame(&partition,pageNo,true);
   mutex.unlock();
   switch (frame->state) {
      case BufferFrame::Empty: frame->data=partition.writePage(pageNo,frame->pageInfo); frame->state=BufferFrame::Write; break;
      case BufferFrame::Read: frame->data=partition.writeReadPage(frame->pageInfo); frame->state=BufferFrame::Write; break;
      case BufferFrame::Write: break;
      case BufferFrame::WriteDirty: break;
   }
   return frame;
}
//---------------------------------------------------------------------------
const BufferFrame* BufferManager::readPageShared(Partition& partition,unsigned pageNo)
   // Read a page. Page is shared and not modified
{
   mutex.lock();
   BufferFrame* frame=findBufferFrame(&partition,pageNo,false);
   // Empty frames are always locked exclusive. Mark intention to prepare for reads
   if (frame->state==BufferFrame::Empty)
      frame->intentionLock++;
   mutex.unlock();

   switch (frame->state) {
      case BufferFrame::Empty:
         frame->data=const_cast<void*>(partition.readPage(pageNo,frame->pageInfo));
         frame->state=BufferFrame::Read;
         // Change X latch to S latch
         frame->latch.unlock();
         frame->latch.lockShared();
         // And release the intention lock
         mutex.lock();
         frame->intentionLock--;
         mutex.unlock();
         break;
      case BufferFrame::Read: break;
      case BufferFrame::Write: break;
      case BufferFrame::WriteDirty: break;
   }
   return frame;
}
//---------------------------------------------------------------------------
const BufferFrame* BufferManager::readPageExclusive(Partition& partition,unsigned pageNo)
   // Read a page. Page is exclusive and not modifed
{
   mutex.lock();
   BufferFrame* frame=findBufferFrame(&partition,pageNo,true);
   mutex.unlock();
   switch (frame->state) {
      case BufferFrame::Empty: frame->data=const_cast<void*>(partition.readPage(pageNo,frame->pageInfo)); frame->state=BufferFrame::Read; break;
      case BufferFrame::Read: break;
      case BufferFrame::Write: break;
      case BufferFrame::WriteDirty: break;
   }
   return frame;
}
//---------------------------------------------------------------------------
void BufferManager::unfixPage(const BufferFrame* cframe)
   // Release an (unmodified) page
{
   // Was this the last reference?
   BufferFrame* frame=const_cast<BufferFrame*>(cframe);
   Partition* oldPartition=frame->partition;
   unsigned oldPageNo=frame->pageNo;
   if (frame->latch.unlock()) {
      // Dirty pages are released by the background writer
      if (frame->state==BufferFrame::WriteDirty)
         return;

      mutex.lock();
      // Is this really the last reference?
      if (frame->latch.tryLockExclusive()) {
         // Still the same?
         if ((frame->partition==oldPartition)&&(frame->pageNo==oldPageNo)) {
            // Then release it
            switch (frame->state) {
               case BufferFrame::Empty: break;
               case BufferFrame::Read: frame->partition->finishReadPage(frame->pageInfo); frame->state=BufferFrame::Empty; break;
               case BufferFrame::Write: frame->partition->finishWrittenPage(frame->pageInfo); frame->state=BufferFrame::Empty; break;
               case BufferFrame::WriteDirty: break;
            }
            // And release the buffer frame itself
            if ((frame->state==BufferFrame::Empty)&&(!frame->intentionLock)) {
               directory.erase(PageID(frame->partition,frame->pageNo));
               frame->next=releasedFrames;
               releasedFrames=frame;
               frame->partition=0;
               frame->pageNo=0;
            }
         }
         frame->latch.unlock();
      }
      mutex.unlock();
   }
}
//---------------------------------------------------------------------------
void BufferManager::unfixDirtyPageWithoutRecovery(BufferFrame* frame)
   // Release a dirty page without recovery information. Recovery is handled by Transaction::unfixDirtyPage
{
   // Mark as modiied
   if (frame->state!=BufferFrame::WriteDirty) {
      mutex.lock();
      frame->state=BufferFrame::WriteDirty;
      if (++dirtCounter>dirtLimit) {
         flusherNotify.notify(mutex);
      }
      mutex.unlock();
   }
   // And release the latch
   frame->latch.unlock();
}
//---------------------------------------------------------------------------
void BufferManager::markDirtyWithoutRecovery(BufferFrame* frame)
   // Release a dirty page without recovery information. Recovery is handled by Transaction::unfixDirtyPage
{
   // Mark as modiied
   if (frame->state!=BufferFrame::WriteDirty) {
      mutex.lock();
      frame->state=BufferFrame::WriteDirty;
      if (++dirtCounter>dirtLimit) {
         flusherNotify.notify(mutex);
      }
      mutex.unlock();
   }
}
//---------------------------------------------------------------------------
bool BufferManager::doFlush()
   // Write dirty unfixed pages
{
   // Are we simulating a crash?
   if (doCrash) { dirtCounter=0; return true; }

   // Prepare a list of dirty pages
   static const unsigned maxCollect = 1024;
   const unsigned collectCount = (maxCollect<(dirtLimit/2))?maxCollect:(dirtLimit/2);
   BufferFrame* list[maxCollect];
   Partition*   partitionList[maxCollect];

   // Scan the buffer and find dirty pages
   bool         fixedDirty=false;
   uint64_t     forceLSN=0;
   unsigned     totalCount=0,partitionCount=0;
   dirtCounter=0;
   for (std::map<PageID,BufferFrame*>::iterator iter=directory.begin(),limit=directory.end();iter!=limit;++iter) {
      if ((*iter).second->state==BufferFrame::WriteDirty) {
         BufferFrame& frame=*((*iter).second);
         if (totalCount<collectCount) {
            if (frame.latch.tryLockShared()) {
               list[totalCount++]=&frame;
               if (frame.lsn>forceLSN) forceLSN=frame.lsn;
               if ((!partitionCount)||(frame.partition!=partitionList[partitionCount-1]))
                  partitionList[partitionCount++]=frame.partition;
            } else {
               fixedDirty=true;
               dirtCounter++;
            }
         } else dirtCounter++;
      }
   }
   // No dirty pages found? Then stop immediately
   if (!totalCount) {
      return !fixedDirty;
   }

   // Release the mutex and write pages
   mutex.unlock();
   if (logManager)
      logManager->force(forceLSN);
   for (unsigned index=0;index<totalCount;index++) {
      list[index]->partition->flushWrittenPage(list[index]->pageInfo);
   }

   // Grab the mutex and mark the pages as written
   mutex.lock();
   for (unsigned index=0;index<totalCount;index++) {
      BufferFrame* frame=list[index];
      frame->state=BufferFrame::Write;
      Partition* oldPartition=frame->partition;
      unsigned oldPageNo=frame->pageNo;
      if (frame->latch.unlock()) {
         // Last reference?
         if (frame->latch.tryLockExclusive()) {
            if ((frame->partition==oldPartition)&&(frame->pageNo==oldPageNo)) {
               // Then release it
               switch (frame->state) {
                  case BufferFrame::Empty: break;
                  case BufferFrame::Read: frame->partition->finishReadPage(frame->pageInfo); frame->state=BufferFrame::Empty; break;
                  case BufferFrame::Write: frame->partition->finishWrittenPage(frame->pageInfo); frame->state=BufferFrame::Empty; break;
                  case BufferFrame::WriteDirty: break;
               }
               // Release the buffer frame if there is no contention
               if ((frame->state==BufferFrame::Empty)&&(!frame->intentionLock)) {
                  directory.erase(PageID(frame->partition,frame->pageNo));
                  frame->next=releasedFrames;
                  releasedFrames=frame;
                  frame->partition=0;
                  frame->pageNo=0;
               }
            }
            frame->latch.unlock();
         }
      }
   }

   // Set a checkpoint if nevessary
   if (logManager&&checkpointsEnabled) {
      pagesSinceLastCheckpoint+=totalCount;
      if (pagesSinceLastCheckpoint>checkpointLimit) {
         // Initiate via log-manager to avoid parallel modifications to page LSNs
         mutex.unlock();
         for (unsigned index=0;index<partitionCount;index++)
            partitionList[index]->flush();
         logManager->initiateCheckpoint();
         mutex.lock();
         pagesSinceLastCheckpoint=0;
      }
   }

   return !fixedDirty;
}
//---------------------------------------------------------------------------
void BufferManager::startFlusher(void* ptr)
   // Start the writer
{
   BufferManager* buf=static_cast<BufferManager*>(ptr);

   buf->mutex.lock();
   while (!buf->flusherDie) {
      if (buf->dirtCounter<=buf->dirtLimit) {
         buf->flusherNotify.wait(buf->mutex);
      }
      buf->doFlush();
      buf->flusherDone.notifyAll(buf->mutex);
   }
   buf->flusherDead=true;
   buf->flusherDeadNotify.notify(buf->mutex);
   buf->mutex.unlock();
}
//---------------------------------------------------------------------------
