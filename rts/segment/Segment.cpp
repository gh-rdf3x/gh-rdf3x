#include "rts/segment/Segment.hpp"
#include "rts/buffer/BufferManager.hpp"
#include "rts/buffer/BufferReference.hpp"
#include "rts/database/DatabasePartition.hpp"
#include "rts/segment/SegmentInventorySegment.hpp"
#include "rts/segment/SpaceInventorySegment.hpp"
#include "rts/transaction/LogAction.hpp"
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
namespace {
//---------------------------------------------------------------------------
LOGACTION2(SegmentInventorySegment,UpdateFreePageList,uint32_t,oldValue,uint32_t,newValue)
//---------------------------------------------------------------------------
void UpdateFreePageList::redo(void* page) const { Segment::writeUint32(static_cast<unsigned char*>(page)+8,newValue); }
void UpdateFreePageList::undo(void* page) const { Segment::writeUint32(static_cast<unsigned char*>(page)+8,newValue); }
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
Segment::Segment(DatabasePartition& partition)
   : partition(partition),id(~0u),freeBlockStart(0),freeBlockLen(0),freeList(0)
   // Constructor
{

}
//---------------------------------------------------------------------------
Segment::~Segment()
   // Destructor
{
}
//---------------------------------------------------------------------------
void Segment::refreshInfo()
   // Refresh the stored info
{
   partition.getSegmentInventory()->getFreeBlock(id,freeBlockStart,freeBlockLen);
   freeList=partition.getSegmentInventory()->getFreeList(id);
}
//---------------------------------------------------------------------------
unsigned Segment::getSegmentData(unsigned slot)
   // Get segment info
{
   return partition.getSegmentInventory()->getCustom(id,slot);
}
//---------------------------------------------------------------------------
void Segment::setSegmentData(unsigned slot,unsigned value)
   // Set segment info
{
   partition.getSegmentInventory()->setCustom(id,slot,value);
}
//---------------------------------------------------------------------------
BufferRequest Segment::readShared(unsigned page) const
   // Read a specific page
{
   return partition.readShared(page);
}
//---------------------------------------------------------------------------
BufferRequestExclusive Segment::readExclusive(unsigned page)
   // Read a specific page
{
   return partition.readExclusive(page);
}
//---------------------------------------------------------------------------
BufferRequestModified Segment::modifyExclusive(unsigned page)
   // Read a specific page
{
   return partition.modifyExclusive(page);
}
//---------------------------------------------------------------------------
bool Segment::allocPage(BufferReferenceModified& page)
   // Allocate a new page
{
   // Any known free pages?
   if (freeList) {
      page=partition.modifyExclusive(freeList);
      freeList=readUint32(static_cast<unsigned char*>(page.getPage())+8);
      partition.getSegmentInventory()->setFreeList(id,freeList);
      return true;
   }

   // Do we have to allocate a new chunk?
   if (!freeBlockLen) {
      unsigned start,len;
      if (!partition.getSpaceInventory()->growSegment(id,1,start,len))
         return false;
      freeBlockStart=start;
      freeBlockLen=len;
   }

   // Use the free block
   page=partition.modifyExclusive(freeBlockStart);
   freeBlockStart++; freeBlockLen--;
   partition.getSegmentInventory()->setFreeBlock(id,freeBlockStart,freeBlockLen);
   return true;
}
//---------------------------------------------------------------------------
bool Segment::allocPageRange(unsigned minSize,unsigned preferredSize,unsigned& start,unsigned& len)
   // Allocate a range of pages
{
   // Sanity checks
   if ((preferredSize<minSize)||(!preferredSize))
      return false;

   // Single page?
   if ((preferredSize==1)&&(freeList)) {
      BufferReferenceExclusive page(readExclusive(freeList));
      freeList=readUint32(static_cast<const unsigned char*>(page.getPage())+8);
      partition.getSegmentInventory()->setFreeList(id,freeList);
      start=page.getPageNo();
      len=1;
      return true;
   }

   // Space available?
   if (freeBlockLen>=minSize) {
      start=freeBlockStart;
      if (freeBlockLen>preferredSize) {
         len=preferredSize;
         freeBlockStart+=preferredSize;
         freeBlockLen-=preferredSize;
      } else {
         len=freeBlockLen;
         freeBlockStart+=freeBlockLen;
         freeBlockLen=0;
      }
      partition.getSegmentInventory()->setFreeBlock(id,freeBlockStart,freeBlockLen);
      return true;
   }

   // No, grow the segment
   if (!partition.getSpaceInventory()->growSegment(id,preferredSize,start,len))
      return false;

   // Reuse space if possible
   if ((!freeBlockLen)&&(len>preferredSize)) {
      freeBlockStart=start+preferredSize;
      freeBlockLen=len-preferredSize;
      len=preferredSize;
      partition.getSegmentInventory()->setFreeBlock(id,freeBlockStart,freeBlockLen);
   }

   return true;
}
//---------------------------------------------------------------------------
void Segment::freePage(BufferReferenceModified& page)
   // Free a previously allocated page
{
   // Update the link
   unsigned pageNo=page.getPageNo();
   UpdateFreePageList(readUint32(static_cast<unsigned char*>(page.getPage())+8),freeList).apply(page);
   partition.getSegmentInventory()->setFreeList(id,pageNo);
   freeList=pageNo;
}
//---------------------------------------------------------------------------
void Segment::writeUint32(unsigned char* data,unsigned value)
   // Helper function. Write a 32bit big-endian value
{
   data[0]=static_cast<unsigned char>(value>>24);
   data[1]=static_cast<unsigned char>(value>>16);
   data[2]=static_cast<unsigned char>(value>>8);
   data[3]=static_cast<unsigned char>(value>>0);
}
//---------------------------------------------------------------------------
unsigned long long Segment::readUint64(const unsigned char* data)
   // Helper function. Read a 64bit big-endian value
{
   unsigned long long result=0;
   for (unsigned index=0;index<8;index++)
      result=(result<<8)|static_cast<unsigned long long>(data[index]);
   return result;
}
//---------------------------------------------------------------------------
