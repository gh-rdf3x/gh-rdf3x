#include "rts/segment/SegmentInventorySegment.hpp"
#include "rts/buffer/BufferReference.hpp"
#include "rts/database/DatabasePartition.hpp"
#include "rts/transaction/LogAction.hpp"
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
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// A inventory page
/// Layout:
/// LSN: 64bit
/// next page: 32bit
/// entries: maxEntries x 16*32bit
class InventoryPage {
   public:
   /// Size of the header
   static const unsigned headerSize = 8+4;
   /// Size of an entry
   static const unsigned entrySize = 16*4;
   /// Maximum number of entries per page
   static const unsigned maxEntries = (BufferReference::pageSize-headerSize)/entrySize;

   private:
   /// Data pointer
   const unsigned char* ptr() const { return reinterpret_cast<const unsigned char*>(this); }
   /// Data pointer
   unsigned char* ptr() { return reinterpret_cast<unsigned char*>(this); }
   /// Get a pointer to an entry
   unsigned char* getEntryPtr(unsigned index) { return ptr()+headerSize+(entrySize*index); }

   friend class InitializeEntry;
   friend class UpdateInventory;
   friend class UpdateFreeBlock;

   public:
   /// Get the next page
   unsigned getNext() const { return Segment::readUint32Aligned(ptr()+8); }
   /// Get a pointer to an entry
   const unsigned char* getEntryPtr(unsigned index) const { return ptr()+headerSize+(entrySize*index); }
   /// Get the type entry
   unsigned getType(unsigned index) const { return Segment::readUint32Aligned(getEntryPtr(index)+0); }
   /// Get a value from a record
   unsigned getValue(unsigned index,unsigned slot) const { return Segment::readUint32Aligned(getEntryPtr(index)+(4*slot)); }

   /// Get as inner page
   static const InventoryPage* interpret(const void* data) { return static_cast<const InventoryPage*>(data); }
   /// Get as inner page
   static InventoryPage* interpret(void* data) { return static_cast<InventoryPage*>(data); }
};
//---------------------------------------------------------------------------
LOGACTION3(SegmentInventorySegment,InitializeEntry,uint32_t,index,LogData,oldValue,LogData,newValue)
//---------------------------------------------------------------------------
void InitializeEntry::redo(void* page) const { memcpy(InventoryPage::interpret(page)->getEntryPtr(index),newValue.ptr,newValue.len); }
void InitializeEntry::undo(void* page) const { memcpy(InventoryPage::interpret(page)->getEntryPtr(index),oldValue.ptr,oldValue.len); }
//---------------------------------------------------------------------------
LOGACTION4(SegmentInventorySegment,UpdateInventory,uint32_t,index,uint32_t,slot,uint32_t,oldValue,uint32_t,newValue)
//---------------------------------------------------------------------------
void UpdateInventory::redo(void* page) const { Segment::writeUint32Aligned(InventoryPage::interpret(page)->getEntryPtr(index)+(4*slot),newValue); }
void UpdateInventory::undo(void* page) const { Segment::writeUint32Aligned(InventoryPage::interpret(page)->getEntryPtr(index)+(4*slot),oldValue); }
//---------------------------------------------------------------------------
LOGACTION5(SegmentInventorySegment,UpdateFreeBlock,uint32_t,index,uint32_t,oldStart,uint32_t,oldLen,uint32_t,newStart,uint32_t,newLen)
//---------------------------------------------------------------------------
void UpdateFreeBlock::redo(void* page) const { Segment::writeUint32Aligned(InventoryPage::interpret(page)->getEntryPtr(index)+(4*2),newStart); Segment::writeUint32Aligned(InventoryPage::interpret(page)->getEntryPtr(index)+(4*3),newLen); }
void UpdateFreeBlock::undo(void* page) const { Segment::writeUint32Aligned(InventoryPage::interpret(page)->getEntryPtr(index)+(4*2),oldStart); Segment::writeUint32Aligned(InventoryPage::interpret(page)->getEntryPtr(index)+(4*3),oldLen); }
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
SegmentInventorySegment::SegmentInventorySegment(DatabasePartition& partition)
   : Segment(partition)
   // Constructor
{
}
//---------------------------------------------------------------------------
SegmentInventorySegment::~SegmentInventorySegment()
   // Destructor
{
}
//---------------------------------------------------------------------------
Segment::Type SegmentInventorySegment::getType() const
   // Get the type
{
   return Segment::Type_SegmentInventory;
}
//---------------------------------------------------------------------------
unsigned SegmentInventorySegment::addSegment(Segment::Type type,unsigned tag)
   // Add a segment
{
   BufferReferenceModified rootPage;
   rootPage=modifyExclusive(root);
   const InventoryPage* inv=InventoryPage::interpret(rootPage.getPage());

   // Scan for a free slot
   for (unsigned index=0;index<InventoryPage::maxEntries;index++)
      if (!inv->getType(index)) {
	 unsigned char newEntry[InventoryPage::entrySize]={0};
	 writeUint32Aligned(newEntry,type);
	 writeUint32Aligned(newEntry+4,tag);
         InitializeEntry(index,LogData(inv->getEntryPtr(index),InventoryPage::entrySize),LogData(newEntry,InventoryPage::entrySize)).apply(rootPage);
         return index;
      }

   // No free space available
   assert(false&&"segment inventory overflow not implemented yet"); // XXX
   return 0;
}
//---------------------------------------------------------------------------
void SegmentInventorySegment::dropSegment(unsigned id)
   // Drop a segment
{
   BufferReferenceModified rootPage;
   rootPage=modifyExclusive(root);
   InventoryPage* inv=InventoryPage::interpret(rootPage.getPage());
   UpdateInventory(id,0,inv->getType(id),0).apply(rootPage);
}
//---------------------------------------------------------------------------
unsigned SegmentInventorySegment::getTag(unsigned id) const
   // Get the tag of a segment (if any)
{
   BufferReference rootPage(readShared(root));
   const InventoryPage* inv=InventoryPage::interpret(rootPage.getPage());
   return inv->getValue(id,1);
}
//---------------------------------------------------------------------------
void SegmentInventorySegment::setTag(unsigned id,unsigned tag)
   // Modify the tag of a segment
{
   BufferReferenceModified rootPage(modifyExclusive(root));
   InventoryPage* inv=InventoryPage::interpret(rootPage.getPage());
   UpdateInventory(id,1,inv->getValue(id,1),tag).apply(rootPage);
}
//---------------------------------------------------------------------------
void SegmentInventorySegment::getFreeBlock(unsigned id,unsigned& start,unsigned& len) const
   // Get the unallocated free block
{
   BufferReference rootPage(readShared(root));
   const InventoryPage* inv=InventoryPage::interpret(rootPage.getPage());
   start=inv->getValue(id,2);
   len=inv->getValue(id,3);
}
//---------------------------------------------------------------------------
void SegmentInventorySegment::setFreeBlock(unsigned id,unsigned start,unsigned len)
   // Set the unallocated free block
{
   BufferReferenceModified rootPage(modifyExclusive(root));
   InventoryPage* inv=InventoryPage::interpret(rootPage.getPage());
   UpdateFreeBlock(id,inv->getValue(id,2),inv->getValue(id,3),start,len).apply(rootPage);
}
//---------------------------------------------------------------------------
unsigned SegmentInventorySegment::getFreeList(unsigned id) const
   // Get the first freed page
{
   BufferReference rootPage(readShared(root));
   const InventoryPage* inv=InventoryPage::interpret(rootPage.getPage());
   return inv->getValue(id,4);
}
//---------------------------------------------------------------------------
void SegmentInventorySegment::setFreeList(unsigned id,unsigned value)
   // Set the first freed page
{
   BufferReferenceModified rootPage(modifyExclusive(root));
   InventoryPage* inv=InventoryPage::interpret(rootPage.getPage());
   UpdateInventory(id,4,inv->getValue(id,4),value).apply(rootPage);
}
//---------------------------------------------------------------------------
unsigned SegmentInventorySegment::getCustom(unsigned  id,unsigned slot) const
   // Get a custom entry. Valid slots 0-10
{
   assert(slot<=10);

   BufferReference rootPage(readShared(root));
   const InventoryPage* inv=InventoryPage::interpret(rootPage.getPage());
   return inv->getValue(id,slot+5);
}
//---------------------------------------------------------------------------
void SegmentInventorySegment::setCustom(unsigned id,unsigned slot,unsigned value)
   // Set a custom entry. Valid slots 0-10
{
   assert(slot<=10);

   BufferReferenceModified rootPage(modifyExclusive(root));
   InventoryPage* inv=InventoryPage::interpret(rootPage.getPage());
   UpdateInventory(id,slot+5,inv->getValue(id,slot+5),value).apply(rootPage);
}
//---------------------------------------------------------------------------
void SegmentInventorySegment::openPartition(DatabasePartition& partition,std::vector<std::pair<Segment::Type,unsigned> >& segments)
   // Open a partition
{
   BufferReference rootPage;
   rootPage=partition.readShared(root);
   const InventoryPage* inv=InventoryPage::interpret(rootPage.getPage());
   for (unsigned index=0;index<InventoryPage::maxEntries;index++) {
      if (!inv->getType(index)) continue;
      while (segments.size()<index)
         segments.push_back(pair<Segment::Type,unsigned>(Segment::Unused,0));
      segments.push_back(pair<Segment::Type,unsigned>(static_cast<Segment::Type>(inv->getType(index)),inv->getValue(index,1)));
   }
}
//---------------------------------------------------------------------------
