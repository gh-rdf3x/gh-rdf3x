#include "rts/segment/SpaceInventorySegment.hpp"
#include "rts/buffer/BufferReference.hpp"
#include "rts/database/DatabasePartition.hpp"
#include "rts/partition/Partition.hpp"
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
/// A inner page.
/// Layout:
/// LSN: 64bit
/// next page: 32bit
/// number of entries; 32bit
/// right most child: 32bit
/// entries: maxEntries x 4*32bit
class InnerNode {
   public:
   /// Size of the header
   static const unsigned headerSize = 8+4+4+4;
   /// Size of an entry
   static const unsigned entrySize = 4*4;
   /// Maximum number of entries per page
   static const unsigned maxEntries = (BufferReference::pageSize-headerSize)/entrySize;

   private:
   /// Data pointer
   const unsigned char* ptr() const { return reinterpret_cast<const unsigned char*>(this); }
   /// Data pointer
   unsigned char* ptr() { return reinterpret_cast<unsigned char*>(this); }

   /// Set the next pointer
   void setNext(unsigned next) { Segment::writeUint32(ptr()+8,next); }
   /// Set the count
   void setCount(unsigned count) { Segment::writeUint32(ptr()+12,count); }
   /// Set the rightmost pointer
   void setRightmost(unsigned rightmost) { Segment::writeUint32(ptr()+16,rightmost); }
   /// Set the child part of an entry
   void setChild(unsigned index,unsigned value) { Segment::writeUint32Aligned(ptr()+headerSize+(entrySize*index)+12,value); }

   /// Format the page
   void format(unsigned next,unsigned count,const void* content,unsigned contentLen,unsigned rightmostChild);
   /// Insert an interval
   void insertInterval(unsigned pos,unsigned segmentId,unsigned from,unsigned to,unsigned child,unsigned nextChild);
   /// Delete an interval
   void deleteInterval(unsigned pos,unsigned newChild);

   friend class BuildInnerNode;
   friend class ShrinkInnerNode;
   friend class InsertInnerInterval;
   friend class UpdateInnerNext;

   public:
   /// Get the next page
   unsigned getNext() const { return Segment::readUint32Aligned(ptr()+8); }
   /// Get the number of entries
   unsigned getCount() const { return Segment::readUint32Aligned(ptr()+12); }
   /// Is the page full?
   bool isFull() const { return getCount()==maxEntries; }
   /// Get a pointer to an entry
   const void* getEntryPtr(unsigned index) const { return ptr()+headerSize+(entrySize*index); }
   /// Get the right-most child
   unsigned getRightmost() const { return Segment::readUint32Aligned(ptr()+16); }
   /// Get the segment id of an entry
   unsigned getSegment(unsigned index) const { return Segment::readUint32Aligned(ptr()+headerSize+(entrySize*index)); }
   /// Get the from part of an entry
   unsigned getFrom(unsigned index) const { return Segment::readUint32Aligned(ptr()+headerSize+(entrySize*index)+4); }
   /// Get the to part of an entry
   unsigned getTo(unsigned index) const { return Segment::readUint32Aligned(ptr()+headerSize+(entrySize*index)+8); }
   /// Get the child part of an entry
   unsigned getChild(unsigned index) const { return Segment::readUint32Aligned(ptr()+headerSize+(entrySize*index)+12); }

   /// Find the approriate position for an interval
   unsigned find(unsigned segmentId,unsigned from,unsigned to) const;

   /// Get as inner page
   static const InnerNode* interpret(const void* data) { return static_cast<const InnerNode*>(data); }
   /// Get as inner page
   static InnerNode* interpret(void* data) { return static_cast<InnerNode*>(data); }
   /// Is an inner page?
   static bool isInner(const void* data) { return ~static_cast<uint32_t>(interpret(data)->getRightmost()); }
};
//---------------------------------------------------------------------------
unsigned InnerNode::find(unsigned segmentId,unsigned from,unsigned to) const
   // Find the approriate position for an interval
{
   unsigned left=0,right=getCount();
   while (left!=right) {
      unsigned middle=(left+right)/2;
      if (getSegment(middle)<segmentId) {
         left=middle+1;
      } else if (getSegment(middle)>segmentId) {
         right=middle;
      } else if (getTo(middle)<from) {
         left=middle+1;
      } else if (getFrom(middle)>to) {
         right=middle;
      } else {
         break;
      }
   }
   return left;
}
//---------------------------------------------------------------------------
void InnerNode::format(unsigned next,unsigned count,const void* content,unsigned contentLen,unsigned rightmostChild)
   // Format the page
{
   Segment::writeUint32(ptr()+8,next);
   Segment::writeUint32(ptr()+12,count);
   Segment::writeUint32(ptr()+16,rightmostChild);
   memcpy(ptr()+headerSize,content,contentLen);
}
//---------------------------------------------------------------------------
void InnerNode::insertInterval(unsigned pos,unsigned segmentId,unsigned from,unsigned to,unsigned child,unsigned nextChild)
   // Insert an interval
{
   unsigned char* slot=ptr()+headerSize+(entrySize*pos);
   unsigned count=getCount();
   memmove(slot+entrySize,slot,(count-pos)*entrySize);
   Segment::writeUint32Aligned(slot,segmentId);
   Segment::writeUint32Aligned(slot+4,from);
   Segment::writeUint32Aligned(slot+8,to);
   Segment::writeUint32Aligned(slot+12,child);
   if (pos==count)
      setRightmost(nextChild); else
      setChild(pos+1,nextChild);
   setCount(count+1);
}
//---------------------------------------------------------------------------
void InnerNode::deleteInterval(unsigned pos,unsigned newChild)
   // Delete an interval
{
   unsigned char* slot=ptr()+headerSize+(entrySize*pos);
   unsigned count=getCount();
   memmove(slot,slot+entrySize,(count-pos-1)*entrySize);
   if (pos==(count-1))
      setRightmost(newChild); else
      setChild(pos,newChild);
   setCount(count-1);
}
//---------------------------------------------------------------------------
LOGACTION4(SpaceInventorySegment,BuildInnerNode,uint32_t,next,uint32_t,count,LogData,content,uint32_t,rightmostChild);
//---------------------------------------------------------------------------
void BuildInnerNode::redo(void* page) const { InnerNode::interpret(page)->format(next,count,content.ptr,content.len,rightmostChild); }
void BuildInnerNode::undo(void* /*page*/) const { /* no undo operation */ }
//---------------------------------------------------------------------------
LOGACTION6(SpaceInventorySegment,ShrinkInnerNode,uint32_t,oldNext,uint32_t,oldCount,uint32_t,oldRightmost,uint32_t,newNext,uint32_t,newCount,uint32_t,newRightmost);
//---------------------------------------------------------------------------
void ShrinkInnerNode::redo(void* page) const { InnerNode::interpret(page)->setNext(newNext); InnerNode::interpret(page)->setNext(newCount); InnerNode::interpret(page)->setRightmost(newRightmost); }
void ShrinkInnerNode::undo(void* page) const { InnerNode::interpret(page)->setNext(oldNext); InnerNode::interpret(page)->setNext(oldCount); InnerNode::interpret(page)->setRightmost(oldRightmost); }
//---------------------------------------------------------------------------
LOGACTION6(SpaceInventorySegment,InsertInnerInterval,uint32_t,pos,uint32_t,segmentId,uint32_t,from,uint32_t,to,uint32_t,child,uint32_t,nextChild)
//---------------------------------------------------------------------------
void InsertInnerInterval::redo(void* page) const { InnerNode::interpret(page)->insertInterval(pos,segmentId,from,to,child,nextChild); }
void InsertInnerInterval::undo(void* page) const { InnerNode::interpret(page)->deleteInterval(pos,child); }
//---------------------------------------------------------------------------
LOGACTION2(SpaceInventorySegment,UpdateInnerNext,uint32_t,oldNext,uint32_t,newNext)
//---------------------------------------------------------------------------
void UpdateInnerNext::redo(void* page) const { InnerNode::interpret(page)->setNext(newNext); }
void UpdateInnerNext::undo(void* page) const { InnerNode::interpret(page)->setNext(oldNext); }
//---------------------------------------------------------------------------
/// A leaf node page.
/// Layout:
/// LSN: 64bit
/// next page: 32bit
/// number of entries; 32bit
/// leaf marker: 32bit
/// entries: maxEntries x 3*32bit
class LeafNode {
   public:
   /// Size of the header
   static const unsigned headerSize = 8+4+4+4;
   /// Size of an entry
   static const unsigned entrySize = 3*4;
   /// Maximum number of entries per page
   static const unsigned maxEntries = (BufferReference::pageSize-headerSize)/entrySize;

   private:
   /// Data pointer
   const unsigned char* ptr() const { return reinterpret_cast<const unsigned char*>(this); }
   /// Data pointer
   unsigned char* ptr() { return reinterpret_cast<unsigned char*>(this); }

   /// Set the next pointer
   void setNext(unsigned next) { Segment::writeUint32(ptr()+8,next); }
   /// Set the count
   void setCount(unsigned count) { Segment::writeUint32(ptr()+12,count); }

   /// Format the page
   void format(unsigned next,unsigned count,const void* content,unsigned contentLen);
   /// Insert an interval
   void insertInterval(unsigned pos,unsigned segmentId,unsigned from,unsigned to);
   /// Delete an interval
   void deleteInterval(unsigned pos);
   /// Update the bounds of an interval
   void updateBounds(unsigned pos,unsigned from,unsigned to);

   friend class BuildLeafNode;
   friend class ShrinkLeafNode;
   friend class InsertLeafInterval;
   friend class DeleteLeafInterval;
   friend class UpdateLeafInterval;

   public:
   /// Get the next page
   unsigned getNext() const { return Segment::readUint32Aligned(ptr()+8); }
   /// Get the number of entries
   unsigned getCount() const { return Segment::readUint32Aligned(ptr()+12); }
   /// Is the page full?
   bool isFull() const { return getCount()==maxEntries; }
   /// Get a pointer to an entry
   const void* getEntryPtr(unsigned index) const { return ptr()+headerSize+(entrySize*index); }
   /// Get the segment id of an entry
   unsigned getSegment(unsigned index) const { return Segment::readUint32Aligned(ptr()+headerSize+(entrySize*index)); }
   /// Get the from part of an entry
   unsigned getFrom(unsigned index) const { return Segment::readUint32Aligned(ptr()+headerSize+(entrySize*index)+4); }
   /// Get the to part of an entry
   unsigned getTo(unsigned index) const { return Segment::readUint32Aligned(ptr()+headerSize+(entrySize*index)+8); }

   /// Find the approriate position for an interval
   unsigned find(unsigned segmentId,unsigned from,unsigned to) const;

   /// Get as inner page
   static const LeafNode* interpret(const void* data) { return static_cast<const LeafNode*>(data); }
   /// Get as inner page
   static LeafNode* interpret(void* data) { return static_cast<LeafNode*>(data); }
};
//---------------------------------------------------------------------------
unsigned LeafNode::find(unsigned segmentId,unsigned from,unsigned to) const
   // Find the approriate position for an interval
{
   unsigned left=0,right=getCount();
   while (left!=right) {
      unsigned middle=(left+right)/2;
      if (getSegment(middle)<segmentId) {
         left=middle+1;
      } else if (getSegment(middle)>segmentId) {
         right=middle;
      } else if (getTo(middle)<from) {
         left=middle+1;
      } else if (getFrom(middle)>to) {
         right=middle;
      } else {
         return middle;
      }
   }
   return left;
}
//---------------------------------------------------------------------------
void LeafNode::format(unsigned next,unsigned count,const void* content,unsigned contentLen)
   // Format the page
{
   Segment::writeUint32(ptr()+8,next);
   Segment::writeUint32(ptr()+12,count);
   Segment::writeUint32(ptr()+16,~static_cast<uint32_t>(0)); // leaf marker
   memcpy(ptr()+headerSize,content,contentLen);
}
//---------------------------------------------------------------------------
void LeafNode::insertInterval(unsigned pos,unsigned segmentId,unsigned from,unsigned to)
   // Insert an interval
{
   unsigned char* slot=ptr()+headerSize+(entrySize*pos);
   unsigned count=getCount();
   memmove(slot+entrySize,slot,(count-pos)*entrySize);
   Segment::writeUint32Aligned(slot,segmentId);
   Segment::writeUint32Aligned(slot+4,from);
   Segment::writeUint32Aligned(slot+8,to);
   setCount(count+1);
}
//---------------------------------------------------------------------------
void LeafNode::deleteInterval(unsigned pos)
   // Delete an interval
{
   unsigned char* slot=ptr()+headerSize+(entrySize*pos);
   unsigned count=getCount();
   memmove(slot,slot+entrySize,(count-pos-1)*entrySize);
   setCount(count-1);
}
//---------------------------------------------------------------------------
void LeafNode::updateBounds(unsigned pos,unsigned from,unsigned to)
   // Update the bounds of an interval
{
   Segment::writeUint32Aligned(ptr()+headerSize+(entrySize*pos)+4,from);
   Segment::writeUint32Aligned(ptr()+headerSize+(entrySize*pos)+8,to);
}
//---------------------------------------------------------------------------
LOGACTION3(SpaceInventorySegment,BuildLeafNode,uint32_t,next,uint32_t,count,LogData,content);
//---------------------------------------------------------------------------
void BuildLeafNode::redo(void* page) const { LeafNode::interpret(page)->format(next,count,content.ptr,content.len); }
void BuildLeafNode::undo(void* /*page*/) const { /* no undo operation */ }
//---------------------------------------------------------------------------
LOGACTION4(SpaceInventorySegment,ShrinkLeafNode,uint32_t,oldNext,uint32_t,oldCount,uint32_t,newNext,uint32_t,newCount);
//---------------------------------------------------------------------------
void ShrinkLeafNode::redo(void* page) const { LeafNode::interpret(page)->setNext(newNext); LeafNode::interpret(page)->setCount(newCount); }
void ShrinkLeafNode::undo(void* page) const { LeafNode::interpret(page)->setNext(oldNext); LeafNode::interpret(page)->setCount(oldCount); }
//---------------------------------------------------------------------------
LOGACTION4(SpaceInventorySegment,InsertLeafInterval,uint32_t,pos,uint32_t,segmentId,uint32_t,from,uint32_t,to)
//---------------------------------------------------------------------------
void InsertLeafInterval::redo(void* page) const { LeafNode::interpret(page)->insertInterval(pos,segmentId,from,to); }
void InsertLeafInterval::undo(void* page) const { LeafNode::interpret(page)->deleteInterval(pos); }
//---------------------------------------------------------------------------
LOGACTION4(SpaceInventorySegment,DeleteLeafInterval,uint32_t,pos,uint32_t,segmentId,uint32_t,from,uint32_t,to)
//---------------------------------------------------------------------------
void DeleteLeafInterval::redo(void* page) const { LeafNode::interpret(page)->deleteInterval(pos); }
void DeleteLeafInterval::undo(void* page) const { LeafNode::interpret(page)->insertInterval(pos,segmentId,from,to); }
//---------------------------------------------------------------------------
LOGACTION5(SpaceInventorySegment,UpdateLeafInterval,uint32_t,pos,uint32_t,oldFrom,uint32_t,oldTo,uint32_t,newFrom,uint32_t,newTo)
//---------------------------------------------------------------------------
void UpdateLeafInterval::redo(void* page) const { LeafNode::interpret(page)->updateBounds(pos,newFrom,newTo); }
void UpdateLeafInterval::undo(void* page) const { LeafNode::interpret(page)->updateBounds(pos,oldFrom,oldTo); }
//---------------------------------------------------------------------------
/// A free page
/// Layout:
/// LSN: 64bit
/// next page: 32bit
/// number of consecutive free pages; 32bit
class FreePage {
   private:
   /// Data pointer
   const unsigned char* ptr() const { return reinterpret_cast<const unsigned char*>(this); }
   /// Data pointer
   unsigned char* ptr() { return reinterpret_cast<unsigned char*>(this); }

   /// Set the next pointer
   void setNext(unsigned next) { Segment::writeUint32(ptr()+8,next); }
   /// Set the range
   void setRange(unsigned count) { Segment::writeUint32(ptr()+12,count); }

   friend class UpdateFreePage;

   public:
   /// Get the next page
   unsigned getNext() const { return Segment::readUint32Aligned(ptr()+8); }
   /// Get the range size
   unsigned getRange() const { return Segment::readUint32Aligned(ptr()+12); }

   /// Get as inner page
   static const FreePage* interpret(const void* data) { return static_cast<const FreePage*>(data); }
   /// Get as inner page
   static FreePage* interpret(void* data) { return static_cast<FreePage*>(data); }
   /// Get as inner page
   static const FreePage* interpret(BufferReference& ref) { return interpret(ref.getPage()); }
   /// Get as inner page
   static FreePage* interpret(BufferReferenceModified& ref) { return interpret(ref.getPage()); }
};
//---------------------------------------------------------------------------
LOGACTION4(SpaceInventorySegment,UpdateFreePage,uint32_t,oldNext,uint32_t,oldRange,uint32_t,newNext,uint32_t,newRange)
//---------------------------------------------------------------------------
void UpdateFreePage::redo(void* page) const { FreePage::interpret(page)->setNext(newNext); FreePage::interpret(page)->setRange(newRange); }
void UpdateFreePage::undo(void* page) const { FreePage::interpret(page)->setNext(oldNext); FreePage::interpret(page)->setRange(oldRange); }
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
SpaceInventorySegment::SpaceInventorySegment(DatabasePartition& partition)
   : Segment(partition)
   // Constructor
{
}
//---------------------------------------------------------------------------
SpaceInventorySegment::~SpaceInventorySegment()
   // Destructor
{
}
//---------------------------------------------------------------------------
Segment::Type SpaceInventorySegment::getType() const
   // Get the type
{
   return Segment::Type_SpaceInventory;
}
//---------------------------------------------------------------------------
void SpaceInventorySegment::formatRoot()
   // Format the root when creating a new partition
{
   BufferReferenceModified rootPage(modifyExclusive(root));
   BuildLeafNode(0,0,LogData(0,0)).apply(rootPage);
}
//---------------------------------------------------------------------------
void SpaceInventorySegment::allocPage(BufferReferenceExclusive& rootPage,BufferReferenceModified& page)
   // Allocate a new page
{
   while (true) {
      // Is there a free page available?
      if (InnerNode::interpret(rootPage.getPage())->getNext()) {
	 // Read the page
	 page=modifyExclusive(InnerNode::interpret(rootPage.getPage())->getNext());
	 FreePage* current=FreePage::interpret(page.getPage());

	 // More than one page?
	 if (current->getRange()>1) {
	    // Access the next page
	    BufferReferenceModified nextFree;
	    nextFree=modifyExclusive(page.getPageNo()+1);
	    FreePage* next=FreePage::interpret(nextFree.getPage());

	    // And update it
	    UpdateFreePage(next->getNext(),next->getRange(),current->getNext(),current->getRange()-1).apply(nextFree);

	    // Update the root
	    BufferReferenceModified root;
	    root.modify(rootPage);
	    UpdateInnerNext(page.getPageNo(),page.getPageNo()+1).applyButKeep(root,rootPage);
	 } else {
	    // Just a single page, update the root
	    BufferReferenceModified root;
	    root.modify(rootPage);
	    UpdateInnerNext(page.getPageNo(),current->getNext()).applyButKeep(root,rootPage);
	 }

	 // We have found a free one
	 return;
      }

      // No free pages, increase the partition
      unsigned start,len;
      if (!partition.partition.grow(1,start,len)) {
	 assert(false&&"unable to grow underlying partition, error handling not implemented yet"); // XXX
      }

      // Format the first page
      BufferReferenceModified nextFree;
      nextFree=modifyExclusive(start);
      UpdateFreePage(0,0,0,len).apply(nextFree);

      // Update the root
      BufferReferenceModified root;
      root.modify(rootPage);
      UpdateInnerNext(0,start).applyButKeep(root,rootPage);
   }
}
//---------------------------------------------------------------------------
void SpaceInventorySegment::splitInner(BufferReferenceExclusive& rootPage,BufferReferenceExclusive& parent,BufferReferenceExclusive& page)
   // Split an inner node
{
   // Do we have to split the root?
   if (page.getPageNo()==root) {
      // Allocate two new pages
      BufferReferenceModified left,right;
      unsigned leftId,rightId;
      allocPage(page,left); leftId=left.getPageNo();
      allocPage(page,right); rightId=right.getPageNo();

      // Build a separator
      const InnerNode* inner=InnerNode::interpret(page.getPage());
      unsigned leftCount=inner->getCount()/2,rightCount=(inner->getCount()-leftCount);
      uint32_t separator[4];
      separator[0]=Segment::toBE(inner->getSegment(leftCount-1));
      separator[1]=Segment::toBE(inner->getFrom(leftCount-1));
      separator[2]=Segment::toBE(inner->getTo(leftCount-1));
      separator[3]=leftId;
      unsigned leftRightmost=inner->getChild(leftCount-1);
      unsigned rightRightmost=inner->getRightmost();
      unsigned next=inner->getNext();

      // Copy the content
      BuildInnerNode(right.getPageNo(),leftCount-1,LogData(inner->getEntryPtr(0),InnerNode::entrySize*(leftCount-1)),leftRightmost).apply(left);
      BuildInnerNode(0,rightCount,LogData(inner->getEntryPtr(leftCount),InnerNode::entrySize*rightCount),rightRightmost).apply(right);

      // And convert the page
      BufferReferenceModified current;
      current.modify(page);
      BuildInnerNode(next,1,LogData(separator,InnerNode::entrySize),rightId).apply(current);
   } else {
      // Allocate a new pages
      BufferReferenceModified left,right;
      unsigned leftId,rightId;
      left.modify(page); leftId=left.getPageNo();
      allocPage((parent.getPageNo()==root)?parent:rootPage,right); rightId=right.getPageNo();

      // Build a separator
      const InnerNode* inner=InnerNode::interpret(left.getPage());
      unsigned leftCount=inner->getCount()/2,rightCount=(inner->getCount()-leftCount);
      uint32_t separator[4];
      separator[0]=Segment::toBE(inner->getSegment(leftCount-1));
      separator[1]=Segment::toBE(inner->getFrom(leftCount-1));
      separator[2]=Segment::toBE(inner->getTo(leftCount-1));
      separator[3]=leftId;
      unsigned leftRightmost=inner->getChild(leftCount-1);
      unsigned rightRightmost=inner->getRightmost();

      // Copy the content
      BuildInnerNode(inner->getNext(),rightCount,LogData(inner->getEntryPtr(leftCount),InnerNode::entrySize*rightCount),rightRightmost).apply(right);
      ShrinkInnerNode(inner->getNext(),inner->getCount(),inner->getRightmost(),rightId,leftCount,leftRightmost).apply(left);

      // And insert into the parent
      BufferReferenceModified current;
      current.modify(parent);
      unsigned pos=InnerNode::interpret(current.getPage())->find(separator[0],separator[1],separator[2]);
      InsertInnerInterval(pos,separator[0],separator[1],separator[2],separator[3],rightId).apply(current);
   }
}
//---------------------------------------------------------------------------
void SpaceInventorySegment::splitLeaf(BufferReferenceExclusive& rootPage,BufferReferenceExclusive& parent,BufferReferenceExclusive& page)
   // Split a leaf node
{
   // Do we have to split the root?
   if (page.getPageNo()==root) {
      // Allocate two new pages
      BufferReferenceModified left,right;
      unsigned leftId,rightId;
      allocPage(page,left); leftId=left.getPageNo();
      allocPage(page,right); rightId=right.getPageNo();

      // Build a separator
      const LeafNode* leaf=LeafNode::interpret(page.getPage());
      unsigned leftCount=leaf->getCount()/2,rightCount=(leaf->getCount()-leftCount);
      uint32_t separator[4];
      separator[0]=Segment::toBE(leaf->getSegment(leftCount-1));
      separator[1]=Segment::toBE(leaf->getFrom(leftCount-1));
      separator[2]=Segment::toBE(leaf->getTo(leftCount-1));
      separator[3]=leftId;
      unsigned next=leaf->getNext();

      // Copy the content
      BuildLeafNode(right.getPageNo(),leftCount,LogData(leaf->getEntryPtr(0),LeafNode::entrySize*leftCount)).apply(left);
      BuildLeafNode(0,rightCount,LogData(leaf->getEntryPtr(leftCount),LeafNode::entrySize*rightCount)).apply(right);


      // And convert the page
      BufferReferenceModified current;
      current.modify(page);
      BuildInnerNode(next,1,LogData(separator,InnerNode::entrySize),rightId).apply(current);
   } else {
      // Allocate a new pages
      BufferReferenceModified left,right;
      unsigned leftId,rightId;
      left.modify(page); leftId=left.getPageNo();
      allocPage((parent.getPageNo()==root)?parent:rootPage,right); rightId=right.getPageNo();

      // Build a separator
      const LeafNode* leaf=LeafNode::interpret(left.getPage());
      unsigned leftCount=leaf->getCount()/2,rightCount=(leaf->getCount()-leftCount);
      uint32_t separator[4];
      separator[0]=Segment::toBE(leaf->getSegment(leftCount-1));
      separator[1]=Segment::toBE(leaf->getFrom(leftCount-1));
      separator[2]=Segment::toBE(leaf->getTo(leftCount-1));
      separator[3]=leftId;

      // Copy the content
      BuildLeafNode(leaf->getNext(),rightCount,LogData(leaf->getEntryPtr(leftCount),LeafNode::entrySize*rightCount)).apply(right);
      ShrinkLeafNode(leaf->getNext(),leaf->getCount(),rightId,leftCount).apply(left);

      // And insert into the parent
      BufferReferenceModified current;
      current.modify(parent);
      unsigned pos=InnerNode::interpret(current.getPage())->find(separator[0],separator[1],separator[2]);
      InsertInnerInterval(pos,separator[0],separator[1],separator[2],separator[3],rightId).apply(current);
   }
}
//---------------------------------------------------------------------------
bool SpaceInventorySegment::findLeaf(BufferReferenceExclusive& parent,BufferReferenceExclusive& page,unsigned segmentId,unsigned from,unsigned to,bool ensureSpace)
   // Find the appropriate leaf node. Returns false if it had to split a page
{
   // Access the root
   BufferReferenceExclusive rootPage;
   page=readExclusive(root);

   // Navigate down
   while (true) {
      // Inner node?
      if (InnerNode::isInner(page.getPage())) {
         const InnerNode* node=InnerNode::interpret(page.getPage());
         // Is the node full?
         if (ensureSpace&&node->isFull()) {
            splitInner(rootPage,parent,page);
            return false;
         }
         // Find the right place
         unsigned pos=node->find(segmentId,from,to);

         // Descend
         unsigned next;
         if (pos>=node->getCount())
            next=node->getRightmost(); else
            next=node->getChild(pos);
         if ((!!parent)&&(parent.getPageNo()==root))
            rootPage.swap(parent);
         parent.swap(page);
         page=readExclusive(next);
      } else { // No, a leaf node
         const LeafNode* node=LeafNode::interpret(page.getPage());
         // Is the node full?
         if (ensureSpace&&node->isFull()) {
            splitLeaf(rootPage,parent,page);
            return false;
         }
         // No, we reached the right node
         return true;
      }
   }
}
//---------------------------------------------------------------------------
void SpaceInventorySegment::insertInterval(unsigned segmentId,unsigned from,unsigned to)
   // Insert an interval. Low level primitive.
{
   while (true) {
      // Find the leaf page
      BufferReferenceExclusive parent,page;
      if (!findLeaf(parent,page,segmentId,from,to,true))
         continue; // Retry if we had to split anything

      // Release the parent, we don't need it any more
      parent.reset();

      // Examine the leaf node
      BufferReferenceModified leafPage;
      leafPage.modify(page);
      LeafNode* leaf=LeafNode::interpret(leafPage.getPage());
      unsigned pos=leaf->find(segmentId,from,to);

      // After the current entry?
      if ((pos<leaf->getCount())&&(leaf->getSegment(pos)==segmentId)&&(from<=leaf->getTo(pos))&&(to>leaf->getTo(pos))) {
         UpdateLeafInterval(pos,leaf->getFrom(pos),leaf->getTo(pos),leaf->getFrom(pos),to).apply(leafPage);
         return;
      }

      // Before the next interval?
      if ((pos<leaf->getCount())&&(leaf->getSegment(pos)==segmentId)&&(from<=leaf->getFrom(pos))&&(to>=leaf->getFrom(pos))) {
         UpdateLeafInterval(pos,leaf->getFrom(pos),leaf->getTo(pos),from,leaf->getTo(pos)).apply(leafPage);
         return;
      }

      // Insert
      InsertLeafInterval(pos,segmentId,from,to).apply(leafPage);
      return;
   }
}
//---------------------------------------------------------------------------
void SpaceInventorySegment::deleteInterval(unsigned segmentId,unsigned from,unsigned to)
   // Delete an interval. Low level primitive.
{
   // Find the leaf page
   BufferReferenceExclusive parent,page;
   findLeaf(parent,page,segmentId,from,to,false);

   // Release the parent, we don't need it any more
   parent.reset();

   // Examine the leaf node
   BufferReferenceModified leafPage;
   leafPage.modify(page);
   LeafNode* leaf=LeafNode::interpret(leafPage.getPage());
   unsigned pos=leaf->find(segmentId,from,to);

   assert(pos<leaf->getCount());
   assert(leaf->getSegment(pos)==segmentId);

   // Delete the full interval?
   if ((from==leaf->getFrom(pos))&&(to==leaf->getTo(pos))) {
      DeleteLeafInterval(pos,segmentId,from,to).apply(leafPage);
      return;
   }

   // Remove from the beginning?
   if ((from==leaf->getFrom(pos))&&(to<leaf->getTo(pos))) {
      UpdateLeafInterval(pos,from,leaf->getTo(pos),to,leaf->getTo(pos)).apply(leafPage);
      return;
   }

   // Remove from the end?
   if ((from>leaf->getFrom(pos))&&(to==leaf->getTo(pos))) {
      UpdateLeafInterval(pos,leaf->getFrom(pos),to,leaf->getFrom(pos),from).apply(leafPage);
      return;
   }

   // Other kind of delete, not supported
   assert(false);
}
//---------------------------------------------------------------------------
bool SpaceInventorySegment::dropSegment(unsigned segmentId)
   // Completely drop a segment
{
   // Refuse to drop ourself...
   if (!segmentId)
      return false;

   // Collect the extent
   auto_lock lock(mutex);
   vector<ExtentEntry> extent;
   if (!getExtentLocked(segmentId,extent))
      return false;

   // And drop it
   for (vector<ExtentEntry>::const_iterator iter=extent.begin(),limit=extent.end();iter!=limit;++iter)
      deleteInterval(segmentId,(*iter).from,(*iter).to);

   // Release the free space
   if (!extent.empty()) {
      BufferReferenceExclusive rootPage;
      rootPage=readExclusive(root);

      // Empty free list?
      if (!InnerNode::interpret(rootPage.getPage())->getNext()) {
	 // Update the root
	 BufferReferenceModified root;
	 root.modify(rootPage);
	 UpdateInnerNext(0,extent[0].from).apply(root);

         // Set the links
	 for (unsigned slot=0;slot<extent.size();slot++) {
	    BufferReferenceModified current;
	    current=modifyExclusive(extent[slot].from);
	    FreePage* freePage=FreePage::interpret(current.getPage());
	    unsigned next=((slot+1)<extent.size())?extent[slot+1].from:0;
	    UpdateFreePage(freePage->getNext(),freePage->getRange(),next,extent[slot].getLength()).apply(current);
	 }
	 // And we are done
	 return true;
      }

      // In front of the free list?
      unsigned slot=0,freeIterator=InnerNode::interpret(rootPage.getPage())->getNext();
      if (freeIterator>extent[slot].from) {
	 // Update the root
	 BufferReferenceModified root;
	 root.modify(rootPage);
	 UpdateInnerNext(freeIterator,extent[slot].from).apply(root);

         // Set the links
	 while (((slot+1)<extent.size())&&(extent[slot+1].from<freeIterator)) {
	    BufferReferenceModified current;
	    current=modifyExclusive(extent[slot].from);
	    FreePage* freePage=FreePage::interpret(current.getPage());
	    UpdateFreePage(freePage->getNext(),freePage->getRange(),extent[slot+1].from,extent[slot].getLength()).apply(current);
	    ++slot;
	 }
	 BufferReferenceModified current;
	 current=modifyExclusive(extent[slot].from);
	 FreePage* freePage=FreePage::interpret(current.getPage());
	 UpdateFreePage(freePage->getNext(),freePage->getRange(),freeIterator,extent[slot].getLength()).apply(current);
	 ++slot;
      }

      // Merge with the free list
      rootPage.reset();
      while (slot<extent.size()) {
	 BufferReferenceExclusive listPage;
	 listPage=readExclusive(freeIterator);
	 freeIterator=FreePage::interpret(listPage.getPage())->getNext();
	 unsigned listRange=FreePage::interpret(listPage.getPage())->getRange();

	 // Did we reach the end of the list?
	 if (!freeIterator) {
	    BufferReferenceModified list;
	    list.modify(listPage);
	    UpdateFreePage(0,listRange,extent[slot].from,listRange).apply(list);

	    // Set the links
	    for (;slot<extent.size();slot++) {
	       BufferReferenceModified current;
	       current=modifyExclusive(extent[slot].from);
	       FreePage* freePage=FreePage::interpret(current.getPage());
	       unsigned next=((slot+1)<extent.size())?extent[slot+1].from:0;
	       UpdateFreePage(freePage->getNext(),freePage->getRange(),next,extent[slot].getLength()).apply(current);
	    }
	    continue;
	 }

	 // Merge
	 if (extent[slot].from<freeIterator) {
	    BufferReferenceModified list;
	    list.modify(listPage);
	    UpdateFreePage(freeIterator,listRange,extent[slot].from,listRange).apply(list);

  	    // Set the links
	    for (;slot<extent.size();slot++) {
	       BufferReferenceModified current;
	       current=modifyExclusive(extent[slot].from);
	       FreePage* freePage=FreePage::interpret(current.getPage());
	       unsigned next;
	       if ((slot+1)<extent.size()) {
		  if (extent[slot+1].from<freeIterator)
		     next=extent[slot+1].from; else
		     next=freeIterator;
	       } else next=freeIterator;
	       UpdateFreePage(freePage->getNext(),freePage->getRange(),next,extent[slot].getLength()).apply(current);
	       if (next==freeIterator) {
                  ++slot;
                  break;
               }
	    }
	 }
      }
   }

   return true;
}
//---------------------------------------------------------------------------
bool SpaceInventorySegment::getExtentLocked(unsigned segmentId,std::vector<ExtentEntry>& extent)
   // Get the extend of a segment. The mutex must be held!
{
   // Access the root
   BufferReference page;
   page=readShared(root);

   // Navigate down
   while (true) {
      // Inner node?
      if (InnerNode::isInner(page.getPage())) {
         const InnerNode* node=InnerNode::interpret(page.getPage());
         // Find the right place
         unsigned pos=node->find(segmentId,0,0);

         // Descend
         unsigned next;
         if (pos>=node->getCount())
            next=node->getRightmost(); else
            next=node->getChild(pos);
	 page=readShared(next);
      } else { // No, a leaf node
         const LeafNode* node=LeafNode::interpret(page.getPage());

         // Find the right place
         unsigned pos=node->find(segmentId,0,0);
	 if ((pos>=node->getCount())||(node->getSegment(pos)!=segmentId))
	    return false;

	 // Collect all entriees
	 extent.clear();
	 while (true) {
	    // Did we reach the end of the page?
	    if (pos>=node->getCount()) {
	       unsigned next=node->getNext();
	       if ((!next)||(page.getPageNo()==root)) break;
	       page=readShared(next);
	       node=LeafNode::interpret(page.getPage());
	       pos=0;
	       continue;
	    }
	    // A new segment?
	    if (node->getSegment(pos)!=segmentId)
	       break;

	    // Store the entry
	    extent.push_back(ExtentEntry(node->getFrom(pos),node->getTo(pos)));
	    ++pos;
	 }
         return true;
      }
   }
}
//---------------------------------------------------------------------------
bool SpaceInventorySegment::getExtent(unsigned segmentId,vector<ExtentEntry>& extent)
   // Get the extend of a segment
{
   auto_lock lock(mutex);
   return getExtentLocked(segmentId,extent);
}
//---------------------------------------------------------------------------
bool SpaceInventorySegment::growSegment(unsigned segmentId,unsigned minIncrease,unsigned& start,unsigned& len)
   // Grow a segment
{
   auto_lock lock(mutex);

   // Compute the current size
   unsigned currentLen=0;
   {
      vector<ExtentEntry> extent;
      getExtentLocked(segmentId,extent);
      for (vector<ExtentEntry>::const_iterator iter=extent.begin(),limit=extent.end();iter!=limit;++iter)
         currentLen+=(*iter).getLength();
   }

   // Sanity check to avoid performance degradation
   if (minIncrease<(currentLen/100))
      minIncrease=currentLen/100;

   // Compute a desired increase
   unsigned desiredIncrease=currentLen/8;
   if (desiredIncrease<minIncrease)
      desiredIncrease=minIncrease;

   // Find the best free slot
   unsigned last=0,lastSize=0,lastLast=0,current;
   unsigned bestMin=0,bestMinPrev=0,bestMinSize=~0u;
   unsigned bestPref=0,bestPrefPrev=0,bestPrefSize=~0u;
   { BufferReference page(readShared(root)); current=InnerNode::interpret(page.getPage())->getNext(); }
   while (current) {
      BufferReference page(readShared(current));

      // Fits?
      unsigned size=FreePage::interpret(page)->getRange();
      if (size>=minIncrease) {
         if (size<bestMinSize) {
            bestMin=current;
            bestMinPrev=last;
            bestMinSize=size;
         }
      }
      if (size>=desiredIncrease) {
         if (size<bestPrefSize) {
            bestPref=current;
            bestPrefPrev=last;
            bestPrefSize=size;
         }
      }

      // Go to the next page
      lastLast=last; last=current; lastSize=size;
      current=FreePage::interpret(page)->getNext();
   }

   // No chunk of the desired size found? Then fall back to the minimum size
   if (!bestPref) {
      bestPref=bestMin;
      bestPrefPrev=bestMinPrev;
      bestPrefSize=bestMinSize;
   }

   // Got a suitable position?
   unsigned chunkStart,chunkSize,chunkNext;
   if (bestPref) {
      BufferReference chunk(readShared(bestPref));
      chunkStart=bestPref;
      chunkSize=FreePage::interpret(chunk.getPage())->getRange();
      chunkNext=FreePage::interpret(chunk.getPage())->getNext();
      last=bestPrefPrev;
   } else {
      // No, grow the partition
      if (!partition.partition.grow(desiredIncrease,chunkStart,chunkSize))
         return false;
      // Merge with the last free chunk if appropriate
      if ((last+lastSize)==chunkStart) {
         chunkSize+=lastSize;
         chunkStart=last;
         last=lastLast;
      }
      chunkNext=0;
   }

   // Choose a suitable size, avoid small tails if it seems reasonable
   unsigned size;
   if (chunkSize<(2*desiredIncrease)) {
      size=chunkSize;
   } else {
     size=desiredIncrease;
   }
   start=chunkStart; len=size;

   // Break the chunk if necessary
   if (size<chunkSize) {
      BufferReferenceModified chunkTail(modifyExclusive(chunkStart+size));
      UpdateFreePage(FreePage::interpret(chunkTail)->getNext(),FreePage::interpret(chunkTail)->getRange(),chunkNext,chunkSize-size).apply(chunkTail);
      chunkNext=chunkStart+size;
   }

   // Update the chain
   if (last) {
      BufferReferenceModified listPage(modifyExclusive(last));
      UpdateFreePage(FreePage::interpret(listPage)->getNext(),FreePage::interpret(listPage)->getRange(),chunkNext,FreePage::interpret(listPage)->getRange()).apply(listPage);
   } else {
      BufferReferenceModified rootPage(modifyExclusive(root));
      UpdateInnerNext(InnerNode::interpret(rootPage.getPage())->getNext(),chunkNext).apply(rootPage);
   }

   // And remember the new chunk
   insertInterval(segmentId,chunkStart,chunkStart+size);

   return true;
}
//---------------------------------------------------------------------------
