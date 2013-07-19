#ifndef H_rts_segment_SpaceInventorySegment
#define H_rts_segment_SpaceInventorySegment
//---------------------------------------------------------------------------
#include "rts/segment/Segment.hpp"
#include "infra/osdep/Mutex.hpp"
#include <vector>
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
class BufferReferenceExclusive;
class BufferReferenceModified;
//---------------------------------------------------------------------------
/// A segment containing all space consumption information
class SpaceInventorySegment : public Segment
{
   public:
   /// The segment id
   static const Segment::Type ID = Segment::Type_SpaceInventory;
   /// Possible actions
   enum Action {
      Action_BuildInnerNode,Action_ShrinkInnerNode,Action_InsertInnerInterval,Action_UpdateInnerNext,
      Action_BuildLeafNode,Action_ShrinkLeafNode,Action_InsertLeafInterval,Action_DeleteLeafInterval,Action_UpdateLeafInterval,
      Action_UpdateFreePage
   };
   /// An extent entry
   struct ExtentEntry {
      /// The range [from,to[
      unsigned from,to;

      /// Constructor
      ExtentEntry(unsigned from,unsigned to) : from(from),to(to) {}

      /// The length
      unsigned getLength() const { return to-from; }
   };

   /// Helper to allow test access to private members
   class TestInterface;

   private:
   /// The position of the root. Intentionally hard coded, there is only one space inventory per partition.
   /// Note: the "next" pointer of the root is start of the free list!
   static const unsigned root = 2;

   /// The mutex
   Mutex mutex;

   /// Allocate a new page
   void allocPage(BufferReferenceExclusive& rootPage,BufferReferenceModified& page);
   /// Split an inner node
   void splitInner(BufferReferenceExclusive& rootPage,BufferReferenceExclusive& parent,BufferReferenceExclusive& page);
   /// Split a leaf node
   void splitLeaf(BufferReferenceExclusive& rootPage,BufferReferenceExclusive& parent,BufferReferenceExclusive& page);
   /// Find the appropriate leaf node. Returns false if it had to split a page
   bool findLeaf(BufferReferenceExclusive& parent,BufferReferenceExclusive& page,unsigned segmentId,unsigned from,unsigned to,bool ensureSpace);

   /// Insert an interval. Low level primitive.
   void insertInterval(unsigned segmentId,unsigned from,unsigned to);
   /// Delete an interval. Low level primitive.
   void deleteInterval(unsigned segmentId,unsigned from,unsigned to);
   /// Get the extend of a segment
   bool getExtentLocked(unsigned segmentId,std::vector<ExtentEntry>& extent);

   /// Format the root when creating a new partition
   void formatRoot();

   /// Must initialize the segments
   friend class DatabasePartition;

   public:
   /// Constructor
   SpaceInventorySegment(DatabasePartition& partition);
   /// Destructor
   ~SpaceInventorySegment();

   /// Get the type
   Type getType() const;

   /// Completely drop a segment
   bool dropSegment(unsigned segmentId);
   /// Get the extend of a segment
   bool getExtent(unsigned segmentId,std::vector<ExtentEntry>& extent);
   /// Grow a segment
   bool growSegment(unsigned id,unsigned minIncrease,unsigned& start,unsigned& len);
};
//---------------------------------------------------------------------------
#endif
