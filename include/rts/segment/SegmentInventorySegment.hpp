#ifndef H_rts_segment_SegmentInventorySegment
#define H_rts_segment_SegmentInventorySegment
//---------------------------------------------------------------------------
#include "rts/segment/Segment.hpp"
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
/// A segment containing the directory of all segments on this partition
class SegmentInventorySegment : public Segment
{
   public:
   /// The segment id
   static const Segment::Type ID = Segment::Type_SegmentInventory;
   /// Possible actions
   enum Action { Action_InitializeEntry,Action_UpdateInventory,Action_UpdateFreeBlock,Action_UpdateFreePageList };

   private:
   /// The position of the root. Intentionally hard coded, there is only one segment inventory per partition.
   static const unsigned root = 3;

   friend class DatabasePartition;

   public:
   /// Constructor
   SegmentInventorySegment(DatabasePartition& partition);
   /// Destructor
   ~SegmentInventorySegment();

   /// Get the type
   Type getType() const;

   /// Add a segment, gives the new ID
   unsigned addSegment(Segment::Type type,unsigned tag=0);
   /// Drop a segment
   void dropSegment(unsigned id);

   /// Get the tag of a segment (if any)
   unsigned getTag(unsigned id) const;
   /// Modify the tag of a segment
   void setTag(unsigned id,unsigned tag);
   /// Get the unallocated free block
   void getFreeBlock(unsigned id,unsigned& start,unsigned& len) const;
   /// Set the unallocated free block
   void setFreeBlock(unsigned id,unsigned start,unsigned len);
   /// Get the first freed page
   unsigned getFreeList(unsigned id) const;
   /// Set the first freed page
   void setFreeList(unsigned id,unsigned page);
   /// Get a custom entry. Valid slots 0-10
   unsigned getCustom(unsigned  id,unsigned slot) const;
   /// Set a custom entry. Valid slots 0-10
   void setCustom(unsigned id,unsigned slot,unsigned value);

   /// Open a partition
   static void openPartition(DatabasePartition& partition,std::vector<std::pair<Segment::Type,unsigned> >& segments);
};
//---------------------------------------------------------------------------
#endif
