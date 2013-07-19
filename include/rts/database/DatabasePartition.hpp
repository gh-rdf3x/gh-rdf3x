#ifndef H_rts_database_DatabasePartition
#define H_rts_database_DatabasePartition
//---------------------------------------------------------------------------
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
class BufferManager;
class BufferRequest;
class BufferRequestExclusive;
class BufferRequestModified;
class Partition;
class Segment;
class SegmentInventorySegment;
class SpaceInventorySegment;
//---------------------------------------------------------------------------
/// Manager for all segments contained in one partition
class DatabasePartition
{
   public:
   /// Tags to mark special segments
   enum Tag {
      Tag_Generic = 0, Tag_SpaceInventory, Tag_SegmentInventory, Tag_Schema,
      // These are somewhat ad-hoc, but useful until we get a real schema segment
      Tag_SPO,Tag_Tag_SOP,Tag_OPS,Tag_OSP,Tag_PSO,Tag_POS,
      Tag_SP,Tag_SO,Tag_OP,Tag_OS,Tag_PS,Tag_PO,
      Tag_S,Tag_O,Tag_P,
      Tag_Dictionary,Tag_ExactStatistics
   };

   private:
   /// The buffer manager
   BufferManager& bufferManager;
   /// The partition
   Partition& partition;
   /// All segments
   std::vector<std::pair<Segment*,unsigned> > segments;

   /// The space inventory must be able to grow the underlying partition
   friend class SpaceInventorySegment;

   public:
   /// Constructor
   DatabasePartition(BufferManager& buffer,Partition& partition);
   /// Destructor
   ~DatabasePartition();

   /// Initialize a new partition with a space inventory and a segment inventory
   void create();
   /// Open an existing partition, reconstruct segments as required
   void open();

   /// Read a specific page
   BufferRequest readShared(unsigned page) const;
   /// Read a specific page
   BufferRequestExclusive readExclusive(unsigned page);
   /// Read a specific page
   BufferRequestModified modifyExclusive(unsigned page);

   /// Get the space inventory
   SpaceInventorySegment* getSpaceInventory();
   /// Get the segment inventory
   SegmentInventorySegment* getSegmentInventory();

   /// Add a segment
   void addSegment(Segment* seg,unsigned tag=0);
   /// Lookup the first segment with this tag
   Segment* lookupSegmentBase(unsigned tag);
   /// Lookup the first segment with this tag
   template <class T> T* lookupSegment(unsigned tag) { return dynamic_cast<T*>(lookupSegmentBase(tag)); }
};
//---------------------------------------------------------------------------
#endif
