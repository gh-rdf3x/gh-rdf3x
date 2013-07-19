#include "rts/database/DatabasePartition.hpp"
#include "rts/buffer/BufferReference.hpp"
#include "rts/partition/Partition.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/ExactStatisticsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/segment/SegmentInventorySegment.hpp"
#include "rts/segment/SpaceInventorySegment.hpp"
#include "rts/segment/PredicateSetSegment.hpp"
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
using namespace std;
//---------------------------------------------------------------------------
DatabasePartition::DatabasePartition(BufferManager& bufferManager,Partition& partition)
   : bufferManager(bufferManager),partition(partition)
   // Constructor
{
}
//---------------------------------------------------------------------------
DatabasePartition::~DatabasePartition()
   // Destructor
{
   for (vector<pair<Segment*,unsigned> >::iterator iter=segments.begin(),limit=segments.end();iter!=limit;++iter)
      delete (*iter).first;
}
//---------------------------------------------------------------------------
void DatabasePartition::create()
   // Initialize a new partition with a space inventory and a segment inventory
{
   assert(segments.empty());

   // Ensure a sufficient partition size
   if (partition.getSize()<(SegmentInventorySegment::root+1)) {
      unsigned start,len;
      if (!partition.grow((SegmentInventorySegment::root+1)-partition.getSize(),start,len))
	 assert(false&&"increasing the partition failed");
   }

   // Create a space inventory
   SpaceInventorySegment* spaceInv=new SpaceInventorySegment(*this);
   spaceInv->id=0;
   spaceInv->formatRoot();
   spaceInv->insertInterval(0,SpaceInventorySegment::root,SpaceInventorySegment::root);
   segments.push_back(pair<Segment*,unsigned>(spaceInv,Tag_SpaceInventory));

   // Create a segment inventory
   SegmentInventorySegment* segInv=new SegmentInventorySegment(*this);
   segInv->id=1;
   spaceInv->insertInterval(1,SegmentInventorySegment::root,SegmentInventorySegment::root);
   segInv->addSegment(Segment::Type_SpaceInventory,Tag_SpaceInventory);
   segInv->addSegment(Segment::Type_SegmentInventory,Tag_SegmentInventory);
   segments.push_back(pair<Segment*,unsigned>(segInv,Tag_SegmentInventory));
}
//---------------------------------------------------------------------------
void DatabasePartition::open()
   // Open an existing partition, reconstruct segments as required
{
   assert(segments.empty());

   // Retrieve all segment types
   vector<pair<Segment::Type,unsigned> > segmentTypes;
   SegmentInventorySegment::openPartition(*this,segmentTypes);

   // Reconstruct segments
   unsigned id=0;
   for (vector<pair<Segment::Type,unsigned> >::const_iterator iter=segmentTypes.begin(),limit=segmentTypes.end();iter!=limit;++iter) {
      Segment* seg=0;
      switch ((*iter).first) {
	 case Segment::Unused: segments.push_back(pair<Segment*,unsigned>(0,0)); continue;
	 case Segment::Type_SpaceInventory: seg=new SpaceInventorySegment(*this); break;
	 case Segment::Type_SegmentInventory: seg=new SegmentInventorySegment(*this); break;
         case Segment::Type_Facts: seg=new FactsSegment(*this); break;
         case Segment::Type_AggregatedFacts: seg=new AggregatedFactsSegment(*this); break;
         case Segment::Type_FullyAggregatedFacts: seg=new FullyAggregatedFactsSegment(*this); break;
         case Segment::Type_Dictionary: seg=new DictionarySegment(*this); break;
         case Segment::Type_ExactStatistics: seg=new ExactStatisticsSegment(*this); break;
         case Segment::Type_BTree: break; // Pseudo-Type
         case Segment::Type_PredicateSet: seg=new PredicateSetSegment(*this); break;
      }
      assert(seg);
      seg->id=id++;
      segments.push_back(pair<Segment*,unsigned>(seg,(*iter).second));
   }

   // Refresh the stored info
   for (vector<pair<Segment*,unsigned> >::const_iterator iter=segments.begin(),limit=segments.end();iter!=limit;++iter)
      (*iter).first->refreshInfo();
}
//---------------------------------------------------------------------------
BufferRequest DatabasePartition::readShared(unsigned page) const
   // Read a specific page
{
   return BufferRequest(bufferManager,partition,page);
}
//---------------------------------------------------------------------------
BufferRequestExclusive DatabasePartition::readExclusive(unsigned page)
   // Read a specific page
{
   return BufferRequestExclusive(bufferManager,partition,page);
}
//---------------------------------------------------------------------------
BufferRequestModified DatabasePartition::modifyExclusive(unsigned page)
   // Read a specific page
{
   return BufferRequestModified(bufferManager,partition,page);
}
//---------------------------------------------------------------------------
SpaceInventorySegment* DatabasePartition::getSpaceInventory()
   // Get the space inventory
{
   return static_cast<SpaceInventorySegment*>(segments[0].first);
}
//---------------------------------------------------------------------------
SegmentInventorySegment* DatabasePartition::getSegmentInventory()
   // Get the segment inventory
{
   return static_cast<SegmentInventorySegment*>(segments[1].first);
}
//---------------------------------------------------------------------------
void DatabasePartition::addSegment(Segment* seg,unsigned tag)
   // Add a segment
{
   seg->id=getSegmentInventory()->addSegment(seg->getType(),tag);
   while (seg->id>=segments.size())
      segments.push_back(pair<Segment*,unsigned>(0,0));
   segments[seg->id].first=seg;
   segments[seg->id].second=tag;
}
//---------------------------------------------------------------------------
Segment* DatabasePartition::lookupSegmentBase(unsigned tag)
   // Lookup the first segment with this tag
{
   for (vector<pair<Segment*,unsigned> >::const_iterator iter=segments.begin(),limit=segments.end();iter!=limit;++iter)
      if ((*iter).second==tag)
         return (*iter).first;
   return 0;
}
//---------------------------------------------------------------------------
