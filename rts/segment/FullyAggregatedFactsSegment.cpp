#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/database/DatabaseBuilder.hpp"
#include "rts/segment/BTree.hpp"
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
// Info slots
static const unsigned slotTableStart = 0;
static const unsigned slotIndexRoot = 1;
static const unsigned slotPages = 2;
static const unsigned slotGroups1 = 3;
//---------------------------------------------------------------------------
/// An index
class FullyAggregatedFactsSegment::IndexImplementation
{
   public:
   /// The size of an inner key
   static const unsigned innerKeySize = 4;
   /// An inner key
   struct InnerKey {
      /// The values
      unsigned value1;

      /// Constructor
      InnerKey() : value1(0) {}
      /// Constructor
      explicit InnerKey(unsigned value1) : value1(value1) {}

      /// Compare
      bool operator==(const InnerKey& o) const { return (value1==o.value1); }
      /// Compare
      bool operator<(const InnerKey& o) const { return (value1<o.value1); }
   };
   /// Read an inner key
   static void readInnerKey(InnerKey& key,const unsigned char* ptr) {
      key.value1=Segment::readUint32Aligned(ptr);
   }
   /// Write an inner key
   static void writeInnerKey(unsigned char* ptr,const InnerKey& key) {
      Segment::writeUint32Aligned(ptr,key.value1);
   }
   /// A leaf entry
   struct LeafEntry {
      /// The key value
      unsigned value1;
      /// THe payload
      unsigned count;

      /// Compare
      bool operator==(const LeafEntry& o) const { return (value1==o.value1); }
      /// Compare
      bool operator<(const LeafEntry& o) const { return (value1<o.value1); }
      /// Compare
      bool operator<(const InnerKey& o) const { return (value1<o.value1); }
   };
   /// A leaf entry source
   class LeafEntrySource {
      private:
      /// The real source
      FullyAggregatedFactsSegment::Source& source;

      public:
      /// Constructor
      LeafEntrySource(FullyAggregatedFactsSegment::Source& source) : source(source) {}

      /// Read the next entry
      bool next(LeafEntry& l) { return source.next(l.value1,l.count); }
      /// Mark last entry as conflict
      void markAsConflict() { source.markAsDuplicate(); }
   };
   /// Derive an inner key
   static InnerKey deriveInnerKey(const LeafEntry& e) { return InnerKey(e.value1); }
   /// Read the first leaf entry
   static void readFirstLeafEntryKey(InnerKey& key,const unsigned char* ptr) {
      key.value1=Segment::readUint32Aligned(ptr);
   }

   private:
   /// The segment
   FullyAggregatedFactsSegment& segment;

   public:
   /// Constructor
   explicit IndexImplementation(FullyAggregatedFactsSegment& segment) : segment(segment) {}

   /// Get the segment
   Segment& getSegment() const { return segment; }
   /// Read a specific page
   BufferRequest readShared(unsigned page) const { return segment.readShared(page); }
   /// Read a specific page
   BufferRequestExclusive readExclusive(unsigned page) const { return segment.readExclusive(page); }
   /// Allocate a new page
   bool allocPage(BufferReferenceModified& page) { return segment.allocPage(page); }
   /// Get the root page
   unsigned getRootPage() const { return segment.indexRoot; }
   /// Set the root page
   void setRootPage(unsigned page);
   /// Store info about the leaf pages
   void updateLeafInfo(unsigned firstLeaf,unsigned leafCount);

   /// Check for duplicates/conflicts and "merge" if equired
   static bool mergeConflictWith(const LeafEntry& newEntry,LeafEntry& oldEntry) { if (newEntry==oldEntry) { oldEntry.count+=newEntry.count; return true; } else return false; }

   /// Pack leaf entries
   static unsigned packLeafEntries(unsigned char* writer,unsigned char* limit,vector<LeafEntry>::const_iterator entriesStart,vector<LeafEntry>::const_iterator entriesLimit);
   /// Unpack leaf entries
   static void unpackLeafEntries(vector<LeafEntry>& entries,const unsigned char* reader,const unsigned char* limit);
};
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::IndexImplementation::setRootPage(unsigned page)
   // Se the root page
{
   segment.indexRoot=page;
   segment.setSegmentData(slotIndexRoot,segment.indexRoot);
}
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::IndexImplementation::updateLeafInfo(unsigned firstLeaf,unsigned leafCount)
   // Store info about the leaf pages
{
   segment.tableStart=firstLeaf;
   segment.setSegmentData(slotTableStart,segment.tableStart);

   segment.pages=leafCount;
   segment.setSegmentData(slotPages,segment.pages);
}
//---------------------------------------------------------------------------
static unsigned bytes0(unsigned v)
   // Compute the number of bytes required to encode a value with 0 compression
{
   if (v>=(1<<24))
      return 4; else
   if (v>=(1<<16))
      return 3; else
   if (v>=(1<<8)) return 2;
   if (v>0)
      return 1; else
      return 0;
}
//---------------------------------------------------------------------------
static unsigned char* writeDelta0(unsigned char* writer,unsigned value)
   // Write an integer with varying size with 0 compression
{
   if (value>=(1<<24)) {
      Segment::writeUint32(writer,value);
      return writer+4;
   } else if (value>=(1<<16)) {
      writer[0]=value>>16;
      writer[1]=(value>>8)&0xFF;
      writer[2]=value&0xFF;
      return writer+3;
   } else if (value>=(1<<8)) {
      writer[0]=value>>8;
      writer[1]=value&0xFF;
      return writer+2;
   } else if (value>0) {
      writer[0]=value;
      return writer+1;
   } else return writer;
}
//---------------------------------------------------------------------------
unsigned FullyAggregatedFactsSegment::IndexImplementation::packLeafEntries(unsigned char* writer,unsigned char* writerLimit,vector<FullyAggregatedFactsSegment::IndexImplementation::LeafEntry>::const_iterator entriesStart,vector<FullyAggregatedFactsSegment::IndexImplementation::LeafEntry>::const_iterator entriesLimit)
   // Pack the facts into leaves using prefix compression
{
   unsigned lastValue1=0;
   unsigned value1,count;

   // Store the first entry
   if (entriesStart==entriesLimit)
      return 0;
   if ((writer+8)>writerLimit)
      return 0;
   Segment::writeUint32Aligned(writer,lastValue1=(*entriesStart).value1);
   Segment::writeUint32Aligned(writer+4,(*entriesStart).count);
   writer+=8;

   // Store the remaining entries
   for (vector<LeafEntry>::const_iterator iter=entriesStart+1;iter!=entriesLimit;++iter) {
      // Compute the length
      value1=(*iter).value1; count=(*iter).count;
      unsigned len;
      if (value1==lastValue1) {
         // Duplicate, must not happen!
         continue;
      }
      if (((value1-lastValue1)<16)&&(count<=8))
         len=1; else
         len=1+bytes0(value1-lastValue1-1)+bytes0(count-1);

      // Entry too big?
      if ((writer+len)>writerLimit) {
         memset(writer,0,writerLimit-writer);
         return iter-entriesStart;
      }

      // No, pack it
      if (((value1-lastValue1)<16)&&(count<=8)) {
         *(writer++)=((count-1)<<4)|(value1-lastValue1);
      } else {
         *(writer++)=0x80|((bytes0(value1-lastValue1-1)*5)+bytes0(count-1));
         writer=writeDelta0(writer,value1-lastValue1-1);
         writer=writeDelta0(writer,count-1);
      }
      lastValue1=value1;
   }

   // Done, everything fitted
   memset(writer,0,writerLimit-writer);
   return entriesLimit-entriesStart;
}
//---------------------------------------------------------------------------
static inline unsigned readDelta1(const unsigned char* pos) { return pos[0]; }
static unsigned readDelta2(const unsigned char* pos) { return (pos[0]<<8)|pos[1]; }
static unsigned readDelta3(const unsigned char* pos) { return (pos[0]<<16)|(pos[1]<<8)|pos[2]; }
static unsigned readDelta4(const unsigned char* pos) { return (pos[0]<<24)|(pos[1]<<16)|(pos[2]<<8)|pos[3]; }
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::IndexImplementation::unpackLeafEntries(vector<FullyAggregatedFactsSegment::IndexImplementation::LeafEntry>& entries,const unsigned char* reader,const unsigned char* limit)
   // Read the facts stored on a leaf page
{
   // Decompress the first triple
   LeafEntry e;
   e.value1=readUint32Aligned(reader); reader+=4;
   e.count=readUint32Aligned(reader); reader+=4;
   entries.push_back(e);

   // Decompress the remainder of the page
   while (reader<limit) {
      // Decode the header byte
      unsigned info=*(reader++);
      // Small gap only?
      if (info<0x80) {
         if (!info)
            break;
         e.count=(info>>4)+1;
         e.value1+=(info&15);
         entries.push_back(e);
         continue;
      }
      // Decode it
      switch (info&127) {
         case 0: e.value1+=1; e.count=1; break;
         case 1: e.value1+=1; e.count=readDelta1(reader)+1; reader+=1; break;
         case 2: e.value1+=1; e.count=readDelta2(reader)+1; reader+=2; break;
         case 3: e.value1+=1; e.count=readDelta3(reader)+1; reader+=3; break;
         case 4: e.value1+=1; e.count=readDelta4(reader)+1; reader+=4; break;
         case 5: e.value1+=readDelta1(reader)+1; e.count=1; reader+=1; break;
         case 6: e.value1+=readDelta1(reader)+1; e.count=readDelta1(reader+1)+1; reader+=2; break;
         case 7: e.value1+=readDelta1(reader)+1; e.count=readDelta2(reader+1)+1; reader+=3; break;
         case 8: e.value1+=readDelta1(reader)+1; e.count=readDelta3(reader+1)+1; reader+=4; break;
         case 9: e.value1+=readDelta1(reader)+1; e.count=readDelta4(reader+1)+1; reader+=5; break;
         case 10: e.value1+=readDelta2(reader)+1; e.count=1; reader+=2; break;
         case 11: e.value1+=readDelta2(reader)+1; e.count=readDelta1(reader+2)+1; reader+=3; break;
         case 12: e.value1+=readDelta2(reader)+1; e.count=readDelta2(reader+2)+1; reader+=4; break;
         case 13: e.value1+=readDelta2(reader)+1; e.count=readDelta3(reader+2)+1; reader+=5; break;
         case 14: e.value1+=readDelta2(reader)+1; e.count=readDelta4(reader+2)+1; reader+=6; break;
         case 15: e.value1+=readDelta3(reader)+1; e.count=1; reader+=3; break;
         case 16: e.value1+=readDelta3(reader)+1; e.count=readDelta1(reader+3)+1; reader+=4; break;
         case 17: e.value1+=readDelta3(reader)+1; e.count=readDelta2(reader+3)+1; reader+=5; break;
         case 18: e.value1+=readDelta3(reader)+1; e.count=readDelta3(reader+3)+1; reader+=6; break;
         case 19: e.value1+=readDelta3(reader)+1; e.count=readDelta4(reader+3)+1; reader+=7; break;
         case 20: e.value1+=readDelta4(reader)+1; e.count=1; reader+=4; break;
         case 21: e.value1+=readDelta4(reader)+1; e.count=readDelta1(reader+4)+1; reader+=5; break;
         case 22: e.value1+=readDelta4(reader)+1; e.count=readDelta2(reader+4)+1; reader+=6; break;
         case 23: e.value1+=readDelta4(reader)+1; e.count=readDelta3(reader+4)+1; reader+=7; break;
         case 24: e.value1+=readDelta4(reader)+1; e.count=readDelta4(reader+4)+1; reader+=8; break;
      }
      entries.push_back(e);
   }
}
//---------------------------------------------------------------------------
/// An index
class FullyAggregatedFactsSegment::Index : public BTree<IndexImplementation>
{
   public:
   /// Constructor
   explicit Index(FullyAggregatedFactsSegment& segment) : BTree<IndexImplementation>(segment) {}

   /// Size of the leaf header (used for scans)
   using BTree<IndexImplementation>::leafHeaderSize;
};
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment::Source::~Source()
   // Destructor
{
}
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment::FullyAggregatedFactsSegment(DatabasePartition& partition)
   : Segment(partition),tableStart(0),indexRoot(0),pages(0),groups1(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::Type FullyAggregatedFactsSegment::getType() const
   // Get the type
{
   return Segment::Type_FullyAggregatedFacts;
}
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::refreshInfo()
   // Refresh segment info stored in the partition
{
   Segment::refreshInfo();

   tableStart=getSegmentData(slotTableStart);
   indexRoot=getSegmentData(slotIndexRoot);
   pages=getSegmentData(slotPages);
   groups1=getSegmentData(slotGroups1);
}
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::loadFullyAggregatedFacts(Source& reader)
   // Load the triples aggregated into the database
{
   Index::LeafEntrySource source(reader);
   Index(*this).performBulkload(source);
}
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::loadCounts(unsigned groups1)
   // Load count statistics
{
   this->groups1=groups1; setSegmentData(slotGroups1,groups1);
}
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::update(FullyAggregatedFactsSegment::Source& reader)
   // Load new facts into the segment
{
   Index::LeafEntrySource source(reader);
   Index(*this).performUpdate(source);
}
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment::Scan::Hint::Hint()
   // Constructor
{
}
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment::Scan::Hint::~Hint()
   // Destructor
{
}
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment::Scan::Scan(Hint* hint)
   : seg(0),hint(hint)
   // Constructor
{
}
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment::Scan::~Scan()
   // Destructor
{
   close();
}
//---------------------------------------------------------------------------
bool FullyAggregatedFactsSegment::Scan::first(FullyAggregatedFactsSegment& segment)
   // Start a new scan over the whole segment
{
   current=segment.readShared(segment.tableStart);
   seg=&segment;
   pos=posLimit=0;

   return next();
}
//---------------------------------------------------------------------------
bool FullyAggregatedFactsSegment::Scan::first(FullyAggregatedFactsSegment& segment,unsigned start1)
   // Start a new scan starting from the first entry >= the start condition
{
   // Lookup the right page
   if (!Index(segment).findLeaf(current,Index::InnerKey(start1)))
      return false;

   // Place the iterator
   seg=&segment;
   pos=posLimit=0;

   // Skip over leading entries that are too small
   while (true) {
      if (!next())
         return false;

      if (getValue1()>=start1)
         return true;
   }
}
//---------------------------------------------------------------------------
bool FullyAggregatedFactsSegment::Scan::find(unsigned value1)
    // Perform a binary search
{
   const Triple* l=pos,*r=posLimit;
   while (l<r) {
      const Triple* m=l+((r-l)/2);
      if (m->value1>value1) {
         r=m;
      } else if (value1>m->value1) {
         l=m+1;
      } else {
         pos=m;
         return true;
      }
   }
   pos=l;
   return pos<posLimit;
}
//---------------------------------------------------------------------------
bool FullyAggregatedFactsSegment::Scan::readNextPage()
   // Read the next page
{
   // Alread read the first page? Then read the next one
   if (pos-1) {
      const unsigned char* page=static_cast<const unsigned char*>(current.getPage());
      unsigned nextPage=readUint32Aligned(page+8);
      if (!nextPage)
         return false;
      current=seg->readShared(nextPage);
   }

   // Decompress the first triple
   const unsigned char* page=static_cast<const unsigned char*>(current.getPage());
   const unsigned char* reader=page+Index::leafHeaderSize,*limit=page+BufferReference::pageSize;
   unsigned value1=readUint32Aligned(reader); reader+=4;
   unsigned count=readUint32Aligned(reader); reader+=4;
   Triple* writer=triples;
   (*writer).value1=value1;
   (*writer).count=count;
   ++writer;

   // Decompress the remainder of the page
   while (reader<limit) {
      // Decode the header byte
      unsigned info=*(reader++);
      // Small gap only?
      if (info<0x80) {
         if (!info)
            break;
         count=(info>>4)+1;
         value1+=(info&15);
         (*writer).value1=value1;
         (*writer).count=count;
         ++writer;
         continue;
      }
      // Decode the parts
      switch (info&127) {
         case 0: value1+=1; count=1; break;
         case 1: value1+=1; count=readDelta1(reader)+1; reader+=1; break;
         case 2: value1+=1; count=readDelta2(reader)+1; reader+=2; break;
         case 3: value1+=1; count=readDelta3(reader)+1; reader+=3; break;
         case 4: value1+=1; count=readDelta4(reader)+1; reader+=4; break;
         case 5: value1+=readDelta1(reader)+1; count=1; reader+=1; break;
         case 6: value1+=readDelta1(reader)+1; count=readDelta1(reader+1)+1; reader+=2; break;
         case 7: value1+=readDelta1(reader)+1; count=readDelta2(reader+1)+1; reader+=3; break;
         case 8: value1+=readDelta1(reader)+1; count=readDelta3(reader+1)+1; reader+=4; break;
         case 9: value1+=readDelta1(reader)+1; count=readDelta4(reader+1)+1; reader+=5; break;
         case 10: value1+=readDelta2(reader)+1; count=1; reader+=2; break;
         case 11: value1+=readDelta2(reader)+1; count=readDelta1(reader+2)+1; reader+=3; break;
         case 12: value1+=readDelta2(reader)+1; count=readDelta2(reader+2)+1; reader+=4; break;
         case 13: value1+=readDelta2(reader)+1; count=readDelta3(reader+2)+1; reader+=5; break;
         case 14: value1+=readDelta2(reader)+1; count=readDelta4(reader+2)+1; reader+=6; break;
         case 15: value1+=readDelta3(reader)+1; count=1; reader+=3; break;
         case 16: value1+=readDelta3(reader)+1; count=readDelta1(reader+3)+1; reader+=4; break;
         case 17: value1+=readDelta3(reader)+1; count=readDelta2(reader+3)+1; reader+=5; break;
         case 18: value1+=readDelta3(reader)+1; count=readDelta3(reader+3)+1; reader+=6; break;
         case 19: value1+=readDelta3(reader)+1; count=readDelta4(reader+3)+1; reader+=7; break;
         case 20: value1+=readDelta4(reader)+1; count=1; reader+=4; break;
         case 21: value1+=readDelta4(reader)+1; count=readDelta1(reader+4)+1; reader+=5; break;
         case 22: value1+=readDelta4(reader)+1; count=readDelta2(reader+4)+1; reader+=6; break;
         case 23: value1+=readDelta4(reader)+1; count=readDelta3(reader+4)+1; reader+=7; break;
         case 24: value1+=readDelta4(reader)+1; count=readDelta4(reader+4)+1; reader+=8; break;
      }
      (*writer).value1=value1;
      (*writer).count=count;
      ++writer;
   }

   // Update the entries
   pos=triples;
   posLimit=writer;

   // Check if we should make a skip
   if (hint) {
      unsigned next1=triples[0].value1;
      while (true) {
         // Compute the next hint
         hint->next(next1);

         // No entry on this page?
         const Triple* oldPos=pos;
         if (!find(next1)) {
            if (!Index(*seg).findLeaf(current,Index::InnerKey(next1)))
               return false;
            pos=posLimit=0;
            ++pos;
            return readNextPage();
         }

         // Stop if we are at a suitable position
         if (oldPos==pos)
            break;
      }
   }

   return true;
}
//---------------------------------------------------------------------------
void FullyAggregatedFactsSegment::Scan::close()
   // Close the scan
{
   seg=0;
   current.reset();
}
//---------------------------------------------------------------------------
