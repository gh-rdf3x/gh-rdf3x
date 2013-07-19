#include "rts/segment/AggregatedFactsSegment.hpp"
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
static const unsigned slotGroups2 = 4;
//---------------------------------------------------------------------------
/// Compare
static inline bool greater(unsigned a1,unsigned a2,unsigned b1,unsigned b2) {
   return (a1>b1)||((a1==b1)&&(a2>b2));
}
//---------------------------------------------------------------------------
/// An index
class AggregatedFactsSegment::IndexImplementation
{
   public:
   /// The size of an inner key
   static const unsigned innerKeySize = 2*4;
   /// An inner key
   struct InnerKey {
      /// The values
      unsigned value1,value2;

      /// Constructor
      InnerKey() : value1(0),value2(0) {}
      /// Constructor
      InnerKey(unsigned value1,unsigned value2) : value1(value1),value2(value2) {}

      /// Compare
      bool operator==(const InnerKey& o) const { return (value1==o.value1)&&(value2==o.value2); }
      /// Compare
      bool operator<(const InnerKey& o) const { return (value1<o.value1)||((value1==o.value1)&&(value2<o.value2)); }
   };
   /// Read an inner key
   static void readInnerKey(InnerKey& key,const unsigned char* ptr) {
      key.value1=Segment::readUint32Aligned(ptr);
      key.value2=Segment::readUint32Aligned(ptr+4);
   }
   /// Write an inner key
   static void writeInnerKey(unsigned char* ptr,const InnerKey& key) {
      Segment::writeUint32Aligned(ptr,key.value1);
      Segment::writeUint32Aligned(ptr+4,key.value2);
   }
   /// A leaf entry
   struct LeafEntry {
      /// The key values
      unsigned value1,value2;
      /// THe payload
      unsigned count;

      /// Compare
      bool operator==(const LeafEntry& o) const { return (value1==o.value1)&&(value2==o.value2); }
      /// Compare
      bool operator<(const LeafEntry& o) const { return (value1<o.value1)||((value1==o.value1)&&(value2<o.value2)); }
      /// Compare
      bool operator<(const InnerKey& o) const { return (value1<o.value1)||((value1==o.value1)&&(value2<o.value2)); }
   };
   /// A leaf entry source
   class LeafEntrySource {
      private:
      /// The real source
      AggregatedFactsSegment::Source& source;

      public:
      /// Constructor
      LeafEntrySource(AggregatedFactsSegment::Source& source) : source(source) {}

      /// Read the next entry
      bool next(LeafEntry& l) { return source.next(l.value1,l.value2,l.count); }
      /// Mark last entry as conflict
      void markAsConflict() { source.markAsDuplicate(); }
   };
   /// Derive an inner key
   static InnerKey deriveInnerKey(const LeafEntry& e) { return InnerKey(e.value1,e.value2); }
   /// Read the first leaf entry
   static void readFirstLeafEntryKey(InnerKey& key,const unsigned char* ptr) {
      key.value1=Segment::readUint32Aligned(ptr);
      key.value2=Segment::readUint32Aligned(ptr+4);
   }

   private:
   /// The segment
   AggregatedFactsSegment& segment;

   public:
   /// Constructor
   explicit IndexImplementation(AggregatedFactsSegment& segment) : segment(segment) {}

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
void AggregatedFactsSegment::IndexImplementation::setRootPage(unsigned page)
   // Se the root page
{
   segment.indexRoot=page;
   segment.setSegmentData(slotIndexRoot,segment.indexRoot);
}
//---------------------------------------------------------------------------
void AggregatedFactsSegment::IndexImplementation::updateLeafInfo(unsigned firstLeaf,unsigned leafCount)
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
unsigned AggregatedFactsSegment::IndexImplementation::packLeafEntries(unsigned char* writer,unsigned char* writerLimit,vector<AggregatedFactsSegment::IndexImplementation::LeafEntry>::const_iterator entriesStart,vector<AggregatedFactsSegment::IndexImplementation::LeafEntry>::const_iterator entriesLimit)
   // Pack the facts into leaves using prefix compression
{
   unsigned lastValue1=0,lastValue2=0;
   unsigned value1,value2,count;

   // Store the first entry
   if (entriesStart==entriesLimit)
      return 0;
   if ((writer+12)>writerLimit)
      return 0;
   Segment::writeUint32Aligned(writer,lastValue1=(*entriesStart).value1);
   Segment::writeUint32Aligned(writer+4,lastValue2=(*entriesStart).value2);
   Segment::writeUint32Aligned(writer+8,(*entriesStart).count);
   writer+=12;

   // Store the remaining entries
   for (vector<LeafEntry>::const_iterator iter=entriesStart+1;iter!=entriesLimit;++iter) {
      // Compute the length
      value1=(*iter).value1; value2=(*iter).value2; count=(*iter).count;
      unsigned len;
      if ((value1==lastValue1)&&(value2==lastValue2)) {
         // Duplicate, must not happen!
         continue;
      }
      if ((value1==lastValue1)&&(count<5)&&((value2-lastValue2)<32))
         len=1; else
         len=1+bytes0(value1-lastValue1)+bytes0(value2)+bytes0(count-1);

      // Entry too big?
      if ((writer+len)>writerLimit) {
         memset(writer,0,writerLimit-writer);
         return iter-entriesStart;
      }

      // No, pack it
      if ((value1==lastValue1)&&(count<5)&&((value2-lastValue2)<32)) {
         *(writer++)=((count-1)<<5)|(value2-lastValue2);
      } else {
         *(writer++)=0x80|((bytes0(value1-lastValue1)*25)+(bytes0(value2)*5)+bytes0(count-1));
         writer=writeDelta0(writer,value1-lastValue1);
         writer=writeDelta0(writer,value2);
         writer=writeDelta0(writer,count-1);
      }
      lastValue1=value1; lastValue2=value2;
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
void AggregatedFactsSegment::IndexImplementation::unpackLeafEntries(vector<AggregatedFactsSegment::IndexImplementation::LeafEntry>& entries,const unsigned char* reader,const unsigned char* limit)
   // Read the facts stored on a leaf page
{
   // Decompress the first triple
   LeafEntry e;
   e.value1=readUint32Aligned(reader); reader+=4;
   e.value2=readUint32Aligned(reader); reader+=4;
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
         e.count=(info>>5)+1;
         e.value2+=(info&31);
         entries.push_back(e);
         continue;
      }
      // Decode it
      switch (info&127) {
         case 0: e.value2=0; e.count=1; break;
         case 1: e.value2=0; e.count=readDelta1(reader); reader+=1; break;
         case 2: e.value2=0; e.count=readDelta2(reader); reader+=2; break;
         case 3: e.value2=0; e.count=readDelta3(reader); reader+=3; break;
         case 4: e.value2=0; e.count=readDelta4(reader); reader+=4; break;
         case 5: e.value2=readDelta1(reader); e.count=1; reader+=1; break;
         case 6: e.value2=readDelta1(reader); e.count=readDelta1(reader+1)+1; reader+=2; break;
         case 7: e.value2=readDelta1(reader); e.count=readDelta2(reader+1)+1; reader+=3; break;
         case 8: e.value2=readDelta1(reader); e.count=readDelta3(reader+1)+1; reader+=4; break;
         case 9: e.value2=readDelta1(reader); e.count=readDelta4(reader+1)+1; reader+=5; break;
         case 10: e.value2=readDelta2(reader); e.count=1; reader+=2; break;
         case 11: e.value2=readDelta2(reader); e.count=readDelta1(reader+2)+1; reader+=3; break;
         case 12: e.value2=readDelta2(reader); e.count=readDelta2(reader+2)+1; reader+=4; break;
         case 13: e.value2=readDelta2(reader); e.count=readDelta3(reader+2)+1; reader+=5; break;
         case 14: e.value2=readDelta2(reader); e.count=readDelta4(reader+2)+1; reader+=6; break;
         case 15: e.value2=readDelta3(reader); e.count=1; reader+=3; break;
         case 16: e.value2=readDelta3(reader); e.count=readDelta1(reader+3)+1; reader+=4; break;
         case 17: e.value2=readDelta3(reader); e.count=readDelta2(reader+3)+1; reader+=5; break;
         case 18: e.value2=readDelta3(reader); e.count=readDelta3(reader+3)+1; reader+=6; break;
         case 19: e.value2=readDelta3(reader); e.count=readDelta4(reader+3)+1; reader+=7; break;
         case 20: e.value2=readDelta4(reader); e.count=1; reader+=4; break;
         case 21: e.value2=readDelta4(reader); e.count=readDelta1(reader+4)+1; reader+=5; break;
         case 22: e.value2=readDelta4(reader); e.count=readDelta2(reader+4)+1; reader+=6; break;
         case 23: e.value2=readDelta4(reader); e.count=readDelta3(reader+4)+1; reader+=7; break;
         case 24: e.value2=readDelta4(reader); e.count=readDelta4(reader+4)+1; reader+=8; break;
         case 25: e.value1+=readDelta1(reader); e.value2=0; e.count=1; reader+=1; break;
         case 26: e.value1+=readDelta1(reader); e.value2=0; e.count=readDelta1(reader+1)+1; reader+=2; break;
         case 27: e.value1+=readDelta1(reader); e.value2=0; e.count=readDelta2(reader+1)+1; reader+=3; break;
         case 28: e.value1+=readDelta1(reader); e.value2=0; e.count=readDelta3(reader+1)+1; reader+=4; break;
         case 29: e.value1+=readDelta1(reader); e.value2=0; e.count=readDelta4(reader+1)+1; reader+=5; break;
         case 30: e.value1+=readDelta1(reader); e.value2=readDelta1(reader+1); e.count=1; reader+=2; break;
         case 31: e.value1+=readDelta1(reader); e.value2=readDelta1(reader+1); e.count=readDelta1(reader+2)+1; reader+=3; break;
         case 32: e.value1+=readDelta1(reader); e.value2=readDelta1(reader+1); e.count=readDelta2(reader+2)+1; reader+=4; break;
         case 33: e.value1+=readDelta1(reader); e.value2=readDelta1(reader+1); e.count=readDelta3(reader+2)+1; reader+=5; break;
         case 34: e.value1+=readDelta1(reader); e.value2=readDelta1(reader+1); e.count=readDelta4(reader+2)+1; reader+=6; break;
         case 35: e.value1+=readDelta1(reader); e.value2=readDelta2(reader+1); e.count=1; reader+=3; break;
         case 36: e.value1+=readDelta1(reader); e.value2=readDelta2(reader+1); e.count=readDelta1(reader+3)+1; reader+=4; break;
         case 37: e.value1+=readDelta1(reader); e.value2=readDelta2(reader+1); e.count=readDelta2(reader+3)+1; reader+=5; break;
         case 38: e.value1+=readDelta1(reader); e.value2=readDelta2(reader+1); e.count=readDelta3(reader+3)+1; reader+=6; break;
         case 39: e.value1+=readDelta1(reader); e.value2=readDelta2(reader+1); e.count=readDelta4(reader+3)+1; reader+=7; break;
         case 40: e.value1+=readDelta1(reader); e.value2=readDelta3(reader+1); e.count=1; reader+=4; break;
         case 41: e.value1+=readDelta1(reader); e.value2=readDelta3(reader+1); e.count=readDelta1(reader+4)+1; reader+=5; break;
         case 42: e.value1+=readDelta1(reader); e.value2=readDelta3(reader+1); e.count=readDelta2(reader+4)+1; reader+=6; break;
         case 43: e.value1+=readDelta1(reader); e.value2=readDelta3(reader+1); e.count=readDelta3(reader+4)+1; reader+=7; break;
         case 44: e.value1+=readDelta1(reader); e.value2=readDelta3(reader+1); e.count=readDelta4(reader+4)+1; reader+=8; break;
         case 45: e.value1+=readDelta1(reader); e.value2=readDelta4(reader+1); e.count=1; reader+=5; break;
         case 46: e.value1+=readDelta1(reader); e.value2=readDelta4(reader+1); e.count=readDelta1(reader+5)+1; reader+=6; break;
         case 47: e.value1+=readDelta1(reader); e.value2=readDelta4(reader+1); e.count=readDelta2(reader+5)+1; reader+=7; break;
         case 48: e.value1+=readDelta1(reader); e.value2=readDelta4(reader+1); e.count=readDelta3(reader+5)+1; reader+=8; break;
         case 49: e.value1+=readDelta1(reader); e.value2=readDelta4(reader+1); e.count=readDelta4(reader+5)+1; reader+=9; break;
         case 50: e.value1+=readDelta2(reader); e.value2=0; e.count=1; reader+=2; break;
         case 51: e.value1+=readDelta2(reader); e.value2=0; e.count=readDelta1(reader+2)+1; reader+=3; break;
         case 52: e.value1+=readDelta2(reader); e.value2=0; e.count=readDelta2(reader+2)+1; reader+=4; break;
         case 53: e.value1+=readDelta2(reader); e.value2=0; e.count=readDelta3(reader+2)+1; reader+=5; break;
         case 54: e.value1+=readDelta2(reader); e.value2=0; e.count=readDelta4(reader+2)+1; reader+=6; break;
         case 55: e.value1+=readDelta2(reader); e.value2=readDelta1(reader+2); e.count=1; reader+=3; break;
         case 56: e.value1+=readDelta2(reader); e.value2=readDelta1(reader+2); e.count=readDelta1(reader+3)+1; reader+=4; break;
         case 57: e.value1+=readDelta2(reader); e.value2=readDelta1(reader+2); e.count=readDelta2(reader+3)+1; reader+=5; break;
         case 58: e.value1+=readDelta2(reader); e.value2=readDelta1(reader+2); e.count=readDelta3(reader+3)+1; reader+=6; break;
         case 59: e.value1+=readDelta2(reader); e.value2=readDelta1(reader+2); e.count=readDelta4(reader+3)+1; reader+=7; break;
         case 60: e.value1+=readDelta2(reader); e.value2=readDelta2(reader+2); e.count=1; reader+=4; break;
         case 61: e.value1+=readDelta2(reader); e.value2=readDelta2(reader+2); e.count=readDelta1(reader+4)+1; reader+=5; break;
         case 62: e.value1+=readDelta2(reader); e.value2=readDelta2(reader+2); e.count=readDelta2(reader+4)+1; reader+=6; break;
         case 63: e.value1+=readDelta2(reader); e.value2=readDelta2(reader+2); e.count=readDelta3(reader+4)+1; reader+=7; break;
         case 64: e.value1+=readDelta2(reader); e.value2=readDelta2(reader+2); e.count=readDelta4(reader+4)+1; reader+=8; break;
         case 65: e.value1+=readDelta2(reader); e.value2=readDelta3(reader+2); e.count=1; reader+=5; break;
         case 66: e.value1+=readDelta2(reader); e.value2=readDelta3(reader+2); e.count=readDelta1(reader+5)+1; reader+=6; break;
         case 67: e.value1+=readDelta2(reader); e.value2=readDelta3(reader+2); e.count=readDelta2(reader+5)+1; reader+=7; break;
         case 68: e.value1+=readDelta2(reader); e.value2=readDelta3(reader+2); e.count=readDelta3(reader+5)+1; reader+=8; break;
         case 69: e.value1+=readDelta2(reader); e.value2=readDelta3(reader+2); e.count=readDelta4(reader+5)+1; reader+=9; break;
         case 70: e.value1+=readDelta2(reader); e.value2=readDelta4(reader+2); e.count=1; reader+=6; break;
         case 71: e.value1+=readDelta2(reader); e.value2=readDelta4(reader+2); e.count=readDelta1(reader+6)+1; reader+=7; break;
         case 72: e.value1+=readDelta2(reader); e.value2=readDelta4(reader+2); e.count=readDelta2(reader+6)+1; reader+=8; break;
         case 73: e.value1+=readDelta2(reader); e.value2=readDelta4(reader+2); e.count=readDelta3(reader+6)+1; reader+=9; break;
         case 74: e.value1+=readDelta2(reader); e.value2=readDelta4(reader+2); e.count=readDelta4(reader+6)+1; reader+=10; break;
         case 75: e.value1+=readDelta3(reader); e.value2=0; e.count=1; reader+=3; break;
         case 76: e.value1+=readDelta3(reader); e.value2=0; e.count=readDelta1(reader+3)+1; reader+=4; break;
         case 77: e.value1+=readDelta3(reader); e.value2=0; e.count=readDelta2(reader+3)+1; reader+=5; break;
         case 78: e.value1+=readDelta3(reader); e.value2=0; e.count=readDelta3(reader+3)+1; reader+=6; break;
         case 79: e.value1+=readDelta3(reader); e.value2=0; e.count=readDelta4(reader+3)+1; reader+=7; break;
         case 80: e.value1+=readDelta3(reader); e.value2=readDelta1(reader+3); e.count=1; reader+=4; break;
         case 81: e.value1+=readDelta3(reader); e.value2=readDelta1(reader+3); e.count=readDelta1(reader+4)+1; reader+=5; break;
         case 82: e.value1+=readDelta3(reader); e.value2=readDelta1(reader+3); e.count=readDelta2(reader+4)+1; reader+=6; break;
         case 83: e.value1+=readDelta3(reader); e.value2=readDelta1(reader+3); e.count=readDelta3(reader+4)+1; reader+=7; break;
         case 84: e.value1+=readDelta3(reader); e.value2=readDelta1(reader+3); e.count=readDelta4(reader+4)+1; reader+=8; break;
         case 85: e.value1+=readDelta3(reader); e.value2=readDelta2(reader+3); e.count=1; reader+=5; break;
         case 86: e.value1+=readDelta3(reader); e.value2=readDelta2(reader+3); e.count=readDelta1(reader+5)+1; reader+=6; break;
         case 87: e.value1+=readDelta3(reader); e.value2=readDelta2(reader+3); e.count=readDelta2(reader+5)+1; reader+=7; break;
         case 88: e.value1+=readDelta3(reader); e.value2=readDelta2(reader+3); e.count=readDelta3(reader+5)+1; reader+=8; break;
         case 89: e.value1+=readDelta3(reader); e.value2=readDelta2(reader+3); e.count=readDelta4(reader+5)+1; reader+=9; break;
         case 90: e.value1+=readDelta3(reader); e.value2=readDelta3(reader+3); e.count=1; reader+=6; break;
         case 91: e.value1+=readDelta3(reader); e.value2=readDelta3(reader+3); e.count=readDelta1(reader+6)+1; reader+=7; break;
         case 92: e.value1+=readDelta3(reader); e.value2=readDelta3(reader+3); e.count=readDelta2(reader+6)+1; reader+=8; break;
         case 93: e.value1+=readDelta3(reader); e.value2=readDelta3(reader+3); e.count=readDelta3(reader+6)+1; reader+=9; break;
         case 94: e.value1+=readDelta3(reader); e.value2=readDelta3(reader+3); e.count=readDelta4(reader+6)+1; reader+=10; break;
         case 95: e.value1+=readDelta3(reader); e.value2=readDelta4(reader+3); e.count=1; reader+=7; break;
         case 96: e.value1+=readDelta3(reader); e.value2=readDelta4(reader+3); e.count=readDelta1(reader+7)+1; reader+=8; break;
         case 97: e.value1+=readDelta3(reader); e.value2=readDelta4(reader+3); e.count=readDelta2(reader+7)+1; reader+=9; break;
         case 98: e.value1+=readDelta3(reader); e.value2=readDelta4(reader+3); e.count=readDelta3(reader+7)+1; reader+=10; break;
         case 99: e.value1+=readDelta3(reader); e.value2=readDelta4(reader+3); e.count=readDelta4(reader+7)+1; reader+=11; break;
         case 100: e.value1+=readDelta4(reader); e.value2=0; e.count=1; reader+=4; break;
         case 101: e.value1+=readDelta4(reader); e.value2=0; e.count=readDelta1(reader+4)+1; reader+=5; break;
         case 102: e.value1+=readDelta4(reader); e.value2=0; e.count=readDelta2(reader+4)+1; reader+=6; break;
         case 103: e.value1+=readDelta4(reader); e.value2=0; e.count=readDelta3(reader+4)+1; reader+=7; break;
         case 104: e.value1+=readDelta4(reader); e.value2=0; e.count=readDelta4(reader+4)+1; reader+=8; break;
         case 105: e.value1+=readDelta4(reader); e.value2=readDelta1(reader+4); e.count=1; reader+=5; break;
         case 106: e.value1+=readDelta4(reader); e.value2=readDelta1(reader+4); e.count=readDelta1(reader+5)+1; reader+=6; break;
         case 107: e.value1+=readDelta4(reader); e.value2=readDelta1(reader+4); e.count=readDelta2(reader+5)+1; reader+=7; break;
         case 108: e.value1+=readDelta4(reader); e.value2=readDelta1(reader+4); e.count=readDelta3(reader+5)+1; reader+=8; break;
         case 109: e.value1+=readDelta4(reader); e.value2=readDelta1(reader+4); e.count=readDelta4(reader+5)+1; reader+=9; break;
         case 110: e.value1+=readDelta4(reader); e.value2=readDelta2(reader+4); e.count=1; reader+=6; break;
         case 111: e.value1+=readDelta4(reader); e.value2=readDelta2(reader+4); e.count=readDelta1(reader+6)+1; reader+=7; break;
         case 112: e.value1+=readDelta4(reader); e.value2=readDelta2(reader+4); e.count=readDelta2(reader+6)+1; reader+=8; break;
         case 113: e.value1+=readDelta4(reader); e.value2=readDelta2(reader+4); e.count=readDelta3(reader+6)+1; reader+=9; break;
         case 114: e.value1+=readDelta4(reader); e.value2=readDelta2(reader+4); e.count=readDelta4(reader+6)+1; reader+=10; break;
         case 115: e.value1+=readDelta4(reader); e.value2=readDelta3(reader+4); e.count=1; reader+=7; break;
         case 116: e.value1+=readDelta4(reader); e.value2=readDelta3(reader+4); e.count=readDelta1(reader+7)+1; reader+=8; break;
         case 117: e.value1+=readDelta4(reader); e.value2=readDelta3(reader+4); e.count=readDelta2(reader+7)+1; reader+=9; break;
         case 118: e.value1+=readDelta4(reader); e.value2=readDelta3(reader+4); e.count=readDelta3(reader+7)+1; reader+=10; break;
         case 119: e.value1+=readDelta4(reader); e.value2=readDelta3(reader+4); e.count=readDelta4(reader+7)+1; reader+=11; break;
         case 120: e.value1+=readDelta4(reader); e.value2=readDelta4(reader+4); e.count=1; reader+=8; break;
         case 121: e.value1+=readDelta4(reader); e.value2=readDelta4(reader+4); e.count=readDelta1(reader+8)+1; reader+=9; break;
         case 122: e.value1+=readDelta4(reader); e.value2=readDelta4(reader+4); e.count=readDelta2(reader+8)+1; reader+=10; break;
         case 123: e.value1+=readDelta4(reader); e.value2=readDelta4(reader+4); e.count=readDelta3(reader+8)+1; reader+=11; break;
         case 124: e.value1+=readDelta4(reader); e.value2=readDelta4(reader+4); e.count=readDelta4(reader+8)+1; reader+=12; break;
      }
      entries.push_back(e);
   }
}
//---------------------------------------------------------------------------
/// An index
class AggregatedFactsSegment::Index : public BTree<IndexImplementation>
{
   public:
   /// Constructor
   Index(AggregatedFactsSegment& seg) : BTree<IndexImplementation>(seg) {}

   using BTree<IndexImplementation>::leafHeaderSize;
};
//---------------------------------------------------------------------------
AggregatedFactsSegment::Source::~Source()
   // Destructor
{
}
//---------------------------------------------------------------------------
AggregatedFactsSegment::AggregatedFactsSegment(DatabasePartition& partition)
   : Segment(partition),tableStart(0),indexRoot(0),pages(0),groups1(0),groups2(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::Type AggregatedFactsSegment::getType() const
   // Get the type
{
   return Segment::Type_AggregatedFacts;
}
//---------------------------------------------------------------------------
void AggregatedFactsSegment::refreshInfo()
   // Refresh segment info stored in the partition
{
   Segment::refreshInfo();

   tableStart=getSegmentData(slotTableStart);
   indexRoot=getSegmentData(slotIndexRoot);
   pages=getSegmentData(slotPages);
   groups1=getSegmentData(slotGroups1);
   groups2=getSegmentData(slotGroups2);
}
//---------------------------------------------------------------------------
void AggregatedFactsSegment::loadAggregatedFacts(Source& reader)
   // Load the triples aggregated into the database
{
   Index::LeafEntrySource source(reader);
   Index(*this).performBulkload(source);
}
//---------------------------------------------------------------------------
void AggregatedFactsSegment::loadCounts(unsigned groups1,unsigned groups2)
   // Load count statistics
{
   this->groups1=groups1; setSegmentData(slotGroups1,groups1);
   this->groups2=groups2; setSegmentData(slotGroups2,groups2);
}
//---------------------------------------------------------------------------
void AggregatedFactsSegment::update(AggregatedFactsSegment::Source& reader)
   // Load new facts into the segment
{
   Index::LeafEntrySource source(reader);
   Index(*this).performUpdate(source);
}
//---------------------------------------------------------------------------
AggregatedFactsSegment::Scan::Hint::Hint()
   // Constructor
{
}
//---------------------------------------------------------------------------
AggregatedFactsSegment::Scan::Hint::~Hint()
   // Destructor
{
}
//---------------------------------------------------------------------------
AggregatedFactsSegment::Scan::Scan(Hint* hint)
   : seg(0),hint(hint)
   // Constructor
{
}
//---------------------------------------------------------------------------
AggregatedFactsSegment::Scan::~Scan()
   // Destructor
{
   close();
}
//---------------------------------------------------------------------------
bool AggregatedFactsSegment::Scan::first(AggregatedFactsSegment& segment)
   // Start a new scan over the whole segment
{
   current=segment.readShared(segment.tableStart);
   seg=&segment;
   pos=posLimit=0;

   return next();
}
//---------------------------------------------------------------------------
bool AggregatedFactsSegment::Scan::first(AggregatedFactsSegment& segment,unsigned start1,unsigned start2)
   // Start a new scan starting from the first entry >= the start condition
{
   // Lookup the right page
   if (!Index(segment).findLeaf(current,Index::InnerKey(start1,start2)))
      return false;

   // Place the iterator
   seg=&segment;
   pos=posLimit=0;

   // Skip over leading entries that are too small
   while (true) {
      if (!next())
         return false;

      if ((getValue1()>start1)||((getValue1()==start1)&&(getValue2()>=start2)))
         return true;
   }
}
//---------------------------------------------------------------------------
bool AggregatedFactsSegment::Scan::find(unsigned value1,unsigned value2)
    // Perform a binary search
{
   const Triple* l=pos,*r=posLimit;
   while (l<r) {
      const Triple* m=l+((r-l)/2);
      if (greater(m->value1,m->value2,value1,value2)) {
         r=m;
      } else if (greater(value1,value2,m->value1,m->value2)) {
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
bool AggregatedFactsSegment::Scan::readNextPage()
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
   unsigned value2=readUint32Aligned(reader); reader+=4;
   unsigned count=readUint32Aligned(reader); reader+=4;
   Triple* writer=triples;
   (*writer).value1=value1;
   (*writer).value2=value2;
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
         count=(info>>5)+1;
         value2+=(info&31);
         (*writer).value1=value1;
         (*writer).value2=value2;
         (*writer).count=count;
         ++writer;
         continue;
      }
      // Decode the parts
      switch (info&127) {
         case 0: value2=0; count=1; break;
         case 1: value2=0; count=readDelta1(reader); reader+=1; break;
         case 2: value2=0; count=readDelta2(reader); reader+=2; break;
         case 3: value2=0; count=readDelta3(reader); reader+=3; break;
         case 4: value2=0; count=readDelta4(reader); reader+=4; break;
         case 5: value2=readDelta1(reader); count=1; reader+=1; break;
         case 6: value2=readDelta1(reader); count=readDelta1(reader+1)+1; reader+=2; break;
         case 7: value2=readDelta1(reader); count=readDelta2(reader+1)+1; reader+=3; break;
         case 8: value2=readDelta1(reader); count=readDelta3(reader+1)+1; reader+=4; break;
         case 9: value2=readDelta1(reader); count=readDelta4(reader+1)+1; reader+=5; break;
         case 10: value2=readDelta2(reader); count=1; reader+=2; break;
         case 11: value2=readDelta2(reader); count=readDelta1(reader+2)+1; reader+=3; break;
         case 12: value2=readDelta2(reader); count=readDelta2(reader+2)+1; reader+=4; break;
         case 13: value2=readDelta2(reader); count=readDelta3(reader+2)+1; reader+=5; break;
         case 14: value2=readDelta2(reader); count=readDelta4(reader+2)+1; reader+=6; break;
         case 15: value2=readDelta3(reader); count=1; reader+=3; break;
         case 16: value2=readDelta3(reader); count=readDelta1(reader+3)+1; reader+=4; break;
         case 17: value2=readDelta3(reader); count=readDelta2(reader+3)+1; reader+=5; break;
         case 18: value2=readDelta3(reader); count=readDelta3(reader+3)+1; reader+=6; break;
         case 19: value2=readDelta3(reader); count=readDelta4(reader+3)+1; reader+=7; break;
         case 20: value2=readDelta4(reader); count=1; reader+=4; break;
         case 21: value2=readDelta4(reader); count=readDelta1(reader+4)+1; reader+=5; break;
         case 22: value2=readDelta4(reader); count=readDelta2(reader+4)+1; reader+=6; break;
         case 23: value2=readDelta4(reader); count=readDelta3(reader+4)+1; reader+=7; break;
         case 24: value2=readDelta4(reader); count=readDelta4(reader+4)+1; reader+=8; break;
         case 25: value1+=readDelta1(reader); value2=0; count=1; reader+=1; break;
         case 26: value1+=readDelta1(reader); value2=0; count=readDelta1(reader+1)+1; reader+=2; break;
         case 27: value1+=readDelta1(reader); value2=0; count=readDelta2(reader+1)+1; reader+=3; break;
         case 28: value1+=readDelta1(reader); value2=0; count=readDelta3(reader+1)+1; reader+=4; break;
         case 29: value1+=readDelta1(reader); value2=0; count=readDelta4(reader+1)+1; reader+=5; break;
         case 30: value1+=readDelta1(reader); value2=readDelta1(reader+1); count=1; reader+=2; break;
         case 31: value1+=readDelta1(reader); value2=readDelta1(reader+1); count=readDelta1(reader+2)+1; reader+=3; break;
         case 32: value1+=readDelta1(reader); value2=readDelta1(reader+1); count=readDelta2(reader+2)+1; reader+=4; break;
         case 33: value1+=readDelta1(reader); value2=readDelta1(reader+1); count=readDelta3(reader+2)+1; reader+=5; break;
         case 34: value1+=readDelta1(reader); value2=readDelta1(reader+1); count=readDelta4(reader+2)+1; reader+=6; break;
         case 35: value1+=readDelta1(reader); value2=readDelta2(reader+1); count=1; reader+=3; break;
         case 36: value1+=readDelta1(reader); value2=readDelta2(reader+1); count=readDelta1(reader+3)+1; reader+=4; break;
         case 37: value1+=readDelta1(reader); value2=readDelta2(reader+1); count=readDelta2(reader+3)+1; reader+=5; break;
         case 38: value1+=readDelta1(reader); value2=readDelta2(reader+1); count=readDelta3(reader+3)+1; reader+=6; break;
         case 39: value1+=readDelta1(reader); value2=readDelta2(reader+1); count=readDelta4(reader+3)+1; reader+=7; break;
         case 40: value1+=readDelta1(reader); value2=readDelta3(reader+1); count=1; reader+=4; break;
         case 41: value1+=readDelta1(reader); value2=readDelta3(reader+1); count=readDelta1(reader+4)+1; reader+=5; break;
         case 42: value1+=readDelta1(reader); value2=readDelta3(reader+1); count=readDelta2(reader+4)+1; reader+=6; break;
         case 43: value1+=readDelta1(reader); value2=readDelta3(reader+1); count=readDelta3(reader+4)+1; reader+=7; break;
         case 44: value1+=readDelta1(reader); value2=readDelta3(reader+1); count=readDelta4(reader+4)+1; reader+=8; break;
         case 45: value1+=readDelta1(reader); value2=readDelta4(reader+1); count=1; reader+=5; break;
         case 46: value1+=readDelta1(reader); value2=readDelta4(reader+1); count=readDelta1(reader+5)+1; reader+=6; break;
         case 47: value1+=readDelta1(reader); value2=readDelta4(reader+1); count=readDelta2(reader+5)+1; reader+=7; break;
         case 48: value1+=readDelta1(reader); value2=readDelta4(reader+1); count=readDelta3(reader+5)+1; reader+=8; break;
         case 49: value1+=readDelta1(reader); value2=readDelta4(reader+1); count=readDelta4(reader+5)+1; reader+=9; break;
         case 50: value1+=readDelta2(reader); value2=0; count=1; reader+=2; break;
         case 51: value1+=readDelta2(reader); value2=0; count=readDelta1(reader+2)+1; reader+=3; break;
         case 52: value1+=readDelta2(reader); value2=0; count=readDelta2(reader+2)+1; reader+=4; break;
         case 53: value1+=readDelta2(reader); value2=0; count=readDelta3(reader+2)+1; reader+=5; break;
         case 54: value1+=readDelta2(reader); value2=0; count=readDelta4(reader+2)+1; reader+=6; break;
         case 55: value1+=readDelta2(reader); value2=readDelta1(reader+2); count=1; reader+=3; break;
         case 56: value1+=readDelta2(reader); value2=readDelta1(reader+2); count=readDelta1(reader+3)+1; reader+=4; break;
         case 57: value1+=readDelta2(reader); value2=readDelta1(reader+2); count=readDelta2(reader+3)+1; reader+=5; break;
         case 58: value1+=readDelta2(reader); value2=readDelta1(reader+2); count=readDelta3(reader+3)+1; reader+=6; break;
         case 59: value1+=readDelta2(reader); value2=readDelta1(reader+2); count=readDelta4(reader+3)+1; reader+=7; break;
         case 60: value1+=readDelta2(reader); value2=readDelta2(reader+2); count=1; reader+=4; break;
         case 61: value1+=readDelta2(reader); value2=readDelta2(reader+2); count=readDelta1(reader+4)+1; reader+=5; break;
         case 62: value1+=readDelta2(reader); value2=readDelta2(reader+2); count=readDelta2(reader+4)+1; reader+=6; break;
         case 63: value1+=readDelta2(reader); value2=readDelta2(reader+2); count=readDelta3(reader+4)+1; reader+=7; break;
         case 64: value1+=readDelta2(reader); value2=readDelta2(reader+2); count=readDelta4(reader+4)+1; reader+=8; break;
         case 65: value1+=readDelta2(reader); value2=readDelta3(reader+2); count=1; reader+=5; break;
         case 66: value1+=readDelta2(reader); value2=readDelta3(reader+2); count=readDelta1(reader+5)+1; reader+=6; break;
         case 67: value1+=readDelta2(reader); value2=readDelta3(reader+2); count=readDelta2(reader+5)+1; reader+=7; break;
         case 68: value1+=readDelta2(reader); value2=readDelta3(reader+2); count=readDelta3(reader+5)+1; reader+=8; break;
         case 69: value1+=readDelta2(reader); value2=readDelta3(reader+2); count=readDelta4(reader+5)+1; reader+=9; break;
         case 70: value1+=readDelta2(reader); value2=readDelta4(reader+2); count=1; reader+=6; break;
         case 71: value1+=readDelta2(reader); value2=readDelta4(reader+2); count=readDelta1(reader+6)+1; reader+=7; break;
         case 72: value1+=readDelta2(reader); value2=readDelta4(reader+2); count=readDelta2(reader+6)+1; reader+=8; break;
         case 73: value1+=readDelta2(reader); value2=readDelta4(reader+2); count=readDelta3(reader+6)+1; reader+=9; break;
         case 74: value1+=readDelta2(reader); value2=readDelta4(reader+2); count=readDelta4(reader+6)+1; reader+=10; break;
         case 75: value1+=readDelta3(reader); value2=0; count=1; reader+=3; break;
         case 76: value1+=readDelta3(reader); value2=0; count=readDelta1(reader+3)+1; reader+=4; break;
         case 77: value1+=readDelta3(reader); value2=0; count=readDelta2(reader+3)+1; reader+=5; break;
         case 78: value1+=readDelta3(reader); value2=0; count=readDelta3(reader+3)+1; reader+=6; break;
         case 79: value1+=readDelta3(reader); value2=0; count=readDelta4(reader+3)+1; reader+=7; break;
         case 80: value1+=readDelta3(reader); value2=readDelta1(reader+3); count=1; reader+=4; break;
         case 81: value1+=readDelta3(reader); value2=readDelta1(reader+3); count=readDelta1(reader+4)+1; reader+=5; break;
         case 82: value1+=readDelta3(reader); value2=readDelta1(reader+3); count=readDelta2(reader+4)+1; reader+=6; break;
         case 83: value1+=readDelta3(reader); value2=readDelta1(reader+3); count=readDelta3(reader+4)+1; reader+=7; break;
         case 84: value1+=readDelta3(reader); value2=readDelta1(reader+3); count=readDelta4(reader+4)+1; reader+=8; break;
         case 85: value1+=readDelta3(reader); value2=readDelta2(reader+3); count=1; reader+=5; break;
         case 86: value1+=readDelta3(reader); value2=readDelta2(reader+3); count=readDelta1(reader+5)+1; reader+=6; break;
         case 87: value1+=readDelta3(reader); value2=readDelta2(reader+3); count=readDelta2(reader+5)+1; reader+=7; break;
         case 88: value1+=readDelta3(reader); value2=readDelta2(reader+3); count=readDelta3(reader+5)+1; reader+=8; break;
         case 89: value1+=readDelta3(reader); value2=readDelta2(reader+3); count=readDelta4(reader+5)+1; reader+=9; break;
         case 90: value1+=readDelta3(reader); value2=readDelta3(reader+3); count=1; reader+=6; break;
         case 91: value1+=readDelta3(reader); value2=readDelta3(reader+3); count=readDelta1(reader+6)+1; reader+=7; break;
         case 92: value1+=readDelta3(reader); value2=readDelta3(reader+3); count=readDelta2(reader+6)+1; reader+=8; break;
         case 93: value1+=readDelta3(reader); value2=readDelta3(reader+3); count=readDelta3(reader+6)+1; reader+=9; break;
         case 94: value1+=readDelta3(reader); value2=readDelta3(reader+3); count=readDelta4(reader+6)+1; reader+=10; break;
         case 95: value1+=readDelta3(reader); value2=readDelta4(reader+3); count=1; reader+=7; break;
         case 96: value1+=readDelta3(reader); value2=readDelta4(reader+3); count=readDelta1(reader+7)+1; reader+=8; break;
         case 97: value1+=readDelta3(reader); value2=readDelta4(reader+3); count=readDelta2(reader+7)+1; reader+=9; break;
         case 98: value1+=readDelta3(reader); value2=readDelta4(reader+3); count=readDelta3(reader+7)+1; reader+=10; break;
         case 99: value1+=readDelta3(reader); value2=readDelta4(reader+3); count=readDelta4(reader+7)+1; reader+=11; break;
         case 100: value1+=readDelta4(reader); value2=0; count=1; reader+=4; break;
         case 101: value1+=readDelta4(reader); value2=0; count=readDelta1(reader+4)+1; reader+=5; break;
         case 102: value1+=readDelta4(reader); value2=0; count=readDelta2(reader+4)+1; reader+=6; break;
         case 103: value1+=readDelta4(reader); value2=0; count=readDelta3(reader+4)+1; reader+=7; break;
         case 104: value1+=readDelta4(reader); value2=0; count=readDelta4(reader+4)+1; reader+=8; break;
         case 105: value1+=readDelta4(reader); value2=readDelta1(reader+4); count=1; reader+=5; break;
         case 106: value1+=readDelta4(reader); value2=readDelta1(reader+4); count=readDelta1(reader+5)+1; reader+=6; break;
         case 107: value1+=readDelta4(reader); value2=readDelta1(reader+4); count=readDelta2(reader+5)+1; reader+=7; break;
         case 108: value1+=readDelta4(reader); value2=readDelta1(reader+4); count=readDelta3(reader+5)+1; reader+=8; break;
         case 109: value1+=readDelta4(reader); value2=readDelta1(reader+4); count=readDelta4(reader+5)+1; reader+=9; break;
         case 110: value1+=readDelta4(reader); value2=readDelta2(reader+4); count=1; reader+=6; break;
         case 111: value1+=readDelta4(reader); value2=readDelta2(reader+4); count=readDelta1(reader+6)+1; reader+=7; break;
         case 112: value1+=readDelta4(reader); value2=readDelta2(reader+4); count=readDelta2(reader+6)+1; reader+=8; break;
         case 113: value1+=readDelta4(reader); value2=readDelta2(reader+4); count=readDelta3(reader+6)+1; reader+=9; break;
         case 114: value1+=readDelta4(reader); value2=readDelta2(reader+4); count=readDelta4(reader+6)+1; reader+=10; break;
         case 115: value1+=readDelta4(reader); value2=readDelta3(reader+4); count=1; reader+=7; break;
         case 116: value1+=readDelta4(reader); value2=readDelta3(reader+4); count=readDelta1(reader+7)+1; reader+=8; break;
         case 117: value1+=readDelta4(reader); value2=readDelta3(reader+4); count=readDelta2(reader+7)+1; reader+=9; break;
         case 118: value1+=readDelta4(reader); value2=readDelta3(reader+4); count=readDelta3(reader+7)+1; reader+=10; break;
         case 119: value1+=readDelta4(reader); value2=readDelta3(reader+4); count=readDelta4(reader+7)+1; reader+=11; break;
         case 120: value1+=readDelta4(reader); value2=readDelta4(reader+4); count=1; reader+=8; break;
         case 121: value1+=readDelta4(reader); value2=readDelta4(reader+4); count=readDelta1(reader+8)+1; reader+=9; break;
         case 122: value1+=readDelta4(reader); value2=readDelta4(reader+4); count=readDelta2(reader+8)+1; reader+=10; break;
         case 123: value1+=readDelta4(reader); value2=readDelta4(reader+4); count=readDelta3(reader+8)+1; reader+=11; break;
         case 124: value1+=readDelta4(reader); value2=readDelta4(reader+4); count=readDelta4(reader+8)+1; reader+=12; break;
      }
      (*writer).value1=value1;
      (*writer).value2=value2;
      (*writer).count=count;
      ++writer;
   }

   // Update the entries
   pos=triples;
   posLimit=writer;

   // Check if we should make a skip
   if (hint) {
      unsigned next1=triples[0].value1,next2=triples[0].value2;
      while (true) {
         // Compute the next hint
         hint->next(next1,next2);

         // No entry on this page?
         const Triple* oldPos=pos;
         if (!find(next1,next2)) {
            if (!Index(*seg).findLeaf(current,Index::InnerKey(next1,next2)))
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
void AggregatedFactsSegment::Scan::close()
   // Close the scan
{
   seg=0;
   current.reset();
}
//---------------------------------------------------------------------------
