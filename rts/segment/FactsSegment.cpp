#include "rts/segment/FactsSegment.hpp"
#include "rts/database/DatabaseBuilder.hpp"
#include "rts/transaction/LogAction.hpp"
#include "rts/segment/BTree.hpp"
#include <stdio.h>
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
static const unsigned slotCardinality = 5;
//---------------------------------------------------------------------------
/// Compare
static inline bool greater(unsigned a1,unsigned a2,unsigned a3,unsigned b1,unsigned b2,unsigned b3) {
   return (a1>b1)||
          ((a1==b1)&&((a2>b2)||
                      ((a2==b2)&&(a3>b3))));
}
//---------------------------------------------------------------------------
/// An index
class FactsSegment::IndexImplementation
{
   public:
   /// The size of an inner key
   static const unsigned innerKeySize = 3*4;
   /// An inner key
   struct InnerKey {
      /// The values
      unsigned value1,value2,value3;

      /// Constructor
      InnerKey() : value1(0),value2(0),value3(0) {}
      /// Constructor
      InnerKey(unsigned value1,unsigned value2,unsigned value3) : value1(value1),value2(value2),value3(value3) {}

      /// Compare
      bool operator==(const InnerKey& o) const { return (value1==o.value1)&&(value2==o.value2)&&(value3==o.value3); }
      /// Compare
      bool operator<(const InnerKey& o) const { return (value1<o.value1)||((value1==o.value1)&&((value2<o.value2)||((value2==o.value2)&&(value3<o.value3)))); }
   };
   /// Read an inner key
   static void readInnerKey(InnerKey& key,const unsigned char* ptr) {
      key.value1=Segment::readUint32Aligned(ptr);
      key.value2=Segment::readUint32Aligned(ptr+4);
      key.value3=Segment::readUint32Aligned(ptr+8);
   }
   /// Write an inner key
   static void writeInnerKey(unsigned char* ptr,const InnerKey& key) {
      Segment::writeUint32Aligned(ptr,key.value1);
      Segment::writeUint32Aligned(ptr+4,key.value2);
      Segment::writeUint32Aligned(ptr+8,key.value3);
   }
   /// A leaf entry
   struct LeafEntry {
      /// The values
      unsigned value1,value2,value3,created,deleted;

      /// Compare
      bool operator<(const LeafEntry& o) const { return (value1<o.value1)||((value1==o.value1)&&((value2<o.value2)||((value2==o.value2)&&((value3<o.value3)||((value3==o.value3)&&(created<o.created))||((value3==o.value3)&&(deleted<o.deleted)))))); }
      /// Compare
      bool operator<(const InnerKey& o) const { return (value1<o.value1)||((value1==o.value1)&&((value2<o.value2)||((value2==o.value2)&&(value3<o.value3)))); }
   };
   /// A leaf entry source
   class LeafEntrySource {
      private:
      /// The real source
      FactsSegment::Source& source;

      public:
      /// Constructor
      LeafEntrySource(FactsSegment::Source& source) : source(source) {}

      /// Read the next entry
      bool next(LeafEntry& l) { return source.next(l.value1,l.value2,l.value3,l.created,l.deleted); }
      /// Mark last entry as conflict
      void markAsConflict() { source.markAsDuplicate(); }
   };
   /// Derive an inner key
   static InnerKey deriveInnerKey(const LeafEntry& e) { return InnerKey(e.value1,e.value2,e.value3); }
   /// Read the first leaf entry
   static void readFirstLeafEntryKey(InnerKey& key,const unsigned char* ptr) {
      key.value1=Segment::readUint32Aligned(ptr);
      key.value2=Segment::readUint32Aligned(ptr+4);
      key.value3=Segment::readUint32Aligned(ptr+8);
   }

   private:
   /// The segment
   FactsSegment& segment;

   public:
   /// Constructor
   explicit IndexImplementation(FactsSegment& segment) : segment(segment) {}

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

   /// Check for duplicates/conflicts and "merge" if required
   static bool mergeConflictWith(const LeafEntry& newEntry,LeafEntry& oldEntry) { return (newEntry.value1==oldEntry.value1)&&(newEntry.value2==oldEntry.value2)&&(newEntry.value3==oldEntry.value3)&&(!~oldEntry.deleted); }

   /// Pack leaf entries
   static unsigned packLeafEntries(unsigned char* writer,unsigned char* limit,vector<LeafEntry>::const_iterator entriesStart,vector<LeafEntry>::const_iterator entriesLimit);
   /// Unpack leaf entries
   static void unpackLeafEntries(vector<LeafEntry>& entries,const unsigned char* reader,const unsigned char* limit);
};
//---------------------------------------------------------------------------
void FactsSegment::IndexImplementation::setRootPage(unsigned page)
   // Se the root page
{
   segment.indexRoot=page;
   segment.setSegmentData(slotIndexRoot,segment.indexRoot);
}
//---------------------------------------------------------------------------
void FactsSegment::IndexImplementation::updateLeafInfo(unsigned firstLeaf,unsigned leafCount)
   // Store info about the leaf pages
{
   segment.tableStart=firstLeaf;
   segment.setSegmentData(slotTableStart,segment.tableStart);

   segment.pages=leafCount;
   segment.setSegmentData(slotPages,segment.pages);
}
//---------------------------------------------------------------------------
static unsigned bytes(unsigned v)
   // Compute the number of bytes required to encode a value
{
   if (v>=(1<<24))
      return 4; else
   if (v>=(1<<16))
      return 3; else
   if (v>=(1<<8)) return 2; else
      return 1;
}
//---------------------------------------------------------------------------
static unsigned char* writeDelta(unsigned char* writer,unsigned value)
   // Write an integer with varying size
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
   } else {
      writer[0]=value;
      return writer+1;
   }
}
//---------------------------------------------------------------------------
unsigned FactsSegment::IndexImplementation::packLeafEntries(unsigned char* writer,unsigned char* writerLimit,vector<FactsSegment::IndexImplementation::LeafEntry>::const_iterator entriesStart,vector<FactsSegment::IndexImplementation::LeafEntry>::const_iterator entriesLimit)
   // Pack the facts into leaves using prefix compression
{
   unsigned lastValue1,lastValue2,lastValue3,lastDeleted;
   // we need a copy of deleted values, but we don't want to put them into lastValues because the compression should not be applied for them
   unsigned lastDeleted1=0, lastDeleted2=0, lastDeleted3=0;
   //number of deleted first triples
   unsigned deletedFirstEntry=0;
   unsigned value1,value2,value3;
   unsigned created=0u,deleted=~0u,lc=created,ld=deleted;
   // Store the first entry
   if (entriesStart==entriesLimit)
      return 0;
   if ((writer+12)>writerLimit)
      return 0;

   while (entriesStart!=entriesLimit &&~(*entriesStart).deleted){
	  // printf("deleting the first element!\n");
	  // printf("first values: %u %u %u, deleted: %u \n",(*entriesStart).value1,(*entriesStart).value2,(*entriesStart).value3,(*entriesStart).deleted);
	   lastDeleted1=(*entriesStart).value1; lastDeleted2=(*entriesStart).value2; lastDeleted3=(*entriesStart).value3;
	   entriesStart++;
	   deletedFirstEntry++;

	   if (entriesStart==entriesLimit)
	      return deletedFirstEntry;
	   // do we need to skip one more entry?
	   if (lastDeleted1==(*(entriesStart)).value1 && lastDeleted2 == (*(entriesStart)).value2 && lastDeleted3==(*(entriesStart)).value3 ){
		   deletedFirstEntry++;
		   entriesStart++;
		   if (entriesStart==entriesLimit){
		      return deletedFirstEntry;
		   }

	   }
   }


   Segment::writeUint32Aligned(writer,lastValue1=(*entriesStart).value1);
   Segment::writeUint32Aligned(writer+4,lastValue2=(*entriesStart).value2);
   Segment::writeUint32Aligned(writer+8,lastValue3=(*entriesStart).value3);
   lastDeleted=(*entriesStart).deleted;
   writer+=12;
   if (((*entriesStart).created!=created)||((*entriesStart).deleted!=deleted)) {
      if ((writer+9)>writerLimit)
         return 0;
      *(writer)=0x80|28; writer++;
      created=(*entriesStart).created;
      deleted=(*entriesStart).deleted;
      Segment::writeUint32(writer,created); writer+=4;
      Segment::writeUint32(writer,deleted); writer+=4;
   }

   // Store the remaining entries
   for (vector<LeafEntry>::const_iterator iter=entriesStart+1;iter!=entriesLimit;++iter) {
      // Compute the length
      value1=(*iter).value1; value2=(*iter).value2; value3=(*iter).value3;
      // Delete the triple
      if (~(*iter).deleted){
   	     printf("   deleting the tuple!\n");
         lastDeleted1=value1; lastDeleted2=value2; lastDeleted3=value3; lastDeleted=(*iter).deleted;
   	     continue;
      }

      unsigned len;

      if (value1==lastDeleted1 && value2 == lastDeleted2 && value3 == lastDeleted3){
    	  continue;
      }

      if (value1==lastValue1) {
         if (value2==lastValue2) {
            if (value3==lastValue3) {
               // Skipping a duplicate
               if (((*iter).created==created)&&((*iter).deleted==deleted)){
                  continue;
               }

               // Both stamp must change, otherwise we get inconsistent data!
               assert(((*iter).created!=created)&&((*iter).deleted!=deleted));

               // Compute the representation
               if ((*iter).created==lc) {
                  if ((*iter).deleted==ld) {
                     if ((writer+2)<writerLimit) {
                        writer[0]=0x80|22; swap(lc,created);
                        writer[1]=0x80|27; swap(ld,deleted);
                        writer+=2;
                        continue;
                     }
                  } else {
                     if ((writer+6)<writerLimit) {
                        writer[0]=0x80|22; swap(lc,created);
                        writer[1]=0x80|25; ld=deleted; Segment::writeUint32(writer+2,deleted=(*iter).deleted);
                        writer+=2;
                        continue;
                     }
                  }
               } else if ((*iter).deleted==ld) {
                  if ((writer+6)<writerLimit) {
                     writer[0]=0x80|20; lc=created; Segment::writeUint32(writer+1,created=(*iter).created);
                     writer[5]=0x80|27; swap(ld,deleted);
                     writer+=6;
                     continue;
                  }
               } else {
                  if ((writer+10)<writerLimit) {
                     writer[0]=0x80|20; lc=created; Segment::writeUint32(writer+1,created=(*iter).created);
                     writer[5]=0x80|25; ld=deleted; Segment::writeUint32(writer+6,deleted=(*iter).deleted);
                     writer+=10;
                     continue;
                  }
               }
               // fallback to "does not fit"
               len=writerLimit-writer+1;
            } else {
               if ((value3-lastValue3)<128)
                  len=1; else
                  len=1+bytes(value3-lastValue3-128);
            }
         } else {
            len=1+bytes(value2-lastValue2)+bytes(value3);
         }
      } else {
         len=1+bytes(value1-lastValue1)+bytes(value2)+bytes(value3);
      }

      // Compute header for version changes
      if ((*iter).created!=created) {
         if ((*iter).created==lc)
            len+=1; else
            len+=5;
      }
      if ((*iter).deleted!=deleted) {
         if ((*iter).deleted==ld)
            len+=1; else
            len+=5;
      }

      // Entry too big?
      if ((writer+len)>writerLimit) {
         memset(writer,0,writerLimit-writer);
         return iter-entriesStart;
      }

      // No, write versioning info if needed
      if ((*iter).created!=created) {
         if ((*iter).created==lc) {
            writer[0]=0x80|22; swap(lc,created);
            ++writer;
         } else {
            writer[0]=0x80|20; lc=created; Segment::writeUint32(writer+1,created=(*iter).created);
            writer+=5;
         }
      }
      if ((*iter).deleted!=deleted) {
         if ((*iter).deleted==ld) {
            writer[0]=0x80|26; swap(ld,deleted);
            ++writer;
         } else {
            writer[0]=0x80|24; ld=deleted; Segment::writeUint32(writer+1,deleted=(*iter).deleted);
            writer+=5;
         }
      }

      // Pack the triple
      if (value1==lastValue1) {
         if (value2==lastValue2) {
            if (value3==lastValue3) {
               // Skipping a duplicate
               continue;
            } else {
               if ((value3-lastValue3)<128) {
                  *(writer++)=value3-lastValue3;
               } else {
                  *(writer++)=0x80|(bytes(value3-lastValue3-128)-1);
                  writer=writeDelta(writer,value3-lastValue3-128);
               }
            }
         } else {
            *(writer++)=0x80|(bytes(value2-lastValue2)<<2)|(bytes(value3)-1);
            writer=writeDelta(writer,value2-lastValue2);
            writer=writeDelta(writer,value3);
         }
      } else {
         *(writer++)=0xC0|((bytes(value1-lastValue1)-1)<<4)|((bytes(value2)-1)<<2)|(bytes(value3)-1);
         writer=writeDelta(writer,value1-lastValue1);
         writer=writeDelta(writer,value2);
         writer=writeDelta(writer,value3);
      }
      lastValue1=value1; lastValue2=value2; lastValue3=value3; lastDeleted=(*iter).deleted;
   }

   // Done, everything fitted
   memset(writer,0,writerLimit-writer);
   // consider also those deleted first triples
   return entriesLimit-entriesStart + deletedFirstEntry;
}
//---------------------------------------------------------------------------
static inline unsigned readDelta1(const unsigned char* pos) { return pos[0]; }
static unsigned readDelta2(const unsigned char* pos) { return (pos[0]<<8)|pos[1]; }
static unsigned readDelta3(const unsigned char* pos) { return (pos[0]<<16)|(pos[1]<<8)|pos[2]; }
static unsigned readDelta4(const unsigned char* pos) { return (pos[0]<<24)|(pos[1]<<16)|(pos[2]<<8)|pos[3]; }
//---------------------------------------------------------------------------
void FactsSegment::IndexImplementation::unpackLeafEntries(vector<FactsSegment::IndexImplementation::LeafEntry>& entries,const unsigned char* reader,const unsigned char* limit)
   // Read the facts stored on a leaf page
{
   // Decompress the first triple
   LeafEntry e;
   e.value1=readUint32Aligned(reader); reader+=4;
   e.value2=readUint32Aligned(reader); reader+=4;
   e.value3=readUint32Aligned(reader); reader+=4;
   e.created=0; e.deleted=~0u;
   unsigned lc=e.created,ld=e.deleted;
   if ((reader<limit)&&(reader[0]==(0x80|28))) {
      reader++;
      e.created=Segment::readUint32(reader); reader+=4;
      e.deleted=Segment::readUint32(reader); reader+=4;
   }
   entries.push_back(e);

   // Decompress the remainder of the page
   while (reader<limit) {
      // Decode the header byte
      unsigned info=*(reader++);
      // Small gap only?
      if (info<0x80) {
         if (!info)
            break;
         e.value3+=info;
         entries.push_back(e);
         continue;
      }
      // Decode it
      switch (info&127) {
         case 0: e.value3+=readDelta1(reader)+128; reader+=1; break;
         case 1: e.value3+=readDelta2(reader)+128; reader+=2; break;
         case 2: e.value3+=readDelta3(reader)+128; reader+=3; break;
         case 3: e.value3+=readDelta4(reader)+128; reader+=4; break;
         case 4: e.value2+=readDelta1(reader); e.value3=readDelta1(reader+1); reader+=2; break;
         case 5: e.value2+=readDelta1(reader); e.value3=readDelta2(reader+1); reader+=3; break;
         case 6: e.value2+=readDelta1(reader); e.value3=readDelta3(reader+1); reader+=4; break;
         case 7: e.value2+=readDelta1(reader); e.value3=readDelta4(reader+1); reader+=5; break;
         case 8: e.value2+=readDelta2(reader); e.value3=readDelta1(reader+2); reader+=3; break;
         case 9: e.value2+=readDelta2(reader); e.value3=readDelta2(reader+2); reader+=4; break;
         case 10: e.value2+=readDelta2(reader); e.value3=readDelta3(reader+2); reader+=5; break;
         case 11: e.value2+=readDelta2(reader); e.value3=readDelta4(reader+2); reader+=6; break;
         case 12: e.value2+=readDelta3(reader); e.value3=readDelta1(reader+3); reader+=4; break;
         case 13: e.value2+=readDelta3(reader); e.value3=readDelta2(reader+3); reader+=5; break;
         case 14: e.value2+=readDelta3(reader); e.value3=readDelta3(reader+3); reader+=6; break;
         case 15: e.value2+=readDelta3(reader); e.value3=readDelta4(reader+3); reader+=7; break;
         case 16: e.value2+=readDelta4(reader); e.value3=readDelta1(reader+4); reader+=5; break;
         case 17: e.value2+=readDelta4(reader); e.value3=readDelta2(reader+4); reader+=6; break;
         case 18: e.value2+=readDelta4(reader); e.value3=readDelta3(reader+4); reader+=7; break;
         case 19: e.value2+=readDelta4(reader); e.value3=readDelta4(reader+4); reader+=8; break;
         case 20: lc=e.created; e.created=readUint32(reader); reader+=4; continue;
         case 21: lc=e.created; e.created=readUint32(reader); reader+=4; break;
         case 22: swap(e.created,lc); continue;
         case 23: swap(e.created,lc); break;
         case 24: ld=e.deleted; e.deleted=readUint32(reader); reader+=4; continue;
         case 25: ld=e.deleted; e.deleted=readUint32(reader); reader+=4; break;
         case 26: swap(e.deleted,ld); continue;
         case 27: swap(e.deleted,ld); break;
         case 28: // Time marker in header, must not occur in compressed stream!
         case 29: case 30: case 31: break;
         case 32: case 33: case 34: case 35: case 36: case 37: case 38: case 39: case 40: case 41: case 42: case 43:
         case 44: case 45: case 46: case 47: case 48: case 49: case 50: case 51: case 52: case 53: case 54: case 55:
         case 56: case 57: case 58: case 59: case 60: case 61: case 62: case 63:
         case 64: e.value1+=readDelta1(reader); e.value2=readDelta1(reader+1); e.value3=readDelta1(reader+2); reader+=3; break;
         case 65: e.value1+=readDelta1(reader); e.value2=readDelta1(reader+1); e.value3=readDelta2(reader+2); reader+=4; break;
         case 66: e.value1+=readDelta1(reader); e.value2=readDelta1(reader+1); e.value3=readDelta3(reader+2); reader+=5; break;
         case 67: e.value1+=readDelta1(reader); e.value2=readDelta1(reader+1); e.value3=readDelta4(reader+2); reader+=6; break;
         case 68: e.value1+=readDelta1(reader); e.value2=readDelta2(reader+1); e.value3=readDelta1(reader+3); reader+=4; break;
         case 69: e.value1+=readDelta1(reader); e.value2=readDelta2(reader+1); e.value3=readDelta2(reader+3); reader+=5; break;
         case 70: e.value1+=readDelta1(reader); e.value2=readDelta2(reader+1); e.value3=readDelta3(reader+3); reader+=6; break;
         case 71: e.value1+=readDelta1(reader); e.value2=readDelta2(reader+1); e.value3=readDelta4(reader+3); reader+=7; break;
         case 72: e.value1+=readDelta1(reader); e.value2=readDelta3(reader+1); e.value3=readDelta1(reader+4); reader+=5; break;
         case 73: e.value1+=readDelta1(reader); e.value2=readDelta3(reader+1); e.value3=readDelta2(reader+4); reader+=6; break;
         case 74: e.value1+=readDelta1(reader); e.value2=readDelta3(reader+1); e.value3=readDelta3(reader+4); reader+=7; break;
         case 75: e.value1+=readDelta1(reader); e.value2=readDelta3(reader+1); e.value3=readDelta4(reader+4); reader+=8; break;
         case 76: e.value1+=readDelta1(reader); e.value2=readDelta4(reader+1); e.value3=readDelta1(reader+5); reader+=6; break;
         case 77: e.value1+=readDelta1(reader); e.value2=readDelta4(reader+1); e.value3=readDelta2(reader+5); reader+=7; break;
         case 78: e.value1+=readDelta1(reader); e.value2=readDelta4(reader+1); e.value3=readDelta3(reader+5); reader+=8; break;
         case 79: e.value1+=readDelta1(reader); e.value2=readDelta4(reader+1); e.value3=readDelta4(reader+5); reader+=9; break;
         case 80: e.value1+=readDelta2(reader); e.value2=readDelta1(reader+2); e.value3=readDelta1(reader+3); reader+=4; break;
         case 81: e.value1+=readDelta2(reader); e.value2=readDelta1(reader+2); e.value3=readDelta2(reader+3); reader+=5; break;
         case 82: e.value1+=readDelta2(reader); e.value2=readDelta1(reader+2); e.value3=readDelta3(reader+3); reader+=6; break;
         case 83: e.value1+=readDelta2(reader); e.value2=readDelta1(reader+2); e.value3=readDelta4(reader+3); reader+=7; break;
         case 84: e.value1+=readDelta2(reader); e.value2=readDelta2(reader+2); e.value3=readDelta1(reader+4); reader+=5; break;
         case 85: e.value1+=readDelta2(reader); e.value2=readDelta2(reader+2); e.value3=readDelta2(reader+4); reader+=6; break;
         case 86: e.value1+=readDelta2(reader); e.value2=readDelta2(reader+2); e.value3=readDelta3(reader+4); reader+=7; break;
         case 87: e.value1+=readDelta2(reader); e.value2=readDelta2(reader+2); e.value3=readDelta4(reader+4); reader+=8; break;
         case 88: e.value1+=readDelta2(reader); e.value2=readDelta3(reader+2); e.value3=readDelta1(reader+5); reader+=6; break;
         case 89: e.value1+=readDelta2(reader); e.value2=readDelta3(reader+2); e.value3=readDelta2(reader+5); reader+=7; break;
         case 90: e.value1+=readDelta2(reader); e.value2=readDelta3(reader+2); e.value3=readDelta3(reader+5); reader+=8; break;
         case 91: e.value1+=readDelta2(reader); e.value2=readDelta3(reader+2); e.value3=readDelta4(reader+5); reader+=9; break;
         case 92: e.value1+=readDelta2(reader); e.value2=readDelta4(reader+2); e.value3=readDelta1(reader+6); reader+=7; break;
         case 93: e.value1+=readDelta2(reader); e.value2=readDelta4(reader+2); e.value3=readDelta2(reader+6); reader+=8; break;
         case 94: e.value1+=readDelta2(reader); e.value2=readDelta4(reader+2); e.value3=readDelta3(reader+6); reader+=9; break;
         case 95: e.value1+=readDelta2(reader); e.value2=readDelta4(reader+2); e.value3=readDelta4(reader+6); reader+=10; break;
         case 96: e.value1+=readDelta3(reader); e.value2=readDelta1(reader+3); e.value3=readDelta1(reader+4); reader+=5; break;
         case 97: e.value1+=readDelta3(reader); e.value2=readDelta1(reader+3); e.value3=readDelta2(reader+4); reader+=6; break;
         case 98: e.value1+=readDelta3(reader); e.value2=readDelta1(reader+3); e.value3=readDelta3(reader+4); reader+=7; break;
         case 99: e.value1+=readDelta3(reader); e.value2=readDelta1(reader+3); e.value3=readDelta4(reader+4); reader+=8; break;
         case 100: e.value1+=readDelta3(reader); e.value2=readDelta2(reader+3); e.value3=readDelta1(reader+5); reader+=6; break;
         case 101: e.value1+=readDelta3(reader); e.value2=readDelta2(reader+3); e.value3=readDelta2(reader+5); reader+=7; break;
         case 102: e.value1+=readDelta3(reader); e.value2=readDelta2(reader+3); e.value3=readDelta3(reader+5); reader+=8; break;
         case 103: e.value1+=readDelta3(reader); e.value2=readDelta2(reader+3); e.value3=readDelta4(reader+5); reader+=9; break;
         case 104: e.value1+=readDelta3(reader); e.value2=readDelta3(reader+3); e.value3=readDelta1(reader+6); reader+=7; break;
         case 105: e.value1+=readDelta3(reader); e.value2=readDelta3(reader+3); e.value3=readDelta2(reader+6); reader+=8; break;
         case 106: e.value1+=readDelta3(reader); e.value2=readDelta3(reader+3); e.value3=readDelta3(reader+6); reader+=9; break;
         case 107: e.value1+=readDelta3(reader); e.value2=readDelta3(reader+3); e.value3=readDelta4(reader+6); reader+=10; break;
         case 108: e.value1+=readDelta3(reader); e.value2=readDelta4(reader+3); e.value3=readDelta1(reader+7); reader+=8; break;
         case 109: e.value1+=readDelta3(reader); e.value2=readDelta4(reader+3); e.value3=readDelta2(reader+7); reader+=9; break;
         case 110: e.value1+=readDelta3(reader); e.value2=readDelta4(reader+3); e.value3=readDelta3(reader+7); reader+=10; break;
         case 111: e.value1+=readDelta3(reader); e.value2=readDelta4(reader+3); e.value3=readDelta4(reader+7); reader+=11; break;
         case 112: e.value1+=readDelta4(reader); e.value2=readDelta1(reader+4); e.value3=readDelta1(reader+5); reader+=6; break;
         case 113: e.value1+=readDelta4(reader); e.value2=readDelta1(reader+4); e.value3=readDelta2(reader+5); reader+=7; break;
         case 114: e.value1+=readDelta4(reader); e.value2=readDelta1(reader+4); e.value3=readDelta3(reader+5); reader+=8; break;
         case 115: e.value1+=readDelta4(reader); e.value2=readDelta1(reader+4); e.value3=readDelta4(reader+5); reader+=9; break;
         case 116: e.value1+=readDelta4(reader); e.value2=readDelta2(reader+4); e.value3=readDelta1(reader+6); reader+=7; break;
         case 117: e.value1+=readDelta4(reader); e.value2=readDelta2(reader+4); e.value3=readDelta2(reader+6); reader+=8; break;
         case 118: e.value1+=readDelta4(reader); e.value2=readDelta2(reader+4); e.value3=readDelta3(reader+6); reader+=9; break;
         case 119: e.value1+=readDelta4(reader); e.value2=readDelta2(reader+4); e.value3=readDelta4(reader+6); reader+=10; break;
         case 120: e.value1+=readDelta4(reader); e.value2=readDelta3(reader+4); e.value3=readDelta1(reader+7); reader+=8; break;
         case 121: e.value1+=readDelta4(reader); e.value2=readDelta3(reader+4); e.value3=readDelta2(reader+7); reader+=9; break;
         case 122: e.value1+=readDelta4(reader); e.value2=readDelta3(reader+4); e.value3=readDelta3(reader+7); reader+=10; break;
         case 123: e.value1+=readDelta4(reader); e.value2=readDelta3(reader+4); e.value3=readDelta4(reader+7); reader+=11; break;
         case 124: e.value1+=readDelta4(reader); e.value2=readDelta4(reader+4); e.value3=readDelta1(reader+8); reader+=9; break;
         case 125: e.value1+=readDelta4(reader); e.value2=readDelta4(reader+4); e.value3=readDelta2(reader+8); reader+=10; break;
         case 126: e.value1+=readDelta4(reader); e.value2=readDelta4(reader+4); e.value3=readDelta3(reader+8); reader+=11; break;
         case 127: e.value1+=readDelta4(reader); e.value2=readDelta4(reader+4); e.value3=readDelta4(reader+8); reader+=12; break;
      }
      entries.push_back(e);
   }
}
//---------------------------------------------------------------------------
/// An index
class FactsSegment::Index : public BTree<IndexImplementation>
{
   public:
   /// Constructor
   explicit Index(FactsSegment& segment) : BTree<IndexImplementation>(segment) {}

   /// Size of the leaf header (used for scans)
   using BTree<IndexImplementation>::leafHeaderSize;
};
//---------------------------------------------------------------------------
FactsSegment::Source::~Source()
   // Destructor
{
}
//---------------------------------------------------------------------------
FactsSegment::FactsSegment(DatabasePartition& partition)
   : Segment(partition),tableStart(0),indexRoot(0),pages(0),groups1(0),groups2(0),cardinality(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::Type FactsSegment::getType() const
   // Get the type
{
   return Segment::Type_Facts;
}
//---------------------------------------------------------------------------
void FactsSegment::refreshInfo()
   // Refresh segment info stored in the partition
{
   Segment::refreshInfo();

   tableStart=getSegmentData(slotTableStart);
   indexRoot=getSegmentData(slotIndexRoot);
   pages=getSegmentData(slotPages);
   groups1=getSegmentData(slotGroups1);
   groups2=getSegmentData(slotGroups2);
   cardinality=getSegmentData(slotCardinality);
}
//---------------------------------------------------------------------------
void FactsSegment::loadFullFacts(Source& reader)
   // Load the triples into the database
{
   Index::LeafEntrySource source(reader);
   Index(*this).performBulkload(source);
}
//---------------------------------------------------------------------------
void FactsSegment::loadCounts(unsigned groups1,unsigned groups2,unsigned cardinality)
   // Load count statistics
{
   this->groups1=groups1; setSegmentData(slotGroups1,groups1);
   this->groups2=groups2; setSegmentData(slotGroups2,groups2);
   this->cardinality=cardinality; setSegmentData(slotCardinality,cardinality);
}
//---------------------------------------------------------------------------
void FactsSegment::update(FactsSegment::Source& reader)
   // Load new facts into the segment
{
   Index::LeafEntrySource source(reader);
   Index(*this).performUpdate(source);
}
//---------------------------------------------------------------------------
FactsSegment::Scan::Hint::Hint()
   // Constructor
{
}
//---------------------------------------------------------------------------
FactsSegment::Scan::Hint::~Hint()
   // Destructor
{
}
//---------------------------------------------------------------------------
FactsSegment::Scan::Scan(Hint* hint,unsigned time)
   : seg(0),hint(hint),time(time)
   // Constructor
{
}
//---------------------------------------------------------------------------
FactsSegment::Scan::~Scan()
   // Destructor
{
   close();
}
//---------------------------------------------------------------------------
bool FactsSegment::Scan::first(FactsSegment& segment)
   // Start a new scan over the whole segment
{
   current=segment.readShared(segment.tableStart);
   seg=&segment;
   pos=posLimit=0;

   return next();
}
//---------------------------------------------------------------------------
bool FactsSegment::Scan::first(FactsSegment& segment,unsigned start1,unsigned start2,unsigned start3)
   // Start a new scan starting from the first entry >= the start condition
{
   // Lookup the right page
   if (!Index(segment).findLeaf(current,Index::InnerKey(start1,start2,start3)))
      return false;

   // Place the iterator
   seg=&segment;
   pos=posLimit=0;

   // Skip over leading entries that are too small
   while (true) {
      if (!next())
         return false;

      if ((getValue1()>start1)||
          ((getValue1()==start1)&&((getValue2()>start2)||
                              ((getValue2()==start2)&&(getValue3()>=start3)))))
         return true;
   }
}
//---------------------------------------------------------------------------
bool FactsSegment::Scan::find(unsigned value1,unsigned value2,unsigned value3)
    // Perform a binary search
{
   const Triple* l=pos,*r=posLimit;
   while (l<r) {
      const Triple* m=l+((r-l)/2);
      if (greater(m->value1,m->value2,m->value3,value1,value2,value3)) {
         r=m;
      } else if (greater(value1,value2,value3,m->value1,m->value2,m->value3)) {
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
static bool skipInTime(const unsigned char*& oreader,const unsigned char* limit,unsigned& v1,unsigned& v2,unsigned& v3,unsigned& oc,unsigned& od,unsigned& olc,unsigned& old,unsigned time)
   // Skip
{
   const unsigned char* reader=oreader;
   unsigned value1=v1,value2=v2,value3=v3;
   unsigned created=oc,deleted=od;
   unsigned lc=olc,ld=old;
   bool produce=false;

   // Decompress the remainder of the page
   while (reader<limit) {
      // Decode the header byte
      unsigned info=*(reader++);
      // Small gap only?
      if (info<0x80) {
         if (!info)
            break;
         value3+=info;
         continue;
      }
      // Decode it
      switch (info&127) {
         case 0: value3+=readDelta1(reader)+128; reader+=1; break;
         case 1: value3+=readDelta2(reader)+128; reader+=2; break;
         case 2: value3+=readDelta3(reader)+128; reader+=3; break;
         case 3: value3+=readDelta4(reader)+128; reader+=4; break;
         case 4: value2+=readDelta1(reader); value3=readDelta1(reader+1); reader+=2; break;
         case 5: value2+=readDelta1(reader); value3=readDelta2(reader+1); reader+=3; break;
         case 6: value2+=readDelta1(reader); value3=readDelta3(reader+1); reader+=4; break;
         case 7: value2+=readDelta1(reader); value3=readDelta4(reader+1); reader+=5; break;
         case 8: value2+=readDelta2(reader); value3=readDelta1(reader+2); reader+=3; break;
         case 9: value2+=readDelta2(reader); value3=readDelta2(reader+2); reader+=4; break;
         case 10: value2+=readDelta2(reader); value3=readDelta3(reader+2); reader+=5; break;
         case 11: value2+=readDelta2(reader); value3=readDelta4(reader+2); reader+=6; break;
         case 12: value2+=readDelta3(reader); value3=readDelta1(reader+3); reader+=4; break;
         case 13: value2+=readDelta3(reader); value3=readDelta2(reader+3); reader+=5; break;
         case 14: value2+=readDelta3(reader); value3=readDelta3(reader+3); reader+=6; break;
         case 15: value2+=readDelta3(reader); value3=readDelta4(reader+3); reader+=7; break;
         case 16: value2+=readDelta4(reader); value3=readDelta1(reader+4); reader+=5; break;
         case 17: value2+=readDelta4(reader); value3=readDelta2(reader+4); reader+=6; break;
         case 18: value2+=readDelta4(reader); value3=readDelta3(reader+4); reader+=7; break;
         case 19: value2+=readDelta4(reader); value3=readDelta4(reader+4); reader+=8; break;
         case 20: lc=created; created=Segment::readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) continue; else goto done;
         case 21: lc=created; created=Segment::readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) continue; else { produce=true; goto done; }
         case 22: swap(created,lc); if ((created>time)||(time>=deleted)) continue; else goto done;
         case 23: swap(created,lc); if ((created>time)||(time>=deleted)) continue; else { produce=true; goto done; }
         case 24: ld=deleted; deleted=Segment::readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) continue; else goto done;
         case 25: ld=deleted; deleted=Segment::readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) continue; else { produce=true; goto done; }
         case 26: swap(deleted,ld); if ((created>time)||(time>=deleted)) continue; else goto done;
         case 27: swap(deleted,ld); if ((created>time)||(time>=deleted)) continue; else { produce=true; goto done; }
         case 28: // first version marker, must not occur in triple stream!
         case 29: case 30: case 31: break;
         case 32: case 33: case 34: case 35: case 36: case 37: case 38: case 39: case 40: case 41: case 42: case 43:
         case 44: case 45: case 46: case 47: case 48: case 49: case 50: case 51: case 52: case 53: case 54: case 55:
         case 56: case 57: case 58: case 59: case 60: case 61: case 62: case 63:
         case 64: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta1(reader+2); reader+=3; break;
         case 65: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta2(reader+2); reader+=4; break;
         case 66: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta3(reader+2); reader+=5; break;
         case 67: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta4(reader+2); reader+=6; break;
         case 68: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta1(reader+3); reader+=4; break;
         case 69: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta2(reader+3); reader+=5; break;
         case 70: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta3(reader+3); reader+=6; break;
         case 71: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta4(reader+3); reader+=7; break;
         case 72: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta1(reader+4); reader+=5; break;
         case 73: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta2(reader+4); reader+=6; break;
         case 74: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta3(reader+4); reader+=7; break;
         case 75: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta4(reader+4); reader+=8; break;
         case 76: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta1(reader+5); reader+=6; break;
         case 77: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta2(reader+5); reader+=7; break;
         case 78: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta3(reader+5); reader+=8; break;
         case 79: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta4(reader+5); reader+=9; break;
         case 80: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta1(reader+3); reader+=4; break;
         case 81: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta2(reader+3); reader+=5; break;
         case 82: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta3(reader+3); reader+=6; break;
         case 83: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta4(reader+3); reader+=7; break;
         case 84: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta1(reader+4); reader+=5; break;
         case 85: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta2(reader+4); reader+=6; break;
         case 86: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta3(reader+4); reader+=7; break;
         case 87: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta4(reader+4); reader+=8; break;
         case 88: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta1(reader+5); reader+=6; break;
         case 89: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta2(reader+5); reader+=7; break;
         case 90: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta3(reader+5); reader+=8; break;
         case 91: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta4(reader+5); reader+=9; break;
         case 92: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta1(reader+6); reader+=7; break;
         case 93: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta2(reader+6); reader+=8; break;
         case 94: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta3(reader+6); reader+=9; break;
         case 95: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta4(reader+6); reader+=10; break;
         case 96: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta1(reader+4); reader+=5; break;
         case 97: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta2(reader+4); reader+=6; break;
         case 98: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta3(reader+4); reader+=7; break;
         case 99: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta4(reader+4); reader+=8; break;
         case 100: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta1(reader+5); reader+=6; break;
         case 101: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta2(reader+5); reader+=7; break;
         case 102: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta3(reader+5); reader+=8; break;
         case 103: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta4(reader+5); reader+=9; break;
         case 104: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta1(reader+6); reader+=7; break;
         case 105: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta2(reader+6); reader+=8; break;
         case 106: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta3(reader+6); reader+=9; break;
         case 107: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta4(reader+6); reader+=10; break;
         case 108: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta1(reader+7); reader+=8; break;
         case 109: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta2(reader+7); reader+=9; break;
         case 110: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta3(reader+7); reader+=10; break;
         case 111: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta4(reader+7); reader+=11; break;
         case 112: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta1(reader+5); reader+=6; break;
         case 113: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta2(reader+5); reader+=7; break;
         case 114: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta3(reader+5); reader+=8; break;
         case 115: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta4(reader+5); reader+=9; break;
         case 116: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta1(reader+6); reader+=7; break;
         case 117: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta2(reader+6); reader+=8; break;
         case 118: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta3(reader+6); reader+=9; break;
         case 119: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta4(reader+6); reader+=10; break;
         case 120: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta1(reader+7); reader+=8; break;
         case 121: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta2(reader+7); reader+=9; break;
         case 122: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta3(reader+7); reader+=10; break;
         case 123: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta4(reader+7); reader+=11; break;
         case 124: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta1(reader+8); reader+=9; break;
         case 125: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta2(reader+8); reader+=10; break;
         case 126: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta3(reader+8); reader+=11; break;
         case 127: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta4(reader+8); reader+=12; break;
      }
   }

   // Done
   done:
   oreader=reader;
   v1=value1; v2=value2; v3=value3;
   oc=created; od=deleted; olc=lc; old=ld;
   return produce;
}
//---------------------------------------------------------------------------
static FactsSegment::Triple* decompress(const unsigned char* reader,const unsigned char* limit,FactsSegment::Triple* writer,unsigned time)
   // Decompress triples
{
   // Decompress the first triple
   unsigned value1=Segment::readUint32Aligned(reader); reader+=4;
   unsigned value2=Segment::readUint32Aligned(reader); reader+=4;
   unsigned value3=Segment::readUint32Aligned(reader); reader+=4;
   unsigned created=0,deleted=~0u;
   unsigned lc=created,ld=deleted;
   if ((reader<limit)&&(reader[0]==(0x80|28))) {
      reader++;
      created=Segment::readUint32(reader); reader+=4;
      deleted=Segment::readUint32(reader); reader+=4;
   }
   if (((created<=time)&&(time<deleted))||(skipInTime(reader,limit,value1,value2,value3,created,deleted,lc,ld,time))) {
      (*writer).value1=value1;
      (*writer).value2=value2;
      (*writer).value3=value3;
      ++writer;
   }

   // Decompress the remainder of the page
   while (reader<limit) {
      // Decode the header byte
      unsigned info=*(reader++);
      // Small gap only?
      if (info<0x80) {
         if (!info)
            break;
         value3+=info;
         (*writer).value1=value1;
         (*writer).value2=value2;
         (*writer).value3=value3;
         ++writer;
         continue;
      }
      // Decode it
      switch (info&127) {
         case 0: value3+=readDelta1(reader)+128; reader+=1; break;
         case 1: value3+=readDelta2(reader)+128; reader+=2; break;
         case 2: value3+=readDelta3(reader)+128; reader+=3; break;
         case 3: value3+=readDelta4(reader)+128; reader+=4; break;
         case 4: value2+=readDelta1(reader); value3=readDelta1(reader+1); reader+=2; break;
         case 5: value2+=readDelta1(reader); value3=readDelta2(reader+1); reader+=3; break;
         case 6: value2+=readDelta1(reader); value3=readDelta3(reader+1); reader+=4; break;
         case 7: value2+=readDelta1(reader); value3=readDelta4(reader+1); reader+=5; break;
         case 8: value2+=readDelta2(reader); value3=readDelta1(reader+2); reader+=3; break;
         case 9: value2+=readDelta2(reader); value3=readDelta2(reader+2); reader+=4; break;
         case 10: value2+=readDelta2(reader); value3=readDelta3(reader+2); reader+=5; break;
         case 11: value2+=readDelta2(reader); value3=readDelta4(reader+2); reader+=6; break;
         case 12: value2+=readDelta3(reader); value3=readDelta1(reader+3); reader+=4; break;
         case 13: value2+=readDelta3(reader); value3=readDelta2(reader+3); reader+=5; break;
         case 14: value2+=readDelta3(reader); value3=readDelta3(reader+3); reader+=6; break;
         case 15: value2+=readDelta3(reader); value3=readDelta4(reader+3); reader+=7; break;
         case 16: value2+=readDelta4(reader); value3=readDelta1(reader+4); reader+=5; break;
         case 17: value2+=readDelta4(reader); value3=readDelta2(reader+4); reader+=6; break;
         case 18: value2+=readDelta4(reader); value3=readDelta3(reader+4); reader+=7; break;
         case 19: value2+=readDelta4(reader); value3=readDelta4(reader+4); reader+=8; break;
         case 20: lc=created; created=Segment::readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) goto doSkipInTime; else continue;
         case 21: lc=created; created=Segment::readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) goto doSkipInTime; else break;
         case 22: swap(created,lc); if ((created>time)||(time>=deleted)) goto doSkipInTime; else continue;
         case 23: swap(created,lc); if ((created>time)||(time>=deleted)) goto doSkipInTime; else break;
         case 24: ld=deleted; deleted=Segment::readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) goto doSkipInTime; else continue;
         case 25: ld=deleted; deleted=Segment::readUint32(reader); reader+=4; if ((created>time)||(time>=deleted)) goto doSkipInTime; else break;
         case 26: swap(deleted,ld); if ((created>time)||(time>=deleted)) goto doSkipInTime; else continue;
         case 27: swap(deleted,ld); if ((created>time)||(time>=deleted)) goto doSkipInTime; else break;
            doSkipInTime: // Version outside the bounds, skip
            if (skipInTime(reader,limit,value1,value2,value3,created,deleted,lc,ld,time))
               break;
            continue;
         case 28: // first version marker, but not occur in compressed stream!
         case 29: case 30: case 31: break;
         case 32: case 33: case 34: case 35: case 36: case 37: case 38: case 39: case 40: case 41: case 42: case 43:
         case 44: case 45: case 46: case 47: case 48: case 49: case 50: case 51: case 52: case 53: case 54: case 55:
         case 56: case 57: case 58: case 59: case 60: case 61: case 62: case 63:
         case 64: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta1(reader+2); reader+=3; break;
         case 65: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta2(reader+2); reader+=4; break;
         case 66: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta3(reader+2); reader+=5; break;
         case 67: value1+=readDelta1(reader); value2=readDelta1(reader+1); value3=readDelta4(reader+2); reader+=6; break;
         case 68: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta1(reader+3); reader+=4; break;
         case 69: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta2(reader+3); reader+=5; break;
         case 70: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta3(reader+3); reader+=6; break;
         case 71: value1+=readDelta1(reader); value2=readDelta2(reader+1); value3=readDelta4(reader+3); reader+=7; break;
         case 72: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta1(reader+4); reader+=5; break;
         case 73: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta2(reader+4); reader+=6; break;
         case 74: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta3(reader+4); reader+=7; break;
         case 75: value1+=readDelta1(reader); value2=readDelta3(reader+1); value3=readDelta4(reader+4); reader+=8; break;
         case 76: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta1(reader+5); reader+=6; break;
         case 77: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta2(reader+5); reader+=7; break;
         case 78: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta3(reader+5); reader+=8; break;
         case 79: value1+=readDelta1(reader); value2=readDelta4(reader+1); value3=readDelta4(reader+5); reader+=9; break;
         case 80: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta1(reader+3); reader+=4; break;
         case 81: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta2(reader+3); reader+=5; break;
         case 82: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta3(reader+3); reader+=6; break;
         case 83: value1+=readDelta2(reader); value2=readDelta1(reader+2); value3=readDelta4(reader+3); reader+=7; break;
         case 84: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta1(reader+4); reader+=5; break;
         case 85: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta2(reader+4); reader+=6; break;
         case 86: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta3(reader+4); reader+=7; break;
         case 87: value1+=readDelta2(reader); value2=readDelta2(reader+2); value3=readDelta4(reader+4); reader+=8; break;
         case 88: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta1(reader+5); reader+=6; break;
         case 89: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta2(reader+5); reader+=7; break;
         case 90: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta3(reader+5); reader+=8; break;
         case 91: value1+=readDelta2(reader); value2=readDelta3(reader+2); value3=readDelta4(reader+5); reader+=9; break;
         case 92: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta1(reader+6); reader+=7; break;
         case 93: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta2(reader+6); reader+=8; break;
         case 94: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta3(reader+6); reader+=9; break;
         case 95: value1+=readDelta2(reader); value2=readDelta4(reader+2); value3=readDelta4(reader+6); reader+=10; break;
         case 96: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta1(reader+4); reader+=5; break;
         case 97: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta2(reader+4); reader+=6; break;
         case 98: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta3(reader+4); reader+=7; break;
         case 99: value1+=readDelta3(reader); value2=readDelta1(reader+3); value3=readDelta4(reader+4); reader+=8; break;
         case 100: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta1(reader+5); reader+=6; break;
         case 101: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta2(reader+5); reader+=7; break;
         case 102: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta3(reader+5); reader+=8; break;
         case 103: value1+=readDelta3(reader); value2=readDelta2(reader+3); value3=readDelta4(reader+5); reader+=9; break;
         case 104: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta1(reader+6); reader+=7; break;
         case 105: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta2(reader+6); reader+=8; break;
         case 106: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta3(reader+6); reader+=9; break;
         case 107: value1+=readDelta3(reader); value2=readDelta3(reader+3); value3=readDelta4(reader+6); reader+=10; break;
         case 108: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta1(reader+7); reader+=8; break;
         case 109: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta2(reader+7); reader+=9; break;
         case 110: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta3(reader+7); reader+=10; break;
         case 111: value1+=readDelta3(reader); value2=readDelta4(reader+3); value3=readDelta4(reader+7); reader+=11; break;
         case 112: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta1(reader+5); reader+=6; break;
         case 113: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta2(reader+5); reader+=7; break;
         case 114: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta3(reader+5); reader+=8; break;
         case 115: value1+=readDelta4(reader); value2=readDelta1(reader+4); value3=readDelta4(reader+5); reader+=9; break;
         case 116: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta1(reader+6); reader+=7; break;
         case 117: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta2(reader+6); reader+=8; break;
         case 118: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta3(reader+6); reader+=9; break;
         case 119: value1+=readDelta4(reader); value2=readDelta2(reader+4); value3=readDelta4(reader+6); reader+=10; break;
         case 120: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta1(reader+7); reader+=8; break;
         case 121: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta2(reader+7); reader+=9; break;
         case 122: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta3(reader+7); reader+=10; break;
         case 123: value1+=readDelta4(reader); value2=readDelta3(reader+4); value3=readDelta4(reader+7); reader+=11; break;
         case 124: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta1(reader+8); reader+=9; break;
         case 125: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta2(reader+8); reader+=10; break;
         case 126: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta3(reader+8); reader+=11; break;
         case 127: value1+=readDelta4(reader); value2=readDelta4(reader+4); value3=readDelta4(reader+8); reader+=12; break;
      }
      (*writer).value1=value1;
      (*writer).value2=value2;
      (*writer).value3=value3;
      ++writer;
   }
   return writer;
}
//---------------------------------------------------------------------------
bool FactsSegment::Scan::readNextPage()
   // Read the next entry
{
   readNext:

   // Alread read the first page? Then read the next one
   if (pos-1) {
      const unsigned char* page=static_cast<const unsigned char*>(current.getPage());
      unsigned nextPage=readUint32Aligned(page+8);
      if (!nextPage)
         return false;
      current=seg->readShared(nextPage);
   }

   // Decompress the triples
   const unsigned char* page=static_cast<const unsigned char*>(current.getPage());
   pos=triples;
   posLimit=decompress(page+Index::leafHeaderSize,page+BufferReference::pageSize,triples,time);

   // Empty page? Can happen due to versioning
   if (pos==posLimit)
      goto readNext;

   // Check if we should make a skip
   if (hint) {
      unsigned next1=triples[0].value1,next2=triples[0].value2,next3=triples[0].value3;
      while (true) {
         // Compute the next hint
         hint->next(next1,next2,next3);

         // No entry on this page?
         const Triple* oldPos=pos;
         if (!find(next1,next2,next3)) {
            if (!Index(*seg).findLeaf(current,Index::InnerKey(next1,next2,next3)))
               return false;
            pos=posLimit=0;
            ++pos;
            goto readNext;
         }

         // Stop if we are at a suitable position
         if (oldPos==pos)
            break;
      }
   }

   return true;
}
//---------------------------------------------------------------------------
void FactsSegment::Scan::close()
   // Close the scan
{
   seg=0;
   current.reset();
}
//---------------------------------------------------------------------------
