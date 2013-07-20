#include "rts/segment/ExactStatisticsSegment.hpp"
#include "infra/osdep/MemoryMappedFile.hpp"
#include "infra/util/fastlz.hpp"
#include "rts/database/Database.hpp"
#include "rts/database/DatabaseBuilder.hpp"
#include "rts/database/DatabasePartition.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include <algorithm>
#include <vector>
#include <cstring>
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
// Info slots
static const unsigned slotDirectoryPage = 0;
//---------------------------------------------------------------------------
ExactStatisticsSegment::ExactStatisticsSegment(DatabasePartition& partition)
   : Segment(partition),c2ps(0),c2po(0),c2so(0),c1s(0),c1p(0),c1o(0),c0ss(0),c0sp(0),c0so(0),c0ps(0),c0pp(0),c0po(0),c0os(0),c0op(0),c0oo(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::Type ExactStatisticsSegment::getType() const
   // Get the type
{
   return Segment::Type_ExactStatistics;
}
//---------------------------------------------------------------------------
void ExactStatisticsSegment::refreshInfo()
   // Refresh segment info stored in the partition
{
   Segment::refreshInfo();

   directoryPage=getSegmentData(slotDirectoryPage);
   BufferReference page(readShared(directoryPage));
   const unsigned char* directory=static_cast<const unsigned char*>(page.getPage());
   c2ps=readUint32(directory);
   c2po=readUint32(directory+4);
   c2so=readUint32(directory+8);
   c1s=readUint32(directory+12);
   c1p=readUint32(directory+16);
   c1o=readUint32(directory+20);
   c0ss=readUint64(directory+24);
   c0sp=readUint64(directory+32);
   c0so=readUint64(directory+40);
   c0ps=readUint64(directory+48);
   c0pp=readUint64(directory+56);
   c0po=readUint64(directory+64);
   c0os=readUint64(directory+72);
   c0op=readUint64(directory+80);
   c0oo=readUint64(directory+88);
   totalCardinality=readUint64(directory+96);
}
//---------------------------------------------------------------------------
AggregatedFactsSegment& ExactStatisticsSegment::getAggregatedFacts(unsigned order) const
   // Lookup a segment
{
   return *getPartition().lookupSegment<AggregatedFactsSegment>(DatabasePartition::Tag_SP+order);
}
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment& ExactStatisticsSegment::getFullyAggregatedFacts(unsigned order) const
   // Lookup a segment
{
   return *getPartition().lookupSegment<FullyAggregatedFactsSegment>(DatabasePartition::Tag_S+(order/2));
}
//---------------------------------------------------------------------------
unsigned ExactStatisticsSegment::getCardinality(unsigned subjectConstant,unsigned predicateConstant,unsigned objectConstant) const
   // Compute the cardinality of a single pattern
{
   if (~subjectConstant) {
      if (~predicateConstant) {
         if (~objectConstant) {
            return 1;
         } else {
            AggregatedFactsSegment::Scan scan;
            if (scan.first(getAggregatedFacts(Database::Order_Subject_Predicate_Object),subjectConstant,predicateConstant))
               return scan.getCount();
            return 1;
         }
      } else {
         if (~objectConstant) {
            AggregatedFactsSegment::Scan scan;
            if (scan.first(getAggregatedFacts(Database::Order_Subject_Object_Predicate),subjectConstant,objectConstant))
               return scan.getCount();
            return 1;
         } else {
            FullyAggregatedFactsSegment::Scan scan;
            if (scan.first(getFullyAggregatedFacts(Database::Order_Subject_Predicate_Object),subjectConstant))
               return scan.getCount();
            return 1;
         }
      }
   } else {
      if (~predicateConstant) {
         if (~objectConstant) {
            AggregatedFactsSegment::Scan scan;
            if (scan.first(getAggregatedFacts(Database::Order_Predicate_Object_Subject),predicateConstant,objectConstant))
               return scan.getCount();
            return 1;
         } else {
            FullyAggregatedFactsSegment::Scan scan;
            if (scan.first(getFullyAggregatedFacts(Database::Order_Predicate_Subject_Object),predicateConstant))
               return scan.getCount();
            return 1;
         }
      } else {
         if (~objectConstant) {
            FullyAggregatedFactsSegment::Scan scan;
            if (scan.first(getFullyAggregatedFacts(Database::Order_Object_Subject_Predicate),objectConstant))
               return scan.getCount();
            return 1;
         } else {
            return totalCardinality;
         }
      }
   }
}
//---------------------------------------------------------------------------
static unsigned long long readUintV(const unsigned char*& reader)
   // Unpack a variable length entry
{
   unsigned long long result=0;
   unsigned shift=0;
   while (true) {
      unsigned char c=*(reader++);
      result=result|(static_cast<unsigned long long>(c&0x7F)<<shift);
      shift+=7;
      if (!(c&0x80)) break;
   }
   return result;
}
//---------------------------------------------------------------------------
bool ExactStatisticsSegment::getJoinInfo2(unsigned root,unsigned value1,unsigned value2,unsigned long long& s,unsigned long long& p,unsigned long long& o) const
   // Lookup join cardinalities for two constants
{
   // Traverse the B-Tree
#define readInner1(page,slot) Segment::readUint32Aligned((page)+24+12*(slot))
#define readInner2(page,slot) Segment::readUint32Aligned((page)+24+12*(slot)+4)
#define readInnerPage(page,slot) Segment::readUint32Aligned((page)+24+12*(slot)+8)
#define greater(a1,a2,b1,b2) (((a1)>(b1))||(((a1)==(b1))&&((a2)>(b2))))
   BufferReference ref;
   ref=readShared(root);
   while (true) {
      const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
      // Inner node?
      if (readUint32Aligned(page+8)==0xFFFFFFFF) {
         // Perform a binary search. The test is more complex as we only have the upper bound for ranges
         unsigned left=0,right=readUint32Aligned(page+16);
         while (left!=right) {
            unsigned middle=(left+right)/2;
            unsigned middle1=readInner1(page,middle),middle2=readInner2(page,middle);
            if (greater(value1,value2,middle1,middle2)) {
               left=middle+1;
            } else if ((!middle)||(greater(value1,value2,readInner1(page,middle-1),readInner2(page,middle-1)))) {
               ref=readShared(readInnerPage(page,middle));
               break;
            } else {
               right=middle;
            }
         }
         // Unsuccessful search?
         if (left==right) {
            ref.reset();
            return false;
         }
      } else {
         // A leaf node
         break;
      }
   }
#undef greater
#undef readInnerPage
#undef readInner2
#undef readInner1

   // Decompress the leaf page
   const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
   unsigned char buffer[10*BufferReference::pageSize];
   unsigned compressedLen=Segment::readUint32(page+12);
   fastlz_decompress(page+16,compressedLen,buffer,sizeof(buffer));

   // Find the potential range for matches
   const unsigned char* reader=buffer;
   unsigned count,currentEntry;
   count=readUintV(reader);
   currentEntry=readUintV(reader);
   unsigned min=count,max=0;
   if (currentEntry==value1) {
      min=max=0;
   }
   for (unsigned index=1;index<count;index++) {
      currentEntry+=readUintV(reader);
      if (currentEntry==value1) {
         if (index<min) min=index;
         max=index;
      }
   }
   if (min>max) return false;

   // Find the exact position
   unsigned pos=count;
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (v&1)
         currentEntry=(v>>1); else
         currentEntry+=(v>>1);
      if ((currentEntry==value2)&&(index>=min)&&(index<=max))
         pos=index;
   }
   if (pos==count) return false;

   // Lookup the join matches
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) s=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) p=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) o=v;
   }

   return true;
}
//---------------------------------------------------------------------------
bool ExactStatisticsSegment::getJoinInfo1(unsigned root,unsigned value1,unsigned long long& s1,unsigned long long& p1,unsigned long long& o1,unsigned long long& s2,unsigned long long& p2,unsigned long long& o2) const
   // Lookup join cardinalities for two constants
{
   // Traverse the B-Tree
#define readInner1(page,slot) Segment::readUint32Aligned(page+24+8*(slot))
#define readInnerPage(page,slot) Segment::readUint32Aligned(page+24+8*(slot)+4)
   BufferReference ref;
   ref=readShared(root);
   while (true) {
      const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
      // Inner node?
      if (readUint32Aligned(page+8)==0xFFFFFFFF) {
         // Perform a binary search. The test is more complex as we only have the upper bound for ranges
         unsigned left=0,right=readUint32Aligned(page+16);
         while (left!=right) {
            unsigned middle=(left+right)/2;
            unsigned middle1=readInner1(page,middle);
            if (value1>middle1) {
               left=middle+1;
            } else if ((!middle)||(value1>readInner1(page,middle-1))) {
               ref=readShared(readInnerPage(page,middle));
               break;
            } else {
               right=middle;
            }
         }
         // Unsuccessful search?
         if (left==right) {
            ref.reset();
            return false;
         }
      } else {
         // A leaf node
         break;
      }
   }
#undef readInnerPage
#undef readInner1

   // Decompress the leaf page
   const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
   unsigned char buffer[10*BufferReference::pageSize];
   unsigned compressedLen=Segment::readUint32(page+12);
   fastlz_decompress(page+16,compressedLen,buffer,sizeof(buffer));

   // Find the exact position
   const unsigned char* reader=buffer;
   unsigned count,currentEntry;
   count=readUintV(reader);
   currentEntry=readUintV(reader);
   unsigned pos=count;
   if (currentEntry==value1) {
      pos=0;
   }
   for (unsigned index=1;index<count;index++) {
      currentEntry+=readUintV(reader);
      if (currentEntry==value1) {
         pos=index;
      }
   }
   if (pos==count) return false;

   // Lookup the join matches
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) s1=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) p1=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) o1=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) s2=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) p2=v;
   }
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=readUintV(reader);
      if (index==pos) o2=v;
   }

   return true;
}
//---------------------------------------------------------------------------
bool ExactStatisticsSegment::getJoinInfo(unsigned long long* joinInfo,unsigned subjectConstant,unsigned predicateConstant,unsigned objectConstant) const
   // Lookup join cardinalities
{
   // Reset all entries to "impossible" first
   for (unsigned index=0;index<9;index++)
      joinInfo[index]=~static_cast<unsigned long long>(0);

   // Now look up relevant data
   if (~subjectConstant) {
      if (~predicateConstant) {
         if (~objectConstant) {
            return false; // all constants, cannot participate in a join!
         } else {
            return getJoinInfo2(c2ps,predicateConstant,subjectConstant,joinInfo[6],joinInfo[7],joinInfo[8]);
         }
      } else {
         if (~objectConstant) {
            return getJoinInfo2(c2so,subjectConstant,objectConstant,joinInfo[3],joinInfo[4],joinInfo[5]);
         } else {
            return getJoinInfo1(c1s,subjectConstant,joinInfo[3],joinInfo[4],joinInfo[5],joinInfo[6],joinInfo[7],joinInfo[8]);
         }
      }
   } else {
      if (~predicateConstant) {
         if (~objectConstant) {
            return getJoinInfo2(c2po,predicateConstant,objectConstant,joinInfo[0],joinInfo[1],joinInfo[2]);
         } else {
            return getJoinInfo1(c1p,predicateConstant,joinInfo[0],joinInfo[1],joinInfo[2],joinInfo[6],joinInfo[7],joinInfo[8]);
         }
      } else {
         if (~objectConstant) {
            return getJoinInfo1(c1o,objectConstant,joinInfo[0],joinInfo[1],joinInfo[2],joinInfo[3],joinInfo[4],joinInfo[5]);
         } else {
            joinInfo[0]=c0ss; joinInfo[1]=c0sp; joinInfo[2]=c0so;
            joinInfo[3]=c0ps; joinInfo[4]=c0pp; joinInfo[5]=c0po;
            joinInfo[6]=c0os; joinInfo[7]=c0op; joinInfo[8]=c0oo;
            return true;
         }
      }
   }
}
//---------------------------------------------------------------------------
double ExactStatisticsSegment::getJoinSelectivity(bool s1c,unsigned s1,bool p1c,unsigned p1,bool o1c,unsigned o1,bool s2c,unsigned s2,bool p2c,unsigned p2,bool o2c,unsigned o2) const
   // Compute the join selectivity
{
   // Compute the individual sizes
   double card1=getCardinality(s1c?s1:~0u,p1c?p1:~0u,o1c?o1:~0u);
   double card2=getCardinality(s2c?s2:~0u,p2c?p2:~0u,o2c?o2:~0u);

   // Check that 1 is smaller than 2
   if (card2<card1) {
      swap(card1,card2);
      swap(s1c,s2c);
      swap(s1,s2);
      swap(p1c,p2c);
      swap(p1,p2);
      swap(o1c,o2c);
      swap(o1,o2);
   }

   // Lookup the join info
   unsigned long long joinInfo[9]; double crossCard;
   if (!getJoinInfo(joinInfo,s1c?s1:~0u,p1c?p1:~0u,o1c?o1:~0u)) {
      // Could no locate 1, check 2
      if (!getJoinInfo(joinInfo,s2c?s2:~0u,p2c?p2:~0u,o2c?o2:~0u)) {
         // Could not locate either, guess!
         return 1; // we could guess 0 here, as the entry was not found, but this might be due to stale statistics
      } else {
         crossCard=card2*static_cast<double>(totalCardinality);
      }
   } else {
      crossCard=card1*static_cast<double>(totalCardinality);
   }

   // And construct the most likely result size
   double resultSize=crossCard;
   if ((s1==s2)&&(!s1c)&&(!s2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[0]));
      if (p1c&&o1c) resultSize=min(resultSize,card2);
      if (p2c&&o2c) resultSize=min(resultSize,card1);
   }
   if ((s1==p2)&&(!s1c)&&(!p2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[1]));
      if (p1c&&o1c) resultSize=min(resultSize,card2);
      if (s2c&&o2c) resultSize=min(resultSize,card1);
   }
   if ((s1==o2)&&(!s1c)&&(!o2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[2]));
      if (p1c&&o1c) resultSize=min(resultSize,card2);
      if (s2c&&p2c) resultSize=min(resultSize,card1);
   }
   if ((p1==s2)&&(!p1c)&&(!s2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[3]));
      if (s1c&&o1c) resultSize=min(resultSize,card2);
      if (p2c&&o2c) resultSize=min(resultSize,card1);
   }
   if ((p1==p2)&&(!p1c)&&(!p2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[4]));
      if (s1c&&o1c) resultSize=min(resultSize,card2);
      if (s2c&&o2c) resultSize=min(resultSize,card1);
   }
   if ((p1==o2)&&(!p1c)&&(!o2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[5]));
      if (s1c&&o1c) resultSize=min(resultSize,card2);
      if (s2c&&p2c) resultSize=min(resultSize,card1);
   }
   if ((o1==s2)&&(!o1c)&&(!s2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[6]));
      if (s1c&&p1c) resultSize=min(resultSize,card2);
      if (p2c&&o2c) resultSize=min(resultSize,card1);
   }
   if ((o1==p2)&&(!o1c)&&(!p2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[7]));
      if (s1c&&p1c) resultSize=min(resultSize,card2);
      if (s2c&&o2c) resultSize=min(resultSize,card1);
   }
   if ((o1==o2)&&(!o1c)&&(!o2c)) {
      resultSize=min(resultSize,static_cast<double>(joinInfo[8]));
      if (s1c&&p1c) resultSize=min(resultSize,card2);
      if (s2c&&p2c) resultSize=min(resultSize,card1);
   }

   // Derive selectivity
   return resultSize/crossCard;
}
//---------------------------------------------------------------------------
/// Output for two-constant statistics
class ExactStatisticsSegment::Dumper2 {
   private:
   /// An entry
   struct Entry {
      /// The constant values
      unsigned value1,value2;
      /// The join partners
      unsigned long long s,p,o;
   };
   /// The maximum number of entries per page
   static const unsigned maxEntries = 32768;

   /// The segment
   ExactStatisticsSegment& seg;
   /// Page chainer
   DatabaseBuilder::PageChainer chainer;
   /// The entries
   Entry entries[maxEntries];
   /// The current count
   unsigned count;
   /// The page bounadries
   vector<pair<pair<unsigned,unsigned>,unsigned> >& boundaries;

   /// Write entries to a buffer
   bool writeEntries(unsigned count,unsigned char* pageBuffer);
   /// Write some entries
   void writeSome();

   public:
   /// Constructor
   Dumper2(ExactStatisticsSegment& seg,vector<pair<pair<unsigned,unsigned>,unsigned> >& boundaries) : seg(seg),chainer(8),count(0),boundaries(boundaries) {}

   /// Add an entry
   void add(unsigned value1,unsigned value2,unsigned long long s,unsigned long long p,unsigned long long o);
   /// Flush pending entries
   void flush();
};
//---------------------------------------------------------------------------
static unsigned char* writeUIntV(unsigned char* writer,unsigned long long v)
   // Write a value with variable length
{
   while (v>=128) {
      *writer=static_cast<unsigned char>((v&0x7F)|0x80);
      v>>=7;
      ++writer;
   }
   *writer=static_cast<unsigned char>(v);
   return writer+1;
}
//---------------------------------------------------------------------------
bool ExactStatisticsSegment::Dumper2::writeEntries(unsigned count,unsigned char* pageBuffer)
   // Write a page
{
   // Refuse to handle empty ranges
   if (!count)
      return false;

   // Temp space
   static const unsigned maxSize=10*BufferReference::pageSize;
   unsigned char buffer1[maxSize+32];
   unsigned char buffer2[maxSize+(maxSize/15)];

   // Write the entries
   unsigned char* writer=buffer1,*limit=buffer1+maxSize;
   writer=writeUIntV(writer,count);
   writer=writeUIntV(writer,entries[0].value1);
   for (unsigned index=1;index<count;index++) {
      writer=writeUIntV(writer,entries[index].value1-entries[index-1].value1);
      if (writer>limit) return false;
   }
   unsigned last=~0u;
   for (unsigned index=0;index<count;index++) {
      unsigned long long v=static_cast<unsigned long long>(entries[index].value2)<<1;
      if (entries[index].value2<last)
         v=(static_cast<unsigned long long>(entries[index].value2)<<1)|1; else
         v=static_cast<unsigned long long>(entries[index].value2-last)<<1;
      last=entries[index].value2;
      writer=writeUIntV(writer,v);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].s);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].p);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].o);
      if (writer>limit) return false;
   }

   // Compress them
   unsigned len=fastlz_compress(buffer1,writer-buffer1,buffer2);
   if (len>=(BufferReference::pageSize-16))
      return false;

   // And write the page
   writeUint32(pageBuffer+8,0);
   writeUint32(pageBuffer+12,len);
   memcpy(pageBuffer+16,buffer2,len);
   memset(pageBuffer+16+len,0,BufferReference::pageSize-(16+len));

   return true;
}
//---------------------------------------------------------------------------
void ExactStatisticsSegment::Dumper2::writeSome()
   /// Write some entries
{
   // Find the maximum fill size
   unsigned char pageBuffer[2*BufferReference::pageSize];
   unsigned l=0,r=count,best=1;
   while (l<r) {
      unsigned m=(l+r)/2;
      if (writeEntries(m+1,pageBuffer)) {
         if (m+1>best)
            best=m+1;
         l=m+1;
      } else {
         r=m;
      }
   }
   // Write the page
   writeEntries(best,pageBuffer);
   chainer.store(&seg,pageBuffer);
   boundaries.push_back(pair<pair<unsigned,unsigned>,unsigned>(pair<unsigned,unsigned>(entries[best-1].value1,entries[best-1].value2),chainer.getPageNo()));

   // And move the entries
   memmove(entries,entries+best,sizeof(Entry)*(count-best));
   count-=best;
}
//---------------------------------------------------------------------------
void ExactStatisticsSegment::Dumper2::add(unsigned value1,unsigned value2,unsigned long long s,unsigned long long p,unsigned long long o)
   // Add an entry
{
   // Full? Then write some entries
   if (count==maxEntries)
      writeSome();

   // Append
   entries[count].value1=value1;
   entries[count].value2=value2;
   entries[count].s=s;
   entries[count].p=p;
   entries[count].o=o;
   ++count;
}
//---------------------------------------------------------------------------
void ExactStatisticsSegment::Dumper2::flush()
   // Flush pending entries
{
   while (count)
      writeSome();
   chainer.finish();
}
//---------------------------------------------------------------------------
/// Output for one-constant statistics
class ExactStatisticsSegment::Dumper1 {
   private:
   /// An entry
   struct Entry {
      /// The constant value
      unsigned value1;
      /// The join partners
      unsigned long long s1,p1,o1,s2,p2,o2;
   };
   /// The maximum number of entries per page
   static const unsigned maxEntries = 32768;

   /// The segment
   ExactStatisticsSegment& seg;
   /// The page chainer
   DatabaseBuilder::PageChainer chainer;
   /// The entries
   Entry entries[maxEntries];
   /// The current count
   unsigned count;
   /// The page bounadries
   vector<pair<unsigned,unsigned> >& boundaries;

   /// Write entries to a buffer
   bool writeEntries(unsigned count,unsigned char* pageBuffer);
   /// Write some entries
   void writeSome();

   public:
   /// Constructor
   Dumper1(ExactStatisticsSegment& seg,vector<pair<unsigned,unsigned> >& boundaries) : seg(seg),chainer(8),count(0),boundaries(boundaries) {}

   /// Add an entry
   void add(unsigned value1,unsigned long long s1,unsigned long long p1,unsigned long long o1,unsigned long long s2,unsigned long long p2,unsigned long long o2);
   /// Flush pending entries
   void flush();
};
//---------------------------------------------------------------------------
bool ExactStatisticsSegment::Dumper1::writeEntries(unsigned count,unsigned char* pageBuffer)
   // Write a page
{
   // Refuse to handle empty ranges
   if (!count)
      return false;

   // Temp space
   static const unsigned maxSize=10*BufferReference::pageSize;
   unsigned char buffer1[maxSize+32];
   unsigned char buffer2[maxSize+(maxSize/15)];

   // Write the entries
   unsigned char* writer=buffer1,*limit=buffer1+maxSize;
   writer=writeUIntV(writer,count);
   writer=writeUIntV(writer,entries[0].value1);
   for (unsigned index=1;index<count;index++) {
      writer=writeUIntV(writer,entries[index].value1-entries[index-1].value1);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].s1);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].p1);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].o1);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].s2);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].p2);
      if (writer>limit) return false;
   }
   for (unsigned index=0;index<count;index++) {
      writer=writeUIntV(writer,entries[index].o2);
      if (writer>limit) return false;
   }

   // Compress them
   unsigned len=fastlz_compress(buffer1,writer-buffer1,buffer2);
   if (len>=(BufferReference::pageSize-16))
      return false;

   // And write the page
   writeUint32(pageBuffer+8,0);
   writeUint32(pageBuffer+12,len);
   memcpy(pageBuffer+16,buffer2,len);
   memset(pageBuffer+16+len,0,BufferReference::pageSize-(16+len));

   return true;
}
//---------------------------------------------------------------------------
void ExactStatisticsSegment::Dumper1::writeSome()
   /// Write some entries
{
   // Find the maximum fill size
   unsigned char pageBuffer[2*BufferReference::pageSize];
   unsigned l=0,r=count,best=1;
   while (l<r) {
      unsigned m=(l+r)/2;
      if (writeEntries(m+1,pageBuffer)) {
         if (m+1>best)
            best=m+1;
         l=m+1;
      } else {
         r=m;
      }
   }
   // Write the page
   writeEntries(best,pageBuffer);
   chainer.store(&seg,pageBuffer);
   boundaries.push_back(pair<unsigned,unsigned>(entries[best-1].value1,chainer.getPageNo()));

   // And move the entries
   memmove(entries,entries+best,sizeof(Entry)*(count-best));
   count-=best;
}
//---------------------------------------------------------------------------
void ExactStatisticsSegment::Dumper1::add(unsigned value1,unsigned long long s1,unsigned long long p1,unsigned long long o1,unsigned long long s2,unsigned long long p2,unsigned long long o2)
   // Add an entry
{
   // Full? Then write some entries
   if (count==maxEntries)
      writeSome();

   // Append
   entries[count].value1=value1;
   entries[count].s1=s1;
   entries[count].p1=p1;
   entries[count].o1=o1;
   entries[count].s2=s2;
   entries[count].p2=p2;
   entries[count].o2=o2;
   ++count;
}
//---------------------------------------------------------------------------
void ExactStatisticsSegment::Dumper1::flush()
   // Flush pending entries
{
   while (count)
      writeSome();
   chainer.finish();
}
//---------------------------------------------------------------------------
static void addCounts(const char* countMap,unsigned id,unsigned long long multiplicity,unsigned long long& countS,unsigned long long & countP,unsigned long long& countO)
   // Add all counts
{
   const unsigned* base=reinterpret_cast<const unsigned*>(countMap)+(3*id);
   countS+=multiplicity*static_cast<unsigned long long>(base[0]);
   countP+=multiplicity*static_cast<unsigned long long>(base[1]);
   countO+=multiplicity*static_cast<unsigned long long>(base[2]);
}
//---------------------------------------------------------------------------
static void computeExact2Leaves(DatabasePartition& part,ExactStatisticsSegment& seg,vector<pair<pair<unsigned,unsigned>,unsigned> >& boundaries,Database::DataOrder order,const char* countMap)
   // Compute the exact statistics for patterns with two constants
{
   ExactStatisticsSegment::Dumper2 dumper(seg,boundaries);

   FactsSegment::Scan scan;
   if (scan.first(*part.lookupSegment<FactsSegment>(DatabasePartition::Tag_SPO+order))) {
      // And scan
      unsigned last1=~0u,last2=~0u;
      unsigned long long countS=0,countP=0,countO=0;
      do {
         // A new entry?
         if ((scan.getValue1()!=last1)||(scan.getValue2()!=last2)) {
            if (~last1) {
               dumper.add(last1,last2,countS,countP,countO);
            }
            last1=scan.getValue1();
            last2=scan.getValue2();
            countS=0;
            countP=0;
            countO=0;
         }
         // Add entries
         addCounts(countMap,scan.getValue3(),1,countS,countP,countO);
      } while (scan.next());
      // Add the last entry
      if (~last1) {
         dumper.add(last1,last2,countS,countP,countO);
      }
   }

   // Write pending entries if any
   dumper.flush();
}
//---------------------------------------------------------------------------
static void writeUint32(unsigned char* target,unsigned value)
   // Write a 32bit value
{
   target[0]=value>>24;
   target[1]=(value>>16)&0xFF;
   target[2]=(value>>8)&0xFF;
   target[3]=value&0xFF;
}
//---------------------------------------------------------------------------
static void writeUint64(unsigned char* target,unsigned long long value)
   // Write a 64bit value
{
   for (unsigned index=0;index<8;index++)
      target[index]=static_cast<unsigned char>((value>>(8*(7-index)))&0xFF);
}
//---------------------------------------------------------------------------
static void computeExact2Inner(ExactStatisticsSegment& seg,const vector<pair<pair<unsigned,unsigned>,unsigned> >& data,vector<pair<pair<unsigned,unsigned>,unsigned> >& boundaries)
   // Create inner nodes
{
   const unsigned headerSize = 24; // LSN+marker+next+count+padding
   DatabaseBuilder::PageChainer chainer(12);
   unsigned char buffer[BufferReference::pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   for (vector<pair<pair<unsigned,unsigned>,unsigned> >::const_iterator iter=data.begin(),limit=data.end();iter!=limit;++iter) {
      // Do we have to start a new page?
      if ((bufferPos+12)>BufferReference::pageSize) {
         writeUint32(buffer+8,0xFFFFFFFF);
         writeUint32(buffer+16,bufferCount);
         writeUint32(buffer+20,0);
         for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
            buffer[index]=0;
         chainer.store(&seg,buffer);
         boundaries.push_back(pair<pair<unsigned,unsigned>,unsigned>((*(iter-1)).first,chainer.getPageNo()));
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the entry
      writeUint32(buffer+bufferPos,(*iter).first.first); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).first.second); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).second); bufferPos+=4;
      bufferCount++;
   }
   // Write the least page
   writeUint32(buffer+8,0xFFFFFFFF);
   writeUint32(buffer+16,bufferCount);
   writeUint32(buffer+20,0);
   for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
      buffer[index]=0;
   chainer.store(&seg,buffer);
   boundaries.push_back(pair<pair<unsigned,unsigned>,unsigned>(data.back().first,chainer.getPageNo()));
   chainer.finish();
}
//---------------------------------------------------------------------------
static unsigned computeExact2(DatabasePartition& part,ExactStatisticsSegment& seg,Database::DataOrder order,const char* countMap)
   // Compute the exact statistics for patterns with two constants
{
   // Write the leave nodes
   vector<pair<pair<unsigned,unsigned>,unsigned> > boundaries;
   computeExact2Leaves(part,seg,boundaries,order,countMap);

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<pair<pair<unsigned,unsigned>,unsigned> > newBoundaries;
      computeExact2Inner(seg,boundaries,newBoundaries);
      return newBoundaries.back().second;
   }

   // Write the inner nodes
   while (boundaries.size()>1) {
      vector<pair<pair<unsigned,unsigned>,unsigned> > newBoundaries;
      computeExact2Inner(seg,boundaries,newBoundaries);
      swap(boundaries,newBoundaries);
   }
   return boundaries.back().second;
}
//---------------------------------------------------------------------------
static void computeExact1Leaves(DatabasePartition& part,ExactStatisticsSegment& seg,vector<pair<unsigned,unsigned> >& boundaries,Database::DataOrder order1,Database::DataOrder order2,const char* countMap)
   // Compute the exact statistics for patterns with one constant
{
   ExactStatisticsSegment::Dumper1 dumper(seg,boundaries);

   AggregatedFactsSegment::Scan scan1,scan2;
   if (scan1.first(*part.lookupSegment<AggregatedFactsSegment>(DatabasePartition::Tag_SP+order1))&&
       scan2.first(*part.lookupSegment<AggregatedFactsSegment>(DatabasePartition::Tag_SP+order2))) {
      // Scan
      bool done=false;
      while (!done) {
         // Read scan1
         unsigned last1=scan1.getValue1();
         unsigned long long countS1=0,countP1=0,countO1=0;
         while (true) {
            if (scan1.getValue1()!=last1)
               break;
            addCounts(countMap,scan1.getValue2(),scan1.getCount(),countS1,countP1,countO1);
            if (!scan1.next()) {
               done=true;
               break;
            }
         }

         // Read scan2
         unsigned last2=scan2.getValue1();
         unsigned long long countS2=0,countP2=0,countO2=0;
         while (true) {
            if (scan2.getValue1()!=last2)
               break;
            addCounts(countMap,scan2.getValue2(),scan2.getCount(),countS2,countP2,countO2);
            if (!scan2.next()) {
               done=true;
               break;
            }
         }

         // Produce output tuple
         assert(last1==last2);
         dumper.add(last1,countS1,countP1,countO1,countS2,countP2,countO2);
      }
   }

   // Write pending entries if any
   dumper.flush();
}
//---------------------------------------------------------------------------
static void computeExact1Inner(ExactStatisticsSegment& seg,const vector<pair<unsigned,unsigned> >& data,vector<pair<unsigned,unsigned> >& boundaries)
   // Create inner nodes
{
   const unsigned headerSize = 24; // LSN+marker+next+count+padding
   DatabaseBuilder::PageChainer chainer(12);
   unsigned char buffer[BufferReference::pageSize];
   unsigned bufferPos=headerSize,bufferCount=0;

   for (vector<pair<unsigned,unsigned> >::const_iterator iter=data.begin(),limit=data.end();iter!=limit;++iter) {
      // Do we have to start a new page?
      if ((bufferPos+8)>BufferReference::pageSize) {
         writeUint32(buffer+8,0xFFFFFFFF);
         writeUint32(buffer+16,bufferCount);
         writeUint32(buffer+20,0);
         for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
            buffer[index]=0;
         chainer.store(&seg,buffer);
         boundaries.push_back(pair<unsigned,unsigned>((*(iter-1)).first,chainer.getPageNo()));
         bufferPos=headerSize; bufferCount=0;
      }
      // Write the entry
      writeUint32(buffer+bufferPos,(*iter).first); bufferPos+=4;
      writeUint32(buffer+bufferPos,(*iter).second); bufferPos+=4;
      bufferCount++;
   }
   // Write the least page
   writeUint32(buffer+8,0xFFFFFFFF);
   writeUint32(buffer+16,bufferCount);
   writeUint32(buffer+20,0);
   for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
      buffer[index]=0;
   chainer.store(&seg,buffer);
   boundaries.push_back(pair<unsigned,unsigned>(data.back().first,chainer.getPageNo()));
   chainer.finish();
}
//---------------------------------------------------------------------------
static unsigned computeExact1(DatabasePartition& part,ExactStatisticsSegment& seg,Database::DataOrder order1,Database::DataOrder order2,const char* countMap)
   // Compute the exact statistics for patterns with one constant
{
   // Write the leave nodes
   vector<pair<unsigned,unsigned> > boundaries;
   computeExact1Leaves(part,seg,boundaries,order1,order2,countMap);

   // Only one leaf node? Special case this
   if (boundaries.size()==1) {
      vector<pair<unsigned,unsigned> > newBoundaries;
      computeExact1Inner(seg,boundaries,newBoundaries);
      return newBoundaries.back().second;
   }

   // Write the inner nodes
   while (boundaries.size()>1) {
      vector<pair<unsigned,unsigned> > newBoundaries;
      computeExact1Inner(seg,boundaries,newBoundaries);
      swap(boundaries,newBoundaries);
   }
   return boundaries.back().second;
}
//---------------------------------------------------------------------------
static unsigned long long computeExact0(MemoryMappedFile& countMap,unsigned ofs1,unsigned ofs2)
   // Compute the exact statistics for patterns without constants
{
   const unsigned* data=reinterpret_cast<const unsigned*>(countMap.getBegin());
   const unsigned* limit=reinterpret_cast<const unsigned*>(countMap.getEnd());
   unsigned long long result=0;
   for (const unsigned* iter=data;iter<limit;iter+=3) {
      result+=static_cast<unsigned long long>(iter[ofs1])*static_cast<unsigned long long>(iter[ofs2]);
   }
   return result;
}
//---------------------------------------------------------------------------
void ExactStatisticsSegment::computeExactStatistics(MemoryMappedFile& countMap)
   // Compute exact statistics (after loading)
{
   DatabasePartition& part=getPartition();

   // Compute the exact 2 statistics
   unsigned exactPS=computeExact2(part,*this,Database::Order_Predicate_Subject_Object,countMap.getBegin());
   unsigned exactPO=computeExact2(part,*this,Database::Order_Predicate_Object_Subject,countMap.getBegin());
   unsigned exactSO=computeExact2(part,*this,Database::Order_Subject_Object_Predicate,countMap.getBegin());

   // Compute the exact 1 statistics
   unsigned exactS=computeExact1(part,*this,Database::Order_Subject_Predicate_Object,Database::Order_Subject_Object_Predicate,countMap.getBegin());
   unsigned exactP=computeExact1(part,*this,Database::Order_Predicate_Subject_Object,Database::Order_Predicate_Object_Subject,countMap.getBegin());
   unsigned exactO=computeExact1(part,*this,Database::Order_Object_Subject_Predicate,Database::Order_Object_Predicate_Subject,countMap.getBegin());

   // Compute the exact 0 statistics
   unsigned long long exact0SS=computeExact0(countMap,0,0);
   unsigned long long exact0SP=computeExact0(countMap,0,1);
   unsigned long long exact0SO=computeExact0(countMap,0,2);
   unsigned long long exact0PS=computeExact0(countMap,1,0);
   unsigned long long exact0PP=computeExact0(countMap,1,1);
   unsigned long long exact0PO=computeExact0(countMap,1,2);
   unsigned long long exact0OS=computeExact0(countMap,2,0);
   unsigned long long exact0OP=computeExact0(countMap,2,1);
   unsigned long long exact0OO=computeExact0(countMap,2,2);

   // And number of tuples
   unsigned cardinality=part.lookupSegment<FactsSegment>(DatabasePartition::Tag_SPO)->getCardinality();

   // Write the directory page
   BufferReferenceModified page;
   allocPage(page);
   unsigned char* directory=static_cast<unsigned char*>(page.getPage());
   writeUint32(directory,exactPS);
   writeUint32(directory+4,exactPO);
   writeUint32(directory+8,exactSO);
   writeUint32(directory+12,exactS);
   writeUint32(directory+16,exactP);
   writeUint32(directory+20,exactO);
   writeUint64(directory+24,exact0SS);
   writeUint64(directory+32,exact0SP);
   writeUint64(directory+40,exact0SO);
   writeUint64(directory+48,exact0PS);
   writeUint64(directory+56,exact0PP);
   writeUint64(directory+64,exact0PO);
   writeUint64(directory+72,exact0OS);
   writeUint64(directory+80,exact0OP);
   writeUint64(directory+88,exact0OO);
   writeUint64(directory+96,cardinality);
   directoryPage=page.getPageNo();
   page.unfixWithoutRecovery();
   setSegmentData(slotDirectoryPage,directoryPage);
}
//---------------------------------------------------------------------------
