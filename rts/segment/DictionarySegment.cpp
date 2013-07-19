#include "rts/segment/DictionarySegment.hpp"
#include "rts/buffer/BufferReference.hpp"
#include "rts/segment/BTree.hpp"
#include "infra/util/Hash.hpp"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <vector>
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
static const unsigned slotNextId = 1;
static const unsigned slotMappingStart = 2;
static const unsigned slotIndexRoot = 3;
//---------------------------------------------------------------------------
const unsigned entriesOnFirstMappingPage = (BufferReference::pageSize-16)/8;
const unsigned entriesPerMappingPage = (BufferReference::pageSize-8)/8;
//---------------------------------------------------------------------------
/// Index hash-value -> string
class DictionarySegment::HashIndexImplementation
{
   public:
   /// The size of an inner key
   static const unsigned innerKeySize = 4;
   /// An inner key
   struct InnerKey {
      /// The values
      unsigned hash;

      /// Constructor
      InnerKey() : hash(0) {}
      /// Constructor
      InnerKey(unsigned hash) : hash(hash) {}

      /// Compare
      bool operator==(const InnerKey& o) const { return (hash==o.hash); }
      /// Compare
      bool operator<(const InnerKey& o) const { return (hash<o.hash); }
   };
   /// Read an inner key
   static void readInnerKey(InnerKey& key,const unsigned char* ptr) {
      key.hash=Segment::readUint32Aligned(ptr);
   }
   /// Write an inner key
   static void writeInnerKey(unsigned char* ptr,const InnerKey& key) {
      Segment::writeUint32Aligned(ptr,key.hash);
   }
   /// A leaf entry
   struct LeafEntry {
      /// The key value
      unsigned hash;
      /// The payload
      unsigned page;

      /// Compare
      bool operator==(const LeafEntry& o) const { return (hash==o.hash); }
      /// Compare
      bool operator<(const LeafEntry& o) const { return (hash<o.hash); }
      /// Compare
      bool operator<(const InnerKey& o) const { return (hash<o.hash); }
   };
   /// A leaf entry source
   class LeafEntrySource {
      private:
      /// The real source
      DictionarySegment::HashSource& source;

      public:
      /// Constructor
      LeafEntrySource(DictionarySegment::HashSource& source) : source(source) {}

      /// Read the next entry
      bool next(LeafEntry& l) { return source.next(l.hash,l.page); }
      /// Mark last entry as conflict
      void markAsConflict() { }
   };
   /// Derive an inner key
   static InnerKey deriveInnerKey(const LeafEntry& e) { return InnerKey(e.hash); }
   /// Read the first leaf entry
   static void readFirstLeafEntryKey(InnerKey& key,const unsigned char* ptr) {
      key.hash=Segment::readUint32Aligned(ptr+4);
   }

   private:
   /// The segment
   DictionarySegment& segment;

   public:
   /// Constructor
   explicit HashIndexImplementation(DictionarySegment& segment) : segment(segment) {}

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
   static bool mergeConflictWith(const LeafEntry& newEntry,LeafEntry& oldEntry) { return (oldEntry.hash==newEntry.hash)&&(oldEntry.page==newEntry.page); }

   /// Pack leaf entries
   static unsigned packLeafEntries(unsigned char* writer,unsigned char* limit,vector<LeafEntry>::const_iterator entriesStart,vector<LeafEntry>::const_iterator entriesLimit);
   /// Unpack leaf entries
   static void unpackLeafEntries(vector<LeafEntry>& entries,const unsigned char* reader,const unsigned char* limit);
};
//---------------------------------------------------------------------------
void DictionarySegment::HashIndexImplementation::setRootPage(unsigned page)
   // Se the root page
{
   segment.indexRoot=page;
   segment.setSegmentData(slotIndexRoot,segment.indexRoot);
}
//---------------------------------------------------------------------------
void DictionarySegment::HashIndexImplementation::updateLeafInfo(unsigned /*firstLeaf*/,unsigned /*leafCount*/)
   // Store info about the leaf pages
{
}
//---------------------------------------------------------------------------
unsigned DictionarySegment::HashIndexImplementation::packLeafEntries(unsigned char* writer,unsigned char* writerLimit,vector<DictionarySegment::HashIndexImplementation::LeafEntry>::const_iterator entriesStart,vector<DictionarySegment::HashIndexImplementation::LeafEntry>::const_iterator entriesLimit)
   // Store the hash/page pairs
{
   // Too small?
   if ((writerLimit-writer)<4)
      return 0;

   // Compute the output len
   unsigned maxLen=((writerLimit-writer)-4)/8;
   unsigned inputLen=entriesLimit-entriesStart;
   unsigned len=min(maxLen,inputLen);

   // Write the count
   Segment::writeUint32Aligned(writer,len); writer+=4;

   // Store the entries
   for (unsigned index=0;index<len;++index,++entriesStart) {
      Segment::writeUint32Aligned(writer,(*entriesStart).hash); writer+=4;
      Segment::writeUint32Aligned(writer,(*entriesStart).page); writer+=4;
   }

   // Pad the remaining space
   memset(writer,0,writerLimit-writer);
   return len;
}
//---------------------------------------------------------------------------
void DictionarySegment::HashIndexImplementation::unpackLeafEntries(vector<DictionarySegment::HashIndexImplementation::LeafEntry>& entries,const unsigned char* reader,const unsigned char* /*limit*/)
   // Read the hash/page pairs
{
   // Read the len
   unsigned len=Segment::readUint32Aligned(reader); reader+=4;

   // Read the entries
   entries.resize(len);
   for (unsigned index=0;index<len;index++) {
      entries[index].hash=Segment::readUint32Aligned(reader); reader+=4;
      entries[index].page=Segment::readUint32Aligned(reader); reader+=4;
   }
}
//---------------------------------------------------------------------------
/// Index hash-value -> string
class DictionarySegment::HashIndex : public BTree<HashIndexImplementation>
{
   public:
   /// Constructor
   explicit HashIndex(DictionarySegment& segment) : BTree<HashIndexImplementation>(segment) {}

   /// Size of the leaf header (used for scans)
   using BTree<HashIndexImplementation>::leafHeaderSize;
};
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
LOGACTION3(DictionarySegment,UpdateMapping,uint32_t,slot,LogData,oldValue,LogData,newValue)
//---------------------------------------------------------------------------
void UpdateMapping::redo(void* page) const { memcpy(static_cast<unsigned char*>(page)+8+8*slot,newValue.ptr,newValue.len); }
void UpdateMapping::undo(void* page) const { memcpy(static_cast<unsigned char*>(page)+8+8*slot,oldValue.ptr,oldValue.len); }
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
DictionarySegment::StringSource::~StringSource()
   // Destructor
{
}
//---------------------------------------------------------------------------
DictionarySegment::IdSource::~IdSource()
   // Destructor
{
}
//---------------------------------------------------------------------------
DictionarySegment::HashSource::~HashSource()
   // Destructor
{
}
//---------------------------------------------------------------------------
DictionarySegment::DictionarySegment(DatabasePartition& partition)
   : Segment(partition),tableStart(0),nextId(0),indexRoot(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
Segment::Type DictionarySegment::getType() const
   // Get the type
{
   return Segment::Type_Dictionary;
}
//---------------------------------------------------------------------------
void DictionarySegment::refreshInfo()
   // Refresh segment info stored in the partition
{
   Segment::refreshInfo();

   tableStart=getSegmentData(slotTableStart);
   nextId=getSegmentData(slotNextId);
   mappings.push_back(pair<unsigned,unsigned>(getSegmentData(slotMappingStart),0));
   indexRoot=getSegmentData(slotIndexRoot);
}
//---------------------------------------------------------------------------
void DictionarySegment::refreshMapping()
   // Refresh the mapping table if needed
{
   // Check if we only know the start mapge
   if ((mappings.size()==1)&&(mappings[0].second==0)) {
      unsigned iter=mappings[0].first;
      mappings.clear();
      // Walk the chain
      for (;iter;) {
         BufferReference ref(readShared(iter));
         unsigned next=Segment::readUint32Aligned(static_cast<const unsigned char*>(ref.getPage())+8);
         unsigned len=Segment::readUint32Aligned(static_cast<const unsigned char*>(ref.getPage())+8+4);
         mappings.push_back(pair<unsigned,unsigned>(iter,len));
         iter=next;
      }
   }
}
//---------------------------------------------------------------------------
static inline unsigned getLiteralLen(unsigned header) { return header&0x00FFFFFF; }
static inline unsigned getLiteralType(unsigned header) { return header>>24; }
//---------------------------------------------------------------------------
bool DictionarySegment::lookupOnPage(unsigned pageNo,const string& text,::Type::ID type,unsigned subType,unsigned hash,unsigned& id)
   // Lookup an id for a given string on a certain page in the raw string table
{
   BufferReference ref(readShared(pageNo));
   const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
   unsigned count=readUint32Aligned(page+12),pos=16;

   for (unsigned index=0;index<count;index++) {
      if (pos+12>BufferReference::pageSize)
         break;
      unsigned header=readUint32(page+pos+8);
      unsigned len=getLiteralLen(header),currentType=getLiteralType(header);
      if ((currentType==static_cast<unsigned>(type))&&(readUint32(page+pos+4)==hash)&&(len==text.length())) {
         // Examine the sub-type if any
         unsigned ofs=pos+12;
         bool match=true;
         if (::Type::hasSubType(static_cast< ::Type::ID>(currentType))) {
            unsigned currentSubType=readUint32(page+ofs);
            if (subType!=currentSubType)
               match=false;
            ofs+=4;
         }
         // Check if the string is really identical
         if (match&&(memcmp(page+ofs,text.c_str(),len)==0)) {
            id=readUint32(page+pos);
            return true;
         }
      }
      pos+=12+len+(::Type::hasSubType(static_cast< ::Type::ID>(currentType))?4:0);
   }
   return false;
}
//---------------------------------------------------------------------------
bool DictionarySegment::lookup(const string& text,::Type::ID type,unsigned subType,unsigned& id)
   // Lookup an id for a given string
{
   // Determine the hash value
   unsigned hash=Hash::hash(text,(type<<24)^subType);

   // Find the leaf page
   BufferReference ref;
   if (!HashIndex(*this).findLeaf(ref,HashIndex::InnerKey(hash)))
      return false;

   // A leaf node. Perform a binary search on the exact value.
   const unsigned char* page=static_cast<const unsigned char*>(ref.getPage());
   unsigned left=0,right=readUint32Aligned(page+HashIndex::leafHeaderSize);
   while (left!=right) {
      unsigned middle=(left+right)/2;
      unsigned hashAtMiddle=readUint32Aligned(page+HashIndex::leafHeaderSize+4+8*middle);
      if (hash>hashAtMiddle) {
         left=middle+1;
      } else if (hash<hashAtMiddle) {
         right=middle;
      } else {
         // We found a match. Adjust the bounds as there can be collisions
         left=middle;
         right=readUint32Aligned(page+HashIndex::leafHeaderSize);
         while (left&&(readUint32Aligned(page+HashIndex::leafHeaderSize+4+8*(left-1))==hash))
            --left;
         break;
      }
   }
   // Unsuccessful search?
   if (left==right)
      return false;

   // Scan all candidates in the collision list
   for (;left<right;++left) {
      // End of the collision list?
      if (readUint32Aligned(page+HashIndex::leafHeaderSize+4+8*left)!=hash)
         return false;
      // No, lookup the candidate
      if (lookupOnPage(readUint32Aligned(page+HashIndex::leafHeaderSize+4+(8*left)+4),text,type,subType,hash,id))
         return true;
   }
   // We reached the end of the page
   return false;
}
//---------------------------------------------------------------------------
bool DictionarySegment::lookupById(unsigned id,const char*& start,const char*& stop,::Type::ID& type,unsigned& subType)
   // Lookup a string for a given id
{
   // Fill the mappings if needed
   refreshMapping();

   // Find the relevant mapping chunk
   unsigned mappingStart,relId=id;
   for (vector<pair<unsigned,unsigned> >::const_iterator iter=mappings.begin(),limit=mappings.end();;++iter) {
      if (iter==limit)
         return false;
      unsigned mappingsInChunk=entriesOnFirstMappingPage+(((*iter).second-1)*entriesPerMappingPage);
      if (relId<mappingsInChunk) {
         mappingStart=(*iter).first;
         break;
      } else {
         relId-=mappingsInChunk;
      }
   }

   // Compute position in directory
   unsigned dirPage,dirSlot;
   if (relId<entriesOnFirstMappingPage) {
      dirPage=mappingStart;
      dirSlot=relId+1;
   } else {
      dirPage=mappingStart+1+((relId-entriesOnFirstMappingPage)/entriesPerMappingPage);
      dirSlot=(relId-entriesOnFirstMappingPage)%entriesPerMappingPage;
   }

   // Lookup the direct mapping entry
   BufferReference ref(readShared(dirPage));
   unsigned pageNo=readUint32(static_cast<const unsigned char*>(ref.getPage())+8+8*dirSlot);
   unsigned ofsLen=readUint32(static_cast<const unsigned char*>(ref.getPage())+8+8*dirSlot+4);
   unsigned ofs=ofsLen>>16,len=(ofsLen&0xFFFF);

   // Now search the entry on the page itself
   ref=readShared(pageNo);
   const char* page=static_cast<const char*>(ref.getPage());

   // Read the type info
   unsigned typeLen=readUint32(reinterpret_cast<const unsigned char*>(page+ofs+8));
   type=static_cast< ::Type::ID>(typeLen>>24);
   len=(typeLen&0x00FFFFFF);

   // Has a sub type?
   if (::Type::hasSubType(type)) {
      ofs+=4;
      subType=readUint32(reinterpret_cast<const unsigned char*>(page+ofs+8));
   } else {
      subType=0;
   }

   // And return the string bounds
   start=page+ofs+12; stop=start+len;

   return true;
}
//---------------------------------------------------------------------------
void DictionarySegment::loadStrings(StringSource& reader)
   // Load the raw strings (must be in id order)
{
   static const unsigned pageSize = BufferReference::pageSize;

   // Prepare the buffer
   const unsigned headerSize = 16; // LSN+next+count
   DatabaseBuilder::PageChainer chainer(8);
   unsigned char* buffer=0;
   unsigned bufferPos=headerSize,bufferCount=0;

   // Read the strings
   unsigned len; const char* data;
   ::Type::ID type; unsigned subType;
   unsigned id=0;
   while (reader.next(len,data,type,subType)) {
      // Is the page full?
      if ((bufferPos+12+len+(::Type::hasSubType(type)?4:0)>pageSize)&&(bufferCount)) {
         for (unsigned index=bufferPos;index<pageSize;index++)
            buffer[index]=0;
         writeUint32(buffer+12,bufferCount);
         buffer=static_cast<unsigned char*>(chainer.nextPage(this));
         bufferPos=headerSize; bufferCount=0;
      }
      // Check the len, handle an overlong string
      if (bufferPos+12+len+(::Type::hasSubType(type)?4:0)>pageSize) {
         // Write the first page
         unsigned hash=Hash::hash(data,len,(type<<24)^subType);
         writeUint32(buffer+12,1);
         writeUint32(buffer+bufferPos,id);
         writeUint32(buffer+bufferPos+4,hash);
         writeUint32(buffer+bufferPos+8,len|(type<<24));
         bufferPos+=12;
         if (::Type::hasSubType(type)) {
            writeUint32(buffer+bufferPos,subType);
            bufferPos+=4;
         }
         memcpy(buffer+bufferPos,data,pageSize-bufferPos);
         reader.rememberInfo(chainer.getPageNo(),(headerSize<<16)|(0xFFFF),hash);
         buffer=static_cast<unsigned char*>(chainer.nextPage(this));
         ++id;

         // Write all intermediate pages
         const char* dataIter=data;
         unsigned iterLen=len;
         dataIter+=pageSize-bufferPos;
         iterLen-=pageSize-bufferPos;
         while (iterLen>(pageSize-headerSize)) {
            writeUint32(buffer+12,0);
            memcpy(buffer+headerSize,dataIter,pageSize-headerSize);
            buffer=static_cast<unsigned char*>(chainer.nextPage(this));
            dataIter+=pageSize-headerSize;
            iterLen-=pageSize-headerSize;
         }

         // Write the last page
         if (iterLen) {
            writeUint32(buffer+12,0);
            memcpy(buffer+headerSize,dataIter,iterLen);
            for (unsigned index=headerSize+iterLen;index<pageSize;index++)
               buffer[index]=0;
            buffer=static_cast<unsigned char*>(chainer.nextPage(this));
         }
         bufferPos=headerSize;

         continue;
      }

      // Hash the current string...
      unsigned hash=Hash::hash(data,len,(type<<24)^subType);

      // ...store it...
      if (!buffer)
         buffer=static_cast<unsigned char*>(chainer.nextPage(this));
      unsigned ofs=bufferPos;
      writeUint32(buffer+bufferPos,id); bufferPos+=4;
      writeUint32(buffer+bufferPos,hash); bufferPos+=4;
      writeUint32(buffer+bufferPos,len|(type<<24)); bufferPos+=4;
      if (::Type::hasSubType(type)) {
         writeUint32(buffer+bufferPos,subType);
         bufferPos+=4;
      }
      for (unsigned index=0;index<len;index++)
         buffer[bufferPos++]=data[index];
      ++bufferCount;

      // ...and remember its position
      reader.rememberInfo(chainer.getPageNo(),(ofs<<16)|(len),hash);
      ++id;
   }
   // Flush the last page
   if (buffer) {
      for (unsigned index=bufferPos;index<pageSize;index++)
         buffer[index]=0;
      writeUint32(buffer+12,bufferCount);
   }
   chainer.finish();

   // Remember start and count
   tableStart=chainer.getFirstPageNo();
   setSegmentData(slotTableStart,tableStart);
   nextId=id;
   setSegmentData(slotNextId,nextId);
}
//---------------------------------------------------------------------------
void DictionarySegment::loadStringMappings(IdSource& reader)
   // Load the string mappings (must be in id order)
{
   // Prepare the buffer
   unsigned char buffer[BufferReference::pageSize]={0};
   unsigned bufferPos=8+8;
   unsigned firstPage=0;

   // Dump the page number
   unsigned stringPage,stringOfsLen;
   while (reader.next(stringPage,stringOfsLen)) {
      // Is the page full?
      if (bufferPos==BufferReference::pageSize) {
         BufferReferenceModified currentPage;
         allocPage(currentPage);
         if (!firstPage) firstPage=currentPage.getPageNo();
         memcpy(currentPage.getPage(),buffer,BufferReference::pageSize);
         currentPage.unfixWithoutRecovery();
         bufferPos=8;
      }
      // Write the page number and ofs/len
      writeUint32(buffer+bufferPos,stringPage); bufferPos+=4;
      writeUint32(buffer+bufferPos,stringOfsLen); bufferPos+=4;
   }
   // Write the last page
   for (unsigned index=bufferPos;index<BufferReference::pageSize;index++)
      buffer[index]=0;
   BufferReferenceModified currentPage;
   allocPage(currentPage);
   if (!firstPage) firstPage=currentPage.getPageNo();
   memcpy(currentPage.getPage(),buffer,BufferReference::pageSize);
   unsigned lastPage=currentPage.getPageNo();
   currentPage.unfixWithoutRecovery();

   // Update the head
   currentPage=modifyExclusive(firstPage);
   writeUint32(static_cast<unsigned char*>(currentPage.getPage())+8+4,lastPage-firstPage+1);
   currentPage.unfixWithoutRecovery();

   // And remember the start
   mappings.push_back(pair<unsigned,unsigned>(firstPage,lastPage-firstPage+1));
   setSegmentData(slotMappingStart,firstPage);
}
//---------------------------------------------------------------------------
void DictionarySegment::loadStringHashes(HashSource& reader)
   // Write the string index
{
   HashIndex::LeafEntrySource source(reader);
   HashIndex(*this).performBulkload(source);
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
struct EntryInfo {
   /// The info
   unsigned page,ofsLen,hash;

   /// Constructor
   EntryInfo(unsigned page,unsigned ofsLen,unsigned hash) : page(page),ofsLen(ofsLen),hash(hash) {}
};
//---------------------------------------------------------------------------
struct SortByHash { bool operator()(const EntryInfo& a,const EntryInfo& b) const { return a.hash<b.hash; } };
//---------------------------------------------------------------------------
class EntryInfoReader {
   private:
   /// Iterator
   vector<EntryInfo>::const_iterator iter,limit;

   public:
   /// Constructor
   EntryInfoReader(vector<EntryInfo>::const_iterator iter,vector<EntryInfo>::const_iterator limit) : iter(iter),limit(limit) {}

   /// The next entry
   bool next(DictionarySegment::HashIndex::LeafEntry& e);
   /// Mark as conflict
   void markAsConflict() {}
};
//---------------------------------------------------------------------------
bool EntryInfoReader::next(DictionarySegment::HashIndex::LeafEntry& e)
   // The next entry
{
   if (iter!=limit) {
      e.hash=(*iter).hash;
      e.page=(*iter).page;
      ++iter;
      return true;
   } else return false;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
void DictionarySegment::appendLiterals(const std::vector<Literal>& literals)
   // Load new strings into the dictionary
{
   static const unsigned pageSize = BufferReference::pageSize;

   // Prepare the buffer
   const unsigned headerSize = 16; // LSN+next+count
   DatabaseBuilder::PageChainer chainer(8);
   unsigned char* buffer=0;
   unsigned bufferPos=headerSize,bufferCount=0;

   // Read the strings
   unsigned id=nextId,oldNextId=nextId;
   vector<EntryInfo> info;
   info.reserve(literals.size());
   for (vector<Literal>::const_iterator iter=literals.begin(),limit=literals.end();iter!=limit;++iter) {
      const char* data=(*iter).str.c_str();
      unsigned len=(*iter).str.size();
      // Is the page full?
      if ((bufferPos+12+len+(::Type::hasSubType((*iter).type)?4:0)>pageSize)&&(bufferCount)) {
         for (unsigned index=bufferPos;index<pageSize;index++)
            buffer[index]=0;
         writeUint32(buffer+12,bufferCount);
         buffer=static_cast<unsigned char*>(chainer.nextPage(this));
         bufferPos=headerSize; bufferCount=0;
      }
      // Check the len, handle an overlong string
      if (bufferPos+12+len+(::Type::hasSubType((*iter).type)?4:0)>pageSize) {
         // Write the first page
         unsigned hash=Hash::hash(data,len,(((*iter).type)<<24)^((*iter).subType));
         writeUint32(buffer+12,1);
         writeUint32(buffer+bufferPos,id);
         writeUint32(buffer+bufferPos+4,hash);
         writeUint32(buffer+bufferPos+8,len|((*iter).type<<24));
         bufferPos+=12;
         if (::Type::hasSubType((*iter).type)) {
            writeUint32(buffer+bufferPos,(*iter).subType);
            bufferPos+=4;
         }
         memcpy(buffer+bufferPos,data,pageSize-bufferPos);
         info.push_back(EntryInfo(chainer.getPageNo(),(headerSize<<16)|(0xFFFF),hash));
         buffer=static_cast<unsigned char*>(chainer.nextPage(this));
         ++id;

         // Write all intermediate pages
         const char* dataIter=data;
         unsigned iterLen=len;
         dataIter+=pageSize-bufferPos;
         iterLen-=pageSize-bufferPos;
         while (iterLen>(pageSize-headerSize)) {
            writeUint32(buffer+12,0);
            memcpy(buffer+headerSize,dataIter,pageSize-headerSize);
            buffer=static_cast<unsigned char*>(chainer.nextPage(this));
            dataIter+=pageSize-headerSize;
            iterLen-=pageSize-headerSize;
         }

         // Write the last page
         if (iterLen) {
            writeUint32(buffer+12,0);
            memcpy(buffer+headerSize,dataIter,iterLen);
            for (unsigned index=headerSize+iterLen;index<pageSize;index++)
               buffer[index]=0;
            buffer=static_cast<unsigned char*>(chainer.nextPage(this));
         }
         bufferPos=headerSize;

         continue;
      }

      // Hash the current string...
      unsigned hash=Hash::hash(data,len,(((*iter).type)<<24)^((*iter).subType));

      // ...store it...
      if (!buffer)
         buffer=static_cast<unsigned char*>(chainer.nextPage(this));
      unsigned ofs=bufferPos;
      writeUint32(buffer+bufferPos,id); bufferPos+=4;
      writeUint32(buffer+bufferPos,hash); bufferPos+=4;
      writeUint32(buffer+bufferPos,len|(((*iter).type)<<24)); bufferPos+=4;
      if (::Type::hasSubType((*iter).type)) {
         writeUint32(buffer+bufferPos,(*iter).subType);
         bufferPos+=4;
      }
      for (unsigned index=0;index<len;index++)
         buffer[bufferPos++]=data[index];
      ++bufferCount;

      // ...and remember its position
      info.push_back(EntryInfo(chainer.getPageNo(),(ofs<<16)|(len),hash));
      ++id;
   }
   // Flush the last page
   if (buffer) {
      for (unsigned index=bufferPos;index<pageSize;index++)
         buffer[index]=0;
      writeUint32(buffer+12,bufferCount);
   }
   // XXX link to tableStart (or even better put everything behind the existing chain)
   chainer.finish();

   // Remember start and count
   tableStart=chainer.getFirstPageNo();
   setSegmentData(slotTableStart,tableStart);
   nextId=id;
   setSegmentData(slotNextId,nextId);

   // Store ids in the existing mapping if possible
   refreshMapping();
   unsigned mappingSize=0,mappingIds=0;
   for (vector<pair<unsigned,unsigned> >::const_iterator iter=mappings.begin(),limit=mappings.end();iter!=limit;++iter) {
      mappingSize+=(*iter).second;
      mappingIds+=entriesOnFirstMappingPage+(((*iter).second-1)*entriesPerMappingPage);
   }
   if (oldNextId<mappingIds) {
      unsigned lastStart=mappings.back().first;
      unsigned lastIdStart=mappingIds-(entriesOnFirstMappingPage+((mappings.back().second-1)*entriesPerMappingPage));
      unsigned page,slot;
      if ((oldNextId-lastIdStart)>=entriesOnFirstMappingPage) {
         unsigned relId=oldNextId-lastIdStart-entriesOnFirstMappingPage;
         page=lastStart+1+(relId/entriesPerMappingPage);
         slot=relId%entriesPerMappingPage;
      } else {
         page=lastStart;
         slot=(oldNextId-lastIdStart)+1;
      }
      for (unsigned index=0,limit=min(mappingIds-oldNextId,nextId-oldNextId);index<limit;) {
         BufferReferenceModified ref(modifyExclusive(page));
         unsigned char newEntries[BufferReference::pageSize];
         unsigned chunk=min(entriesPerMappingPage-slot,limit-index);
         for (unsigned index2=0;index2<chunk;++index2) {
            Segment::writeUint32Aligned(newEntries+(8*index2),info[index+index2].page);
            Segment::writeUint32Aligned(newEntries+(8*index2)+4,info[index+index2].ofsLen);
         }
         UpdateMapping(slot,LogData(static_cast<const unsigned char*>(ref.getPage())+8+(8*slot),8*chunk),LogData(newEntries,8*chunk)).apply(ref);
         index+=chunk;
         slot=0;
      }
   }

   // Create a new mapping for additional entries if necessary
   if (nextId>mappingIds) {
      // Compute the required number of pages
      unsigned extraIds=(nextId-mappingIds),requiredPages;
      if (extraIds>entriesOnFirstMappingPage)
         requiredPages=1+(extraIds-entriesOnFirstMappingPage+entriesPerMappingPage-1)/entriesPerMappingPage; else
         requiredPages=1;

      // Allocate a range
      unsigned start,len;
      allocPageRange(requiredPages,max(requiredPages,mappingSize/2),start,len);

      // Write the pages
      unsigned slot=1;
      for (unsigned index=mappingIds-oldNextId,limit=nextId-oldNextId,page=start;index<limit;++page) {
         BufferReferenceModified ref(modifyExclusive(page));
         unsigned char newEntries[BufferReference::pageSize];
         unsigned chunk=min(entriesPerMappingPage-slot,limit-index);
         if (chunk<(entriesPerMappingPage-slot))
            memset(newEntries,0,sizeof(newEntries));
         if (slot==1) {
            Segment::writeUint32Aligned(newEntries,0);
            Segment::writeUint32Aligned(newEntries,len);
         }
         for (unsigned index2=0;index2<chunk;++index2) {
            Segment::writeUint32Aligned(newEntries+(8*index2),info[index+index2].page);
            Segment::writeUint32Aligned(newEntries+(8*index2)+4,info[index+index2].ofsLen);
         }
         UpdateMapping(0,LogData(static_cast<const unsigned char*>(ref.getPage())+8,BufferReference::pageSize-8),LogData(newEntries,BufferReference::pageSize-8)).apply(ref);
         index+=chunk;
         slot=0;
      }

      // Update the chain
      {
         BufferReferenceModified ref(modifyExclusive(mappings.back().first));
         unsigned char newHeader[8];
         memcpy(newHeader,static_cast<const unsigned char*>(ref.getPage())+8,8);
         Segment::writeUint32Aligned(newHeader,start);
         UpdateMapping(0,LogData(static_cast<const unsigned char*>(ref.getPage())+8,8),LogData(newHeader,8)).apply(ref);
      }
      mappings.push_back(pair<unsigned,unsigned>(start,len));
   }

   // Load hash->pos mapping
   sort(info.begin(),info.end(),SortByHash());
   EntryInfoReader reader(info.begin(),info.end());
   HashIndex(*this).performUpdate(reader);
}
//---------------------------------------------------------------------------
