#include "rts/database/DatabaseBuilder.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "infra/osdep/MemoryMappedFile.hpp"
#include "../rdf3xload/Sorter.hpp"
#include "../rdf3xload/TempFile.hpp"
#include <iostream>
#include <map>
#include <set>
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
bool smallAddressSpace()
   // Is the address space too small?
{
   return sizeof(void*)<8;
}
//---------------------------------------------------------------------------
static const char* skipCounts(const char* ptr) { return TempFile::skipId(TempFile::skipId(TempFile::skipId(TempFile::skipId(ptr)))); }
//---------------------------------------------------------------------------
static int cmpCounts(const char* a,const char* b)
   // Order two count entries
{
   uint64_t ida,sa,pa,oa,idb,sb,pb,ob;
   TempFile::readId(TempFile::readId(TempFile::readId(TempFile::readId(a,ida),sa),pa),oa);
   TempFile::readId(TempFile::readId(TempFile::readId(TempFile::readId(b,idb),sb),pb),ob);

   if (pa>pb) return -1;
   if (pa<pb) return 1;
   if (sa>sb) return -1;
   if (sa<sb) return 1;
   if (oa>ob) return -1;
   if (oa<ob) return 1;
   if (ida<idb) return -1;
   if (ida>idb) return 1;
   return 0;
}
//---------------------------------------------------------------------------
static const char* skipIdMap(const char* ptr) { return TempFile::skipId(TempFile::skipId(ptr)); }
//---------------------------------------------------------------------------
static int cmpIdMap(const char* a,const char* b)
   // Order two id map entries
{
   uint64_t olda,newa,oldb,newb;
   TempFile::readId(TempFile::readId(a,olda),newa);
   TempFile::readId(TempFile::readId(b,oldb),newb);

   if (olda<oldb) return -1;
   if (olda>oldb) return 1;
   if (newa<newb) return -1;
   if (newa>newb) return 1;
   return 0;
}
//---------------------------------------------------------------------------
static void reorderIds(Database& db,TempFile& idMap,TempFile& dictionary)
   // Reorganizing the ids
{
   cerr << "Reordering the id space..." << endl;

   // Collect all subtypes
   map<unsigned,unsigned> subTypes;
   set<unsigned> observedSubTypes;
   {
      DictionarySegment& dict=db.getDictionary();
      for (unsigned index=0,limit=dict.getNextId();index<limit;++index) {
         const char* start,*stop; Type::ID type; unsigned subType;
         if (dict.lookupById(index,start,stop,type,subType)&&Type::hasSubType(type))
            subTypes[subType];
      }
   }


   // Scan all aggregated indices at once
   TempFile counts(idMap.getBaseFile());
   {
      FullyAggregatedFactsSegment::Scan scanS,scanO,scanP;
      bool doneS=!scanS.first(db.getFullyAggregatedFacts(Database::Order_Subject_Predicate_Object));
      bool doneP=!scanP.first(db.getFullyAggregatedFacts(Database::Order_Predicate_Subject_Object));
      bool doneO=!scanO.first(db.getFullyAggregatedFacts(Database::Order_Object_Subject_Predicate));
      for (unsigned id=0;(!doneS)||(!doneP)||(!doneO);++id) {
         unsigned s,p,o;
         if (doneS||(scanS.getValue1()>id)) {
            s=0;
         } else {
            s=scanS.getCount();
            doneS=!scanS.next();
         }
         if (doneP||(scanP.getValue1()>id)) {
            p=0;
         } else {
            p=scanP.getCount();
            doneP=!scanP.next();
         }
         if (doneO||(scanO.getValue1()>id)) {
            o=0;
         } else {
            o=scanO.getCount();
            doneO=!scanO.next();
         }
         // Put subtypes early
         if (subTypes.count(id)) {
            observedSubTypes.insert(id);
            o=(~0u)-1;
         }
         counts.writeId(id);
         counts.writeId(s);
         counts.writeId(p);
         counts.writeId(o);
      }
   }

   // Write additional entries for subtypes (if necessary)
   for (map<unsigned,unsigned>::const_iterator iter=subTypes.begin(),limit=subTypes.end();iter!=limit;++iter)
      if (!observedSubTypes.count((*iter).first)) {
         counts.writeId((*iter).first);
         counts.writeId(0);
         counts.writeId(0);
         counts.writeId((~0u)-1);
      }

   // Sort all entries
   TempFile sortedCounts(idMap.getBaseFile());
   Sorter::sort(counts,sortedCounts,skipCounts,cmpCounts);
   counts.discard();

   // Compute new ids
   TempFile newIds(idMap.getBaseFile());
   {
      MemoryMappedFile in;
      if (!in.open(sortedCounts.getFile().c_str()))
         throw;
      DictionarySegment& dict=db.getDictionary();
      unsigned newId=0;
      for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;++newId) {
         uint64_t id,s,p,o;
         iter=TempFile::readId(TempFile::readId(TempFile::readId(TempFile::readId(iter,id),s),p),o);
         // Write the new id
         newIds.writeId(id);
         newIds.writeId(newId);
         if (subTypes.count(id))
            subTypes[id]=newId;

         // And produce the new dictionary entry
         const char* start,*stop; Type::ID type; unsigned subType;
         if (dict.lookupById(id,start,stop,type,subType)) {
            dictionary.writeString(stop-start,start);
            if (Type::hasSubType(type))
               dictionary.writeId(static_cast<uint64_t>(type)|(static_cast<uint64_t>(subTypes[subType])<<8)); else
               dictionary.writeId(type);

         } else {
            // Cannot happen!
            dictionary.writeString(0,0);
            dictionary.writeId(0);
         }
      }
   }

   // Sort the id map
   Sorter::sort(newIds,idMap,skipIdMap,cmpIdMap);

   dictionary.close();
}
//---------------------------------------------------------------------------
static const char* skipIdIdId(const char* reader)
   // Skip a materialized id/id/id
{
   return TempFile::skipId(TempFile::skipId(TempFile::skipId(reader)));
}
//---------------------------------------------------------------------------
static int compareValue(const char* left,const char* right)
   // Sort by integer value
{
   uint64_t leftId,rightId;
   TempFile::readId(left,leftId);
   TempFile::readId(right,rightId);
   if (leftId<rightId)
      return -1;
   if (leftId>rightId)
      return 1;
   return 0;
}
//---------------------------------------------------------------------------
static inline int cmpValue(uint64_t l,uint64_t r) { return (l<r)?-1:((l>r)?1:0); }
//---------------------------------------------------------------------------
static inline int cmpTriples(uint64_t l1,uint64_t l2,uint64_t l3,uint64_t r1,uint64_t r2,uint64_t r3)
   // Compar two triples
{
   int c=cmpValue(l1,r1);
   if (c) return c;
   c=cmpValue(l2,r2);
   if (c) return c;
   return cmpValue(l3,r3);
}
//---------------------------------------------------------------------------
static inline void loadTriple(const char* data,uint64_t& v1,uint64_t& v2,uint64_t& v3)
   // Load a triple
{
   TempFile::readId(TempFile::readId(TempFile::readId(data,v1),v2),v3);
}
//---------------------------------------------------------------------------
static int compare123(const char* left,const char* right)
   // Sort by id
{
   uint64_t l1,l2,l3,r1,r2,r3;
   loadTriple(left,l1,l2,l3);
   loadTriple(right,r1,r2,r3);

   return cmpTriples(l1,l2,l3,r1,r2,r3);
}
//---------------------------------------------------------------------------
static int compare132(const char* left,const char* right)
   // Sort by id
{
   uint64_t l1,l2,l3,r1,r2,r3;
   loadTriple(left,l1,l2,l3);
   loadTriple(right,r1,r2,r3);

   return cmpTriples(l1,l3,l2,r1,r3,r2);
}
//---------------------------------------------------------------------------
static int compare213(const char* left,const char* right)
   // Sort by id
{
   uint64_t l1,l2,l3,r1,r2,r3;
   loadTriple(left,l1,l2,l3);
   loadTriple(right,r1,r2,r3);

   return cmpTriples(l2,l1,l3,r2,r1,r3);
}
//---------------------------------------------------------------------------
static int compare231(const char* left,const char* right)
   // Sort by id
{
   uint64_t l1,l2,l3,r1,r2,r3;
   loadTriple(left,l1,l2,l3);
   loadTriple(right,r1,r2,r3);

   return cmpTriples(l2,l3,l1,r2,r3,r1);
}
//---------------------------------------------------------------------------
static int compare312(const char* left,const char* right)
   // Sort by id
{
   uint64_t l1,l2,l3,r1,r2,r3;
   loadTriple(left,l1,l2,l3);
   loadTriple(right,r1,r2,r3);

   return cmpTriples(l3,l1,l2,r3,r1,r2);
}
//---------------------------------------------------------------------------
static int compare321(const char* left,const char* right)
   // Sort by id
{
   uint64_t l1,l2,l3,r1,r2,r3;
   loadTriple(left,l1,l2,l3);
   loadTriple(right,r1,r2,r3);

   return cmpTriples(l3,l2,l1,r3,r2,r1);
}
//---------------------------------------------------------------------------
static void remapFacts(Database& db,const TempFile& idMap,TempFile& facts)
   // Remap the facts
{
   MemoryMappedFile map;
   if (!map.open(idMap.getFile().c_str()))
      throw;

   // Resolve the subject
   TempFile subjectResolved(idMap.getBaseFile());
   {
      FactsSegment::Scan scan;
      uint64_t from=0,to=0;
      const char* reader=map.getBegin();
      if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object))) do {
         uint64_t subject=scan.getValue1(),predicate=scan.getValue2(),object=scan.getValue3();
         while (from<subject)
            reader=TempFile::readId(TempFile::readId(reader,from),to);
         subjectResolved.writeId(predicate);
         subjectResolved.writeId(object);
         subjectResolved.writeId(to);
      } while (scan.next());
   }

   // Sort by predicate
   TempFile sortedByPredicate(idMap.getBaseFile());
   Sorter::sort(subjectResolved,sortedByPredicate,skipIdIdId,compareValue);
   subjectResolved.discard();

   // Resolve the predicate
   TempFile predicateResolved(idMap.getBaseFile());
   {
      MemoryMappedFile in;
      if (!in.open(sortedByPredicate.getFile().c_str()))
         throw;
      uint64_t from=0,to=0;
      const char* reader=map.getBegin();
      for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
         uint64_t subject,predicate,object;
         iter=TempFile::readId(iter,predicate);
         iter=TempFile::readId(iter,object);
         iter=TempFile::readId(iter,subject);
         while (from<predicate)
            reader=TempFile::readId(TempFile::readId(reader,from),to);
         predicateResolved.writeId(object);
         predicateResolved.writeId(subject);
         predicateResolved.writeId(to);
      }
   }
   sortedByPredicate.discard();

   // Sort by object
   TempFile sortedByObject(idMap.getBaseFile());
   Sorter::sort(predicateResolved,sortedByObject,skipIdIdId,compareValue);
   predicateResolved.discard();

   // Resolve the object
   TempFile objectResolved(idMap.getBaseFile());
   {
      MemoryMappedFile in;
      if (!in.open(sortedByObject.getFile().c_str()))
         throw;
      uint64_t from=0,to=0;
      const char* reader=map.getBegin();
      for (const char* iter=in.getBegin(),*limit=in.getEnd();iter!=limit;) {
         uint64_t subject,predicate,object;
         iter=TempFile::readId(iter,object);
         iter=TempFile::readId(iter,subject);
         iter=TempFile::readId(iter,predicate);
         while (from<object)
            reader=TempFile::readId(TempFile::readId(reader,from),to);
         objectResolved.writeId(subject);
         objectResolved.writeId(predicate);
         objectResolved.writeId(to);
      }
   }
   sortedByObject.discard();

   // Final sort by subject, predicate, object, eliminaing duplicates
   Sorter::sort(objectResolved,facts,skipIdIdId,compare123,true);
}
//---------------------------------------------------------------------------
namespace {
class FactsLoader : public DatabaseBuilder::FactsReader {
   protected:
   /// Map to the input
   MemoryMappedFile in;
   /// Points into the data
   const char* iter,*limit;

   /// Read an id
   static const char* readId(const char* d,unsigned& v) { uint64_t x; d=TempFile::readId(d,x); v=x; return d; }

   public:
   /// Constructor
   FactsLoader(TempFile& file) { file.close(); if (!in.open(file.getFile().c_str())) throw; iter=in.getBegin(); limit=in.getEnd(); }

   /// Reset
   void reset() { iter=in.getBegin(); }
};
class Load123 : public FactsLoader { public: Load123(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v1),v2),v3); return true; } else return false; } };
class Load132 : public FactsLoader { public: Load132(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v1),v3),v2); return true; } else return false; } };
class Load213 : public FactsLoader { public: Load213(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v2),v1),v3); return true; } else return false; } };
class Load231 : public FactsLoader { public: Load231(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v3),v1),v2); return true; } else return false; } };
class Load312 : public FactsLoader { public: Load312(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v2),v3),v1); return true; } else return false; } };
class Load321 : public FactsLoader { public: Load321(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v3),v2),v1); return true; } else return false; } };
}
//---------------------------------------------------------------------------
static void loadFacts(DatabaseBuilder& builder,TempFile& facts)
   // Load the facts
{
   cout << "Loading triples..." << endl;
   // Order 0
   {
      Load123 loader(facts);
      builder.loadFacts(0,loader);
   }
   // Order 1
   {
      TempFile sorted(facts.getBaseFile());
      Sorter::sort(facts,sorted,skipIdIdId,compare132);
      Load132 loader(sorted);
      builder.loadFacts(1,loader);
   }
   // Order 2
   {
      TempFile sorted(facts.getBaseFile());
      Sorter::sort(facts,sorted,skipIdIdId,compare321);
      Load321 loader(sorted);
      builder.loadFacts(2,loader);
   }
   // Order 3
   {
      TempFile sorted(facts.getBaseFile());
      Sorter::sort(facts,sorted,skipIdIdId,compare312);
      Load312 loader(sorted);
      builder.loadFacts(3,loader);
   }
   // Order 4
   {
      TempFile sorted(facts.getBaseFile());
      Sorter::sort(facts,sorted,skipIdIdId,compare213);
      Load213 loader(sorted);
      builder.loadFacts(4,loader);
   }
   // Order 5
   {
      TempFile sorted(facts.getBaseFile());
      Sorter::sort(facts,sorted,skipIdIdId,compare231);
      Load231 loader(sorted);
      builder.loadFacts(5,loader);
   }
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// Load the strings from the table
class StringReader : public DatabaseBuilder::StringsReader {
   private:
   /// The input file
   MemoryMappedFile in;
   /// The output file
   TempFile out;
   /// Pointers to the data
   const char* iter,*limit;

   public:
   /// Constructor
   StringReader(TempFile& file) : out(file.getBaseFile()) { file.close(); if (!in.open(file.getFile().c_str())) throw; iter=in.getBegin(); limit=in.getEnd(); }

   /// Close the input
   void closeIn() { in.close(); }
   /// Get the output
   TempFile& getOut() { out.close(); return out; }

   /// Read the next entry
   bool next(unsigned& len,const char*& data,Type::ID& type,unsigned& subType);
   /// Remember string info
   void rememberInfo(unsigned page,unsigned ofs,unsigned hash);
};
//---------------------------------------------------------------------------
bool StringReader::next(unsigned& len,const char*& data,Type::ID& type,unsigned& subType)
   // Read the next entry
{
   if (iter==limit)
      return false;
   iter=TempFile::readString(iter,len,data);
   uint64_t typeInfo;
   iter=TempFile::readId(iter,typeInfo);
   type=static_cast<Type::ID>(typeInfo&0xFF);
   subType=static_cast<unsigned>(typeInfo>>8);
   return true;
}
//---------------------------------------------------------------------------
void StringReader::rememberInfo(unsigned page,unsigned ofs,unsigned hash)
   // Remember string info
{
   out.writeId(hash);
   out.writeId(page);
   out.writeId(ofs);
}
//---------------------------------------------------------------------------
/// Read the string mapping
class StringMappingReader : public DatabaseBuilder::StringInfoReader
{
   private:
   /// The input
   MemoryMappedFile in;
   /// Points into the data
   const char* iter,*limit;

   public:
   /// Constructor
   StringMappingReader(TempFile& file) { file.close(); if (!in.open(file.getFile().c_str())) throw; iter=in.getBegin(); limit=in.getEnd(); }

   /// Read the next entry
   bool next(unsigned& v1,unsigned& v2);
};
//---------------------------------------------------------------------------
bool StringMappingReader::next(unsigned& v1,unsigned& v2)
   // Read the next entry
{
   if (iter==limit)
      return false;
   uint64_t i1,i2,i3;
   iter=TempFile::readId(TempFile::readId(TempFile::readId(iter,i1),i2),i3);
   v1=i2; v2=i3;
   return true;
}
//---------------------------------------------------------------------------
/// Read the string hashes
class StringHashesReader : public DatabaseBuilder::StringInfoReader
{
   private:
   /// The input
   MemoryMappedFile in;
   /// Points into the data
   const char* iter,*limit;

   public:
   /// Constructor
   StringHashesReader(TempFile& file) { file.close(); if (!in.open(file.getFile().c_str())) throw; iter=in.getBegin(); limit=in.getEnd(); }

   /// Read the next entry
   bool next(unsigned& v1,unsigned& v2);
};
//---------------------------------------------------------------------------
bool StringHashesReader::next(unsigned& v1,unsigned& v2)
   // Read the next entry
{
   if (iter==limit)
      return false;
   uint64_t i1,i2,i3;
   iter=TempFile::readId(TempFile::readId(TempFile::readId(iter,i1),i2),i3);
   v1=i1; v2=i2;
   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
static void loadStrings(DatabaseBuilder& builder,TempFile& stringTable)
   // Load the strings
{
   cout << "Loading strings..." << endl;

   // Load the raw strings
   StringReader reader(stringTable);
   builder.loadStrings(reader);
   reader.closeIn();

   // Load the strings mappings
   {
      StringMappingReader infoReader(reader.getOut());
      builder.loadStringMappings(infoReader);
   }

   // Load the hash->page mappings
   {
      TempFile sortedByHash(stringTable.getBaseFile());
      Sorter::sort(reader.getOut(),sortedByHash,skipIdIdId,compareValue);
      StringHashesReader infoReader(sortedByHash);
      builder.loadStringHashes(infoReader);
   }
}
//---------------------------------------------------------------------------
static void loadStatistics(DatabaseBuilder& builder,TempFile& facts)
   // Compute the statistics
{
   cout << "Computing statistics..." << endl;

   TempFile tmp(facts.getBaseFile());
   tmp.close();
   builder.computeExactStatistics(tmp.getFile().c_str());
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Warn first
   if (smallAddressSpace())
      cerr << "Warning: Running RDF-3X on a 32 bit system is not supported and will fail for large data sets. Please use a 64 bit system instead!" << endl;

   // Greeting
   cerr << "RDF-3X database reorganizer" << endl
        << "(c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x" << endl;

   // Check the arguments
   if (argc!=2) {
      cerr << "usage: " << argv[0] << " <database>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   // Reorder the ids
   TempFile idMap(argv[1]),dictionary(argv[1]);
   reorderIds(db,idMap,dictionary);

   // Remapping the facts
   TempFile facts(argv[1]);
   remapFacts(db,idMap,facts);

   // Create a new database file
   db.close();
   TempFile newDB(argv[1]);
   newDB.close();
   DatabaseBuilder builder(newDB.getFile().c_str());

   // Load the facts
   loadFacts(builder,facts);
   facts.discard();

   // Load the strings
   loadStrings(builder,dictionary);
   dictionary.discard();

   // Load the statistics
   loadStatistics(builder,facts);

   // Move back
   builder.close();
   rename(newDB.getFile().c_str(),argv[1]);
}
//---------------------------------------------------------------------------
