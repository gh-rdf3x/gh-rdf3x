#include "rts/database/DatabaseBuilder.hpp"
#include "infra/osdep/MemoryMappedFile.hpp"
#include "../rdf3xload/Sorter.hpp"
#include "../rdf3xload/TempFile.hpp"
#include <fstream>
#include <iostream>
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
namespace {
//---------------------------------------------------------------------------
bool readFacts(TempFile& facts,const char* fileName)
   // Read the facts table
{
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   while (true) {
      unsigned subject,predicate,object;
      in >> subject >> predicate >> object;
      if (!in.good()) break;
      facts.writeId(subject);
      facts.writeId(predicate);
      facts.writeId(object);
   }

   return true;
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
   FactsLoader(TempFile& file) { file.close(); assert(in.open(file.getFile().c_str())); iter=in.getBegin(); limit=in.getEnd(); }

   /// Reset
   void reset() { iter=in.getBegin(); }
};
class Load123 : public FactsLoader { public: Load123(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v1),v2),v3); return true; } else return false; } };
class Load132 : public FactsLoader { public: Load132(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v1),v3),v2); return true; } else return false; } };
class Load213 : public FactsLoader { public: Load213(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v2),v1),v3); return true; } else return false; } };
class Load231 : public FactsLoader { public: Load231(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v3),v1),v2); return true; } else return false; } };
class Load312 : public FactsLoader { public: Load312(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v2),v3),v1); return true; } else return false; } };
class Load321 : public FactsLoader { public: Load321(TempFile& file) : FactsLoader(file) {} bool next(unsigned& v1,unsigned& v2,unsigned& v3) { if (iter!=limit) { iter=readId(readId(readId(iter,v3),v2),v1); return true; } else return false; } };
//---------------------------------------------------------------------------
void dumpFacts(DatabaseBuilder& builder,TempFile& facts)
   // Dump all 6 orderings into the database
{
   cout << "Loading triples..." << endl;

   // Order 0
   {
      TempFile sorted(facts.getBaseFile());
      Sorter::sort(facts,sorted,skipIdIdId,compare123);
      Load123 loader(sorted);
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
/// Read strings from a file
class StringReader : public DatabaseBuilder::StringsReader {
   private:
   /// The input
   ifstream& in;
   /// The remembered data
   TempFile& stringInfo;
   /// The next id
   unsigned id;
   /// The next string
   std::string s;

   public:
   /// Constructor
   StringReader(ifstream& in,TempFile& stringInfo) : in(in),stringInfo(stringInfo),id(0) {}

   /// Read the next string
   bool next(unsigned& len,const char*& data,Type::ID& type,unsigned& subType);
   /// Remember a string position and hash
   void rememberInfo(unsigned page,unsigned ofs,unsigned hash);
};
//---------------------------------------------------------------------------
bool StringReader::next(unsigned& len,const char*& data,Type::ID& type,unsigned& subType)
   // Read the next string
{
   unsigned nextId,typeId;
   in >> nextId >> typeId >> subType;
   in.get();
   s.clear();
   while (true) {
      char c;
      if (!in.get(c)) return false;
      if ((c=='\n')||(c=='\r')) break;
      if (c=='\\') {
         if (!in.get(c)) return false;
         switch (c) {
            case '\\': s+='\\'; break;
            case 'n': s+='\n'; break;
            case 'r': s+='\r'; break;
            case 't': s+='\t'; break;
            case 'x': {
               unsigned high,low;
               if (!in.get(c)) break;
               if ((c>='0')&&(c<='9')) high=c-'0'; else
               if ((c>='A')&&(c<='F')) high=c-'A'+10; else
               if ((c>='a')&&(c<='f')) high=c-'a'+10; else
                  high=0;
               if (!in.get(c)) break;
               if ((c>='0')&&(c<='9')) low=c-'0'; else
               if ((c>='A')&&(c<='F')) low=c-'A'+10; else
               if ((c>='a')&&(c<='f')) low=c-'a'+10; else
                  low=0;
               s+=static_cast<char>((high<<4)|low);
               } break;
            default: s+=c;
         }
      } else s+=c;
   }

   type=static_cast<Type::ID>(typeId);
   if (id!=nextId) {
      cerr << "error: got id " << nextId << ", expected " << id << endl << "strings must be sorted 0,1,2,..." << endl;
      throw;
   } else {
      id++;
   }
   while (s.length()&&((s[s.length()-1]=='\r')||(s[s.length()-1]=='\n')))
      s=s.substr(0,s.length()-1);
   len=s.size();
   data=s.c_str();
   return true;
}
//---------------------------------------------------------------------------
void StringReader::rememberInfo(unsigned page,unsigned ofs,unsigned hash)
   // Remember a string position and hash
{
   stringInfo.writeId(hash);
   stringInfo.writeId(page);
   stringInfo.writeId(ofs);
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
   StringMappingReader(TempFile& file) { file.close(); assert(in.open(file.getFile().c_str())); iter=in.getBegin(); limit=in.getEnd(); }

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
   StringHashesReader(TempFile& file) { file.close(); assert(in.open(file.getFile().c_str())); iter=in.getBegin(); limit=in.getEnd(); }

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
static bool readAndPackStrings(DatabaseBuilder& builder,const char* fileName,TempFile& stringInfo)
   // Read the facts table and pack it into the output file
{
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   // Scan the strings and dump them
   {
      StringReader reader(in,stringInfo);
      builder.loadStrings(reader);
   }

   return true;
}
//---------------------------------------------------------------------------
bool buildDatabase(DatabaseBuilder& builder,const char* target,const char* factsFile,const char* stringsFile)
   // Build the initial database
{
   // Process the facts
   {
      // Read the facts table
      cout << "Reading the facts table..." << endl;
      TempFile facts(target);
      if (!readFacts(facts,factsFile))
         return false;

      // Produce the different orderings
      dumpFacts(builder,facts);
   }

   // Process the strings
   {
      // Read the strings table
      cout << "Reading the strings table..." << endl;
      TempFile stringInfo(target);
      if (!readAndPackStrings(builder,stringsFile,stringInfo))
         return 1;

      // Write the string mapping
      cout << "Writing the string mapping..." << endl;
      {
         StringMappingReader reader(stringInfo);
         builder.loadStringMappings(reader);
      }

      // Write the string index
      cout << "Writing the string index..." << endl;
      {
         TempFile sortedByHash(stringInfo.getBaseFile());
         Sorter::sort(stringInfo,sortedByHash,skipIdIdId,compareValue);
         StringHashesReader reader(sortedByHash);
         builder.loadStringHashes(reader);
      }
   }

   return true;
}
//---------------------------------------------------------------------------
bool buildDatabaseStatistics(DatabaseBuilder& builder,const char* targetName)
   // Build the database statistics
{
   cout << "Computing statistics..." << endl;
   string tmpName=targetName;
   tmpName+=".tmp";
   builder.computeExactStatistics(tmpName.c_str());
   remove(tmpName.c_str());

   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=4) {
      cout << "usage: " << argv[0] << " <facts> <strings> <target>" << endl;
      return 1;
   }

   // Build the initial database
   DatabaseBuilder builder(argv[3]);
   if (!buildDatabase(builder,argv[3],argv[1],argv[2]))
      return 1;

   // Compute the missing statistics
   if (!buildDatabaseStatistics(builder,argv[3]))
      return 1;
}
//---------------------------------------------------------------------------
