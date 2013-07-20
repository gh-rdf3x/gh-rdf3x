#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <set>
#include "infra/osdep/MemoryMappedFile.hpp"
#include "../rdf3xload/TempFile.hpp"
#include "../rdf3xload/Sorter.hpp"
#include <unistd.h>
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
/// A RDF triple
struct Triple {
   /// The values as IDs
   unsigned subject,predicate,object;
};
//---------------------------------------------------------------------------
/// Order a RDF triple lexicographically
struct OrderTripleByPredicate {
   bool operator()(const Triple& a,const Triple& b) const {
      return (a.predicate<b.predicate)||
             ((a.predicate==b.predicate)&&((a.subject<b.subject)||
             ((a.subject==b.subject)&&(a.object<b.object))));
   }
};
//---------------------------------------------------------------------------
bool readFacts(TempFile& out,const char* fileName)
   // Read the facts table
{
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   while (true) {
      Triple t;
      in >> t.subject >> t.predicate >> t.object;
      if (!in.good()) break;
      out.write(sizeof(t),reinterpret_cast<char*>(&t));
   }

   return true;
}
//---------------------------------------------------------------------------
static const char* skipTriple(const char* reader)
   // Skip a materialized triple
{
   return reader+sizeof(Triple);
}
//---------------------------------------------------------------------------
static int compareTriple(const char* left,const char* right)
   // Sort by predicate,subject,object
{
   const Triple& l=*reinterpret_cast<const Triple*>(left);
   const Triple& r=*reinterpret_cast<const Triple*>(right);

   if (l.predicate<r.predicate) return -1;
   if (l.predicate>r.predicate) return 1;
   if (l.subject<r.subject) return -1;
   if (l.subject>r.subject) return 1;
   if (l.object<r.object) return -1;
   if (l.object>r.object) return 1;
   return 0;
}
//---------------------------------------------------------------------------
void dumpFacts(ofstream& out,TempFile& rawFacts,const string& name)
   // Dump the facts
{
   // Sort the facts
   TempFile sortedFacts(rawFacts.getBaseFile());
   Sorter::sort(rawFacts,sortedFacts,skipTriple,compareTriple,true);

   // Dump the facts
   {
      unlink("facts.sql");
      ofstream out("facts.sql");
      MemoryMappedFile in;
      in.open(sortedFacts.getFile().c_str());
      const Triple* triplesBegin=reinterpret_cast<const Triple*>(in.getBegin());
      const Triple* triplesEnd=reinterpret_cast<const Triple*>(in.getEnd());
      for (const Triple* iter=triplesBegin,*limit=triplesEnd;iter!=limit;++iter)
         out << (*iter).subject << "\t" << (*iter).predicate << "\t" << (*iter).object << std::endl;
   }

   // And write the copy statement
   out << "drop schema if exists " << name << " cascade;" << endl;
   out << "create schema " << name << ";" << endl;
   out << "create table " << name << ".facts(subject int not null, predicate int not null, object int not null);" << endl;
   out << "copy " << name << ".facts from 'facts.sql';" << endl;

   // Create indices
   out << "create index facts_spo on " << name << ".facts (subject, predicate, object);" << endl;
   out << "create index facts_pso on " << name << ".facts (predicate, subject, object);" << endl;
   out << "create index facts_pos on " << name << ".facts (predicate, object, subject);" << endl;
}
//---------------------------------------------------------------------------
string escapeCopy(const string& s)
   // Escape an SQL string
{
   string result;
   for (string::const_iterator iter=s.begin(),limit=s.end();iter!=limit;++iter) {
      char c=(*iter);
      switch (c) {
         case '\\': result+="\\\\"; break;
         case '\"': result+="\\\""; break;
         case '\'': result+="\\\'"; break;
         case '\t': result+="\\\t"; break;
         case '\n': result+="\\\n"; break;
         case '\r': result+="\\\r"; break;
         case '\0': result+="\\x00"; break;
         default:
            /* if (c<' ') {
               result+='\\';
               result+=c;
            } else */ result+=c;
      }
   }
   return result;
}
//---------------------------------------------------------------------------
static ifstream& getEscapedLine(ifstream& in,string& s)
   // Read a line interpreting escapes
{
   if (!in) return in;
   s.resize(0);
   while (in) {
      char c=in.get();
      if ((c=='\n')||(c=='\r')) break;
      if (c=='\\') {
         c=in.get();
         switch (c) {
             case 'n': s+='\n'; break;
             case 'r': s+='\r'; break;
             case '\\': s+='\\'; break;
             case 'x': {
                unsigned h,l;
                c=in.get();
                if ((c>='0')&&(c<='9')) h=c-'0'; else
                if ((c>='A')&&(c<='F')) h=c-'A'+10; else
                if ((c>='a')&&(c<='f')) h=c-'a'+10; else h=0;
                c=in.get();
                if ((c>='0')&&(c<='9')) l=c-'0'; else
                if ((c>='A')&&(c<='F')) l=c-'A'+10; else
                if ((c>='a')&&(c<='f')) l=c-'a'+10; else l=0;
                c=static_cast<char>((h<<4)|l);
                s+=c;
                } break;
             default: s+='\\'; s+=c; break;
         }
      } else s+=c;
   }
   return in;
}
//---------------------------------------------------------------------------
bool readAndStoreStrings(ofstream& out,const string& name,const char* fileName)
   // Read the facts table and store it in the database
{
   // Read the strings once to find the maximum string len
   unsigned maxLen=4000;
   {
      ifstream in(fileName);
      if (!in.is_open()) {
         cout << "unable to open " << fileName << endl;
         return false;
      }
      string s;
      while (in) {
         if (!getEscapedLine(in,s)) break;
         unsigned l=s.length();
         if (l>maxLen) maxLen=l;
      }
      if (maxLen%100) maxLen+=100-(maxLen%100);
   }

   // Now open the strings again
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   // Prepare the strings table
   out << "create table " << name << ".strings(id int not null primary key, value varchar(" << maxLen << ") not null);" << endl;

   // Scan the strings and dump them
   {
      unlink("strings.sql");
      ofstream out("strings.sql");
      string s;
      while (true) {
         unsigned id,type,subType;
         in >> id >> type >> subType;
         in.get();
         if (!getEscapedLine(in,s)) break;

         // Store the string
         out << id << "\t" << escapeCopy(s) << endl;
      }
   }

   // Add the copy statement
   out << "copy " << name << ".strings from 'strings.sql';" << endl;

   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=4) {
      cout << "usage: " << argv[0] << " <facts> <strings> <name>" << endl;
      return 1;
   }

   // Output
   ofstream out("commands.sql");

   // Process the facts
   {
      // Read the facts table
      cout << "Reading the facts table..." << endl;
      TempFile rawFacts("commands.sql");
      if (!readFacts(rawFacts,argv[1]))
         return 1;

      // Write them to the database
      dumpFacts(out,rawFacts,argv[3]);
   }

   // Process the strings
   {
      // Read the strings table
      cout << "Reading the strings table..." << endl;
      if (!readAndStoreStrings(out,argv[3],argv[2]))
         return 1;
   }
}
//---------------------------------------------------------------------------
