#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <set>
#include "infra/osdep/MemoryMappedFile.hpp"
#include "../rdf3xload/TempFile.hpp"
#include "../rdf3xload/Sorter.hpp"
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
struct Triple { unsigned subject,predicate,object; };
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
void dumpFacts(ofstream& out,TempFile& rawFacts,set<unsigned>& predicates,set<unsigned>& partitionedPredicates)
   // Dump the facts
{
   // Sort the facts
   TempFile sortedFacts(rawFacts.getBaseFile());
   Sorter::sort(rawFacts,sortedFacts,skipTriple,compareTriple,true);

   // Compute the predicate statistics
   MemoryMappedFile in;
   in.open(sortedFacts.getFile().c_str());
   const Triple* triplesBegin=reinterpret_cast<const Triple*>(in.getBegin());
   const Triple* triplesEnd=reinterpret_cast<const Triple*>(in.getEnd());
   unsigned cutOff=0;
   {
      vector<unsigned> statistics;
      statistics.resize(triplesEnd[-1].predicate+1);
      for (const Triple* iter=triplesBegin,*limit=triplesEnd;iter!=limit;++iter)
         statistics[(*iter).predicate]++;
      sort(statistics.begin(),statistics.end(),greater<unsigned>());
      if (statistics.size()>1000)
         cutOff=statistics[999]; else
         cutOff=0;
   }

   // And dump them
   const Triple* lastStart=triplesBegin;
   unsigned smallCount=0;
   unsigned lastPredicate=~0u;
   bool needsBigTable=false;
   for (const Triple* iter=triplesBegin,*limit=triplesEnd;;++iter) {
      if ((iter==limit)||((*iter).predicate!=lastPredicate)) {
         if (iter!=lastStart) {
            if ((iter-lastStart)>=cutOff) {
               out << "create table p" << lastPredicate << "(subject int not null, object int not null);" << endl;
               out << "copy " << (iter-lastStart) << " records into \"p" << lastPredicate << "\" from stdin using delimiters '\\t';" << endl;
               for (;lastStart!=iter;++lastStart)
                  out << (*lastStart).subject << "\t" << (*lastStart).object << endl;
               partitionedPredicates.insert(lastPredicate);
            } else {
               smallCount+=iter-lastStart;
               lastStart=iter;
               needsBigTable=true;
            }
            predicates.insert(lastPredicate);
         }
         if (iter==limit)
            break;
         lastPredicate=(*iter).predicate;
      }
   }

   // Dump the remaining predicate into a big tible if required
   if (needsBigTable) {
      out << "create table otherpredicates(subject int not null, predicate int not null, object int not null);" << endl;
      out << "copy " << smallCount << " records into \"otherpredicates\" from stdin using delimiters '\\t';" << endl;
      lastStart=triplesBegin; lastPredicate=~0u;
      for (const Triple* iter=triplesBegin,*limit=triplesEnd;;++iter) {
         if ((iter==limit)||((*iter).predicate!=lastPredicate)) {
            if (iter!=lastStart) {
               if ((iter-lastStart)>=cutOff) {
                  lastStart=iter;
               } else {
                  for (;lastStart!=iter;++lastStart)
                     out << (*lastStart).subject << "\t" << (*lastStart).predicate << "\t" << (*lastStart).object << endl;
               }
            }
            if (iter==limit)
               break;
            lastPredicate=(*iter).predicate;
         }
      }
   }
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
bool readAndStoreStrings(ofstream& out,const char* fileName,const set<unsigned>& properties,const set<unsigned>& partitionedProperties)
   // Read the facts table and store it in the database
{
   // Read the strings once to find the maximum string len
   unsigned maxLen=4000,lineCount=0;
   {
      ifstream in(fileName);
      if (!in.is_open()) {
         cout << "unable to open " << fileName << endl;
         return false;
      }
      string s;
      while (in) {
         unsigned id,type,subType;
         in >> id >> type >> subType;
         in.get();
	 if (!getEscapedLine(in,s)) break;
         unsigned l=s.length();
         if (l>maxLen) maxLen=l;
         ++lineCount;
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
   // out << "drop table strings;" << endl
   out << "create table strings(id int not null primary key, value varchar(" << maxLen << ") not null);" << endl;

   // Prepare the filter
   static const char* filterStrings[]={"http://simile.mit.edu/2006/01/ontologies/mods3#access","http://simile.mit.edu/2006/01/ontologies/mods3#address","http://simile.mit.edu/2006/01/ontologies/mods3#affiliation","http://simile.mit.edu/2006/01/ontologies/mods3#authority","http://simile.mit.edu/2006/01/ontologies/mods3#catalogingLanguage","http://simile.mit.edu/2006/01/ontologies/mods3#changed","http://simile.mit.edu/2006/01/ontologies/mods3#code","http://simile.mit.edu/2006/01/ontologies/mods3#contents","http://simile.mit.edu/2006/01/ontologies/mods3#copyrightDate","http://simile.mit.edu/2006/01/ontologies/mods3#created","http://simile.mit.edu/2006/01/ontologies/mods3#dateCreated","http://simile.mit.edu/2006/01/ontologies/mods3#dates","http://simile.mit.edu/2006/01/ontologies/mods3#edition","http://simile.mit.edu/2006/01/ontologies/mods3#encoding","http://simile.mit.edu/2006/01/ontologies/mods3#extent","http://simile.mit.edu/2006/01/ontologies/mods3#fullName","http://simile.mit.edu/2006/01/ontologies/mods3#issuance","http://simile.mit.edu/2006/01/ontologies/mods3#language","http://simile.mit.edu/2006/01/ontologies/mods3#nonSort","http://simile.mit.edu/2006/01/ontologies/mods3#origin","http://simile.mit.edu/2006/01/ontologies/mods3#partName","http://simile.mit.edu/2006/01/ontologies/mods3#partNumber","http://simile.mit.edu/2006/01/ontologies/mods3#physicalDescription","http://simile.mit.edu/2006/01/ontologies/mods3#point","http://simile.mit.edu/2006/01/ontologies/mods3#qualifier","http://simile.mit.edu/2006/01/ontologies/mods3#records","http://simile.mit.edu/2006/01/ontologies/mods3#sub","http://www.w3.org/1999/02/22-rdf-syntax-ns#type",0};
   set<unsigned> filteredProperties;

   // Scan the strings and dump them
   vector<pair<unsigned,string> > propertyNames;
   string s;
   out << "copy " << lineCount << " records into \"strings\" from stdin using delimiters '\\t';" << endl;
   while (true) {
      unsigned id,type,subType;
      in >> id >> type >> subType;
      in.get();
      if (!getEscapedLine(in,s)) break;

      // Store the string
      out << id << "\t" << escapeCopy(s) << endl;

      // A known property?
      if (properties.count(id)) {
         propertyNames.push_back(pair<unsigned,string>(id,s));
         for (const char** iter=filterStrings;*iter;++iter)
            if (s==(*iter)) {
               if (partitionedProperties.count(id))
                  filteredProperties.insert(id);
               break;
            }
      }
   }

   // Dump the property names
   // out << "drop table propertynames;" << endl
   out << "create table propertynames (id int not null primary key, name varchar(" << maxLen << ") not null);" << endl;
   out << "copy " << propertyNames.size() << " records into \"propertynames\" from stdin using delimiters '\\t';" << endl;
   for (vector<pair<unsigned,string> >::const_iterator iter=propertyNames.begin(),limit=propertyNames.end();iter!=limit;++iter) {
      out << ((*iter).first) << "\t" << escapeCopy((*iter).second) << endl;
   }

   // Build the views
   //out << "drop view allproperties;" << endl
   out << "create view allproperties as ";
   bool first=true;
   for (vector<pair<unsigned,string> >::const_iterator iter=propertyNames.begin(),limit=propertyNames.end();iter!=limit;++iter) {
      if (!partitionedProperties.count((*iter).first))
         continue;
      if (!first)
         out << " union all";
      first=false;
      out << " (select subject," << (*iter).first << " as predicate,object from p" << (*iter).first <<")";
   }
   if (properties.size()>partitionedProperties.size())
      out << " union all (select subject, predicate, object from otherpredicates)";
   out << ";" << endl;
   //out << "drop view filteredproperties;" << endl
   if (filteredProperties.size()>2) {
      out << "create view filteredproperties as ";
      for (set<unsigned>::const_iterator iter=filteredProperties.begin(),limit=filteredProperties.end();iter!=limit;++iter) {
         if (iter!=filteredProperties.begin())
            out << " union all";
         out << " (select subject," << (*iter) << " as predicate,object from p" << (*iter) <<")";
      }
      out << ";" << endl;
   }

   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=3) {
      cout << "usage: " << argv[0] << " <facts> <strings>" << endl;
      return 1;
   }

   // Output
   ofstream out("commands.sql");

   // Process the facts
   set<unsigned> properties,partitionedProperties;
   {
      // Read the facts table
      cout << "Reading the facts table..." << endl;
      TempFile rawFacts("commands.sql");
      if (!readFacts(rawFacts,argv[1]))
         return 1;

      // Write them to the database
      dumpFacts(out,rawFacts,properties,partitionedProperties);
   }

   // Process the strings
   out.close();
   ofstream out2("commands2.sql");
   {
      // Read the strings table
      cout << "Reading the strings table..." << endl;
      if (!readAndStoreStrings(out2,argv[2],properties,partitionedProperties))
         return 1;
   }

   // Run it
   out2.close();
}
//---------------------------------------------------------------------------
