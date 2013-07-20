#include "infra/osdep/MemoryMappedFile.hpp"
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
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
static bool readStrings(MemoryMappedFile& strings,const char* file,vector<const char*>& indices)
   // Read the strings
{
   cout << "Reading the strings..." << endl;
   if (!strings.open(file)) {
      cout << "Unable to open " << file << endl;
      return false;
   }

   unsigned maxId=0;
   for (unsigned pass=0;pass<2;pass++) {
      for (const char* iter=strings.getBegin(),*limit=strings.getEnd();iter!=limit;++iter) {
         const char* start=iter;
         while ((iter!=limit)&&((*iter)!=' ')&&((*iter)!='\t')) ++iter;
         unsigned id=atoi(string(start,iter).c_str());
         if (iter!=limit) ++iter;
         while ((iter!=limit)&&((*iter)!=' ')&&((*iter)!='\t')) ++iter;
         if (iter!=limit) ++iter;
         while ((iter!=limit)&&((*iter)!=' ')&&((*iter)!='\t')) ++iter;
         if (iter!=limit) ++iter;
         start=iter;
         while ((iter!=limit)&&((*iter)!='\n'))
            ++iter;

         if (pass==0) {
            if (id>maxId)
               maxId=id;
         } else {
            indices[2*id+0]=start;
            indices[2*id+1]=iter;
         }
      }
      if (pass==0)
         indices.resize((maxId+1)*2);
   }

   return true;
}
//---------------------------------------------------------------------------
static bool findSubjects(const char* name,vector<bool>& subjects)
   // Find subjects occuring in the data
{
   cout << "Collecting all subjects..." << endl;

   unsigned maxSubject=0;
   {
      ifstream in(name);
      if (!in) {
         cout << "Unable to open " << name << endl;
         return false;
      }
      while (in) {
         unsigned subject,predicate,object;
         in >> subject >> predicate >> object;
         if (subject>maxSubject)
            maxSubject=subject;
      }
   }
   subjects.resize(maxSubject+1);
   {
      ifstream in(name);
      if (!in) {
         cout << "Unable to open " << name << endl;
         return false;
      }
      while (in) {
         unsigned subject,predicate,object;
         in >> subject >> predicate >> object;
         subjects[subject]=true;
      }
   }

   return true;
}
//---------------------------------------------------------------------------
static void escapeChar(ofstream& out,char c)
   // Escape a character forbidden in a URI
{
   const char hex[17]="0123456789ABCDEF";
   unsigned v=c&0xFF;
   out << '%'  << hex[v>>4] << hex[v&0xF];
}
//---------------------------------------------------------------------------
static void writeURI(ofstream& out,const char* start,const char* limit)
   // Write a URI
{
   for (;start<limit;++start) {
      char c=*start;
      switch (c) {
         case '[': case ']': case '<': case '>': case '#': case '(': case ')': case '%': case '\\':
            escapeChar(out,c);
            break;
         default:
            if (c<=' ')
                escapeChar(out,c); else
                out << c;
      }
   }
}
//---------------------------------------------------------------------------
static bool dumpTriples(ofstream& out,const char* name,const vector<bool>& subjects,const vector<const char*>& indices)
   // Dump the triples in N3 format
{
   cout << "Writing N3 data..." << endl;

   ifstream in(name);
   if (!in) {
      cout << "Unable to open " << name << endl;
      return false;
   }
   while (in) {
      unsigned subject,predicate,object;
      in >> subject >> predicate; if (!in) break; in >> object;
      out << "<"; writeURI(out,indices[2*subject+0],indices[2*subject+1]); out << ">";
      out << " <" << string(indices[2*predicate+0],indices[2*predicate+1]) << ">";
      if (subjects[object]) {
         out << " <"; writeURI(out,indices[2*object+0],indices[2*object+1]); out << ">";
      } else {
         out << " \"";
         for (const char* iter=indices[2*object+0],*limit=indices[2*object+1];iter!=limit;++iter) {
            char c=*iter;
            if (c=='\"')
               out << "\\\""; else
            if (c=='\\')
               out << "\\\\"; else
               out << c;
         }
         out << "\"";
      }
      out << " ." << endl;
   }
   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=4) {
      cout << "usage: " << argv[0] << " <facts> <strings> <output>" << endl;
      return 1;
   }

   // Read the strings
   MemoryMappedFile strings;
   vector<const char*> stringIndices;
   if (!readStrings(strings,argv[2],stringIndices))
      return 1;

   // Find the subjects
   vector<bool> subjects;
   if (!findSubjects(argv[1],subjects))
      return 1;

   // Output
   ofstream out(argv[3]);
   dumpTriples(out,argv[1],subjects,stringIndices);
}
//---------------------------------------------------------------------------
