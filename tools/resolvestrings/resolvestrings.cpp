#include "infra/osdep/GrowableMappedFile.hpp"
#include <iostream>
#include <fstream>
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
int main(int argc,char* argv[])
{
   if (argc!=3) {
      cerr << "usage: " << argv[0] << " <facts> <strings>" << endl;
      return 1;
   }

   // Find the maximum ids
   unsigned maxId=0;
   {
      ifstream in(argv[1]);
      if (!in.is_open()) {
         cerr << "unable to open " << argv[1] << endl;
         return 1;
      }
      cerr << "Initial facts scan" << endl;
      while (true) {
         unsigned subject,predicate,object;
         if (!(in >> subject >> predicate >> object))
            break;
         if (subject>maxId) maxId=subject;
         if (predicate>maxId) maxId=predicate;
         if (object>maxId) maxId=object;
      }
   }
   {
      ifstream in(argv[2]);
      if (!in.is_open()) {
         cerr << "unable to open " << argv[2] << endl;
         return 1;
      }
      cerr << "Initial strings scan" << endl;
      string s;
      while (true) {
         unsigned nextId;
         in >> nextId;
         in.get();
         getline(in,s);
         if (!in.good()) break;
         if (nextId>maxId) maxId=nextId;
      }
   }

   // Find all URIs
   GrowableMappedFile bitmap;
   if (!bitmap.create("resolvestrings.tmp")) {
      cerr << "unable to create map file" << endl;
      return 1;
   }
   unsigned totalSize=((maxId/16384)+1)*16384; // add padding for page effects
   if (!bitmap.growPhysically(totalSize)) {
      remove("resolvestrings.tmp");
      cerr << "unable to grow map file" << endl;
      return 1;
   }
   char* begin,*end;
   if (!bitmap.growMapping(totalSize,begin,end)) {
      remove("resolvestrings.tmp");
      cerr << "unable to map file" << endl;
   }
   {
      ifstream in(argv[1]);
      if (!in.is_open()) {
         cerr << "unable to open " << argv[1] << endl;
         return 1;
      }
      cerr << "Building URI map" << endl;
      while (true) {
         unsigned subject,predicate,object;
         if (!(in >> subject >> predicate >> object))
            break;
         begin[subject]=true;
         begin[predicate]=true;
      }
   }

   // Map the strings
   {
      ifstream in(argv[2]);
      if (!in.is_open()) {
         cerr << "unable to open " << argv[2] << endl;
         return 1;
      }
      cerr << "Resolving string types" << endl;
      string s;
      while (true) {
         unsigned nextId;
         in >> nextId;
         in.get();
         getline(in,s);
         if (!in.good()) break;

         cout << nextId;
         if (begin[nextId])
            cout << "\t0\t0\t"; else
            cout << "\t1\t0\t";

         for (string::const_iterator iter=s.begin(),limit=s.end();iter!=limit;++iter) {
            char c=*iter;
            if (c<=0) {
               unsigned v=(c&0xFF);
               const char hex[]="0123456789ABCDEF";
               cout << "\\x" << hex[v>>4] << hex[v&0xF];
            } else switch (c) {
               case '\\': cout << "\\\\"; break;
               case '\n': cout << "\\n"; break;
               case '\r': cout << "\\r"; break;
               default: cout << c; break;
            }
         }
         cout << "\n";
      }
      cout.flush();
   }
   bitmap.close();
   remove("resolvestrings.tmp");
}
//---------------------------------------------------------------------------
