#include "rts/database/Database.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include <iostream>
#include <cstdlib>
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
static void dumpEntry(DictionarySegment& dict,unsigned id)
   // Write an entry
{
   cout << id << ' ';

   const char* start,*stop;
   Type::ID type; unsigned subType;
   if (dict.lookupById(id,start,stop,type,subType)) {
      for (;start!=stop;++start)
         cout << *start;
   }

   cout << "\n";
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<2) {
      cout << "usage: " << argv[0] << " <database>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cout << "unable to open database " << argv[1] << endl;
      return 1;
   }

   // Collect all subject/object pairs and write trivial records
   FactsSegment& facts=db.getFacts(Database::Order_Subject_Object_Predicate);
   DictionarySegment& dict=db.getDictionary();
   vector<pair<unsigned,unsigned> > combinations;
   static const unsigned trivialIds = 8;
   {
      FactsSegment::Scan scan;
      unsigned current1=~0u,current2=~0u;
      bool nonTrivial=false;
      unsigned trivialRecords=0;
      if (scan.first(facts)) do {
         if ((scan.getValue1()!=current1)||(scan.getValue2()!=current2)) {
            current1=scan.getValue1(); current2=scan.getValue2(); nonTrivial=false;
         }
         if (scan.getValue3()<trivialIds) {
            trivialRecords++;
         } else if (!nonTrivial) {
            nonTrivial=true;
            combinations.push_back(pair<unsigned,unsigned>(scan.getValue1(),scan.getValue2()));
         }
      } while (scan.next());

      cout << trivialRecords << endl;
      if (scan.first(facts)) do {
         if (scan.getValue3()<trivialIds) {
            dumpEntry(dict,scan.getValue1());
            dumpEntry(dict,scan.getValue3());
            dumpEntry(dict,scan.getValue2());
         }
      } while (scan.next());
   }

   // Shuffle them
   for (unsigned index=0,limit=combinations.size();index<limit;index++)
      swap(combinations[index],combinations[index+(rand()%(limit-index))]);

   // Dump a description
   cout << combinations.size() << endl;
   for (vector<pair<unsigned,unsigned> >::const_iterator iter=combinations.begin(),limit=combinations.end();iter!=limit;++iter) {
      vector<unsigned> tags;
      FactsSegment::Scan scan;
      if (scan.first(facts,(*iter).first,(*iter).second,0)) do {
         if ((scan.getValue1()!=(*iter).first)||(scan.getValue2()!=(*iter).second))
            break;
         if (scan.getValue3()>=trivialIds)
            tags.push_back(scan.getValue3());
      } while (scan.next());

      cout << tags.size() << "\n";
      dumpEntry(dict,(*iter).first);
      dumpEntry(dict,(*iter).second);
      for (vector<unsigned>::const_iterator iter2=tags.begin(),limit2=tags.end();iter2!=limit2;++iter2)
         dumpEntry(dict,(*iter2));
   }
}
//---------------------------------------------------------------------------
