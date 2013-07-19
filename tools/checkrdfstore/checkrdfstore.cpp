#include "infra/util/Hash.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
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
struct OrderTriple {
   bool operator()(const Triple& a,const Triple& b) const {
      return (a.subject<b.subject)||
             ((a.subject==b.subject)&&((a.predicate<b.predicate)||
             ((a.predicate==b.predicate)&&(a.object<b.object))));
   }
};
//---------------------------------------------------------------------------
bool readFacts(vector<Triple>& facts,const char* fileName)
   // Read the facts table
{
   ifstream in(fileName);
   if (!in.is_open()) {
      cout << "unable to open " << fileName << endl;
      return false;
   }

   facts.clear();
   while (true) {
      Triple t;
      in >> t.subject >> t.predicate >> t.object;
      if (!in.good()) break;
      facts.push_back(t);
   }

   return true;
}
//---------------------------------------------------------------------------
bool checkFacts(Database& db,unsigned order,const vector<Triple>& facts)
   // Check the facts table
{
   FactsSegment& table=db.getFacts(static_cast<Database::DataOrder>(order));
   FactsSegment::Scan scan;

   vector<Triple>::const_iterator iter=facts.begin(),limit=facts.end();
   unsigned readTuples=0;
   if (scan.first(table)) {
      do {
         ++readTuples;
         if (iter==limit) {
            unsigned realSize=readTuples-1;
            while (scan.next())
               ++readTuples;
            std::cout << "too many tuples in facts table! Got " << readTuples << ", expected " << realSize << std::endl;
            return false;
         }
         if ((scan.getValue1()!=(*iter).subject)||(scan.getValue2()!=(*iter).predicate)||(scan.getValue3()!=(*iter).object)) {
            std::cout << "data mismatch! Expected [" << (*iter).subject << "," << (*iter).predicate << "," << (*iter).object << "], got [" << scan.getValue1() << "," << scan.getValue2() << "," << scan.getValue3() << "]" << std::endl;
            return false;
         }

         // Skip over duplicates
         unsigned s=(*iter).subject,p=(*iter).predicate,o=(*iter).object;
         while ((iter!=limit)&&((*iter).subject==s)&&((*iter).predicate==p)&&((*iter).object==o))
            ++iter;
      } while (scan.next());
   }
   if (iter!=limit) {
      // Compute the real size
      unsigned realSize=readTuples;
      while (iter!=limit) {
         ++realSize;
         unsigned s=(*iter).subject,p=(*iter).predicate,o=(*iter).object;
         while ((iter!=limit)&&((*iter).subject==s)&&((*iter).predicate==p)&&((*iter).object==o))
            ++iter;
      }
      std::cout << "too few tuples in facts table! Got " << readTuples << ", expected " << realSize << std::endl;
      return false;
   }

   return true;
}
//---------------------------------------------------------------------------
bool checkAggregatedFacts(Database& db,unsigned order,const vector<Triple>& facts)
   // Check the aggregated facts table
{
   AggregatedFactsSegment& table=db.getAggregatedFacts(static_cast<Database::DataOrder>(order));
   AggregatedFactsSegment::Scan scan;

   vector<Triple>::const_iterator iter=facts.begin(),limit=facts.end();
   if (scan.first(table)) {
      do {
         if (iter==limit) {
            std::cout << "too many tuples in aggregated facts table!" << std::endl;
            return false;
         }
         // Count the elements in the group
         unsigned count=1;
         unsigned s=(*iter).subject,p=(*iter).predicate,o=(*iter).object;
         while ((iter!=limit)&&((*iter).subject==s)&&((*iter).predicate==p)) {
            if ((*iter).object!=o) {
               o=(*iter).object;
               ++count;
            }
            ++iter;
         }
         if ((scan.getValue1()!=s)||(scan.getValue2()!=p)||(scan.getCount()!=count)) {
            std::cout << "aggregated data mismatch! Expected [" << s << "," << p << "," << count << "], got [" << scan.getValue1() << "," << scan.getValue2() << "," << scan.getCount() << "]" << std::endl;
            return false;
         }

      } while (scan.next());
   }
   if (iter!=limit) {
      std::cout << "too few tuples in aggregated facts table!" << std::endl;
      return false;
   }

   return true;
}
//---------------------------------------------------------------------------
bool checkFacts(Database& db,vector<Triple>& facts)
   // Check all 6 orderings
{
   // Produce the different orderings
   for (unsigned index=0;index<6;index++) {
      cout << "Checking ordering " << (index+1) << endl;

      // Change the values to fit the desired order
      switch (index) {
         case 0: // subject,predicate,object
            break;
         case 1: // subject,object,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).predicate);
            break;
         case 2: // object,predicate,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).subject);
            break;
         case 3: // object,subject,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).object,(*iter).subject);
               std::swap((*iter).object,(*iter).predicate);
            }
            break;
         case 4: // predicate,subject,object
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).subject,(*iter).predicate);
            break;
         case 5: // predicate,object,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).subject,(*iter).predicate);
               std::swap((*iter).object,(*iter).predicate);
            }
            break;
      }

      // Sort the facts accordingly
      sort(facts.begin(),facts.end(),OrderTriple());

      // And compare them with the database
      if (!checkFacts(db,index,facts))
         return false;
      if (!checkAggregatedFacts(db,index,facts))
         return false;

      // Change the values back to the original order
      switch (index) {
         case 0: // subject,predicate,object
            break;
         case 1: // subject,object,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).predicate);
            break;
         case 2: // object,predicate,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).object,(*iter).subject);
            break;
         case 3: // object,subject,predicate
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).object,(*iter).predicate);
               std::swap((*iter).object,(*iter).subject);
            }
            break;
         case 4: // predicate,subject,object
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter)
               std::swap((*iter).subject,(*iter).predicate);
            break;
         case 5: // predicate,object,subject
            for (vector<Triple>::iterator iter=facts.begin(),limit=facts.end();iter!=limit;++iter) {
               std::swap((*iter).object,(*iter).predicate);
               std::swap((*iter).subject,(*iter).predicate);
            }
            break;
      }
   }
   return true;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=4) {
      cout << "usage: " << argv[0] << " <facts> <strings> <db>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[3])) {
      cout << "Unable to open " << argv[3] << endl;
      return 1;
   }

   // Check the facts
   {
      // Read the facts table
      cout << "Reading the facts table..." << endl;
      vector<Triple> facts;
      if (!readFacts(facts,argv[1]))
         return 1;

      // Check the different orderings
      if (!checkFacts(db,facts))
         return 1;
   }

   cout << "rdf store matches original data" << endl;
}
//---------------------------------------------------------------------------
