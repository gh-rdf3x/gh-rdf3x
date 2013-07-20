#include "cts/parser/TurtleParser.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/BulkOperation.hpp"
#include "rts/segment/FactsSegment.hpp"
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

   // Check the arguments
   if (argc<2) {
      cerr << "usage: " << argv[0] << " <database> " << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cerr << "unable to open " << argv[1] << endl;
      return 1;
   }

   {
	   cerr<<"SPO"<<endl;
	   FactsSegment::Scan scan;
	   if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object),0,0,0)) do {
		   cout<<scan.getValue1()<<" "<<scan.getValue2()<<" "<<scan.getValue3()<<endl;
	   }  while (scan.next());
   }

   {
 	   cerr<<"SOP"<<endl;
 	   FactsSegment::Scan scan;
 	   if (scan.first(db.getFacts(Database::Order_Subject_Object_Predicate),0,0,0)) do {
 		   cout<<scan.getValue1()<<" "<<scan.getValue2()<<" "<<scan.getValue3()<<endl;
 	   }  while (scan.next());
   }

   {
 	   cerr<<"OSP"<<endl;
 	   FactsSegment::Scan scan;
 	   if (scan.first(db.getFacts(Database::Order_Object_Subject_Predicate),0,0,0)) do {
 		   cout<<scan.getValue1()<<" "<<scan.getValue2()<<" "<<scan.getValue3()<<endl;
 	   }  while (scan.next());
   }


   {
 	   cerr<<"OPS"<<endl;
 	   FactsSegment::Scan scan;
 	   if (scan.first(db.getFacts(Database::Order_Object_Predicate_Subject),0,0,0)) do {
 		   cout<<scan.getValue1()<<" "<<scan.getValue2()<<" "<<scan.getValue3()<<endl;
 	   }  while (scan.next());
   }

   {
 	   cerr<<"PSO"<<endl;
 	   FactsSegment::Scan scan;
 	   if (scan.first(db.getFacts(Database::Order_Predicate_Subject_Object),0,0,0)) do {
 		   cout<<scan.getValue1()<<" "<<scan.getValue2()<<" "<<scan.getValue3()<<endl;
 	   }  while (scan.next());
   }

   {
 	   cerr<<"POS"<<endl;
 	   FactsSegment::Scan scan;
 	   if (scan.first(db.getFacts(Database::Order_Predicate_Object_Subject),0,0,0)) do {
 		   cout<<scan.getValue1()<<" "<<scan.getValue2()<<" "<<scan.getValue3()<<endl;
 	   }  while (scan.next());
   }

}
//---------------------------------------------------------------------------
