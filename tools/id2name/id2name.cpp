#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include <iostream>
#include "infra/osdep/Timestamp.hpp"
#include <string>
#include <sstream>
#include <fstream>
#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <set>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static void lookupById(Database& db,unsigned id)
   // Lookup a string id
{
   const char* start=0,*stop=0; Type::ID type; unsigned subType;
   db.getDictionary().lookupById(id,start,stop,type,subType);
   cerr<<"name: "<< string(start,stop)<<", type: " <<type<<", subType: "<<subType<<endl;
}
//---------------------------------------------------------------------------
/// lookup the name for internal id
int main(int argc,char* argv[]){
	if ((argc<3)) {
	    cout << "usage: " << argv[0] << " <database> <id>" << endl;
	    return 1;
	}

    // Open the database
	Database db;
	if (!db.open(argv[1])) {
	   cout << "unable to open " << argv[1] << endl;
	   return 1;
	}

	lookupById(db, atoi(argv[2]));


}
