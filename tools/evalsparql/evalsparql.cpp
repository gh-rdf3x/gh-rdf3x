#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/runtime/TemporaryDictionary.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/operator/Scheduler.hpp"
#include <iostream>
#include <fstream>
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
static string readInput(istream& in)
   // Read the input query
{
   string result;
   while (true) {
      string s;
      getline(in,s);
      if (!in.good())
         break;
      result+=s;
      result+='\n';
   }
   return result;
}
//---------------------------------------------------------------------------
static void evalQuery(Database& db,const string& query,bool silent)
   // Evaluate a query
{
   QueryGraph queryGraph;
   {
      // Parse the query
      SPARQLLexer lexer(query);
      SPARQLParser parser(lexer);
      try {
         parser.parse();
      } catch (const SPARQLParser::ParserException& e) {
         cout << "parse error: " << e.message << endl;
         return;
      }

      // And perform the semantic anaylsis
      try {
         SemanticAnalysis semana(db);
         semana.transform(parser,queryGraph);
      } catch (const SemanticAnalysis::SemanticException& e) {
         cout << "semantic error: " << e.message << endl;
         return;
      }
      if (queryGraph.knownEmpty()) {
         cout << "<empty result>" << endl;
         return;
      }
   }

   // Run the optimizer
   PlanGen plangen;
   Plan* plan=plangen.translate(db,queryGraph);
   if (!plan) {
      cout << "plan generation failed" << endl;
      return;
   }
   if (getenv("SHOWCOSTS"))
      plan->print(0);
   if (getenv("DISABLESKIPPING"))
      Operator::disableSkipping=true;

   // Build a physical plan
   TemporaryDictionary tempDict(db.getDictionary());
   Runtime runtime(db,0,&tempDict);
   Operator* operatorTree=CodeGen().translate(runtime,queryGraph,plan,silent);

   if (getenv("SHOWPLAN")) {
      DebugPlanPrinter out(runtime,false);
      operatorTree->print(out);
   }

   // And execute it
   Scheduler scheduler;
   Timestamp start;
   scheduler.execute(operatorTree);
   Timestamp stop;
   cout << "Execution time: " << (stop-start) << " ms" << endl;

   if (getenv("SHOWCARD")) {
      DebugPlanPrinter out(runtime,true);
      operatorTree->print(out);
   }

   delete operatorTree;
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<2) {
      cout << "usage: " << argv[0] << " <database> [sparqlfile] [--silent]" << endl;
      return 1;
   }
   bool silent=false;
   if ((argc>3)&&(string(argv[3])=="--silent"))
      silent=true;

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cout << "unable to open database " << argv[1] << endl;
      return 1;
   }

   // Retrieve the query
   string query;
   if (argc>2) {
      ifstream in(argv[2]);
      if (!in.is_open()) {
         cout << "unable to open " << argv[2] << endl;
         return 1;
      }
      query=readInput(in);
   } else {
      query=readInput(cin);
   }

   // And evaluate it
   evalQuery(db,query,silent);
}
//---------------------------------------------------------------------------
