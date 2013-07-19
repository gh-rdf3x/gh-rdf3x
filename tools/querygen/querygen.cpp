#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "infra/util/Type.hpp"
#include "rts/database/Database.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/operator/Scheduler.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
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
static bool readInput(istream& in,vector<string>& projection,vector<string>& terms)
   // Read the input query
{
   projection.clear();
   terms.clear();

   string s;
   while (true) {
      if (!(in>>s)) {
         if (!projection.empty())
            cerr << "malformed query template" << endl;
         return false;
      }
      if (s=="#") {
         char c;
         while (in.get(c)) if (c=='\n') break;
         continue;
      }
      if (s=="-") break;
      projection.push_back(s);
   }
   while (true) {
      if (!(in>>s))
         break;
      if (s=="#") {
         char c;
         while (in.get(c)) if (c=='\n') break;
         break;
      }
      terms.push_back(s);
   }
   if (terms.size()%3) {
      cerr << "malformed query template" << endl;
      return false;
   }
   return true;
}
//---------------------------------------------------------------------------
static string escapeURI(const char* iter,const char* limit)
   // Escape a string
{
   string s;
   for (;iter!=limit;++iter) {
      char c=(*iter);
      if ((c=='\\')||(c=='<')||(c=='>')||(c=='\"')||(c=='{')||(c=='}')||(c=='^')||(c=='|')||(c=='`')||((c&0xFF)<=0x20))
         s+='\\';
      s+=c;
   }
   return s;
}
//---------------------------------------------------------------------------
static string escape(const char* iter,const char* limit)
   // Escape a string
{
   string s;
   for (;iter!=limit;++iter) {
      char c=(*iter);
      if ((c=='\\')||(c=='\"')||(c=='\n')) s+='\\';
      s+=c;
   }
   return s;
}
//---------------------------------------------------------------------------
static void expandConstants(Database& db,const vector<string>& terms,vector<vector<string> >& constants,unsigned maxSize)
   // Expand the constants
{
   // Count the number of constants
   unsigned constantCount = 0;
   for (vector<string>::const_iterator iter=terms.begin(),limit=terms.end();iter!=limit;++iter)
      if ((*iter)=="@")
         constantCount++;
   if (!constantCount) return;

   // Build the query
   stringstream query;
   query << "select count";
   for (unsigned index=0;index<constantCount;index++)
      query << " ?constant" << index;
   query << " where {";
   constantCount=0;
   for (unsigned index=0;index<terms.size();index++) {
      if (terms[index]=="@")
         query << " ?constant" << (constantCount++); else
         query << terms[index];
      if ((index%3)==2) query << " .";
   }
   query << " } order by desc(count) limit " << maxSize;

   // Translate it
   QueryGraph queryGraph;
   {
      // Parse the query
      string q=query.str();
      SPARQLLexer lexer(q);
      SPARQLParser parser(lexer);
      try {
         parser.parse();
      } catch (const SPARQLParser::ParserException& e) {
         cerr << "parse error: " << e.message << endl;
         return;
      }

      // And perform the semantic anaylsis
      SemanticAnalysis semana(db);
      semana.transform(parser,queryGraph);
      if (queryGraph.knownEmpty()) {
         cerr << "<empty result>" << endl;
         return;
      }
   }

   // Run the optimizer
   PlanGen plangen;
   Plan* plan=plangen.translate(db,queryGraph);
   if (!plan) {
      cerr << "plan generation failed" << endl;
      return;
   }

   // Build a physical plan
   Runtime runtime(db);
   vector<Register*> output;
   Operator* operatorTree=CodeGen().translateIntern(runtime,queryGraph,plan,output);
   if (!operatorTree) {
      cerr << "code generation failed" << endl;
      return;
   }

   // And execute it
   if (operatorTree->first()) do {
      vector<string> values;
      for (vector<Register*>::const_iterator iter=output.begin(),limit=output.end();iter!=limit;++iter) {
         unsigned v=(*iter)->value;
         if (!~v) {
            break;
         } else {
            const char* start,*stop; Type::ID type; unsigned subType;
            if (!db.getDictionary().lookupById(v,start,stop,type,subType))
               break;
            switch (type) {
               case Type::URI: values.push_back("<"+escapeURI(start,stop)+">"); break;
               case Type::Literal: values.push_back("\""+escape(start,stop)+"\""); break;
               case Type::CustomLanguage: values.push_back("\""+escape(start,stop)+"\""); break; // XXX add language
               case Type::CustomType: values.push_back("\""+escape(start,stop)+"\""); break; // XXX add type
               case Type::String: values.push_back("\""+escape(start,stop)+"\"^^<http://www.w3.org/2001/XMLSchema#string>"); break;
               case Type::Integer: values.push_back("\""+escape(start,stop)+"\"^^<http://www.w3.org/2001/XMLSchema#integer>"); break;
               case Type::Decimal: values.push_back("\""+escape(start,stop)+"\"^^<http://www.w3.org/2001/XMLSchema#decimal>"); break;
               case Type::Double: values.push_back("\""+escape(start,stop)+"\"^^<http://www.w3.org/2001/XMLSchema#double>"); break;
               case Type::Boolean: values.push_back("\""+escape(start,stop)+"\"^^<http://www.w3.org/2001/XMLSchema#boolean>"); break;
            }
         }
      }
      if (values.size()==output.size())
         constants.push_back(values);
      if (constants.size()>=maxSize)
         break;
   } while (operatorTree->next());

   delete operatorTree;
}
//---------------------------------------------------------------------------
static void produceQueries(Database& db,const vector<string>& projection,const vector<string>& terms,unsigned count)
   // Produce SPARQL queries
{
   // Expand constants
   vector<vector<string> > constants;
   expandConstants(db,terms,constants,count);

   // And build queries
   for (vector<vector<string> >::const_iterator iter=constants.begin(),limit=constants.end();iter!=limit;++iter) {
      const vector<string>& c=(*iter);
      cout << "select";
      for (vector<string>::const_iterator iter=projection.begin(),limit=projection.end();iter!=limit;++iter)
         cout << " " << (*iter);
      cout << " where {";
      unsigned constantCount=0;
      for (unsigned index=0;index<terms.size();index++) {
         if (terms[index]=="@") {
            cout << " " << c[constantCount++];
         } else {
            cout << " "  << terms[index];
         }
         if ((index%3)==2) cout << " .";
      }
      cout << " }" << endl;
   }
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<2) {
      cout << "usage: " << argv[0] << " <database> [sparqlfile]" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cout << "unable to open database " << argv[1] << endl;
      return 1;
   }

   // Retrieve the query
   vector<string> projection,terms;
   unsigned count = 10000;
   if (argc>2) {
      ifstream in(argv[2]);
      if (!in.is_open()) {
         cout << "unable to open " << argv[2] << endl;
         return 1;
      }
      if ((argc>3)&&(atoi(argv[3])))
         count=atoi(argv[3]);
      while (true) {
         if (!readInput(in,projection,terms))
            break;
         produceQueries(db,projection,terms,count);
      }
   } else {
      while (true) {
         if (!readInput(cin,projection,terms))
            break;
         produceQueries(db,projection,terms,count);
      }
   }
}
//---------------------------------------------------------------------------
