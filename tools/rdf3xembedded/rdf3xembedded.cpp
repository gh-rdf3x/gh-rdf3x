#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/parser/TurtleParser.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/database/Database.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/operator/ResultsPrinter.hpp"
#include "rts/runtime/BulkOperation.hpp"
#include "rts/runtime/DifferentialIndex.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/runtime/TemporaryDictionary.hpp"
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
bool smallAddressSpace()
   // Is the address space too small?
{
   return sizeof(void*)<8;
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// Query types
enum QueryType { RegularQuery, ExplainQuery, InsertQuery, RollbackQuery, UnknownQueryType };
//---------------------------------------------------------------------------
static QueryType classifyQuery(const string& s)
   // Classify a query
{
   SPARQLLexer lexer(s);
   if (lexer.getNext()!=SPARQLLexer::Identifier)
      return UnknownQueryType;
   if (lexer.isKeyword("select"))
      return RegularQuery;
   if (lexer.isKeyword("explain"))
      return ExplainQuery;
   if (lexer.isKeyword("insert"))
      return InsertQuery;
   if (lexer.isKeyword("rollback"))
      return RollbackQuery;
   return UnknownQueryType;
}
//---------------------------------------------------------------------------
static void writeHeader(const QueryGraph& graph,const SPARQLParser& parser)
   // Write the query header
{
   bool first=true;
   for (QueryGraph::projection_iterator iter=graph.projectionBegin(),limit=graph.projectionEnd();iter!=limit;++iter) {
      string name=parser.getVariableName(*iter);
      if (first)
         first=false; else
         cout << ' ';
      for (string::const_iterator iter2=name.begin(),limit2=name.end();iter2!=limit2;++iter2) {
         char c=(*iter2);
         if ((c==' ')||(c=='\n')||(c=='\\'))
            cout << '\\';
         cout << c;
      }
   }
   if ((graph.getDuplicateHandling()==QueryGraph::CountDuplicates)||(graph.getDuplicateHandling()==QueryGraph::ShowDuplicates))
      cout << " count";
   cout << endl;
}
//---------------------------------------------------------------------------
static void runQuery(DifferentialIndex& diffIndex,const string& query)
   // Evaluate a query
{
   QueryGraph queryGraph;
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
      SemanticAnalysis semana(diffIndex);
      semana.transform(parser,queryGraph);
   } catch (const SemanticAnalysis::SemanticException& e) {
      cout << "semantic error: " << e.message << endl;
      return;
   }
   if (queryGraph.knownEmpty()) {
      cout << "ok" << endl;
      writeHeader(queryGraph,parser);
      cout << "\\." << endl;
      cout.flush();
      return;
   }

   // Run the optimizer
   PlanGen plangen;
   Plan* plan=plangen.translate(diffIndex.getDatabase(),queryGraph);
   if (!plan) {
      cout << "internal error plan generation failed" << endl;
      return;
   }

   // Build a physical plan
   TemporaryDictionary tempDict(diffIndex);
   Runtime runtime(diffIndex.getDatabase(),&diffIndex,&tempDict);
   Operator* operatorTree=CodeGen().translate(runtime,queryGraph,plan,false);
   dynamic_cast<ResultsPrinter*>(operatorTree)->setOutputMode(ResultsPrinter::Embedded);

   // Execute it
   cout << "ok" << endl;
   writeHeader(queryGraph,parser);
   if (operatorTree->first()) {
      while (operatorTree->next()) ;
   }
   cout << "\\." << endl;
   cout.flush();

   delete operatorTree;
}
//---------------------------------------------------------------------------
/// Output for the explain command
class ExplainPrinter : public PlanPrinter
{
   private:
   /// The runtime
   Runtime& runtime;
   /// The indentation level
   unsigned indent;
   /// The operator data
   string operatorName,operatorArguments;
   /// Cardinalities
   double expectedOutputCardinality;
   /// Cardinalities
   unsigned observedOutputCardinality;
   /// Currently in an operator?
   bool inOp;

   /// Write the current operator if any
   void flushOperator();

   public:
   /// Constructor
   explicit ExplainPrinter(Runtime& runtime);
   /// Destructor
   ~ExplainPrinter();

   /// Begin a new operator
   void beginOperator(const std::string& name,double expectedOutputCardinality,unsigned observedOutputCardinality);
   /// Add an operator argument annotation
   void addArgumentAnnotation(const std::string& argument);
   /// Add a scan annotation
   void addScanAnnotation(const Register* reg,bool bound);
   /// Add a predicate annotate
   void addEqualPredicateAnnotation(const Register* reg1,const Register* reg2);
   /// Add a materialization annotation
   void addMaterializationAnnotation(const std::vector<Register*>& regs);
   /// Add a generic annotation
   void addGenericAnnotation(const std::string& text);
   /// Close the current operator
   void endOperator();

   /// Format a register (for generic annotations)
   std::string formatRegister(const Register* reg);
   /// Format a constant value (for generic annotations)
   std::string formatValue(unsigned value);
};
//---------------------------------------------------------------------------
ExplainPrinter::ExplainPrinter(Runtime& runtime)
   : runtime(runtime),indent(0),inOp(false)
   // Constructor
{
}
//---------------------------------------------------------------------------
ExplainPrinter::~ExplainPrinter()
   // Destructor
{
}
//---------------------------------------------------------------------------
template <class T> void escapeOutput(T start,T stop)
   // Write a string
{
   for (T iter=start;iter!=stop;++iter) {
      char c=*iter;
      if ((c=='\\')||(c==' ')||(c=='\n')||(c=='\r'))
         cout << "\\";
      cout << c;
   }
}
//---------------------------------------------------------------------------
void ExplainPrinter::flushOperator()
   // Write the current operator if any
{
   if (inOp) {
      inOp=false;
      cout << indent << " \"";
      escapeOutput(operatorName.begin(),operatorName.end());
      cout << "\" \"";
      escapeOutput(operatorArguments.begin(),operatorArguments.end());
      cout << "\" " << expectedOutputCardinality << endl;
      inOp=false;
   }
}
//---------------------------------------------------------------------------
void ExplainPrinter::beginOperator(const std::string& name,double expectedOutputCardinality,unsigned observedOutputCardinality)
   // Begin a new operator
{
   flushOperator();
   operatorName=name;
   operatorArguments="";
   this->expectedOutputCardinality=expectedOutputCardinality;
   this->observedOutputCardinality=observedOutputCardinality;
   inOp=true;
   ++indent;
}
//---------------------------------------------------------------------------
void ExplainPrinter::addArgumentAnnotation(const std::string& argument)
   // Add an operator argument annotation
{
   if (operatorArguments.length())
      operatorArguments+=" ";
   operatorArguments+=argument;
}
//---------------------------------------------------------------------------
void ExplainPrinter::addScanAnnotation(const Register* reg,bool bound)
   // Add a scan annotation
{
   if (operatorArguments.length())
      operatorArguments+=" ";
   if (bound)
      operatorArguments+=formatValue(reg->value); else
      operatorArguments+=formatRegister(reg);
}
//---------------------------------------------------------------------------
void ExplainPrinter::addEqualPredicateAnnotation(const Register* reg1,const Register* reg2)
   // Add a predicate annotate
{
   if (operatorArguments.length())
      operatorArguments+=" ";
   operatorArguments+=formatRegister(reg1);
   operatorArguments+="=";
   operatorArguments+=formatRegister(reg2);
}
//---------------------------------------------------------------------------
void ExplainPrinter::addMaterializationAnnotation(const std::vector<Register*>& /*regs*/)
   // Add a materialization annotation
{
}
//---------------------------------------------------------------------------
void ExplainPrinter::addGenericAnnotation(const std::string& /*text*/)
   // Add a generic annotation
{
}
//---------------------------------------------------------------------------
void ExplainPrinter::endOperator()
   // Close the current operator
{
   flushOperator();
   --indent;
}
//---------------------------------------------------------------------------
string ExplainPrinter::formatRegister(const Register* reg)
   // Format a register (for generic annotations)
{
   stringstream result;
   // Regular register?
   if (runtime.getRegisterCount()&&(reg>=runtime.getRegister(0))&&(reg<=runtime.getRegister(runtime.getRegisterCount()-1))) {
      result << "?" << (reg-runtime.getRegister(0));
   } else {
      // Arbitrary register outside the runtime system. Should not occur except for debugging!
      result << "@0x" << hex << reinterpret_cast<uintptr_t>(reg);
   }
   return result.str();
}
//---------------------------------------------------------------------------
string ExplainPrinter::formatValue(unsigned value)
   // Format a constant value (for generic annotations)
{
   stringstream result;
   if (~value) {
      const char* start,*stop; Type::ID type; unsigned subType;
      if (runtime.getDatabase().getDictionary().lookupById(value,start,stop,type,subType)) {
         result << '\"';
         for (const char* iter=start;iter!=stop;++iter)
           result << *iter;
         result << '\"';
      } else result << "@?" << value;
   } else {
      result << "NULL";
   }
   return result.str();
}
//---------------------------------------------------------------------------
static void explainQuery(DifferentialIndex& diffIndex,const string& query)
   // Explain a query
{
   QueryGraph queryGraph;
   // Parse the query
   SPARQLLexer lexer(query);
   if ((lexer.getNext()!=SPARQLLexer::Identifier)||(!lexer.isKeyword("explain"))) {
      cout << "internal error: explain expected" << endl;
      return;
   }
   SPARQLParser parser(lexer);
   try {
      parser.parse();
   } catch (const SPARQLParser::ParserException& e) {
      cout << "parse error: " << e.message << endl;
      return;
   }

   // And perform the semantic anaylsis
   try {
      SemanticAnalysis semana(diffIndex);
      semana.transform(parser,queryGraph);
   } catch (const SemanticAnalysis::SemanticException& e) {
      cout << "semantic error: " << e.message << endl;
      return;
   }
   if (queryGraph.knownEmpty()) {
      cout << "ok" << endl
           << "indent operator arguments expectedcardinality" << endl
           << "1 \"EmptyScan\" \"\" 0" << endl
           << "\\." << endl;
      cout.flush();
      return;
   }

   // Run the optimizer
   PlanGen plangen;
   Plan* plan=plangen.translate(diffIndex.getDatabase(),queryGraph);
   if (!plan) {
      cout << "internal error plan generation failed" << endl;
      return;
   }

   // Print the plan
   cout << "ok" << endl
        << "indent operator arguments expectedcardinality" << endl;
   Runtime runtime(diffIndex.getDatabase(),&diffIndex);
   ExplainPrinter out(runtime);
   Operator* operatorTree=CodeGen().translate(runtime,queryGraph,plan,false);
   dynamic_cast<ResultsPrinter*>(operatorTree)->getInput()->print(out);
   cout << "\\." << endl;
   cout.flush();

   delete operatorTree;
}
//---------------------------------------------------------------------------
static void insertQuery(DifferentialIndex& diffIndex,const string& query)
   // Insert new triples
{
   // Find the boundaries of the new triples
   string::const_iterator start,stop;
   SPARQLLexer lexer(query);
   if ((lexer.getNext()!=SPARQLLexer::Identifier)||(!lexer.isKeyword("insert"))) {
      cout << "'insert' expected" << endl;
      return;
   }
   if ((lexer.getNext()!=SPARQLLexer::Identifier)||(!lexer.isKeyword("data"))) {
      cout << "'data' expected" << endl;
      return;
   }
   if (lexer.getNext()!=SPARQLLexer::LCurly) {
      cout << "'{' expected" << endl;
      return;
   }
   start=lexer.getReader();
   stop=start;
   while (start==stop) {
      switch (lexer.getNext()) {
         case SPARQLLexer::Eof:
            cout << "'}' expected" << endl;
            return;
         case SPARQLLexer::RCurly:
            stop=lexer.getReader()-1;
            break;
         default: break;
      }
   }
   string turtle(start,stop);
   istringstream in(turtle);

   // Parse the input
   BulkOperation chunk(diffIndex);
   TurtleParser parser(in);
   while (true) {
      // Read the next triple
      std::string subject,predicate,object,objectSubType; Type::ID objectType;
      try {
         if (!parser.parse(subject,predicate,object,objectType,objectSubType))
            break;
      } catch (const TurtleParser::Exception& e) {
         cout << e.message << endl;
         return;
      }
      chunk.insert(subject,predicate,object,objectType,objectSubType);
   }

   // And insert it
   chunk.commit();
   cout << "ok" << endl << endl << "\\." << endl;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   cout.sync_with_stdio(false);

   // Check the arguments
   if (argc!=2) {
      cerr << "usage: " << argv[0] << " <database>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cout << "unable to open database " << argv[1] << endl;
      return 1;
   }
   DifferentialIndex diffIndex(db);
   cout << "RDF-3X protocol 1" << endl;

   // And process queries
   while (true) {
      string query;
      while (true) {
         char c;
         if (!(cin.get(c))) return 0;
         if (c=='\n') break;
         if (c=='\\') {
            if (!(cin.get(c))) return 0;
         }
         query+=c;
      }
      switch (classifyQuery(query)) {
         case ExplainQuery:
            explainQuery(diffIndex,query);
            break;
         case InsertQuery:
            insertQuery(diffIndex,query);
            break;
         case RollbackQuery:
            diffIndex.clear();
            cout << "ok" << endl << endl << "\\." << endl;
            break;
         case RegularQuery:
         default:
            runQuery(diffIndex,query);
            break;
      }
      cout.flush();
   }
}
//---------------------------------------------------------------------------
