#include "cts/parser/TurtleParser.hpp"
#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "infra/osdep/Event.hpp"
#include "infra/osdep/Mutex.hpp"
#include "infra/osdep/Thread.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/database/Database.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/operator/Scheduler.hpp"
#include "rts/runtime/BulkOperation.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/runtime/PredicateLockManager.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <cassert>
#include <cstring>
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
namespace {
//---------------------------------------------------------------------------
/// A database driver
class Driver {
   public:
   /// Constructor
   Driver() {}
   /// Destructor
   virtual ~Driver();

   /// Build the initial database
   virtual bool buildDatabase(TurtleParser& input,unsigned initialSize) = 0;
   /// Prepare a chunk of work
   virtual void prepareChunk(const string& name,TurtleParser& parser,unsigned chunkSize) = 0;
   /// Preprate a query
   virtual string prepareQuery(const string& query) = 0;
   /// The prepare step is done
   virtual void prepareDone() = 0;
   /// Process a query
   virtual void processQuery(const string& query) = 0;
   /// Process a chunk of work
   virtual unsigned processChunk(const string& chunkFile,unsigned delay) = 0;
   /// Process a query and a chunk of work
   virtual unsigned processQueryAndChunk(const string& query,const string& chunkFile,unsigned delay) = 0;
   /// Synchronize to disk
   virtual void sync() = 0;
};
//---------------------------------------------------------------------------
Driver::~Driver()
   // Destructor
{
}
//---------------------------------------------------------------------------
/// A RDF-3X driver
class RDF3XDriver : public Driver
{
   private:
   /// The database
   Database db;
   /// The differential index
   DifferentialIndex* diff;
   /// The predicate locks
   PredicateLockManager locks;
   /// The next transaction id (for locks)
   unsigned nextLockTransaction;
   /// Mutex
   Mutex nextLockTransactionLock;

   public:
   /// Constructor
   RDF3XDriver();
   /// Destructor
   ~RDF3XDriver();

   /// Build the initial database
   bool buildDatabase(TurtleParser& input,unsigned initialSize);
   /// Prepare a chunk of work
   void prepareChunk(const string& name,TurtleParser& parser,unsigned chunkSize);
   /// Preprate a query
   string prepareQuery(const string& query);
   /// The prepare step is done
   void prepareDone();
   /// Process a query
   void processQuery(const string& query);
   /// Process a chunk of work
   unsigned processChunk(const string& chunkFile,unsigned delay);
   /// Process a query and a chunk of work
   unsigned processQueryAndChunk(const string& query,const string& chunkFile,unsigned delay);
   /// Synchronize to disk
   void sync();
};
//---------------------------------------------------------------------------
RDF3XDriver::RDF3XDriver()
   : diff(0),nextLockTransaction(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
RDF3XDriver::~RDF3XDriver()
   // Destructor
{
   remove("updatetest.2.tmp");
   delete diff;
}
//---------------------------------------------------------------------------
static void writeURI(ostream& out,const string& str)
   // Write a URI
{
   out << "<";
   for (string::const_iterator iter=str.begin(),limit=str.end();iter!=limit;++iter) {
      char c=*iter;
      switch (c) {
         case '\t': out << "\\t"; break;
         case '\n': out << "\\n"; break;
         case '\r': out << "\\r"; break;
         case '>': out << "\\>"; break;
         case '\\': out << "\\\\"; break;
         default: out << c; break;
      }
   }
   out << ">";
}
//---------------------------------------------------------------------------
static void writeLiteral(ostream& out,const string& str)
   // Write a literal
{
   out << "\"";
   for (string::const_iterator iter=str.begin(),limit=str.end();iter!=limit;++iter) {
      char c=*iter;
      switch (c) {
         case '\t': out << "\\t"; break;
         case '\n': out << "\\n"; break;
         case '\r': out << "\\r"; break;
         case '\"': out << "\\\""; break;
         case '\\': out << "\\\\"; break;
         default: out << c; break;
      }
   }
   out << "\"";
}
//---------------------------------------------------------------------------
static void dumpSubject(ostream& out,const string& str)
   // Write a subject entry
{
   writeURI(out,str);
}
//---------------------------------------------------------------------------
static void dumpPredicate(ostream& out,const string& str)
   // Write a predicate entry
{
   writeURI(out,str);
}
//---------------------------------------------------------------------------
static void dumpObject(ostream& out,const string& str,Type::ID type,const string& /*subType*/)
   // Write an object entry
{
   // Blank node or URI?
   if (type==Type::URI) {
      writeURI(out,str);
      return;
   }
   // No, a literal value
   writeLiteral(out,str);
}
//---------------------------------------------------------------------------
bool RDF3XDriver::buildDatabase(TurtleParser& input,unsigned initialSize)
   // Build the initial database
{
  {
      ofstream out("updatetest.1.tmp");
      string subject,predicate,object; Type::ID objectType; string objectSubType;
      for (unsigned index=0;index<initialSize;index++) {
         if (!input.parse(subject,predicate,object,objectType,objectSubType))
            break;
         dumpSubject(out,subject);
         out << " ";
         dumpPredicate(out,predicate);
         out << " ";
         dumpObject(out,object,objectType,objectSubType);
         out << "." << endl;
      }
   }
   if (system("./bin/rdf3xload updatetest.2.tmp updatetest.1.tmp")!=0) {
      remove("updatetest.1.tmp");
      cerr << "unable to execute ./bin/rdf3xload" << endl;
      return false;
   }
   remove("updatetest.1.tmp");

   if (!db.open("updatetest.2.tmp")) {
      cerr << "unable to open updatetest.2.tmp" << endl;
      return false;
   }
   diff=new DifferentialIndex(db);

   return true;
}
//---------------------------------------------------------------------------
void RDF3XDriver::prepareChunk(const string& name,TurtleParser& parser,unsigned chunkSize)
   // Prepare a chunk of work
{
   ofstream out(name.c_str());
   string subject,predicate,object; Type::ID objectType; string objectSubType;
   for (unsigned index2=0;index2<chunkSize;index2++) {
      if (!parser.parse(subject,predicate,object,objectType,objectSubType))
         break;
      dumpSubject(out,subject);
      out << " ";
      dumpPredicate(out,predicate);
      out << " ";
      dumpObject(out,object,objectType,objectSubType);
      out << "." << endl;
   }
}
//---------------------------------------------------------------------------
string RDF3XDriver::prepareQuery(const string& query)
   // Preprate a query
{
   return query;
}
//---------------------------------------------------------------------------
void RDF3XDriver::prepareDone()
   // The prepare step is done
{
}
//---------------------------------------------------------------------------
void RDF3XDriver::processQuery(const string& query)
   // Process a query
{
   QueryGraph queryGraph;
   {
      // Parse the query
      SPARQLLexer lexer(query);
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
         // cerr << "<empty result>" << endl;
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
   Operator* operatorTree=CodeGen().translate(runtime,queryGraph,plan,true);

   vector<unsigned> regValues;
   for (unsigned index=0,limit=runtime.getRegisterCount();index<limit;index++)
      regValues.push_back(runtime.getRegister(index)->value);

   // And execute it
   Scheduler scheduler;
   scheduler.execute(operatorTree);

   delete operatorTree;
}
//---------------------------------------------------------------------------
unsigned RDF3XDriver::processChunk(const string& chunkFile,unsigned delay)
   // Process a chunk of work
{
   unsigned processed=0;

   BulkOperation bulk(*diff);
   ifstream in(chunkFile.c_str());
   TurtleParser parser(in);
   string subject,predicate,object; Type::ID objectType; string objectSubType;
   while (true) {
      if (!parser.parse(subject,predicate,object,objectType,objectSubType))
         break;
      bulk.insert(subject,predicate,object,objectType,objectSubType);
      processed++;
   }
   if (delay)
      Thread::sleep(delay);
   bulk.commit();

   return processed;
}
//---------------------------------------------------------------------------
unsigned RDF3XDriver::processQueryAndChunk(const string& query,const string& chunkFile,unsigned delay)
   // Process a query and a chunk of work
{
   // Parse the query
   QueryGraph queryGraph;
   bool knownEmpty=false;
   {
      // Parse the query
      SPARQLLexer lexer(query);
      SPARQLParser parser(lexer);
      try {
         parser.parse();
      } catch (const SPARQLParser::ParserException& e) {
         cerr << "parse error: " << e.message << endl;
         return 0;
      }

      // And perform the semantic anaylsis
      SemanticAnalysis semana(db);
      semana.transform(parser,queryGraph);
      if (queryGraph.knownEmpty()) {
         // cerr << "<empty result>" << endl;
         knownEmpty=true;
      }
   }

   // Read the triples
   unsigned processed=0;
   BulkOperation bulk(*diff);
   ifstream in(chunkFile.c_str());
   TurtleParser parser(in);
   string subject,predicate,object; Type::ID objectType; string objectSubType;
   while (true) {
      if (!parser.parse(subject,predicate,object,objectType,objectSubType))
         break;
      bulk.insert(subject,predicate,object,objectType,objectSubType);
      processed++;
   }

   // Build the locks
   vector<pair<PredicateLockManager::Box,bool> > locks;
   if (!knownEmpty)
   for (vector<QueryGraph::Node>::const_iterator iter=queryGraph.getQuery().nodes.begin(),limit=queryGraph.getQuery().nodes.end();iter!=limit;++iter) {
      const QueryGraph::Node& n=*iter;
      PredicateLockManager::Box b(0,~0u,0,~0u,0,~0u);
      if (n.constSubject)
         b.subjectMin=b.subjectMax=n.subject;
      if (n.constPredicate)
         b.predicateMin=b.predicateMax=n.predicate;
      if (n.constObject)
         b.objectMin=b.objectMax=n.object;
      locks.push_back(pair<PredicateLockManager::Box,bool>(b,false));
   }
   {
      vector<PredicateLockManager::Box> writeLocks;
      bulk.buildCover(20,writeLocks);
      for (vector<PredicateLockManager::Box>::const_iterator iter=writeLocks.begin(),limit=writeLocks.end();iter!=limit;++iter)
         locks.push_back(pair<PredicateLockManager::Box,bool>(*iter,true));
   }

   // Try locking
   unsigned lockTransaction;
   while (true) {
      // Produce a new id
      {
         auto_lock lock(nextLockTransactionLock);
         lockTransaction=nextLockTransaction++;
      }

      // Try to lock everything
      if (this->locks.lockMultiple(lockTransaction,locks))
         break;

      // Locking failed, retry
      //cerr << "locking failed..." << endl;
   }

   // Run the query
   if (!knownEmpty) {
      // Run the optimizer
      PlanGen plangen;
      Plan* plan=plangen.translate(db,queryGraph);
      if (!plan) {
         cerr << "plan generation failed" << endl;
         return 0;
      }

      // Build a physical plan
      Runtime runtime(db);
      Operator* operatorTree=CodeGen().translate(runtime,queryGraph,plan,true);

      vector<unsigned> regValues;
      for (unsigned index=0,limit=runtime.getRegisterCount();index<limit;index++)
         regValues.push_back(runtime.getRegister(index)->value);

      // And execute it
      Scheduler scheduler;
      scheduler.execute(operatorTree);

      delete operatorTree;
   }

   // Commit
   if (delay)
      Thread::sleep(delay);
   bulk.commit();

   // And release all locks
   this->locks.finished(lockTransaction);

   return processed;
}
//---------------------------------------------------------------------------
void RDF3XDriver::sync()
   // Synchronize to disk
{
   diff->sync();
}
//---------------------------------------------------------------------------
/// A PostgreSQL driver
class PostgresDriver : public Driver
{
   private:
   /// The string table
   map<string,unsigned> dictionary;
   /// The job sizes
   map<string,unsigned> jobSize;

   public:
   /// Constructor
   PostgresDriver();
   /// Destructor
   ~PostgresDriver();

   /// Build the initial database
   bool buildDatabase(TurtleParser& input,unsigned initialSize);
   /// Prepare a chunk of work
   void prepareChunk(const string& name,TurtleParser& parser,unsigned chunkSize);
   /// Preprate a query
   string prepareQuery(const string& query);
   /// The prepare step is done
   void prepareDone();
   /// Process a query
   void processQuery(const string& query);
   /// Process a chunk of work
   unsigned processChunk(const string& chunkFile,unsigned delay);
   /// Process a query and a chunk of work
   unsigned processQueryAndChunk(const string& query,const string& chunkFile,unsigned delay);
   /// Synchronize to disk
   void sync();
};
//---------------------------------------------------------------------------
PostgresDriver::PostgresDriver()
   // Constructor
{
}
//---------------------------------------------------------------------------
PostgresDriver::~PostgresDriver()
   // Destructor
{
   if (system("psql -c \"drop schema if exists updatetest cascade;\"")!=0)
      cerr << "warning: psql call failed" << endl;
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
bool PostgresDriver::buildDatabase(TurtleParser& input,unsigned initialSize)
   // Build the initial database
{
   dictionary.clear();
   {
      ofstream out("updatetest.1.tmp");
      out << "drop schema if exists updatetest cascade;" << endl;
      out << "create schema updatetest;" << endl;
      out << "create table updatetest.facts(subject int not null, predicate int not null, object int not null);" << endl;
      out << "copy updatetest.facts from stdin;" << endl;

      string subject,predicate,object; Type::ID objectType; string objectSubType;
      for (unsigned index=0;index<initialSize;index++) {
         if (!input.parse(subject,predicate,object,objectType,objectSubType))
            break;
         unsigned subjectId,predicateId,objectId;
         if (dictionary.count(subject)) {
            subjectId=dictionary[subject];
         } else {
            subjectId=dictionary.size();
            dictionary[subject]=subjectId;
         }
         if (dictionary.count(predicate)) {
            predicateId=dictionary[predicate];
         } else {
            predicateId=dictionary.size();
            dictionary[predicate]=predicateId;
         }
         if (dictionary.count(object)) {
            objectId=dictionary[object];
         } else {
            objectId=dictionary.size();
            dictionary[object]=objectId;
         }
         out << subjectId << "\t" << predicateId << "\t" << objectId << endl;
      }
      out << "\\." << endl;
      out << "create index facts_spo on updatetest.facts (subject, predicate, object);" << endl;
      out << "create index facts_pso on updatetest.facts (predicate, subject, object);" << endl;
      out << "create index facts_pos on updatetest.facts (predicate, object, subject);" << endl;

      out << "create table updatetest.strings(id int not null primary key, value varchar(16000) not null);" << endl;
      out << "copy updatetest.strings from stdin;" << endl;
      for (map<string,unsigned>::const_iterator iter=dictionary.begin(),limit=dictionary.end();iter!=limit;++iter)
         out << (*iter).second << "\t" << escapeCopy((*iter).first) << endl;
      out << "\\." << endl;
   }
   if (system("psql -f updatetest.1.tmp")!=0) {
      remove("updatetest.1.tmp");
      cerr << "unable to execute psql" << endl;
      return false;
   }
   //remove("updatetest.1.tmp");

   return true;
}
//---------------------------------------------------------------------------
void PostgresDriver::prepareChunk(const string& name,TurtleParser& parser,unsigned chunkSize)
   // Prepare a chunk of work
{
   ofstream out(name.c_str());
   vector<unsigned> triples;
   vector<pair<unsigned,string> > added;

   string subject,predicate,object; Type::ID objectType; string objectSubType;
   unsigned size=0;
   for (unsigned index2=0;index2<chunkSize;index2++) {
      if (!parser.parse(subject,predicate,object,objectType,objectSubType))
         break;
//      out << "select id from updatetest.strings where value=E'" << escapeCopy(subject) << "' \\g /dev/null" << endl;
//      out << "select id from updatetest.strings where value=E'" << escapeCopy(predicate) << "' \\g /dev/null" << endl;
//      out << "select id from updatetest.strings where value=E'" << escapeCopy(object) << "' \\g /dev/null" << endl;
      if (dictionary.count(subject)) {
         triples.push_back(dictionary[subject]);
      } else {
         unsigned id=dictionary.size();
         dictionary[subject]=id;
         added.push_back(pair<unsigned,string>(id,subject));
         triples.push_back(id);
      }
      if (dictionary.count(predicate)) {
         triples.push_back(dictionary[predicate]);
      } else {
         unsigned id=dictionary.size();
         dictionary[predicate]=id;
         added.push_back(pair<unsigned,string>(id,predicate));
         triples.push_back(id);
      }
      if (dictionary.count(object)) {
         triples.push_back(dictionary[object]);
      } else {
         unsigned id=dictionary.size();
         dictionary[object]=id;
         added.push_back(pair<unsigned,string>(id,object));
         triples.push_back(id);
      }
      ++size;
   }
   jobSize[name]=size;
   out << "copy updatetest.strings from stdin;" << endl;
   for (vector<pair<unsigned,string> >::const_iterator iter=added.begin(),limit=added.end();iter!=limit;++iter)
      out << (*iter).first << "\t" << escapeCopy((*iter).second) << endl;
   out << "\\." << endl;
   out << "copy updatetest.facts from stdin;" << endl;
   for (unsigned index=0,limit=triples.size();index<limit;index+=3)
      out << triples[index] << "\t" << triples[index+1] << "\t" << triples[index+2] << endl;
   out << "\\." << endl;
}
//---------------------------------------------------------------------------
static string buildFactsAttribute(unsigned id,const char* attribute)
    // Build the attribute name for a facts attribute
{
   stringstream out;
   out << "f" << id << "." << attribute;
   return out.str();
}
//---------------------------------------------------------------------------
string PostgresDriver::prepareQuery(const string& query)
   // Preprate a query
{
   // Parse the query
   SPARQLLexer lexer(query);
   SPARQLParser parser(lexer);
   try {
      parser.parse();
   } catch (const SPARQLParser::ParserException& e) {
      cerr << "parse error: " << e.message << endl;
      return "";
   }

   // Translate it
   stringstream out;
   out << "select ";
   {
      unsigned id=0;
      for (SPARQLParser::projection_iterator iter=parser.projectionBegin(),limit=parser.projectionEnd();iter!=limit;++iter) {
         if (id) out << ",";
         out << "s" << id << ".value";
         id++;
      }
   }
   out << " from (";
   map<unsigned,string> representative;
   {
      unsigned id=0;
      for (vector<SPARQLParser::Pattern>::const_iterator iter=parser.getPatterns().patterns.begin(),limit=parser.getPatterns().patterns.end();iter!=limit;++iter) {
         if (((*iter).subject.type==SPARQLParser::Element::Variable)&&(!representative.count((*iter).subject.id)))
            representative[(*iter).subject.id]=buildFactsAttribute(id,"subject");
         if (((*iter).predicate.type==SPARQLParser::Element::Variable)&&(!representative.count((*iter).predicate.id)))
            representative[(*iter).predicate.id]=buildFactsAttribute(id,"predicate");
         if (((*iter).object.type==SPARQLParser::Element::Variable)&&(!representative.count((*iter).object.id)))
            representative[(*iter).object.id]=buildFactsAttribute(id,"object");
         ++id;
      }
   }
   out << "select ";
   {
      unsigned id=0;
      for (SPARQLParser::projection_iterator iter=parser.projectionBegin(),limit=parser.projectionEnd();iter!=limit;++iter) {
         if (id) out << ",";
         out << representative[*iter] << " as r" << id;
         id++;
      }
   }
   out << " from ";
   {
      unsigned id=0;
      for (vector<SPARQLParser::Pattern>::const_iterator iter=parser.getPatterns().patterns.begin(),limit=parser.getPatterns().patterns.end();iter!=limit;++iter) {
         if (id) out << ",";
         out << "updatetest.facts f" << id;
         ++id;
      }

   }
   out << " where ";
   {
      unsigned id=0; bool first=true;
      for (vector<SPARQLParser::Pattern>::const_iterator iter=parser.getPatterns().patterns.begin(),limit=parser.getPatterns().patterns.end();iter!=limit;++iter) {
         string s=buildFactsAttribute(id,"subject"),p=buildFactsAttribute(id,"predicate"),o=buildFactsAttribute(id,"object");
         if ((*iter).subject.type!=SPARQLParser::Element::Variable) {
            if (first) first=false; else out << " and ";
            unsigned val;
            if (dictionary.count((*iter).subject.value))
               val=dictionary[(*iter).subject.value]; else
               val=dictionary.size();
            out << s << "=" << val;
         } else if (representative[(*iter).subject.id]!=s) {
            if (first) first=false; else out << " and ";
            out << s << "=" << representative[(*iter).subject.id];
         }
         if ((*iter).predicate.type!=SPARQLParser::Element::Variable) {
            if (first) first=false; else out << " and ";
            unsigned val;
            if (dictionary.count((*iter).predicate.value))
               val=dictionary[(*iter).predicate.value]; else
               val=dictionary.size();
            out << p << "=" << val;
         } else if (representative[(*iter).predicate.id]!=p) {
            if (first) first=false; else out << " and ";
            out << p << "=" << representative[(*iter).predicate.id];
         }
         if ((*iter).object.type!=SPARQLParser::Element::Variable) {
            if (first) first=false; else out << " and ";
            unsigned val;
            if (dictionary.count((*iter).object.value))
               val=dictionary[(*iter).object.value]; else
               val=dictionary.size();
            out << o << "=" << val;
         } else if (representative[(*iter).object.id]!=o) {
            if (first) first=false; else out << " and ";
            out << o << "=" << representative[(*iter).object.id];
         }
         ++id;
      }
   }
   out << ") facts";
   {
      unsigned id=0;
      for (SPARQLParser::projection_iterator iter=parser.projectionBegin(),limit=parser.projectionEnd();iter!=limit;++iter) {
         out << ",updatetest.strings s" << id;
         id++;
      }
   }
   out << " where ";
   {
      unsigned id=0;
      for (SPARQLParser::projection_iterator iter=parser.projectionBegin(),limit=parser.projectionEnd();iter!=limit;++iter) {
         if (id) out << " and ";
         out << "s" << id << ".id=facts.r" << id;
         id++;
      }
   }
   out << ";";

   return out.str();
}
//---------------------------------------------------------------------------
void PostgresDriver::prepareDone()
   // The prepare step is done
{
   dictionary.clear();
}
//---------------------------------------------------------------------------
void PostgresDriver::processQuery(const string& query)
   // Process a query
{
   FILE* out=popen("psql","w");
   if (!out) {
      cerr << "warning: psql call failed" << endl;
   }
   fprintf(out,"%s;\n",query.c_str());
   fflush(out);
   fprintf(out,"\\q\n");
   fflush(out);
   pclose(out);
}
//---------------------------------------------------------------------------
unsigned PostgresDriver::processChunk(const string& chunkFile,unsigned delay)
   // Process a chunk of work
{
   FILE* out=popen("psql","w");
   if (!out) {
      cerr << "warning: psql call failed" << endl;
   }
   fprintf(out,"begin transaction;\n");
   fprintf(out,"\\i %s\n",chunkFile.c_str());
   fflush(out);
   if (delay) {
      fprintf(out,"\\! sleep %g\n",static_cast<double>(delay)/1000.0);
      fflush(out);
   }
   fprintf(out,"commit;\n");
   fprintf(out,"\\q\n");
   fflush(out);
   pclose(out);
   return jobSize[chunkFile];
}
//---------------------------------------------------------------------------
unsigned PostgresDriver::processQueryAndChunk(const string& query,const string& chunkFile,unsigned delay)
   // Process a query and a chunk of work
{
   FILE* out=popen("psql","w");
   if (!out) {
      cerr << "warning: psql call failed" << endl;
   }
   fprintf(out,"begin transaction;\n");
   fprintf(out,"%s;\n",query.c_str());
   fflush(out);
   fprintf(out,"\\i %s\n",chunkFile.c_str());
   fflush(out);
   if (delay) {
      fprintf(out,"\\! sleep %g\n",static_cast<double>(delay)/1000.0);
      fflush(out);
   }
   fprintf(out,"commit;\n");
   fprintf(out,"\\q\n");
   fflush(out);
   pclose(out);
   return jobSize[chunkFile];
}
//---------------------------------------------------------------------------
void PostgresDriver::sync()
   // Synchronize to disk
{
}
//---------------------------------------------------------------------------
/// A work description
struct WorkDescription {
   /// The synchronizing mutex
   Mutex mutex;
   /// Notification
   Event event;
   /// The delay model
   unsigned delayModel;
   /// The query model
   unsigned queryModel;
   /// The chunks
   vector<string> chunkFiles;
   /// The queries
   vector<string> queries;
   /// The current work position
   unsigned workPos;
   /// Active workers
   unsigned activeWorkers;
   /// The driver
   Driver* driver;
   /// Total number of processed triples
   unsigned tripleCount;
   /// Total number of processed transactions
   unsigned transactionCount;

   /// Constructor
   WorkDescription() : delayModel(0),queryModel(0),workPos(0),activeWorkers(0),driver(0),tripleCount(0),transactionCount(0) {}
};
//---------------------------------------------------------------------------
static void worker(void* data)
   // A worker thread
{
   WorkDescription& work=*static_cast<WorkDescription*>(data);
   char rndBuffer[32];
   random_data rnd;
   initstate_r(0,rndBuffer,sizeof(rndBuffer),&rnd);

   unsigned processed = 0;
   while (true) {
      // Check for new work
      bool queryMode=false;
      string chunkFile,query;
      work.mutex.lock();
      work.tripleCount+=processed;
      processed=0;
      if ((work.queryModel==1)||(work.queryModel==2)) {
         if ((work.workPos/2)>=work.chunkFiles.size())
            break;
         query=work.queries[(work.workPos/2)%work.queries.size()];
         if (work.workPos&1) {
            chunkFile=work.chunkFiles[work.workPos/2];
         } else {
            queryMode=true;
         }
      } else {
         if (work.workPos>=work.chunkFiles.size())
            break;
         chunkFile=work.chunkFiles[work.workPos];
      }
      work.workPos++;
      work.transactionCount++;
      work.mutex.unlock();

      // Process the chunk
      if (queryMode) {
         work.driver->processQuery(query);
      } else {
         int delay=0;
         if (work.delayModel==1) {
            random_r(&rnd,&delay);
            delay=delay%100;
         }
         if (work.queryModel==2)
            processed=work.driver->processQueryAndChunk(query,chunkFile,delay); else
            processed=work.driver->processChunk(chunkFile,delay);
      }
   }

   work.activeWorkers--;
   work.event.notify(work.mutex);
   work.mutex.unlock();
}
//---------------------------------------------------------------------------
static istream& skipComment(istream& in)
   // Skip comments
{
   while (true) {
      char c=in.peek();
      if ((c==' ')||(c=='\n')||(c=='\r')||(c=='\t')) {
         in.get();
         continue;
      }
      if (c=='#') {
         while (in) {
            c=in.get();
            if ((c=='\n')||(c=='\r'))
               break;
         }
         continue;
      }
      break;
   }
   return in;
}
//---------------------------------------------------------------------------
template <class T> istream& readValue(istream& in,T& value)
   // Read a value from the input stream
{
   // Skip comments
   skipComment(in);

   // Read the entry
   return in >> value;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc!=4) {
      cerr << "usage: " << argv[0] << " <input> <driver> <configuration>" << endl;
      return 1;
   }
   Driver* driver=0;
   if (string(argv[2])=="rdf3x") {
      driver=new RDF3XDriver();
   } else if (string(argv[2])=="postgres") {
      driver=new PostgresDriver();
   } else {
      cerr << "unknown driver " << argv[1] << endl;
      return 1;
   }

   // Read the configuration
   unsigned initialSize,chunkSize,chunkCount,threadCount,delayModel,queryModel;
   vector<string> queries;
   {
      ifstream in(argv[3]);
      if (!in.is_open()) {
         cerr << "unable to open " << argv[3] << endl;
         return 1;
      }
      readValue(in,initialSize);
      readValue(in,chunkSize);
      readValue(in,chunkCount);
      readValue(in,threadCount);
      readValue(in,delayModel);
      readValue(in,queryModel);
      while(true) {
         skipComment(in);
         string s;
         if (!getline(in,s))
            break;
         if (s=="") continue;
         queries.push_back(s);
      }
   }

   // Try to open the input
   ifstream in(argv[1]);
   if (!in.is_open()) {
      cerr << "unable to open " << argv[1] << endl;
      return 1;
   }
   TurtleParser parser(in);

   // Build a small database first
   cerr << "Building initial database..." << endl;
   if (!driver->buildDatabase(parser,initialSize))
      return 1;

   // Prepare some triple chunks
   vector<string> chunkFiles;
   for (unsigned index=0;index<chunkCount;index++) {
      stringstream cname; cname << "updatetest.chunk" << index << ".tmp";
      string name=cname.str();
      driver->prepareChunk(name,parser,chunkSize);
      chunkFiles.push_back(name);
   }
   for (unsigned index=0;index<queries.size();index++)
      queries[index]=driver->prepareQuery(queries[index]);
   driver->prepareDone();

   // Open the database again
   WorkDescription work;
   work.chunkFiles=chunkFiles;
   work.queries=queries;
   work.driver=driver;
   work.delayModel=delayModel;
   work.queryModel=queryModel;

   // Apply some updates
   cerr << "Applying updates..." << endl;
   Timestamp t1;
   if (work.chunkFiles.size()>chunkCount)
      work.chunkFiles.resize(chunkCount);
   work.mutex.lock();
   for (unsigned index=0;index<threadCount;index++) {
      work.activeWorkers++;
      Thread::start(worker,&work);
   }
   while (work.activeWorkers)
      work.event.wait(work.mutex);
   work.mutex.unlock();
   Timestamp t2;
   driver->sync();
   Timestamp t3;

   cerr << "Transaction time: " << (t2-t1) << endl;
   cerr << "Total time: " << (t3-t1) << endl;
   cerr << "Triples/s: " << (work.tripleCount*1000/(t3-t1)) << endl;
   cerr << "Transactions/s: " << (static_cast<double>(work.transactionCount*1000)/(t3-t1)) << endl;

   delete driver;
   for (vector<string>::const_iterator iter=chunkFiles.begin(),limit=chunkFiles.end();iter!=limit;++iter)
       remove((*iter).c_str());

   cerr << "Done." << endl;
}
//---------------------------------------------------------------------------
