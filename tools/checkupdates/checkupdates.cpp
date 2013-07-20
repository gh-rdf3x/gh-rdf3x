#include "cts/parser/TurtleParser.hpp"
#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
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
#include <vector>
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

//---------------------------------------------------------------------------
/// A RDF-3X driver
class RDF3XDriver
{
public:
    struct Triple {
        /// Entries
        string subject,predicate,object;
        Type::ID objectType;
        string objectSubType;

        Triple(string s, string p, string o, Type::ID t, string st):
                subject(s), predicate(p), object(o), objectType(t), objectSubType(st)
        {}

        friend ostream &operator<<(ostream &out, Triple t){
        	out<<"<"<<t.subject<<"> <"<<t.predicate<<"> <"<<t.object<<">.";
        	return out;
        }
    };

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
    /// vector of all triples in the database; we need it to randomly select triples for updates
    vector<Triple> allTriples;
    /// vector of all triples that we inserted as chunks during updates
    vector<Triple> insertedTriples;
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

    void loadAllDataset(const char* name);
    void checkUpdates();
    void prepareTriples(const char* name, const char* output, unsigned tripleCount);
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
    //remove("updatetest.2.tmp");
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
        case '\t':
            out << "\\t";
            break;
        case '\n':
            out << "\\n";
            break;
        case '\r':
            out << "\\r";
            break;
        case '>':
            out << "\\>";
            break;
        case '\\':
            out << "\\\\";
            break;
        default:
            out << c;
            break;
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
        case '\t':
            out << "\\t";
            break;
        case '\n':
            out << "\\n";
            break;
        case '\r':
            out << "\\r";
            break;
        case '\"':
            out << "\\\"";
            break;
        case '\\':
            out << "\\\\";
            break;
        default:
            out << c;
            break;
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
        string subject,predicate,object;
        Type::ID objectType;
        string objectSubType;
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
void RDF3XDriver::loadAllDataset(const char* input)
{
    ifstream in(input);
    TurtleParser parser(in);
    string subject,predicate,object;
    Type::ID objectType;
    string objectSubType;

    while (true) {
        if (!parser.parse(subject,predicate,object,objectType,objectSubType))
            break;
        //  bulk.insert(subject,predicate,object,objectType,objectSubType);

        allTriples.push_back(Triple(subject,predicate,object,objectType,objectSubType));
    }
}


//---------------------------------------------------------------------------
static bool findTriple(RDF3XDriver::Triple t, Database& db, DifferentialIndex& diff) {
    FactsSegment::Scan scan;
    DictionarySegment dict = db.getDictionary();

    ostringstream os1, os2, os3;
    unsigned iSubject, iPredicate, iObject;

    os1 << t.subject;
    if (!dict.lookup(os1.str(),Type::URI, 0, iSubject)) {
        cout<<" (subject not found) ";
        if (diff.lookup(os1.str(),Type::URI, 0, iSubject)) {
            cout<<" (subject in diff index) "<<endl;
        }
        else
            cout<<" (subject not in diff index) "<<endl;
        return false;
    }


    if (!db.getDictionary().lookup(t.object,Type::URI,0, iObject)) {
        cout<<" (object not found) ";
        if (diff.lookup(t.object,Type::URI, 0, iSubject)) {
            cout<<" (object in diff index) ";
        }
        else
            cout<<" (object not in diff index) ";
        return false;
    }

    int count = 0;
    if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object),iSubject,0,0)) do {

            if (scan.getValue1() != iSubject) {
                break;
            }
            //  cout<<lookupId(db, scan.getValue1())<<" "<<lookupId(db, scan.getValue2())<<" "<<lookupId(db, scan.getValue3())<<endl;

            //  string predicate = lookupId(db, scan.getValue2());
            if ((scan.getValue3() == iObject))
                count++;
        } while (scan.next());
    return (count > 0);

}

//---------------------------------------------------------------------------
void RDF3XDriver::checkUpdates()
{
    cout<<"testing the updates..."<<endl;
    for (vector<Triple>::iterator it = insertedTriples.begin(); it != insertedTriples.end(); it++) {
       // cout<<"triple ("<<it->subject<<", "<<it->predicate<<", "<<it->object<<")";

        assert(findTriple(*it, db, *diff));
       // cout<<" OK"<<endl;
    }
    cout<<"updates OK"<<endl;
}

void RDF3XDriver::prepareTriples(const char* name, const char* output, unsigned tripleCount){
    ofstream out(output);
    loadAllDataset(name);

    for (unsigned i=0; i < tripleCount; i++){
    	Triple t = allTriples[rand()%allTriples.size()];
    	insertedTriples.push_back(t);
    	out<<t<<endl;
    }
    out.flush();
}

//---------------------------------------------------------------------------
void RDF3XDriver::prepareChunk(const string& name,TurtleParser& parser,unsigned chunkSize)
// Prepare a chunk of work
{
    ofstream out(name.c_str());
    string subject, predicate, object;
    Type::ID objectType;
    string objectSubType;


    for (unsigned index2=0;index2<chunkSize;index2++) {
        if (!parser.parse(subject,predicate,object,objectType,objectSubType)) {
            cerr<<"stop parsing"<<endl;
            break;
        }
        dumpSubject(out,subject);
        out << " ";
        dumpPredicate(out,predicate);
        out << " ";
        dumpObject(out,object,objectType,objectSubType);
        out << "." << endl;
       // insertedTriples.push_back(Triple(subject,predicate,object,objectType,objectSubType));
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
    string subject,predicate,object;
    Type::ID objectType;
    string objectSubType;
    while (true) {
        if (!parser.parse(subject,predicate,object,objectType,objectSubType))
            break;
        //cout<<subject<<" "<<predicate<<" "<<object<<endl;
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
    string subject,predicate,object;
    Type::ID objectType;
    string objectSubType;
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
    RDF3XDriver* driver;
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
            if (work.workPos>=work.chunkFiles.size()) {
                //cout<<"work pos "<<work.workPos<<" "<<work.chunkFiles.size()<<endl;
                break;
            }
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
                processed=work.driver->processQueryAndChunk(query,chunkFile,delay);
            else {
                processed=work.driver->processChunk(chunkFile,delay);

            }
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
    RDF3XDriver* driver=0;
    if (string(argv[2])=="rdf3x") {
        driver=new RDF3XDriver();
    }  else {
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
        while (true) {
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
    driver->prepareTriples(argv[1],"updatetest.3.tmp",chunkSize*chunkCount);


    ifstream in_updates("updatetest.3.tmp");
    TurtleParser parser_updates(in_updates);
    vector<string> chunkFiles;
    for (unsigned index=0;index<chunkCount;index++) {
        stringstream cname;
        cname << "updatetest.chunk" << index << ".tmp";
        string name=cname.str();
        driver->prepareChunk(name,parser_updates,chunkSize);
        chunkFiles.push_back(name);
    }


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

    driver->checkUpdates();

    cerr << "Transaction time: " << (t2-t1) << endl;
    cerr << "Total time: " << (t3-t1) << endl;
    cerr << "Triples/s: " << (work.tripleCount*1000/(t3-t1)) << endl;
    cerr << "Transactions/s: " << (static_cast<double>(work.transactionCount*1000)/(t3-t1)) << endl;

    delete driver;
    for (vector<string>::const_iterator iter=chunkFiles.begin(),limit=chunkFiles.end();iter!=limit;++iter)
        remove((*iter).c_str());

    remove("updatetest.3.tmp");
    cerr << "Done." << endl;
}
//---------------------------------------------------------------------------
