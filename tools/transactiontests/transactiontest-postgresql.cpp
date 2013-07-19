#include "infra/osdep/Event.hpp"
#include "infra/osdep/Mutex.hpp"
#include "infra/osdep/Thread.hpp"
#include "infra/osdep/Timestamp.hpp"
#include <pqxx/connection>
#include <pqxx/nontransaction>
#include <pqxx/transaction>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <cmath>
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
/// A transaction
struct Transaction {
   /// The id
   unsigned id;
   /// Arrival timestamp
   unsigned arrival;
   /// Wait time
   unsigned delay;
   /// The entries
   unsigned subject,object;
   /// The predicate
   vector<unsigned> predicates;

   /// The response time
   unsigned responseTime;
};
//---------------------------------------------------------------------------
/// Information for a thread
struct ThreadInfo {
   /// The synchronization mutex
   Mutex mutex;
   /// The event
   Event eventReader,eventWriter;
   /// The active threads
   unsigned activeThreads;
   /// The working threads
   unsigned workingThreads;
   /// Timing base
   uint64_t timeBase;

   static const unsigned maxTransactions = 1024;

   /// Stop working?
   bool done;
   /// Tasks
   Transaction* tasks[maxTransactions];
   /// Pointers
   unsigned reader,writer;

   /// Constructor
   ThreadInfo() : activeThreads(0),workingThreads(0),timeBase(0),done(false),reader(0),writer(0) { }

   /// Empty?
   bool empty() const { return reader==writer; }
   /// Full?
   bool full() const { return ((writer+1)%maxTransactions)==reader; }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
static void readEntry(ifstream& in,unsigned& id,string& value)
{
   in >> id;
   in.get();
   getline(in,value);

   if ((value.length()>0)&&(value[value.length()-1]=='\n'))
      value.resize(value.length()-1);
}
//---------------------------------------------------------------------------
static void writeURI(ofstream& out,const string& value)
{
   static const char hex[]="0123456789abcdef";

   out << "\"";
   for (string::const_iterator iter=value.begin(),limit=value.end();iter!=limit;++iter) {
      char c=*iter;
      if ((c&0xFF)<=' ') {
         out << "%" << hex[c>>4] << hex[c&0x0F];
         continue;
      }
      switch (c) {
         case '<': case '>': case '"': case '{': case '}': case '|': case '^': case '`': case '\\':
         case '%':
            out << "%" << hex[c>>4] << hex[c&0x0F];
            break;
         default: out << c; break;
      }
   }
   out << "\"";
}
//---------------------------------------------------------------------------
static unsigned sleeping;
//---------------------------------------------------------------------------
static void transactionWorker(void* i)
{
   ThreadInfo& info=*static_cast<ThreadInfo*>(i);

   // Register the thread
   info.mutex.lock();
   if (info.activeThreads==100) { // safe-guard against too many PostgreSQL connections
      info.mutex.unlock();
      return;
   }
   info.activeThreads++;
   if (info.activeThreads==1)
      info.eventWriter.notifyAll(info.mutex);

   // Open a connection
   pqxx::connection con;

   // Work
   const string empty;
   while ((!info.empty())||(!info.done)) {
      if (info.empty())
         info.eventReader.wait(info.mutex);

      if (!info.empty()) {
         ++info.workingThreads;
         // Take a new transaction
         if (info.full())
            info.eventWriter.notifyAll(info.mutex);
         Transaction& transaction=*info.tasks[info.reader];
         info.reader=(info.reader+1)%ThreadInfo::maxTransactions;
         info.mutex.unlock();

         // Lookup the tags
         pqxx::transaction<> trans(con);
         try {
            stringstream sql;
            sql << "select predicate from transactiontest.facts where object=" << transaction.object << ";";
            trans.exec(sql.str());
         } catch (const exception &e) {
            cerr << e.what() << endl;
            throw;
         }

         // Wait
         sleeping++;
         Thread::sleep(transaction.delay);
         sleeping--;

         // Insert new tags
         try {
            stringstream sql;
            sql << "insert into transactiontest.facts values ";
            for (vector<unsigned>::const_iterator start=transaction.predicates.begin(),limit=transaction.predicates.end(),iter=start;iter!=limit;++iter) {
               if (iter!=start) sql << ",";
               sql << "(" << transaction.subject << "," << (*iter) << "," << transaction.object << ")";
            }
            sql << ";";
            trans.exec(sql.str());
         } catch (const exception &e) {
            cerr << e.what() << endl;
            throw;
         }
         trans.commit();

         // Latch again and check for new tasks
         info.mutex.lock();
         transaction.responseTime=(Thread::getTicks()-info.timeBase)-transaction.arrival;
         --info.workingThreads;
      }
   }

   // Deregister the thread
   info.activeThreads--;
   if (info.activeThreads==0)
      info.eventWriter.notifyAll(info.mutex);
   info.mutex.unlock();
}
//---------------------------------------------------------------------------
static unsigned drawExp(double lambda)
   // Draw an exponentially distributed random variable
{
   unsigned result=-(lambda*log(1.0-drand48()));
   if (result>10*lambda)
      result=10*lambda;
   return result;
}
//---------------------------------------------------------------------------
static void execV(pqxx::nontransaction& trans,const char* sql)
   // Execute a statement and print debug output
{
   cout << sql << endl;
   try {
      trans.exec(sql);
   } catch (const exception &e) {
      cerr << e.what() << endl;
      throw;
   }
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<4) {
      cout << "usage: " << argv[0] << " <database> <transactionlog> <mode>" << endl;
      return 1;
   }
   enum { InsertMode, TransactionMode } mode;
   if (string(argv[3])=="insert") {
      mode=InsertMode;
   } else if (string(argv[3])=="transaction") {
      mode=TransactionMode;
   } else {
      cout << "unknown execution mode " << argv[3] << endl;
      return 1;
   }

   // Produce an input file
   ifstream in(argv[2]);
   if (!in.is_open()) {
      cout << "unable to open transaction log " << argv[2] << endl;
      return 1;
   }
   unsigned simpleCounts,transactionCounts,initialTransactions,initialTriples=0;
   in >> simpleCounts;
   {
      ofstream out("/tmp/facts.tmp"),out2("/tmp/strings.tmp");
      vector<bool> seen;
      for (unsigned index=0;index<simpleCounts;index++) {
         unsigned s,p,o;
         string ss,ps,os;
         readEntry(in,s,ss);
         readEntry(in,p,ps);
         readEntry(in,o,os);

         unsigned m=max(s,max(p,o));
         if (m>=seen.size())
            seen.resize(m+1024+(seen.size()/8));

         if (!seen[s]) {
            out2 << s << "\t";
            writeURI(out2,ss);
            out2 << "\n";
            seen[s]=true;
         }
         if (!seen[p]) {
            out2 << p << "\t";
            writeURI(out2,ps);
            out2 << "\n";
            seen[p]=true;
         }
         if (!seen[o]) {
            out2 << o << "\t";
            writeURI(out2,os);
            out2 << "\n";
            seen[o]=true;
         }

         out << s << "\t" << p << "\t" << o << "\n";
         ++initialTriples;
      }
      in >> transactionCounts;
      initialTransactions=transactionCounts/2;
      for (unsigned index=0;index<initialTransactions;index++) {
         unsigned tagCount;
         in >> tagCount;
         unsigned s,p,o;
         string ss,ps,os;
         readEntry(in,s,ss);
         readEntry(in,o,os);
         for (unsigned index2=0;index2<tagCount;index2++) {
            readEntry(in,p,ps);

            unsigned m=max(s,max(p,o));
            if (m>=seen.size())
               seen.resize(m+1024+(seen.size()/8));

            if (!seen[s]) {
               out2 << s << "\t";
               writeURI(out2,ss);
               out2 << "\n";
               seen[s]=true;
            }
            if (!seen[p]) {
               out2 << p << "\t";
               writeURI(out2,ps);
               out2 << "\n";
               seen[p]=true;
            }
            if (!seen[o]) {
               out2 << o << "\t";
               writeURI(out2,os);
               out2 << "\n";
               seen[o]=true;
            }

            out << s << "\t" << p << "\t" << o << "\n";
            ++initialTriples;
         }
      }
      out.flush();
      out.close();
      out2.flush();
      out2.close();
   }

   // Build the initial database
   {
      Timestamp start;
      {
         pqxx::connection con;
         pqxx::nontransaction trans(con);
         execV(trans,"drop schema if exists transactiontest cascade;");
         execV(trans,"create schema transactiontest;");
         execV(trans,"create table transactiontest.facts(subject int not null, predicate int not null, object int not null);");
         execV(trans,"copy transactiontest.facts from '/tmp/facts.tmp';");
         remove("/tmp/facts.tmp");
         execV(trans,"create index facts_osp on transactiontest.facts (object, subject, predicate);");
         // execV(trans,"create index facts_pso on transactiontest.facts (predicate, subject, object);");
         execV(trans,"create index facts_pos on transactiontest.facts (predicate, object, subject);");
         execV(trans,"create table transactiontest.strings(id int not null primary key, value varchar(16384) not null);");
         execV(trans,"copy transactiontest.strings from '/tmp/strings.tmp';");
         remove("/tmp/strings.tmp");
         execV(trans,"analyze;");
         // simulate dictionary construction
         execV(trans,"with idmap as (select s1.id as oldid,s2.id as newid from transactiontest.strings s1,(select value,min(id) as id from transactiontest.strings group by value) s2 where s1.value=s2.value) select s1.newid,s2.newid,s3.newid from transactiontest.facts f,idmap s1,idmap s2,idmap s3 where f.subject=s1.oldid and f.predicate=s2.oldid and f.predicate=s3.oldid;");
      }
      sync();
      Timestamp stop;

      cout << "bulkload: " << (stop-start) << "ms for " << initialTriples << " triples" << endl;
      cout << "triples per second: " << (static_cast<double>(initialTriples)/(static_cast<double>(stop-start)/1000.0)) << endl;
   }

   // Open the database
   if (mode==InsertMode) {
      pqxx::connection con;
      Timestamp start;
      unsigned total=0;
      pqxx::transaction<> trans(con);
      for (unsigned index=initialTransactions;index<transactionCounts;index++) {
         unsigned tagCount;
         in >> tagCount;
         unsigned s,p,o;
         string ss,ps,os;
         readEntry(in,s,ss);
         readEntry(in,o,os);

         stringstream sql;
         sql << "insert into transactiontest.facts values ";
         for (unsigned index2=0;index2<tagCount;index2++) {
            if (index2) sql << ",";
            sql << "(" << s << "," << p << "," << o << ")";
            ++total;;
         }
         sql << ";";

         trans.exec(sql.str());
      }
      trans.commit();
      Timestamp stop;
      cout << "incremental load: " << (stop-start) << "ms for " << (transactionCounts-initialTransactions) << " transactions, " << total << " triples" << endl;
      cout << "transactions per second: " << (static_cast<double>(transactionCounts-initialTransactions)/(static_cast<double>(stop-start)/1000.0)) << endl;
      cout << "triples per second: " << (static_cast<double>(total)/(static_cast<double>(stop-start)/1000.0)) << endl;
   } else {
      string outputFile="results-postgresql-";
      outputFile+=argv[2];
      ofstream out(outputFile.c_str());

      // Vary the arrival rates
      static const double lambda2 = 2000;
      static const int lambdaSteps[]={300,200,100,80,60,40,20,10,5,0,-1};
      for (unsigned chunk=0;lambdaSteps[chunk]>=0;++chunk) {
         unsigned lambda=lambdaSteps[chunk];
         static const unsigned chunkSize = 10000;
         unsigned from=initialTransactions+chunk*chunkSize,to=from+chunkSize;
         if (from>transactionCounts) from=transactionCounts;
         if (to>transactionCounts) to=transactionCounts;

         // Prepare transactions
         unsigned total=0;
         vector<Transaction> transactions;
         transactions.resize(to-from);
         unsigned nextEvent = 0;
         for (unsigned index=from;index<to;index++) {
            nextEvent+=drawExp(lambda);
            transactions[index-from].id=index-initialTransactions;
            transactions[index-from].arrival=nextEvent;
            transactions[index-from].delay=drawExp(lambda2);

            unsigned tagCount;
            in >> tagCount;
            unsigned s,p,o;
            string ss,ps,os;
            readEntry(in,s,ss);
            readEntry(in,o,os);
            transactions[index-from].subject=s;
            transactions[index-from].object=o;
            for (unsigned index2=0;index2<tagCount;index2++) {
               readEntry(in,p,ps);
               transactions[index-from].predicates.push_back(p);
               ++total;
            }
         }

         // Start the threads
         Timestamp start;
         ThreadInfo threads;
         static const unsigned initialThreadCount = 10;
         for (unsigned index=0;index<initialThreadCount;index++)
            Thread::start(transactionWorker,&threads);
         threads.mutex.lock();
         while (!threads.activeThreads)
            threads.eventWriter.wait(threads.mutex);

         // Process the transactions
         threads.timeBase=Thread::getTicks();
         uint64_t lastShow=0,lastCreated=0;
         for (unsigned index=0,limit=transactions.size();index<limit;) {
            uint64_t now=Thread::getTicks()-threads.timeBase;
            while ((!threads.full())&&(now>=transactions[index].arrival)) {
               threads.tasks[threads.writer]=&(transactions[index]);
               threads.writer=(threads.writer+1)%ThreadInfo::maxTransactions;
               threads.eventReader.notify(threads.mutex);
               if ((++index)>=limit)
                  break;
            }
            if (index>=limit)
               break;
            if (now>(lastShow+10000)) {
               cout << threads.reader << " " << threads.writer << " " << threads.activeThreads << " " << sleeping << " " << threads.workingThreads << endl;
               lastShow=now;
            }
            if ((threads.activeThreads==threads.workingThreads)&&(now>lastCreated)&&(threads.activeThreads<100)) {
               Thread::start(transactionWorker,&threads);
               lastCreated=now;
            }
            if (now<transactions[index].arrival)
               threads.eventWriter.timedWait(threads.mutex,transactions[index].arrival-now); else
               threads.eventWriter.wait(threads.mutex);
         }

         // Shutdown
         threads.done=true;
         threads.eventReader.notifyAll(threads.mutex);
         while (threads.activeThreads)
            threads.eventWriter.wait(threads.mutex);
         threads.mutex.unlock();

         Timestamp stop;
         cout << "transactional load: " << (stop-start) << "ms for " << transactions.size() << " transactions, " << total << " triples" << endl;
         cout << "transactions per second: " << (static_cast<double>(transactions.size())/(static_cast<double>(stop-start)/1000.0)) << endl;
         cout << "triples per second: " << (static_cast<double>(total)/(static_cast<double>(stop-start)/1000.0)) << endl;

         out << lambda << " " << (stop-start) << " " << transactions.size() << " " << total << endl;
         double totalResponse=0;
         for (unsigned index=0,limit=transactions.size();index<limit;++index) {
            if (index) out << " ";
            out << transactions[index].responseTime;
            totalResponse+=transactions[index].responseTime;
         }
         out << endl;
         out.flush();
         cout << "average response time: " << (totalResponse/static_cast<double>(transactions.size())) << endl;
      }
   }
}
//---------------------------------------------------------------------------
