#include "infra/osdep/Event.hpp"
#include "infra/osdep/Thread.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/database/Database.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/runtime/BulkOperation.hpp"
#include "rts/runtime/DifferentialIndex.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cmath>
#include <unistd.h>
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
   string subject,object;
   /// The predicate
   vector<string> predicates;

   /// Measured response time
   unsigned responseTime;
};
//---------------------------------------------------------------------------
/// Information for a thread
struct ThreadInfo {
   /// The synchronization mutex
   Mutex mutex;
   /// The event
   Event eventReader,eventWriter;
   /// The differential index
   DifferentialIndex& diff;
   /// The locks
   PredicateLockManager& locks;
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
   ThreadInfo(DifferentialIndex& diff,PredicateLockManager& locks) : diff(diff),locks(locks),activeThreads(0),workingThreads(0),done(false),reader(0),writer(0) {}

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

   out << "<";
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
   out << ">";
}
//---------------------------------------------------------------------------
static unsigned sleeping;
//---------------------------------------------------------------------------
static void transactionWorker(void* i)
{
   ThreadInfo& info=*static_cast<ThreadInfo*>(i);

   // Register the thread
   info.mutex.lock();
   info.activeThreads++;
   if (info.activeThreads==1)
      info.eventWriter.notifyAll(info.mutex);

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
         unsigned objectId=0;
         if (info.diff.lookup(transaction.object,Type::URI,0,objectId)) {
            unsigned delay=0;
            while (!info.locks.lock(transaction.id,PredicateLockManager::Box(0,~0u,0,~0u,objectId,objectId),true)) {
               Thread::sleep(1<<delay);
               if (delay<10) delay++;
            }
         }
         Register predReg,objReg;
         predReg.reset(); objReg.reset();
         objReg.value=objectId;
         Operator* op=info.diff.createAggregatedScan(Database::Order_Object_Predicate_Subject,0,false,&predReg,false,&objReg,true,0);
         if (op->first()) do {
         } while (op->next());
         delete op;

         // Wait
         sleeping++;
         Thread::sleep(transaction.delay);
         sleeping--;

         // Insert new tags
         BulkOperation bulk(info.diff);
         for (vector<string>::const_iterator iter=transaction.predicates.begin(),limit=transaction.predicates.end();iter!=limit;++iter) {
            bulk.insert(transaction.subject,*iter,transaction.object,Type::URI,empty);
         }
         bulk.commit();

         // Release the lock
         if (objectId)
            info.locks.finished(transaction.id);

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
      ofstream out("transactions.tmp");
      for (unsigned index=0;index<simpleCounts;index++) {
         unsigned s,p,o;
         string ss,ps,os;
         readEntry(in,s,ss);
         readEntry(in,p,ps);
         readEntry(in,o,os);
         writeURI(out,ss);
         out << " ";
         writeURI(out,ps);
         out << " ";
         writeURI(out,os);
         out << ".\n";
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
            writeURI(out,ss);
            out << " ";
            writeURI(out,ps);
            out << " ";
            writeURI(out,os);
            out << ".\n";
            ++initialTriples;
         }
      }
      out.flush();
      out.close();
   }

   // Build the initial database
   {
      Timestamp start;
      string command=string("./bin/rdf3xload ")+argv[1]+" transactions.tmp";
      if (system(command.c_str())!=0) {
         cerr << "build failed" << endl;
         remove("transactions.tmp");
         return 1;
      }
      remove("transactions.tmp");
      sync();
      Timestamp stop;

      cout << "bulkload: " << (stop-start) << "ms for " << initialTriples << " triples" << endl;
      cout << "triples per second: " << (static_cast<double>(initialTriples)/(static_cast<double>(stop-start)/1000.0)) << endl;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],false)) {
      cout << "unable to open database " << argv[1] << endl;
      return 1;
   }

   if (mode==InsertMode) {
      Timestamp start;
      DifferentialIndex diff(db);
      unsigned total=0,inMem=0;
      string empty;
      for (unsigned index=initialTransactions;index<transactionCounts;index++) {
         BulkOperation bulk(diff);
         unsigned tagCount;
         in >> tagCount;
         unsigned s,p,o;
         string ss,ps,os;
         readEntry(in,s,ss);
         readEntry(in,o,os);
         for (unsigned index2=0;index2<tagCount;index2++) {
            readEntry(in,p,ps);
            bulk.insert(ss,ps,os,Type::URI,empty);
            ++total; ++inMem;
         }
         bulk.commit();
         if (inMem>500000) {
            cout << "sync start..." << endl;
            diff.sync();
            cout << "sync done..." << endl;
            inMem=0;
         }
      }
      diff.sync();
      Timestamp stop;
      cout << "incremental load: " << (stop-start) << "ms for " << (transactionCounts-initialTransactions) << " transactions, " << total << " triples" << endl;
      cout << "transactions per second: " << (static_cast<double>(transactionCounts-initialTransactions)/(static_cast<double>(stop-start)/1000.0)) << endl;
      cout << "triples per second: " << (static_cast<double>(total)/(static_cast<double>(stop-start)/1000.0)) << endl;
   } else {
      string outputFile="results-rdf3x-";
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
            transactions[index-from].subject=ss;
            transactions[index-from].object=os;
            for (unsigned index2=0;index2<tagCount;index2++) {
               readEntry(in,p,ps);
               transactions[index-from].predicates.push_back(ps);
               ++total;
            }
         }

         // Start the threads
         Timestamp start;
         DifferentialIndex diff(db);
         PredicateLockManager locks;
         ThreadInfo threads(diff,locks);
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
            if ((threads.activeThreads==threads.workingThreads)&&(now>lastCreated)&&(threads.activeThreads<1000)) {
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

         diff.sync();
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
