import java.io.*;
import com.hp.hpl.jena.graph.GraphEvents;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.sdb.StoreDesc;
import com.hp.hpl.jena.sdb.sql.JDBC;
import com.hp.hpl.jena.sdb.sql.SDBConnection;
import com.hp.hpl.jena.sdb.store.DatabaseType;
import com.hp.hpl.jena.sdb.store.LayoutType;

public class transactiontestjena
{
   // A transaction
   public static final class Transaction {
      // The id
      int id;
      // Arrival timestamp
      long arrival;
      // Wait time
      int delay;
      // The entries
      String subject,object;
      // The predicate
      String[] predicates;

      // Measured response time
      long responseTime;
   };
   // Information for a thread
   public static final class ThreadInfo {
      // The event
      Object eventReader,eventWriter;
      // The active threads
      int activeThreads;
      // The working threads
      int workingThreads;
      // Timing base
      long timeBase;

      static final int maxTransactions = 1024;

      // Stop working?
      boolean done;
      // Tasks
      Transaction[] tasks=new Transaction[maxTransactions];
      // Pointers
      int reader,writer;

      // Constructor
      ThreadInfo() {
         eventReader=new Object();
         eventWriter=new Object();
         activeThreads=0;
         workingThreads=0;
         timeBase=0;
         done=false;
         reader=0;
         writer=0;
      }

      /// Empty?
      boolean empty() { return reader==writer; }
      /// Full?
      boolean full() { return ((writer+1)%maxTransactions)==reader; }
   };

   private static String readEntry(BufferedReader in) throws Exception
   {
      int c;
      while (true) {
         c=in.read();
         if (c<0) return null;
         if (c==' ') break;
      }
      return in.readLine();
   }
   private static int readIntLine(BufferedReader in) throws Exception
   {
      return Integer.parseInt(in.readLine());
   }

   private static final String hex="0123456789abcdef";
   private static String escapeURI(String uri)
   {
      StringBuilder builder=new StringBuilder();
      builder.append("file:///");
      for (int index=0,length=uri.length();index<length;index++) {
         char c=uri.charAt(index);
         if ((c&0xFF)<=' ') {
            builder.append('%');
            builder.append(hex.charAt(c>>4));
            builder.append(hex.charAt(c&0xF));
            continue;
         }
         switch (c) {
            case '<': case '>': case '"': case '{': case '}': case '|': case '^': case '`': case '\\':
            case '[': case ']': case '#':
            case '%':
               builder.append('%');
               builder.append(hex.charAt(c>>4));
               builder.append(hex.charAt(c&0xF));
               break;
            default: builder.append(c); break;
         }
      }
      return builder.toString();
   }
   private static int sleeping = 0;

   private static final class TransactionWorker extends Thread
   {
      private ThreadInfo info;
      TransactionWorker(ThreadInfo info) { this.info=info; }

      public void run() {
         try {
         // Open the store
         StoreDesc storeDesc = new StoreDesc(LayoutType.LayoutTripleNodesHash,DatabaseType.Derby) ;
         String jdbcURL = "jdbc:derby:tempdb";
         SDBConnection conn = new SDBConnection(jdbcURL, null, null) ;
         Store store = SDBFactory.connectStore(conn, storeDesc) ;
         Model model = SDBFactory.connectDefaultModel(store) ;

         // Register the thread
         synchronized(info) {
            info.activeThreads++;
            if (info.activeThreads==1) {
               synchronized(info.eventWriter) { info.eventWriter.notifyAll(); }
            }
         }

         // Work
         while (true) {
            // Take a new transaction
            Transaction transaction;
            synchronized(info) {
               if (info.empty()) {
                  if (info.done)
                     break;
                  transaction=null;
               } else {
                  info.workingThreads++;
                  if (info.full())
                     synchronized(info.eventWriter) { info.eventWriter.notifyAll(); }
                  transaction=info.tasks[info.reader];
                  info.reader=(info.reader+1)%ThreadInfo.maxTransactions;
               }
            }
            if (transaction==null) {
               synchronized(info.eventReader) { info.eventReader.wait(); }
               continue;
            }

            // Lookup the tags
            model.begin();
            for (java.util.Iterator iter=model.query(new SimpleSelector(null,null,model.createResource(transaction.object))).listStatements();iter.hasNext();iter.next()) {
            }

            // Wait
            sleeping++;
            Thread.sleep(transaction.delay);
            sleeping--;

            // Insert new tags
            for (String predicate:transaction.predicates) {
               model.add(model.createStatement(model.createResource(transaction.subject),model.createProperty("",predicate),model.createResource(transaction.object)));
            }
            model.commit();

            // Latch again and check for new tasks
            synchronized(info) {
               transaction.responseTime=(System.currentTimeMillis()-info.timeBase)-transaction.arrival;
               --info.workingThreads;
            }
         }

         // Deregister the thread
         synchronized(info) {
            info.activeThreads--;
            if (info.activeThreads==0) {
               synchronized(info.eventWriter) {
                  info.eventWriter.notifyAll();
               }
            }
         }
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }

   private static java.util.Random rnd=new java.util.Random(1);

   private static int drawExp(double lambda)
      // Draw an exponentially distributed random variable
   {
      int result=(int)(-(lambda*Math.log(1-rnd.nextDouble())));
      if (result<0) result=0;
      if (result>(lambda*10)) result=(int)(lambda*10);
      return result;
   }

   public static void main(String[] args) throws Exception
   {
      // Check the arguments
      if (args.length!=2) {
         System.out.println("usage: transactiontestjena <transactionlog> <mode>");
         return;
      }
      int mode;
      if ("insert".equals(args[1])) {
         mode=0;
      } else if ("transaction".equals(args[1])) {
         mode=1;
      } else {
         System.out.println("unknown execution mode "+args[1]);
         return;
      }

      // Produce an input file
      BufferedReader in=new BufferedReader(new FileReader(args[0]));
      int simpleCounts,transactionCounts,initialTransactions,initialTriples=0;
      simpleCounts=readIntLine(in);
      {
         PrintWriter out=new PrintWriter(new FileWriter("transactions.tmp"));
         for (int index=0;index<simpleCounts;index++) {
            String s=readEntry(in);
            String p=readEntry(in);
            String o=readEntry(in);
            out.println("<"+escapeURI(s)+"> <"+escapeURI(p)+"> <"+escapeURI(o)+">.");
            ++initialTriples;
         }
         transactionCounts=readIntLine(in);
         initialTransactions=transactionCounts/2;
         for (int index=0;index<initialTransactions;index++) {
            int tagCount=readIntLine(in);
            String s=readEntry(in);
            String o=readEntry(in);
            for (int index2=0;index2<tagCount;index2++) {
               String p=readEntry(in);
               out.println("<"+escapeURI(s)+"> <"+escapeURI(p)+"> <"+escapeURI(o)+">.");
               ++initialTriples;
            }
         }
         out.flush();
         out.close();
      }

      // Build the initial database
      StoreDesc storeDesc = new StoreDesc(LayoutType.LayoutTripleNodesHash,DatabaseType.Derby) ;
      JDBC.loadDriverDerby() ;
      String jdbcURL = "jdbc:derby:tempdb;create=true";
      SDBConnection conn = new SDBConnection(jdbcURL, null, null) ;
      Store store = SDBFactory.connectStore(conn, storeDesc) ;
      Model model;
      {
         long start=System.currentTimeMillis();
         store.getTableFormatter().create();
         model=SDBFactory.connectDefaultModel(store);
         model.read(new FileInputStream("transactions.tmp"),"","N-TRIPLE");
         long stop=System.currentTimeMillis();
         System.out.println("bulkload: "+(stop-start)+"ms for "+initialTriples+" triples");
         System.out.println("triples per second: "+(((double)initialTriples)/(((double)stop-start)/1000.0)));
      }

      if (mode==0) {
         long start=System.currentTimeMillis();
         model.begin();
         model.notifyEvent(GraphEvents.startRead);
         int total=0,inMem=0;
         for (int index=initialTransactions;index<transactionCounts;index++) {
            int tagCount=readIntLine(in);
            String s=readEntry(in);
            String o=readEntry(in);
            for (int index2=0;index2<tagCount;index2++) {
               String p=readEntry(in);
               model.add(model.createStatement(model.createResource(s),model.createProperty("",p),model.createResource(o)));
               ++total; ++inMem;
            }
         }
         model.notifyEvent(GraphEvents.finishRead);
         model.commit();
         long stop=System.currentTimeMillis();
         System.out.println("incremental load: "+(stop-start)+"ms for "+(transactionCounts-initialTransactions)+" transactions, "+total+" triples");
         System.out.println("transactions per second: "+(((double)transactionCounts-initialTransactions)/(((double)stop-start)/1000.0)));
         System.out.println("triples per second: "+(((double)total)/(((double)stop-start)/1000.0)));
      } else {
         String outputFile="results-jena-"+args[0];
         PrintWriter out=new PrintWriter(new FileWriter(outputFile));

         // Vary the arrival rates
         final double lambda2 = 2000;
         final int[] lambdaSteps=new int[]{300,200,100,80,60,40,20,10,5,0,-1};
         for (int chunk=0;lambdaSteps[chunk]>=0;++chunk) {
            int lambda=lambdaSteps[chunk];
            final int chunkSize = 10000;
            int from=initialTransactions+chunk*chunkSize,to=from+chunkSize;
            if (from>transactionCounts) from=transactionCounts;
            if (to>transactionCounts) to=transactionCounts;

            // Prepare transactions
            int total=0;
            Transaction[] transactions=new Transaction[to-from];
            int nextEvent = 0;
            for (int index=from;index<to;index++) {
               nextEvent+=drawExp(lambda);
               transactions[index-from]=new Transaction();
               transactions[index-from].id=index-initialTransactions;
               transactions[index-from].arrival=nextEvent;
               transactions[index-from].delay=drawExp(lambda2);

               int tagCount=readIntLine(in);
               String s=readEntry(in);
               String o=readEntry(in);
               transactions[index-from].subject=s;
               transactions[index-from].object=o;
               transactions[index-from].predicates=new String[tagCount];
               for (int index2=0;index2<tagCount;index2++) {
                  String p=readEntry(in);
                  transactions[index-from].predicates[index2]=p;
                  ++total;
               }
            }

            // Start the threads
            long start=System.currentTimeMillis();
            ThreadInfo threads=new ThreadInfo();
            final int initialThreadCount = 10;
            for (int index=0;index<initialThreadCount;index++)
               (new TransactionWorker(threads)).start();
            while (true) {
               synchronized(threads) {
                  if (threads.activeThreads>0)
                     break;
               }
               synchronized(threads.eventWriter) {
                  threads.eventWriter.wait();
               }
            }

            // Process the transactions
            threads.timeBase=System.currentTimeMillis();
            long lastShow=0,lastCreated=0;
            for (int index=0,limit=transactions.length;index<limit;) {
               long now=System.currentTimeMillis()-threads.timeBase;
               while ((!threads.full())&&(now>=transactions[index].arrival)) {
                  synchronized(threads) {
                     threads.tasks[threads.writer]=transactions[index];
                     threads.writer=(threads.writer+1)%ThreadInfo.maxTransactions;
                     synchronized(threads.eventReader) { threads.eventReader.notify(); }
                  }
                  if ((++index)>=limit)
                     break;
               }
               if (index>=limit)
                  break;
               if (now>(lastShow+10000)) {
                  System.out.println(""+threads.reader+" "+threads.writer+" "+threads.activeThreads+" "+sleeping+" "+threads.workingThreads);
                  lastShow=now;
               }
               if ((threads.activeThreads==threads.workingThreads)&&(now>lastCreated)&&(threads.activeThreads<100)) {
                  (new TransactionWorker(threads)).start();
                  lastCreated=now;
               }
               synchronized(threads.eventWriter) {
                  if (now<transactions[index].arrival)
                     threads.eventWriter.wait(transactions[index].arrival-now); else
                     threads.eventWriter.wait();
               }
            }

            // Shutdown
            synchronized(threads) { threads.done=true; }
            synchronized(threads.eventReader) { threads.eventReader.notifyAll(); }
            while (threads.activeThreads>0)
               synchronized(threads.eventWriter) { threads.eventWriter.wait(100); }

            long stop=System.currentTimeMillis();
            System.out.println("transactional load: "+(stop-start)+"ms for "+transactions.length+" transactions, "+total+" triples");
            System.out.println("transactions per second: "+(((double)transactions.length)/(((double)stop-start)/1000.0)));
            System.out.println("triples per second: "+(((double)total)/(((double)stop-start)/1000.0)));

            out.println(""+lambda+" "+(stop-start)+" "+transactions.length+" "+total);
            double totalResponse=0;
            for (int index=0,limit=transactions.length;index<limit;++index) {
               if (index>0) out.print(" ");
               out.print(transactions[index].responseTime);
               totalResponse+=transactions[index].responseTime;
            }
            out.println();
            out.flush();
            System.out.println("average response time: "+(totalResponse/((double)transactions.length)));
         }
      }
   }
}
