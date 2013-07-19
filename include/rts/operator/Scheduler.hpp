#ifndef H_rts_operator_Scheduler
#define H_rts_operator_Scheduler
//---------------------------------------------------------------------------
#include "infra/osdep/Event.hpp"
#include "infra/osdep/Mutex.hpp"
#include <vector>
#include <set>
//---------------------------------------------------------------------------
class Operator;
//---------------------------------------------------------------------------
/// Executes an execution plan, potentially multi-threaded
class Scheduler
{
   public:
   /// An synchronous execution point
   class AsyncPoint {
      public:
      /// Destructor
      virtual ~AsyncPoint();
      /// Run it
      virtual void run() = 0;
   };

   private:
   /// A registered execution point
   struct RegisteredPoint {
      /// The async point
      AsyncPoint& point;
      /// The point class
      unsigned schedulingClass;
      /// The priority
      double priority;
      /// Unfinished executions points this one depends on
      std::set<RegisteredPoint*> dependencies;

      /// Constructor
      RegisteredPoint(AsyncPoint& point,unsigned schedulingClass,double priority);
   };
   /// The registered execution points
   std::vector<RegisteredPoint*> registeredPoints;
   /// A synchronization lock
   Mutex workerLock;
   /// Notification
   Event workerSignal;
   /// The queue of tasks
   std::vector<RegisteredPoint*> workQueue;
   /// The number of threads to use
   unsigned threads;
   /// The number of active threads
   volatile unsigned activeWorkers,workerThreads;
   /// Should the workers die?
   volatile bool workersDie;

   /// Perform the work of a worker thread
   void performWork();
   /// Entry point for worker threads
   static void asyncWorker(void*);

   public:
   /// Constructor
   Scheduler();
   /// Destructor
   ~Scheduler();

   /// The current position within the execution points
   unsigned getRegisteredPoints() const { return registeredPoints.size(); }
   /// Register an async execution point
   void registerAsyncPoint(AsyncPoint& point,unsigned schedulingClass,double priority,unsigned dependencies);

   /// Execute a plan single threaded
   void executeSingleThreaded(Operator* root);
   /// Execute a plan using, using potentially multiple threads
   void execute(Operator* root);
};
//---------------------------------------------------------------------------
#endif
