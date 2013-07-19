#include "rts/operator/Scheduler.hpp"
#include "rts/operator/Operator.hpp"
#include "infra/osdep/Thread.hpp"
#include <cstdlib>
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
Scheduler::AsyncPoint::~AsyncPoint()
   // Destructor
{
}
//---------------------------------------------------------------------------
Scheduler::RegisteredPoint::RegisteredPoint(AsyncPoint& point,unsigned schedulingClass,double priority)
   : point(point),schedulingClass(schedulingClass),priority(priority)
   // Constructor
{
}
//---------------------------------------------------------------------------
Scheduler::Scheduler()
   : activeWorkers(0),workerThreads(0),workersDie(false)
   // Constructor
{
   // How many threads should we use?
   threads=0;
   if (getenv("MAXTHREADS"))
      threads=atoi(getenv("MAXTHREADS"));
   if ((threads<2)||(threads>1000))
      threads=0;

   // Start worker threads
   if (threads) {
      workerLock.lock();
      for (unsigned index=0;index<threads;index++)
         Thread::start(asyncWorker,this);
      workerLock.unlock();
   }
}
//---------------------------------------------------------------------------
Scheduler::~Scheduler()
   // Destructor
{
   // Wait for all workers to stop
   if (threads) {
      workerLock.lock();
      workersDie=true;
      workerSignal.notifyAll(workerLock);
      while (workerThreads)
         workerSignal.wait(workerLock);
      workerLock.unlock();
   }

   // Cleanup registered execution points
   for (vector<RegisteredPoint*>::const_iterator iter=registeredPoints.begin(),limit=registeredPoints.end();iter!=limit;++iter)
      delete *iter;
   registeredPoints.clear();
}
//---------------------------------------------------------------------------
void Scheduler::registerAsyncPoint(AsyncPoint& point,unsigned schedulingClass,double priority,unsigned dependencies)
   // Register an async execution point
{
   RegisteredPoint* p=new RegisteredPoint(point,schedulingClass,priority);
   for (unsigned index=dependencies;index<registeredPoints.size();index++)
      p->dependencies.insert(registeredPoints[index]);
   registeredPoints.push_back(p);
}
//---------------------------------------------------------------------------
void Scheduler::executeSingleThreaded(Operator* root)
   // Execute a plan single threaded
{
   if (root->first()) {
      while (root->next()) ;
   }
}
//---------------------------------------------------------------------------
void Scheduler::performWork()
   // Perform the work of a worker thread
{
   // Register as worker
   workerLock.lock();
   workerThreads++;

   // Perform the work
   while (!workersDie) {
      // Nothing to do?
      if (workQueue.empty()) {
         workerSignal.wait(workerLock);
         continue;
      }
      // Grab the next job
      RegisteredPoint* job=workQueue.front();
      workQueue.erase(workQueue.begin());
      workerSignal.notifyAll(workerLock);

      // Run the job
      activeWorkers++;
      workerLock.unlock();
      job->point.run();
      workerLock.lock();
      activeWorkers--;

      // Eliminate it as done
      for (vector<RegisteredPoint*>::iterator iter=registeredPoints.begin(),limit=registeredPoints.end();iter!=limit;++iter)
         (*iter)->dependencies.erase(job);
      delete job;
      workerSignal.notifyAll(workerLock);
   }

   // Deregister
   workerThreads--;
   workerSignal.notifyAll(workerLock);
   workerLock.unlock();
}
//---------------------------------------------------------------------------
void Scheduler::asyncWorker(void* info)
   // Thread entry point
{
   static_cast<Scheduler*>(info)->performWork();
}
//---------------------------------------------------------------------------
void Scheduler::execute(Operator* root)
   // Execute a plan using, using potentially multiple threads
{
   // Run single threaded?
   if (!threads) {
      executeSingleThreaded(root);
      return;
   }

   // Collect all asynchronous execution points
   for (vector<RegisteredPoint*>::const_iterator iter=registeredPoints.begin(),limit=registeredPoints.end();iter!=limit;++iter)
      delete *iter;
   registeredPoints.clear();
   root->getAsyncInputCandidates(*this);

   // Execute all asynchronous execution points
   while (!registeredPoints.empty()) {
      // Is the worker queue full?
      if ((workQueue.size()+activeWorkers)>=threads) {
         workerSignal.wait(workerLock);
         continue;
      }
      // Search for the best candidate the schedule next
      RegisteredPoint* best=0;
      vector<RegisteredPoint*>::iterator bestPos=registeredPoints.begin();
      for (vector<RegisteredPoint*>::iterator iter=registeredPoints.begin(),limit=registeredPoints.end();iter!=limit;++iter) {
         RegisteredPoint* p=*iter;
         if (p->dependencies.empty()) {
            if ((!best)||(best->schedulingClass>p->schedulingClass)||
                ((best->schedulingClass==p->schedulingClass)&&(best->priority<p->priority))) {
               best=p;
               bestPos=iter;
            }
         }
      }
      // No candidate found?
      if (!best) {
         // Still work?
         if (activeWorkers||workQueue.size()) {
            workerSignal.wait(workerLock);
            continue;
         }
         // No, cyclic dependency!
         throw;
      }
      // Add the task
      workQueue.push_back(best);
      swap(*bestPos,registeredPoints.back());
      registeredPoints.pop_back();
      workerSignal.notifyAll(workerLock);
   }
   while (!workQueue.empty())
      workerSignal.wait(workerLock);
   while (activeWorkers)
      workerSignal.wait(workerLock);
   workerLock.unlock();

   // Now run the main parts if any
   if (root->first()) {
      while (root->next()) ;
   }
}
//---------------------------------------------------------------------------
