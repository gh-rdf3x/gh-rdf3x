#include "rts/operator/NestedLoopJoin.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
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
NestedLoopJoin::NestedLoopJoin(Operator* left,Operator* right,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),left(left),right(right)
   // Constructor
{
}
//---------------------------------------------------------------------------
NestedLoopJoin::~NestedLoopJoin()
   // Destructor
{
   delete left;
   delete right;
}
//---------------------------------------------------------------------------
unsigned NestedLoopJoin::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;

   // Read the first tuple on the left side
   if ((leftCount=left->first())==0)
      return false;

   // Look for tuples on the right side
   unsigned rightCount;
   while ((rightCount=right->first())==0) {
      if ((leftCount=left->next())==0)
         return false;
   }

   unsigned count=leftCount*rightCount;
   observedOutputCardinality+=count;
   return count;
}
//---------------------------------------------------------------------------
unsigned NestedLoopJoin::next()
   // Produce the next tuple
{
   // A simple match?
   unsigned rightCount;
   if ((rightCount=right->next())!=0) {
      unsigned count=leftCount*rightCount;
      observedOutputCardinality+=count;
      return count;
   }

   // No, do we have more tuples on the left hand side?
   if ((leftCount=left->next())==0)
      return false;

   // Yes, look for tuples on the right side
   while ((rightCount=right->first())==0) {
      if ((leftCount=left->next())==0)
         return false;
   }

   unsigned count=leftCount*rightCount;
   observedOutputCardinality+=count;
   return count;
}
//---------------------------------------------------------------------------
void NestedLoopJoin::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("NestedLoopJoin",expectedOutputCardinality,observedOutputCardinality);
   left->print(out);
   right->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
void NestedLoopJoin::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   left->addMergeHint(reg1,reg2);
}
//---------------------------------------------------------------------------
void NestedLoopJoin::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   left->getAsyncInputCandidates(scheduler);
   right->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
