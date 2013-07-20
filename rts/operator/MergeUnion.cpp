#include "rts/operator/MergeUnion.hpp"
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
MergeUnion::MergeUnion(Register* result,Operator* left,Register* leftReg,Operator* right,Register* rightReg,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),left(left),right(right),leftReg(leftReg),rightReg(rightReg),result(result)
   // Constructor
{
}
//---------------------------------------------------------------------------
MergeUnion::~MergeUnion()
   // Destructor
{
   delete left;
   delete right;
}
//---------------------------------------------------------------------------
unsigned MergeUnion::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;

   // Read the first tuple on the left side
   if ((leftCount=left->first())==0) {
      if ((rightCount=right->first())==0) {
         state=done;
         return false;
      }
      result->value=rightReg->value;
      state=leftEmpty;
      observedOutputCardinality+=rightCount;
      return rightCount;
   }
   leftValue=leftReg->value;

   // Read the first tuple on the right side
   if ((rightCount=right->first())==0) {
      result->value=leftReg->value;
      state=rightEmpty;
      observedOutputCardinality+=leftCount;
      return leftCount;
   }
   rightValue=rightReg->value;

   // Handle the cases
   if (leftValue<rightValue) {
      result->value=leftValue;
      state=stepLeft;
      observedOutputCardinality+=leftCount;
      return leftCount;
   } else if (leftValue>rightValue) {
      result->value=rightValue;
      state=stepRight;
      observedOutputCardinality+=rightCount;
      return rightCount;
   } else {
      result->value=leftValue;
      state=stepBoth;

      unsigned count=leftCount+rightCount;
      observedOutputCardinality+=count;
      return count;
   }
}
//---------------------------------------------------------------------------
unsigned MergeUnion::next()
   // Produce the next tuple
{
   switch (state) {
      case done: return false;
      case stepLeft: case stepBoth:
         if ((leftCount=left->next())==0) {
            result->value=rightValue;
            state=leftEmpty;
            observedOutputCardinality+=rightCount;
            return rightCount;
         }
         leftValue=leftReg->value;
         // Fallthrough
      case stepRight:
         if (state!=stepLeft) {
            if ((rightCount=right->next())==0) {
               result->value=leftValue;
               state=rightEmpty;
               observedOutputCardinality+=leftCount;
               return leftCount;
            }
            rightValue=rightReg->value;
         }
         // Handle the cases
         if (leftValue<rightValue) {
            result->value=leftValue;
            state=stepLeft;
            observedOutputCardinality+=leftCount;
            return leftCount;
         } else if (leftValue>rightValue) {
            result->value=rightValue;
            state=stepRight;
            observedOutputCardinality+=rightCount;
            return rightCount;
         } else {
            result->value=leftValue;
            state=stepBoth;

            unsigned count=leftCount+rightCount;
            observedOutputCardinality+=count;
            return count;
         }
      case leftEmpty:
         if ((rightCount=right->next())==0) {
            state=done;
            return false;
         }
         result->value=rightReg->value;
         observedOutputCardinality+=rightCount;
         return rightCount;
      case rightEmpty:
         if ((leftCount=left->next())==0) {
            state=done;
            return false;
         }
         result->value=leftReg->value;
         observedOutputCardinality+=leftCount;
         return leftCount;
   }
   return false;
}
//---------------------------------------------------------------------------
void MergeUnion::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("MergeUnion",expectedOutputCardinality,observedOutputCardinality);
   out.addEqualPredicateAnnotation(result,leftReg);
   out.addEqualPredicateAnnotation(result,rightReg);
   left->print(out);
   right->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
void MergeUnion::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   left->addMergeHint(reg1,reg2);
   right->addMergeHint(reg1,reg2);
}
//---------------------------------------------------------------------------
void MergeUnion::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   left->getAsyncInputCandidates(scheduler);
   right->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
