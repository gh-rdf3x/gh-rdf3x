#include "rts/operator/MergeJoin.hpp"
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
MergeJoin::MergeJoin(Operator* left,Register* leftValue,const std::vector<Register*>& leftTail,Operator* right,Register* rightValue,const std::vector<Register*>& rightTail,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),left(left),right(right),leftValue(leftValue),rightValue(rightValue),
     leftTail(leftTail),rightTail(rightTail),scanState(empty)
   // Constructor
{
   leftShadow.resize(leftTail.size()+2);
   rightShadow.resize(rightTail.size()+2);

   left->addMergeHint(leftValue,rightValue);
   right->addMergeHint(leftValue,rightValue);
}
//---------------------------------------------------------------------------
MergeJoin::~MergeJoin()
   // Destructor
{
   delete left;
   delete right;
}
//---------------------------------------------------------------------------
unsigned MergeJoin::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;

   // Read the first tuples
   if ((leftCount=left->first())==0)
      return false;
   if ((rightCount=right->first())==0)
      return false;

   // The rest is done by next
   scanState=scanHasBoth;
   return next();
}
//---------------------------------------------------------------------------
void MergeJoin::copyLeft()
   // Copy the left tuple into its shadow
{
   leftShadow[0]=leftCount;
   leftShadow[1]=leftValue->value;
   for (unsigned index=0,limit=leftTail.size();index<limit;index++)
      leftShadow[2+index]=leftTail[index]->value;
}
//---------------------------------------------------------------------------
void MergeJoin::swapLeft()
   // Swap the left tuple with its shadow
{
   std::swap(leftCount,leftShadow[0]);
   std::swap(leftValue->value,leftShadow[1]);
   for (unsigned index=0,limit=leftTail.size();index<limit;index++)
      std::swap(leftTail[index]->value,leftShadow[index+2]);
}
//---------------------------------------------------------------------------
void MergeJoin::copyRight()
   // Copy the right tuple into its shadow
{
   rightShadow[0]=rightCount;
   rightShadow[1]=rightValue->value;
   for (unsigned index=0,limit=rightTail.size();index<limit;index++)
      rightShadow[2+index]=rightTail[index]->value;
}
//---------------------------------------------------------------------------
void MergeJoin::swapRight()
   // Swap the right tuple with its shadow
{
   std::swap(rightCount,rightShadow[0]);
   std::swap(rightValue->value,rightShadow[1]);
   for (unsigned index=0,limit=rightTail.size();index<limit;index++)
      std::swap(rightTail[index]->value,rightShadow[index+2]);
}
//---------------------------------------------------------------------------
void MergeJoin::handleNM()
   // Handle the n:m case
{
   // Reset the buffer
   buffer.clear();
   bool hasCurrent=true;
   swapRight();

   // The second tuple is the copy
   leftInCopy=true;

   // Spool the right hande side into the buffer
   while (true) {
      // Materialize
      if (hasCurrent) {
         for (std::vector<unsigned>::const_iterator iter=rightShadow.begin(),limit=rightShadow.end();iter!=limit;++iter)
            buffer.push_back(*iter);
      } else {
         buffer.push_back(rightCount);
         buffer.push_back(rightValue->value);
         for (std::vector<Register*>::const_iterator iter=rightTail.begin(),limit=rightTail.end();iter!=limit;++iter)
            buffer.push_back((*iter)->value);
      }

      // Fetch the next tuple
      if (hasCurrent) {
         hasCurrent=false;
      } else {
         if ((rightCount=right->next())==0) {
            bufferIter=buffer.begin();
            scanState=loopSpooledRightEmpty;
            return;
         }
      }

      // End of the block?
      if (rightValue->value!=rightShadow[1]) {
         swapRight();
         bufferIter=buffer.begin();
         scanState=loopSpooledRightHasData;
         return;
      }
   }
}
//---------------------------------------------------------------------------
unsigned MergeJoin::next()
   // Produce the next tuple
{
   // Repeat until a match is found
   while (true) {
      switch (scanState) {
         case empty: return false;
         case scanHasBothSwapped:
            // Move the copies back in
            swapLeft();
            swapRight();
            // Fallthrough...
         case scanStepLeft: case scanStepBoth:
            // Left side
            if (scanState>scanHasBothSwapped) {
               if ((leftCount=left->next())==0)
                  return false;
            }
            // Fallthrough...
         case scanStepRight:
            // Right side...
            if (scanState>scanStepLeft) {
               if ((rightCount=right->next())==0)
                  return false;
            }
            // Fallthrough...
         case scanHasBoth:
            // Compare
            {
               unsigned l=leftValue->value,r=rightValue->value;
               if (l<r) {
                  scanState=scanStepLeft;
                  continue;
               }
               if (l>r) {
                  scanState=scanStepRight;
                  continue;
               }
            }
            // Match. Store the current values and examine the next tuples
            copyLeft();
            if ((leftCount=left->next())==0) {
               swapLeft();
               scanState=loopEmptyLeft;

               unsigned count=leftCount*rightCount;
               observedOutputCardinality+=count;
               return count;
            }
            swapLeft();
            copyRight();
            if ((rightCount=right->next())==0) {
               swapRight();
               scanState=loopEmptyRightHasData;

               unsigned count=leftCount*rightCount;
               observedOutputCardinality+=count;
               return count;
            }
            swapRight();
            // Match. Is this a 1:n or n:m join?
            if (leftValue->value==leftShadow[1]) {
               if (rightValue->value==rightShadow[1]) {
                  handleNM();
                  continue;
               } else {
                  scanState=loopEqualLeftHasData;

                  unsigned count=leftCount*rightCount;
                  observedOutputCardinality+=count;
                  return count;
               }
            } else if (rightValue->value==rightShadow[1]) {
               scanState=loopEqualRightHasData;

               unsigned count=leftCount*rightCount;
               observedOutputCardinality+=count;
               return count;
            }
            // No, just a single match
            scanState=scanHasBothSwapped;
            { unsigned count=leftCount*rightCount;
            observedOutputCardinality+=count;
            return count; }
         case loopEmptyLeft:
            // Left side is empty, compare with the right side
            if ((rightCount=right->next())==0) {
               scanState=empty;
               return false;
            }
            { unsigned l=leftValue->value,r=rightValue->value;
            if (l<r) {
               if (Operator::disableSkipping) {
                  while (right->next()) ;
               }
               scanState=empty;
               return false;
            }
            if (l==r) {
               unsigned count=leftCount*rightCount;
               observedOutputCardinality+=count;
               return count;
            }}
            continue;
         case loopEmptyRight:
            // Right side is empty, compare with the right side
            if ((leftCount=left->next())==0) {
               scanState=empty;
               return false;
            }
            // Fallthrough...
         case loopEmptyRightHasData:
            if (scanState==loopEmptyRightHasData)
               swapLeft();
            scanState=loopEmptyRight;
            { unsigned l=leftValue->value,r=rightValue->value;
            if (l>r) {
               if (Operator::disableSkipping) {
                  while (left->next()) ;
               }
               scanState=empty;
               return false;
            }
            if (l==r) {
               unsigned count=leftCount*rightCount;
               observedOutputCardinality+=count;
               return count;
            }}
            continue;
         case loopEqualLeftHasData:
            // Reuse the copy
            swapLeft();
            // Fallthough...
         case loopEqualLeft:
            // Block on the left hand side
            if (scanState==loopEqualLeft) {
               if ((leftCount=left->next())==0) {
                  if (Operator::disableSkipping) {
                     while (right->next()) ;
                  }
                  scanState=empty;
                  continue;
               }
            } else scanState=loopEqualLeft;
            // End of block?
            if (leftValue->value!=leftShadow[1]) {
               swapRight();
               scanState=scanHasBoth;
               continue;
            }
            { unsigned count=leftCount*rightCount;
            observedOutputCardinality+=count;
            return count; }
         case loopEqualRightHasData:
            // Reuse the copy
            swapRight();
            // Fallthrough
         case loopEqualRight:
            // Block on the right hand side
            if (scanState==loopEqualRight) {
               if ((rightCount=right->next())==0) {
                  if (Operator::disableSkipping) {
                     while (left->next()) ;
                  }
                  scanState=empty;
                  continue;
               }
            } else scanState=loopEqualRight;
            // End of block?
            if (rightValue->value!=rightShadow[1]) {
               swapLeft();
               scanState=scanHasBoth;
               continue;
            }
            { unsigned count=leftCount*rightCount;
            observedOutputCardinality+=count;
            return count; }
         case loopSpooledRightEmpty:
         case loopSpooledRightHasData:
            // The right hand side is spooled into the buffer...
            if (bufferIter!=buffer.end()) {
               rightCount=*bufferIter; ++bufferIter;
               rightValue->value=*bufferIter; ++bufferIter;
               for (std::vector<Register*>::iterator iter=rightTail.begin(),limit=rightTail.end();iter!=limit;++iter,++bufferIter)
                  (*iter)->value=*bufferIter;
               unsigned count=leftCount*rightCount;
               observedOutputCardinality+=count;
               return count;
            } else {
               // More tuples available on the left hand side?
               if (leftInCopy) {
                  swapLeft();
                  leftInCopy=false;
               } else {
                  if ((leftCount=left->next())==0) {
                     if (Operator::disableSkipping) {
                        while (right->next()) ;
                     }
                     scanState=empty;
                     return false;
                  }
               }
               // Yes. Still within the block?
               if (leftValue->value==leftShadow[1]) {
                  // Yes, continue scanning
                  bufferIter=buffer.begin();
                  continue;
               } else {
                  // No, right hand side empty?
                  if (scanState==loopSpooledRightEmpty) {
                     if (Operator::disableSkipping) {
                        while (left->next()) ;
                     }
                     scanState=empty;
                     return false;
                  } else {
                     swapRight();
                     scanState=scanHasBoth;
                     continue;
                  }
               }
            }
      }
   }
}
//---------------------------------------------------------------------------
void MergeJoin::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("MergeJoin",expectedOutputCardinality,observedOutputCardinality);
   out.addEqualPredicateAnnotation(leftValue,rightValue);
   out.addMaterializationAnnotation(leftTail);
   out.addMaterializationAnnotation(rightTail);
   left->print(out);
   right->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
void MergeJoin::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   left->addMergeHint(reg1,reg2);
   right->addMergeHint(reg1,reg2);
}
//---------------------------------------------------------------------------
void MergeJoin::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   left->getAsyncInputCandidates(scheduler);
   right->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
