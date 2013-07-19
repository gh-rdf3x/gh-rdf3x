#ifndef H_rts_operator_MergeJoin
#define H_rts_operator_MergeJoin
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
#include "rts/operator/Operator.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// A merge join. The input has to be sorted by the join attributes.
class MergeJoin : public Operator
{
   private:
   /// The input
   Operator* left,*right;
   /// The join attributes
   Register* leftValue,*rightValue;
   /// The non-join attributes
   std::vector<Register*> leftTail,rightTail;

   /// Possible states while scanning the input
   enum ScanState {
      empty,
      scanHasBothSwapped,scanStepLeft,scanStepBoth,scanStepRight,scanHasBoth,
      loopEmptyLeft,loopEmptyRight,loopEmptyRightHasData,
      loopEqualLeftHasData,loopEqualLeft,
      loopEqualRightHasData,loopEqualRight,
      loopSpooledRightEmpty,loopSpooledRightHasData
    };

   /// Tuple counts
   unsigned leftCount,rightCount;
   /// Shadow tuples
   std::vector<unsigned> leftShadow,rightShadow;
   /// The buffer
   std::vector<unsigned> buffer;
   /// The iterator over the buffer
   std::vector<unsigned>::const_iterator bufferIter;
   /// The current scan state
   ScanState scanState;
   /// Is a copy of the left hand side available? Only used for loopSpooled*
   bool leftInCopy;

   /// Copy the left tuple into its shadow
   void copyLeft();
   /// Swap the left tuple with its shadow
   void swapLeft();
   /// Copy the right tuple into its shadow
   void copyRight();
   /// Swap the right tuple with its shadow
   void swapRight();

   /// Handle the n:m case
   void handleNM();

   public:
   /// Constructor
   MergeJoin(Operator* left,Register* leftValue,const std::vector<Register*>& leftTail,Operator* right,Register* rightValue,const std::vector<Register*>& rightTail,double expectedOutputCardinality);
   /// Destructor
   ~MergeJoin();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(PlanPrinter& out);
   /// Add a merge join hint
   void addMergeHint(Register* reg1,Register* reg2);
   /// Register parts of the tree that can be executed asynchronous
   void getAsyncInputCandidates(Scheduler& scheduler);
};
//---------------------------------------------------------------------------
#endif
