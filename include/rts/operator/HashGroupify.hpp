#ifndef H_rts_operator_HashGroupify
#define H_rts_operator_HashGroupify
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
#include "infra/util/VarPool.hpp"
#include <vector>
//---------------------------------------------------------------------------
/// A hash based aggregation
class HashGroupify : public Operator
{
   private:
   /// A group
   struct Group {
      /// The next group
      Group* next;
      /// The hash value
      unsigned hash;
      /// The count
      unsigned count;
      /// The values
      unsigned values[];
   };
   /// Helper
   class Rehasher;
   /// Helper
   class Chainer;

   /// The input registers
   std::vector<Register*> values;
   /// The input
   Operator* input;
   /// The groups
   Group* groups,*groupsIter;
   /// The groups pool
   VarPool<Group> groupsPool;

   public:
   /// Constructor
   HashGroupify(Operator* input,const std::vector<Register*>& values,double expectedOutputCardinality);
   /// Destructor
   ~HashGroupify();

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
