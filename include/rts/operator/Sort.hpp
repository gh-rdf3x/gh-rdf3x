#ifndef H_rts_operator_Sort
#define H_rts_operator_Sort
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
#include "rts/operator/Operator.hpp"
#include "infra/util/VarPool.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Database;
//---------------------------------------------------------------------------
/// A sort operator
class Sort : public Operator
{
   private:
   /// A tuple
   struct Tuple {
      /// The count
      unsigned count;
      /// The values
      unsigned values[];
   };
   /// Order specification
   struct Order {
      /// The slot
      unsigned slot;
      /// Descending?
      bool descending;
   };
   class Sorter;

   /// The input registers
   std::vector<Register*> values;
   /// The input
   Operator* input;
   /// The sorted tuples
   std::vector<Tuple*> tuples;
   /// The tuples pool
   VarPool<Tuple> tuplesPool;
   /// The sort order
   std::vector<Order> order;
   /// The dictionary
   DictionarySegment& dict;
   /// Tuples iterator
   std::vector<Tuple*>::const_iterator tuplesIter;

   public:
   /// Constructor
   Sort(Database& db,Operator* input,const std::vector<Register*>& values,const std::vector<std::pair<Register*,bool> >& order,double expectedOutputCardinality);
   /// Destructor
   ~Sort();

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
