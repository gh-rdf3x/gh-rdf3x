#ifndef H_rts_operator_HashJoin
#define H_rts_operator_HashJoin
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
#include "rts/operator/Scheduler.hpp"
#include "infra/util/VarPool.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// A memory based hash join
class HashJoin : public Operator
{
   private:
   /// A hash table entry
   struct Entry {
      /// The next entry
      Entry* next;
      /// The key
      unsigned key;
      /// The count
      unsigned count;
      /// Further values
      unsigned values[];
   };
   /// Hash table task
   class BuildHashTable : public Scheduler::AsyncPoint {
      private:
      /// The operator
      HashJoin& join;
      /// Already done?
      bool done;

      public:
      /// Constructor
      BuildHashTable(HashJoin& join) : join(join),done(false) {}
      /// Perform the task
      void run();
   };
   friend class BuildHashTable;
   /// Probe peek task
   class ProbePeek : public Scheduler::AsyncPoint {
      private:
      /// The operator
      HashJoin& join;
      /// The count
      unsigned count;
      /// Already done?
      bool done;

      friend class HashJoin;

      public:
      /// Constructor
      ProbePeek(HashJoin& join) : join(join),count(0),done(false) {}
      /// Perform the task
      void run();
   };
   friend class ProbePeek;

   /// The input
   Operator* left,*right;
   /// The join attributes
   Register* leftValue,*rightValue;
   /// The non-join attributes
   std::vector<Register*> leftTail,rightTail;
   /// The pool of hash entry
   VarPool<Entry> entryPool;
   /// The hash table
   std::vector<Entry*> hashTable;
   /// The current iter
   Entry* hashTableIter;
   /// The tuple count from the right side
   unsigned rightCount;
   /// Task
   BuildHashTable buildHashTableTask;
   /// Task
   ProbePeek probePeekTask;
   /// Task priorities
   double hashPriority,probePriority;

   /// Insert into the hash table
   void insert(Entry* e);
   /// Lookup an entry
   inline Entry* lookup(unsigned key);

   public:
   /// Constructor
   HashJoin(Operator* left,Register* leftValue,const std::vector<Register*>& leftTail,Operator* right,Register* rightValue,const std::vector<Register*>& rightTail,double hashPriority,double probePriority,double expectedOutputCardinality);
   /// Destructor
   ~HashJoin();

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
