#ifndef H_rts_operator_FullyAggregatedIndexScan
#define H_rts_operator_FullyAggregatedIndexScan
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
#include "rts/database/Database.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// An index scan over the fully aggregated facts table
class FullyAggregatedIndexScan : public Operator
{
   private:
   /// Hints during scanning
   class Hint : public FullyAggregatedFactsSegment::Scan::Hint {
      private:
      /// The scan
      FullyAggregatedIndexScan& scan;

      public:
      /// Constructor
      Hint(FullyAggregatedIndexScan& scan);
      /// Destructor
      ~Hint();
      /// The next hint
      void next(unsigned& value1);
   };
   friend class Hint;

   /// The registers for the different parts of the triple
   Register* value1;
   /// The different boundings
   bool bound1;
   /// The facts segment
   FullyAggregatedFactsSegment& facts;
   /// The data order
   Database::DataOrder order;
   /// The scan
   FullyAggregatedFactsSegment::Scan scan;
   /// The hinting mechanism
   Hint hint;
   /// Merge hints
   std::vector<Register*> merge1;

   /// Constructor
   FullyAggregatedIndexScan(Database& db,Database::DataOrder order,Register* value1,bool bound1,double expectedOutputCardinality);

   // Implementations
   class Scan;
   class ScanPrefix1;

   public:
   /// Destructor
   ~FullyAggregatedIndexScan();

   /// Produce the first tuple
   virtual unsigned first() = 0;
   /// Produce the next tuple
   virtual unsigned next() = 0;

   /// Print the operator tree. Debugging only.
   void print(PlanPrinter& out);
   /// Add a merge join hint
   void addMergeHint(Register* reg1,Register* reg2);
   /// Register parts of the tree that can be executed asynchronous
   void getAsyncInputCandidates(Scheduler& scheduler);

   /// Create a suitable operator
   static FullyAggregatedIndexScan* create(Database& db,Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality);
};
//---------------------------------------------------------------------------
#endif
