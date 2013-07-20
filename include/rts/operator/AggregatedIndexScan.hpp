#ifndef H_rts_operator_AggregatedIndexScan
#define H_rts_operator_AggregatedIndexScan
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
#include "rts/segment/AggregatedFactsSegment.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// An index scan over the aggregated facts table
class AggregatedIndexScan : public Operator
{
   private:
   /// Hints during scanning
   class Hint : public AggregatedFactsSegment::Scan::Hint {
      private:
      /// The scan
      AggregatedIndexScan& scan;

      public:
      /// Constructor
      Hint(AggregatedIndexScan& scan);
      /// Destructor
      ~Hint();
      /// The next hint
      void next(unsigned& value1,unsigned& value2);
   };
   friend class Hint;

   /// The registers for the different parts of the triple
   Register* value1,*value2;
   /// The different boundings
   bool bound1,bound2;
   /// The facts segment
   AggregatedFactsSegment& facts;
   /// The data order
   Database::DataOrder order;
   /// The scan
   AggregatedFactsSegment::Scan scan;
   /// The hinting mechanism
   Hint hint;
   /// Merge hints
   std::vector<Register*> merge1,merge2;

   /// Constructor
   AggregatedIndexScan(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2,double expectedOutputCardinality);

   // Implementations
   class Scan;
   class ScanFilter2;
   class ScanPrefix1;
   class ScanPrefix12;

   public:
   /// Destructor
   ~AggregatedIndexScan();

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
   static AggregatedIndexScan* create(Database& db,Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality);
};
//---------------------------------------------------------------------------
#endif
