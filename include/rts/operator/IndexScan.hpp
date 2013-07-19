#ifndef H_rts_operator_IndexScan
#define H_rts_operator_IndexScan
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
#include "rts/segment/FactsSegment.hpp"
#include <vector>
//---------------------------------------------------------------------------
class Register;
//---------------------------------------------------------------------------
/// An index scan over the facts table
class IndexScan : public Operator
{
   private:
   /// Hints during scanning
   class Hint : public FactsSegment::Scan::Hint {
      private:
      /// The scan
      IndexScan& scan;

      public:
      /// Constructor
      Hint(IndexScan& scan);
      /// Destructor
      ~Hint();
      /// The next hint
      void next(unsigned& value1,unsigned& value2,unsigned& value3);
   };
   friend class Hint;

   /// The registers for the different parts of the triple
   Register* value1,*value2,*value3;
   /// The different boundings
   bool bound1,bound2,bound3;
   /// The facts segment
   FactsSegment& facts;
   /// The data order
   Database::DataOrder order;
   /// The scan
   FactsSegment::Scan scan;
   /// The hinting mechanism
   Hint hint;
   /// Merge hints
   std::vector<Register*> merge1,merge2,merge3;

   /// Constructor
   IndexScan(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality);

   // Implementations
   class Scan;
   class ScanFilter2;
   class ScanFilter3;
   class ScanFilter23;
   class ScanPrefix1;
   class ScanPrefix1Filter3;
   class ScanPrefix12;
   class ScanPrefix123;

   public:
   /// Destructor
   ~IndexScan();

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
   static IndexScan* create(Database& db,Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality);
};
//---------------------------------------------------------------------------
#endif
