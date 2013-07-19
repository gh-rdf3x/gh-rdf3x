#ifndef H_rts_operator_Union
#define H_rts_operator_Union
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
/// A union of multiple input operators. The operators are concatenated, not
/// duplicates are eliminated.
class Union : public Operator
{
   private:
   /// The parts
   std::vector<Operator*> parts;
   /// The register mappings
   std::vector<std::vector<Register*> > mappings;
   /// The initialization lists
   std::vector<std::vector<Register*> > initializations;
   /// The current slot
   unsigned current;

   // Get the first tuple from the current part
   unsigned firstFromPart();

   public:
   /// Constructor
   Union(const std::vector<Operator*>& parts,const std::vector<std::vector<Register*> >& mappings,const std::vector<std::vector<Register*> >& initializations,double expectedOutputCardinality);
   /// Destructor
   ~Union();

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
