#ifndef H_rts_operator_Filter
#define H_rts_operator_Filter
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
/// A selection that checks if a register is within a set of valid values
class Filter : public Operator
{
   private:
   /// The input
   Operator* input;
   /// The filter register
   Register* filter;
   /// The bounds
   unsigned min,max;
   /// The valid values
   std::vector<unsigned char> valid;
   /// Negative filter
   bool exclude;

   public:
   /// Constructor
   Filter(Operator* input,Register* filter,const std::vector<unsigned>& values,bool exclude,double expectedOutputCardinality);
   /// Destructor
   ~Filter();

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
