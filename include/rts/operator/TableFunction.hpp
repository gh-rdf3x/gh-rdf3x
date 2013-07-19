#ifndef H_rts_operator_TableFunction
#define H_rts_operator_TableFunction
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
#include <vector>
#include <string>
//---------------------------------------------------------------------------
class Register;
class Runtime;
//---------------------------------------------------------------------------
/// A table function
class TableFunction : public Operator
{
   public:
   /// A function argument
   struct FunctionArgument {
      /// The register if any
      Register* reg;
      /// The string value if any
      std::string value;
   };

   private:
   /// The input
   Operator* input;
   /// The runtime
   Runtime& runtime;
   /// Function id
   unsigned id;
   /// Function name
   std::string name;
   /// Input variables
   std::vector<FunctionArgument> inputArgs;
   /// Output variables
   std::vector<Register*> outputVars;

   /// Current table values
   std::vector<unsigned> table;
   /// Current table iterator
   std::vector<unsigned>::const_iterator tableIter,tableLimit;
   /// The count of the last input tuple
   unsigned count;

   /// Request new table values
   void requestTable();

   public:
   /// Constructor
   TableFunction(Operator* input,Runtime& runtime,unsigned id,const std::string& name,const std::vector<FunctionArgument>& inputArgs,const std::vector<Register*>& outputVars,double expectedOutputCardinality);
   /// Destructor
   ~TableFunction();

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
