#ifndef H_cts_codegen_CodeGen
#define H_cts_codegen_CodeGen
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
#include <set>
#include <vector>
//---------------------------------------------------------------------------
class Operator;
class Plan;
class Register;
class Runtime;
class QueryGraph;
//---------------------------------------------------------------------------
/// Interfact to the code generation part of the compiletime system
class CodeGen
{
   public:
   /// Collect all variables contained in a plan
   static void collectVariables(std::set<unsigned>& variables,Plan* plan);
   /// Translate an execution plan into an operator tree without output generation
   static Operator* translateIntern(Runtime& runtime,const QueryGraph& query,Plan* plan,std::vector<Register*>& output);
   /// Translate an execution plan into an operator tree
   static Operator* translate(Runtime& runtime,const QueryGraph& query,Plan* plan,bool silent=false);
};
//---------------------------------------------------------------------------
#endif
