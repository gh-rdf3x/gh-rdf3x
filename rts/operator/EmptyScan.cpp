#include "rts/operator/EmptyScan.hpp"
#include "rts/operator/PlanPrinter.hpp"
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
EmptyScan::EmptyScan()
   : Operator(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
EmptyScan::~EmptyScan()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned EmptyScan::first()
   // Produce the first tuple
{
   return false;
}
//---------------------------------------------------------------------------
unsigned EmptyScan::next()
   // Produce the next tuple
{
   return false;
}
//---------------------------------------------------------------------------
void EmptyScan::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("EmptyScan",expectedOutputCardinality,observedOutputCardinality);
   out.endOperator();
}
//---------------------------------------------------------------------------
void EmptyScan::addMergeHint(Register* /*reg1*/,Register* /*reg2*/)
   // Add a merge join hint
{
}
//---------------------------------------------------------------------------
void EmptyScan::getAsyncInputCandidates(Scheduler& /*scheduler*/)
   // Register parts of the tree that can be executed asynchronous
{
}
//---------------------------------------------------------------------------
