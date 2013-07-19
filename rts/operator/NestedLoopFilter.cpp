#include "rts/operator/NestedLoopFilter.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include <algorithm>
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
NestedLoopFilter::NestedLoopFilter(Operator* input,Register* filter,const std::vector<unsigned>& values,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),input(input),filter(filter),values(values)
   // Constructor
{
   std::sort(this->values.begin(),this->values.end());
}
//---------------------------------------------------------------------------
NestedLoopFilter::~NestedLoopFilter()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned NestedLoopFilter::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   for (pos=0;pos<values.size();++pos) {
      filter->value=values[pos];
      unsigned count;
      if ((count=input->first())!=0) {
         observedOutputCardinality+=count;
         return count;
      }
   }
   return false;
}
//---------------------------------------------------------------------------
unsigned NestedLoopFilter::next()
   // Produce the next tuple
{
   // Done?
   if (pos>=values.size())
      return false;

   // More tuples?
   unsigned count;
   if ((count=input->next())!=0) {
      observedOutputCardinality+=count;
      return count;
   }

   // No, go to the next value
   for (++pos;pos<values.size();++pos) {
      filter->value=values[pos];
      if ((count=input->first())!=0) {
         observedOutputCardinality+=count;
         return count;
      }
   }
   return false;
}
//---------------------------------------------------------------------------
void NestedLoopFilter::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("NestedLoopFilter",expectedOutputCardinality,observedOutputCardinality);

   std::string pred=out.formatRegister(filter);
   pred+=" in {";
   bool first=true;
   for (std::vector<unsigned>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter) {
      if (first) first=true; else pred+=" ";
      pred+=out.formatValue(*iter);
   }
   pred+="}";
   out.addGenericAnnotation(pred);

   input->print(out);

   out.endOperator();
}
//---------------------------------------------------------------------------
void NestedLoopFilter::addMergeHint(Register* /*reg1*/,Register* /*reg2*/)
   // Add a merge join hint
{
   // Do not propagate as we break the pipeline
}
//---------------------------------------------------------------------------
void NestedLoopFilter::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
