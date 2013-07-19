#include "rts/operator/Filter.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
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
Filter::Filter(Operator* input,Register* filter,const std::vector<unsigned>& values,bool exclude,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),input(input),filter(filter),exclude(exclude)
   // Constructor
{
   if (values.empty()) {
      min=1;
      max=0;
   } else {
      min=max=values[0];
      for (std::vector<unsigned>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter) {
         if ((*iter)<min)
            min=*iter;
         if ((*iter)>max)
            max=*iter;
      }
      valid.resize(max-min+1);
      for (std::vector<unsigned>::const_iterator iter=values.begin(),limit=values.end();iter!=limit;++iter)
         valid[(*iter)-min]=true;
   }
}
//---------------------------------------------------------------------------
Filter::~Filter()
   // Destructor
{
   delete input;
}
//---------------------------------------------------------------------------
unsigned Filter::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;

   // Do we know the domain?
   if ((!exclude)&&(filter->domain)) {
      ObservedDomainDescription domain;
      for (unsigned index=min;index<=max;index++)
         if (valid[index-min])
            domain.add(index);
      filter->domain->restrictTo(domain);
   }

   // Empty input?
   unsigned count;
   if ((count=input->first())==0)
      return false;

   // Check if valid
   unsigned value=filter->value;
   if (exclude) {
      if ((value>=min)&&(value<=max)&&(valid[value-min]))
         return next();
      observedOutputCardinality+=count;
      return count;
   } else {
      if ((value>=min)&&(value<=max)&&(valid[value-min])) {
         observedOutputCardinality+=count;
         return count;
      }
      return next();
   }
}
//---------------------------------------------------------------------------
unsigned Filter::next()
   // Produce the next tuple
{
   if (exclude) {
      while (true) {
         // Done?
         unsigned count;
         if ((count=input->next())==0)
            return false;

         // Check if valid
         unsigned value=filter->value;
         if ((value>=min)&&(value<=max)&&(valid[value-min]))
            continue;
         observedOutputCardinality+=count;
         return count;
      }
   } else {
      while (true) {
         // Done?
         unsigned count;
         if ((count=input->next())==0)
            return false;

         // Check if valid
         unsigned value=filter->value;
         if ((value>=min)&&(value<=max)&&(valid[value-min])) {
            observedOutputCardinality+=count;
            return count;
         }
      }
   }
}
//---------------------------------------------------------------------------
void Filter::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("Filter",expectedOutputCardinality,observedOutputCardinality);

   std::string pred=out.formatRegister(filter);
   if (exclude) pred+=" not";
   pred+=" in {";
   unsigned id=min; bool first=true;
   for (std::vector<unsigned char>::const_iterator iter=valid.begin(),limit=valid.end();iter!=limit;++iter,++id) {
      if (first) first=false; else pred+=" ";
      pred+=out.formatValue(id);
   }
   pred+="}";
   out.addGenericAnnotation(pred);

   input->print(out);

   out.endOperator();
}
//---------------------------------------------------------------------------
void Filter::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   input->addMergeHint(reg1,reg2);
}
//---------------------------------------------------------------------------
void Filter::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
