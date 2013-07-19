#include "rts/operator/PlanPrinter.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/runtime/Runtime.hpp"
#include "infra/util/Type.hpp"
#include <iostream>
#include <sstream>
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
using namespace std;
//---------------------------------------------------------------------------
PlanPrinter::~PlanPrinter()
   // Destructor
{
}
//---------------------------------------------------------------------------
DebugPlanPrinter::DebugPlanPrinter(Runtime& runtime,bool showObserved)
   : out(cout),runtime(runtime),level(0),showObserved(showObserved)
   // Constructor
{
}
//---------------------------------------------------------------------------
DebugPlanPrinter::DebugPlanPrinter(ostream& out,Runtime& runtime,bool showObserved)
   : out(out),runtime(runtime),level(0),showObserved(showObserved)
   // Constructor
{
}
//---------------------------------------------------------------------------
void DebugPlanPrinter::indent()
   // Indent
{
   for (unsigned index=0;index<level;index++)
      out << " ";
}
//---------------------------------------------------------------------------
void DebugPlanPrinter::beginOperator(const string& name,double expectedOutputCardinality,unsigned observedOutputCardinality)
   // Begin a new operator
{
   indent();
   out << "<" << name << " " << expectedOutputCardinality;
   if (showObserved)
      out << " " << observedOutputCardinality;
   out << endl;
   ++level;
}
//---------------------------------------------------------------------------
void DebugPlanPrinter::addArgumentAnnotation(const string& argument)
   // Add an operator argument annotation
{
   indent();
   out << argument << endl;
}
//---------------------------------------------------------------------------
void DebugPlanPrinter::addScanAnnotation(const Register* reg,bool bound)
   // Add a scan annotation
{
   indent();
   if (bound)
      out << formatValue(reg->value); else
      out << formatRegister(reg);
   out << endl;
}
//---------------------------------------------------------------------------
void DebugPlanPrinter::addEqualPredicateAnnotation(const Register* reg1,const Register* reg2)
   // Add a predicate annotate
{
   indent();
   out << formatRegister(reg1) << "=" << formatRegister(reg2) << endl;
}
//---------------------------------------------------------------------------
void DebugPlanPrinter::addMaterializationAnnotation(const vector<Register*>& regs)
   // Add a materialization annotation
{
   indent();
   out << "[";
   bool first=true;
   for (vector<Register*>::const_iterator iter=regs.begin(),limit=regs.end();iter!=limit;++iter) {
      if (first) first=false; else out << " ";
      out << formatRegister(*iter);
   }
   out << "]" << endl;
}
//---------------------------------------------------------------------------
void DebugPlanPrinter::addGenericAnnotation(const string& text)
   // Add a generic annotation
{
   indent();
   out << text << endl;
}
//---------------------------------------------------------------------------
void DebugPlanPrinter::endOperator()
   // Close the current operator
{
   if (level) --level;
   indent();
   out << ">" << endl;
}
//---------------------------------------------------------------------------
string DebugPlanPrinter::formatRegister(const Register* reg)
   // Format a register (for generic annotations)
{
   stringstream result;
   // Regular register?
   if (runtime.getRegisterCount()&&(reg>=runtime.getRegister(0))&&(reg<=runtime.getRegister(runtime.getRegisterCount()-1))) {
      result << "?" << (reg-runtime.getRegister(0));
   } else {
      // Arbitrary register outside the runtime system. Should not occur except for debugging!
      result << "@0x" << hex << reinterpret_cast<uintptr_t>(reg);
   }
   return result.str();
}
//---------------------------------------------------------------------------
string DebugPlanPrinter::formatValue(unsigned value)
   // Format a constant value (for generic annotations)
{
   stringstream result;
   if (~value) {
      const char* start,*stop; Type::ID type; unsigned subType;
      if (runtime.getDatabase().getDictionary().lookupById(value,start,stop,type,subType)) {
         result << '\"';
         for (const char* iter=start;iter!=stop;++iter)
           result << *iter;
         result << '\"';
      } else result << "@?" << value;
   } else {
      result << "NULL";
   }
   return result.str();
}
//---------------------------------------------------------------------------
