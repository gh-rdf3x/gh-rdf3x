#ifndef H_rts_operator_PlanPrinter
#define H_rts_operator_PlanPrinter
//---------------------------------------------------------------------------
#include <string>
#include <vector>
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
class Runtime;
class Register;
//---------------------------------------------------------------------------
/// Generic plan printing mechanism. Mainly used for debugging.
class PlanPrinter
{
   public:
   /// Destructor
   virtual ~PlanPrinter();

   /// Begin a new operator
   virtual void beginOperator(const std::string& name,double expectedOutputCardinality,unsigned observedOutputCardinality) = 0;
   /// Add an operator argument annotation
   virtual void addArgumentAnnotation(const std::string& argument) = 0;
   /// Add a scan annotation
   virtual void addScanAnnotation(const Register* reg,bool bound) = 0;
   /// Add a predicate annotate
   virtual void addEqualPredicateAnnotation(const Register* reg1,const Register* reg2) = 0;
   /// Add a materialization annotation
   virtual void addMaterializationAnnotation(const std::vector<Register*>& regs) = 0;
   /// Add a generic annotation
   virtual void addGenericAnnotation(const std::string& text) = 0;
   /// Close the current operator
   virtual void endOperator() = 0;

   /// Format a register (for generic annotations)
   virtual std::string formatRegister(const Register* reg) = 0;
   /// Format a constant value (for generic annotations)
   virtual std::string formatValue(unsigned value) = 0;
};
//---------------------------------------------------------------------------
/// Print implementation for debug purposes
class DebugPlanPrinter : public PlanPrinter
{
   private:
   /// The output
   std::ostream& out;
   /// The runtime
   Runtime& runtime;
   /// Indentation level
   unsigned level;
   /// Show observed cardinalities?
   bool showObserved;

   /// Indent
   void indent();

   public:
   /// Constructor
   DebugPlanPrinter(Runtime& runtime,bool showObserved);
   /// Constructor
   DebugPlanPrinter(std::ostream& out,Runtime& runtime,bool showObserved);

   /// Begin a new operator
   void beginOperator(const std::string& name,double expectedOutputCardinality,unsigned observedOutputCardinality);
   /// Add an operator argument annotation
   void addArgumentAnnotation(const std::string& argument);
   /// Add a scan annotation
   void addScanAnnotation(const Register* reg,bool bound);
   /// Add a predicate annotate
   void addEqualPredicateAnnotation(const Register* reg1,const Register* reg2);
   /// Add a materialization annotation
   void addMaterializationAnnotation(const std::vector<Register*>& regs);
   /// Add a generic annotation
   void addGenericAnnotation(const std::string& text);
   /// Close the current operator
   void endOperator();

   /// Format a register (for generic annotations)
   std::string formatRegister(const Register* reg);
   /// Format a constant value (for generic annotations)
   std::string formatValue(unsigned value);
};
//---------------------------------------------------------------------------
#endif
