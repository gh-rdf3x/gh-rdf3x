#ifndef H_rts_operator_Selection
#define H_rts_operator_Selection
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
#include "infra/util/Type.hpp"
#include <vector>
#include <string>
//---------------------------------------------------------------------------
class Register;
class Runtime;
//---------------------------------------------------------------------------
/// Applies a number of selections
class Selection : public Operator
{
   public:
   /// A predicate result
   struct Result {
      /// Possible flags
      enum Flags { idAvailable=1,stringAvailable=2,typeAvailable=4,subTypeAvailable=8,booleanAvailable=16 };

      /// The flags
      unsigned flags;
      /// The id
      unsigned id;
      /// The value
      std::string value;
      /// The type
      Type::ID type;
      /// The sub-type
      unsigned subType;
      /// The sub-type value
      std::string subTypeValue;
      /// The boolean interpretation
      bool boolean;

      /// Constructor
      Result() : flags(0) {}
      /// Destructor
      ~Result();

      /// String available?
      bool hasId() const { return flags&idAvailable; }
      /// String available?
      bool hasString() const { return flags&stringAvailable; }

      /// Ensure that a string is available
      void ensureString(Selection* selection);
      /// Ensure that the type is available
      void ensureType(Selection* selection);
      /// Ensuzre tthat the subtype is available
      void ensureSubType(Selection* selection);
      /// Ensure that a boolean interpretation is available
      void ensureBoolean(Selection* selection);

      /// Set to a boolean value
      void setBoolean(bool v);
      /// Set and id value
      void setId(unsigned v);
      /// Set to a string value
      void setLiteral(const std::string& c);
      /// Set to a string value
      void setIRI(const std::string& c);
   };
   /// Base for predicate evaluation
   class Predicate {
      protected:
      /// The outer selection
      Selection* selection;

      public:
      /// Constructor
      Predicate();
      /// Destructor
      virtual ~Predicate();

      /// Register the selection
      virtual void setSelection(Selection* selection);
      /// Evaluate the predicate
      virtual void eval(Result& result) = 0;
      /// Print the predicate (debugging only)
      virtual std::string print(PlanPrinter& out) = 0;

      /// Check the predicate
      bool check();
   };
   /// Binary operator
   class BinaryPredicate : public Predicate {
      protected:
      /// The input
      Predicate* left,*right;

      public:
      /// Constructor
      BinaryPredicate(Predicate* left,Predicate* right) : left(left),right(right) {}
      /// Destructor
      ~BinaryPredicate();

      /// Register the selection
      void setSelection(Selection* selection);
   };
   /// Unary operator
   class UnaryPredicate : public Predicate {
      protected:
      /// The input
      Predicate* input;

      public:
      /// Constructor
      UnaryPredicate(Predicate* input) : input(input) {}
      /// Destructor
      ~UnaryPredicate();

      /// Register the selection
      void setSelection(Selection* selection);
   };
   /// Logical or
   class Or : public BinaryPredicate {
      public:
      /// Constructor
      Or(Predicate* left,Predicate* right) : BinaryPredicate(left,right) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Logical and
   class And : public BinaryPredicate {
      public:
      /// Constructor
      And(Predicate* left,Predicate* right) : BinaryPredicate(left,right) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Comparison ==
   class Equal : public BinaryPredicate {
      public:
      /// Constructor
      Equal(Predicate* left,Predicate* right) : BinaryPredicate(left,right) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Comparison !=
   class NotEqual : public BinaryPredicate {
      public:
      /// Constructor
      NotEqual(Predicate* left,Predicate* right) : BinaryPredicate(left,right) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Comparison <
   class Less : public BinaryPredicate {
      public:
      /// Constructor
      Less(Predicate* left,Predicate* right) : BinaryPredicate(left,right) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Comparison <=
   class LessOrEqual : public BinaryPredicate {
      public:
      /// Constructor
      LessOrEqual(Predicate* left,Predicate* right) : BinaryPredicate(left,right) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Arithmetic +
   class Plus : public BinaryPredicate {
      public:
      /// Constructor
      Plus(Predicate* left,Predicate* right) : BinaryPredicate(left,right) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Arithmetic -
   class Minus : public BinaryPredicate {
      public:
      /// Constructor
      Minus(Predicate* left,Predicate* right) : BinaryPredicate(left,right) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Arithmetic *
   class Mul : public BinaryPredicate {
      public:
      /// Constructor
      Mul(Predicate* left,Predicate* right) : BinaryPredicate(left,right) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Arithmetic /
   class Div : public BinaryPredicate {
      public:
      /// Constructor
      Div(Predicate* left,Predicate* right) : BinaryPredicate(left,right) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Operator !
   class Not : public UnaryPredicate {
      public:
      /// Constructor
      Not(Predicate* input) : UnaryPredicate(input) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Operator -
   class Neg : public UnaryPredicate {
      public:
      /// Constructor
      Neg(Predicate* input) : UnaryPredicate(input) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// A NULL value
   class Null : public Predicate {
      public:
      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// A false value
   class False : public Predicate {
      public:
      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Variable access
   class Variable : public Predicate {
      private:
      /// The register
      Register* reg;

      public:
      /// Constructor
      Variable(Register* reg) : reg(reg) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Constant
   class ConstantLiteral : public Predicate {
      private:
      /// The id
      unsigned id;

      public:
      /// Constructor
      ConstantLiteral(unsigned id) : id(id) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Constant
   class TemporaryConstantLiteral : public Predicate {
      private:
      /// The value
      std::string value;

      public:
      /// Constructor
      TemporaryConstantLiteral(const std::string& value) : value(value) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Constant
   class ConstantIRI : public Predicate {
      private:
      /// The id
      unsigned id;

      public:
      /// Constructor
      ConstantIRI(unsigned id) : id(id) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Constant
   class TemporaryConstantIRI : public Predicate {
      private:
      /// The value
      std::string value;

      public:
      /// Constructor
      TemporaryConstantIRI(const std::string& value) : value(value) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Function call
   class FunctionCall : public Predicate {
      private:
      /// The function
      std::string func;
      /// Arguments
      std::vector<Predicate*> args;

      public:
      /// Constructor
      FunctionCall(const std::string& func,const std::vector<Predicate*>& args) : func(func),args(args) {}

      /// Register the selection
      void setSelection(Selection* selection);

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Builtin str
   class BuiltinStr : public UnaryPredicate {
      public:
      /// Constructor
      BuiltinStr(Predicate* input) : UnaryPredicate(input) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Builtin lang
   class BuiltinLang : public UnaryPredicate {
      public:
      /// Constructor
      BuiltinLang(Predicate* input) : UnaryPredicate(input) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Builtin langMatches
   class BuiltinLangMatches : public BinaryPredicate {
      public:
      /// Constructor
      BuiltinLangMatches(Predicate* left,Predicate* right) : BinaryPredicate(left,right) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Builtin datatype
   class BuiltinDatatype : public UnaryPredicate {
      public:
      /// Constructor
      BuiltinDatatype(Predicate* input) : UnaryPredicate(input) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Builtin bound
   class BuiltinBound : public UnaryPredicate {
      private:
      /// The register
      //Register* reg;
     

      public:
      /// Constructor
      //BuiltinBound(Register* reg) : reg(reg) {}
      BuiltinBound(Predicate* input) : UnaryPredicate(input) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Builtin sameTerm
   class BuiltinSameTerm : public BinaryPredicate {
      public:
      /// Constructor
      BuiltinSameTerm(Predicate* left,Predicate* right) : BinaryPredicate(left,right) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Builtin isIRI
   class BuiltinIsIRI : public UnaryPredicate {
      public:
      /// Constructor
      BuiltinIsIRI(Predicate* input) : UnaryPredicate(input) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Builtin isBlank
   class BuiltinIsBlank : public UnaryPredicate {
      public:
      /// Constructor
      BuiltinIsBlank(Predicate* input) : UnaryPredicate(input) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Builtin isLiteral
   class BuiltinIsLiteral : public UnaryPredicate {
      public:
      /// Constructor
      BuiltinIsLiteral(Predicate* input) : UnaryPredicate(input) {}

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Builtin RegEx
   class BuiltinRegEx : public Predicate {
      private:
      /// Arguments
      Predicate* arg1,*arg2,*arg3;

      public:
      /// Constructor
      BuiltinRegEx(Predicate* arg1,Predicate* arg2,Predicate* arg3) : arg1(arg1),arg2(arg2),arg3(arg3) {}
      /// Destructor
      ~BuiltinRegEx();

      /// Register the selection
      void setSelection(Selection* selection);

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };
   /// Builtin in
   class BuiltinIn : public Predicate {
      private:
      /// The probe
      Predicate* probe;
      /// The set
      std::vector<Predicate*> args;

      public:
      /// Constructor
      BuiltinIn(Predicate* probe,const std::vector<Predicate*>& args) : probe(probe),args(args) {}

      /// Register the selection
      void setSelection(Selection* selection);

      /// Evaluate the predicate
      void eval(Result& result);
      /// Print the predicate (debugging only)
      std::string print(PlanPrinter& out);
   };

   private:
   /// The input
   Operator* input;
   /// The runtime
   Runtime& runtime;
   /// The predicate
   Predicate* predicate;


   public:
   /// Constructor
   Selection(Operator* input,Runtime& runtime,Predicate* predicate,double expectedOutputCardinality);
   /// Destructor
   ~Selection();

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
