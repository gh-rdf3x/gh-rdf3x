#ifndef H_cts_parser_SPARQLParser
#define H_cts_parser_SPARQLParser

//---------------------------------------------------------------------------
// RDF-3X
// Created by: 
//         Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//         (c) 2008 
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
// 
//  -----------------------------------------------------------------------
//
// Modified by:
//         Giuseppe De Simone and Hancel Gonzalez
//         Advisor: Maria Esther Vidal
//         
// Universidad Simon Bolivar
// 2013,   Caracas - Venezuela.
//
// Added structure in AST for the GJOIN clauses.
//---------------------------------------------------------------------------

#include <map>
#include <string>
#include <vector>
#include <memory>
//---------------------------------------------------------------------------
class SPARQLLexer;
//---------------------------------------------------------------------------
/// A parser for SPARQL input
class SPARQLParser
{
   public:
   /// A parsing exception
   struct ParserException {
      /// The message
      std::string message;

      /// Constructor
      ParserException(const std::string& message);
      /// Constructor
      ParserException(const char* message);
      /// Destructor
      ~ParserException();
   };
   /// An element in a graph pattern
   struct Element {
      /// Possible types
      enum Type { Variable, Literal, IRI };
      /// Possible sub-types for literals
      enum SubType { None, CustomLanguage, CustomType };
      /// The type
      Type type;
      /// The sub-type
      SubType subType;
      /// The value of the sub-type
      std::string subTypeValue;
      /// The literal value
      std::string value;
      /// The id for variables
      unsigned id;
   };
   /// A graph pattern
   struct Pattern {
      /// The entires
      Element subject,predicate,object;

      /// Constructor
      Pattern(Element subject,Element predicate,Element object);
      /// Destructor
      ~Pattern();
   };
   /// A filter entry
   struct Filter {
      /// Possible types
      enum Type {
         Or, And, Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual, Plus, Minus, Mul, Div,
         Not, UnaryPlus, UnaryMinus, Literal, Variable, IRI, Function, ArgumentList,
         Builtin_str, Builtin_lang, Builtin_langmatches, Builtin_datatype, Builtin_bound, Builtin_sameterm,
         Builtin_isiri, Builtin_isblank, Builtin_isliteral, Builtin_regex, Builtin_in
      };

      /// The type
      Type type;
      /// Input arguments
      Filter* arg1,*arg2,*arg3;
      /// The value (for constants)
      std::string value;
      /// The type (for constants)
      std::string valueType;
      /// Possible subtypes or variable ids
      unsigned valueArg;

      /// Constructor
      Filter();
      /// Copy-Constructor
      Filter(const Filter& other);
      /// Destructor
      ~Filter();

      /// Assignment
      Filter& operator=(const Filter& other);
   };
   /// A group of patterns
   struct PatternGroup {
      /// The patterns
      std::vector<Pattern> patterns;
      /// The filter conditions
      std::vector<Filter> filters;
      /// The optional parts
      std::vector<PatternGroup> optional;
      /// The union parts
      std::vector<std::vector<PatternGroup> > unions;
      /// The gjoin parts - Hancel & Giuseppe
      std::vector<std::vector<PatternGroup> > gjoins; 
   };
   /// The projection modifier
   enum ProjectionModifier { Modifier_None, Modifier_Distinct, Modifier_Reduced, Modifier_Count, Modifier_Duplicates };
   /// Sort order
   struct Order {
      /// Variable id
      unsigned id;
      /// Desending
      bool descending;
   };

   private:
   /// The lexer
   SPARQLLexer& lexer;
   /// The registered prefixes
   std::map<std::string,std::string> prefixes;
   /// The named variables
   std::map<std::string,unsigned> namedVariables;
   /// The total variable count
   unsigned variableCount;

   /// The projection modifier
   ProjectionModifier projectionModifier;
   /// The projection clause
   std::vector<unsigned> projection;
   /// The pattern
   PatternGroup patterns;
   /// The sort order
   std::vector<Order> order;
   /// The result limit
   unsigned limit;

   /// Lookup or create a named variable
   unsigned nameVariable(const std::string& name);

   /// Parse an RDF literal
   void parseRDFLiteral(std::string& value,Element::SubType& subType,std::string& valueType);
   /// Parse a "IRIrefOrFunction" production
   Filter* parseIRIrefOrFunction(std::map<std::string,unsigned>& localVars,bool mustCall);
   /// Parse a "BuiltInCall" production
   Filter* parseBuiltInCall(std::map<std::string,unsigned>& localVars);
   /// Parse a "PrimaryExpression" production
   Filter* parsePrimaryExpression(std::map<std::string,unsigned>& localVars);
   /// Parse a "UnaryExpression" production
   Filter* parseUnaryExpression(std::map<std::string,unsigned>& localVars);
   /// Parse a "MultiplicativeExpression" production
   Filter* parseMultiplicativeExpression(std::map<std::string,unsigned>& localVars);
   /// Parse a "AdditiveExpression" production
   Filter* parseAdditiveExpression(std::map<std::string,unsigned>& localVars);
   /// Parse a "NumericExpression" production
   Filter* parseNumericExpression(std::map<std::string,unsigned>& localVars);
   /// Parse a "RelationalExpression" production
   Filter* parseRelationalExpression(std::map<std::string,unsigned>& localVars);
   /// Parse a "ValueLogical" production
   Filter* parseValueLogical(std::map<std::string,unsigned>& localVars);
   /// Parse a "ConditionalAndExpression" production
   Filter* parseConditionalAndExpression(std::map<std::string,unsigned>& localVars);
   /// Parse a "ConditionalOrExpression" production
   Filter* parseConditionalOrExpression(std::map<std::string,unsigned>& localVars);
   /// Parse a "Expression" production
   Filter* parseExpression(std::map<std::string,unsigned>& localVars);
   /// Parse a "BrackettedExpression" production
   Filter* parseBrackettedExpression(std::map<std::string,unsigned>& localVars);
   /// Parse a "Constraint" production
   Filter* parseConstraint(std::map<std::string,unsigned>& localVars);
   /// Parse a filter condition
   void parseFilter(PatternGroup& group,std::map<std::string,unsigned>& localVars);
   /// Parse an entry in a pattern
   Element parsePatternElement(PatternGroup& group,std::map<std::string,unsigned>& localVars);
   /// Parse blank node patterns
   Element parseBlankNode(PatternGroup& group,std::map<std::string,unsigned>& localVars);
   // Parse a graph pattern
   void parseGraphPattern(PatternGroup& group);
   // Parse a group of patterns
   void parseGroupGraphPattern(PatternGroup& group);

   /// Parse the prefix part if any
   void parsePrefix();
   /// Parse the projection
   void parseProjection();
   /// Parse the from part if any
   void parseFrom();
   /// Parse the where part if any
   void parseWhere();
   /// Parse the order by part if any
   void parseOrderBy();
   /// Parse the limit part if any
   void parseLimit();

   public:
   /// Constructor
   explicit SPARQLParser(SPARQLLexer& lexer);
   /// Destructor
   ~SPARQLParser();

   /// Parse the input. Throws an exception in the case of an error
   void parse(bool multiQuery = false);

   /// Get the patterns
   const PatternGroup& getPatterns() const { return patterns; }
   /// Get the name of a variable
   std::string getVariableName(unsigned id) const;

   /// Iterator over the projection clause
   typedef std::vector<unsigned>::const_iterator projection_iterator;
   /// Iterator over the projection
   projection_iterator projectionBegin() const { return projection.begin(); }
   /// Iterator over the projection
   projection_iterator projectionEnd() const { return projection.end(); }

   /// Iterator over the order by clause
   typedef std::vector<Order>::const_iterator order_iterator;
   /// Iterator over the order by clause
   order_iterator orderBegin() const { return order.begin(); }
   /// Iterator over the order by clause
   order_iterator orderEnd() const { return order.end(); }

   /// The projection modifier
   ProjectionModifier getProjectionModifier() const { return projectionModifier; }
   /// The size limit
   unsigned getLimit() const { return limit; }
};
//---------------------------------------------------------------------------
#endif
