#ifndef H_cts_infra_QueryGraph
#define H_cts_infra_QueryGraph

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
// Added node in QueryGraph for the GJOIN clause.
//---------------------------------------------------------------------------

#include <vector>
#include <string>
#include <set>
//---------------------------------------------------------------------------
/// A query graph representing a SPARQL query
class QueryGraph
{
   public:
   /// Possible duplicate handling modes
   enum DuplicateHandling { AllDuplicates, CountDuplicates, ReducedDuplicates, NoDuplicates, ShowDuplicates };

   /// A node in the graph
   struct Node {
      /// The values
      unsigned subject,predicate,object;
      /// Which of the three values are constants?
      bool constSubject,constPredicate,constObject;

      /// Is there an implicit join edge to another node?
      bool canJoin(const Node& other) const;
   };
   /// The potential join edges
   struct Edge {
      /// The endpoints
      unsigned from,to;
      /// Common variables
      std::vector<unsigned> common;

      /// Constructor
      Edge(unsigned from,unsigned to,const std::vector<unsigned>& common);
      /// Destructor
      ~Edge();
   };
   /// A value filter
   struct Filter {
      /// Possible types
      enum Type {
         Or, And, Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual, Plus, Minus, Mul, Div,
         Not, UnaryPlus, UnaryMinus, Literal, Variable, IRI, Null, Function, ArgumentList,
         Builtin_str, Builtin_lang, Builtin_langmatches, Builtin_datatype, Builtin_bound, Builtin_sameterm,
         Builtin_isiri, Builtin_isblank, Builtin_isliteral, Builtin_regex, Builtin_in
      };

      /// The type
      Type type;
      /// Input arguments
      Filter* arg1,*arg2,*arg3;
      /// The id (if possible)
      unsigned id;
      /// The raw value (for constants)
      std::string value;

      /// Constructor
      Filter();
      /// Copy-Constructor
      Filter(const Filter& other);
      /// Destructor
      ~Filter();

      /// Assignment
      Filter& operator=(const Filter& other);

      /// Could be applied?
      bool isApplicable(const std::set<unsigned>& variables) const;
   };
   /// A table function
   struct TableFunction {
      /// An argument
      struct Argument {
         /// The variable id if any
         unsigned id;
         /// The string value
         std::string value;
      };
      /// The function name
      std::string name;
      /// Input
      std::vector<Argument> input;
      /// Output
      std::vector<unsigned> output;
   };
   /// Description of a subquery
   struct SubQuery {
      /// The nodes
      std::vector<Node> nodes;
      /// The edges
      std::vector<Edge> edges;
      /// The filter conditions
      std::vector<Filter> filters;
      /// Optional subqueries
      std::vector<SubQuery> optional;
      /// Union subqueries
      std::vector<std::vector<SubQuery> > unions;
      /// Gjoin subqueries - Hancel & Giuseppe
      std::vector<std::vector<SubQuery> > gjoins;
      /// The table functions
      std::vector<TableFunction> tableFunctions;
   };
   /// Order by entry
   struct Order {
      /// The variable
      unsigned id;
      /// Descending
      bool descending;
   };
   private:
   /// The query itself
   SubQuery query;
   /// The projection
   std::vector<unsigned> projection;
   /// The duplicate handling
   DuplicateHandling duplicateHandling;
   /// The order by clause
   std::vector<Order> order;
   /// Maximum result size
   unsigned limit;
   /// Is the query known to produce an empty result?
   bool knownEmptyResult;

   QueryGraph(const QueryGraph&);
   void operator=(const QueryGraph&);

   public:
   /// Constructor
   QueryGraph();
   /// Destructor
   ~QueryGraph();

   /// Clear the graph
   void clear();
   /// Construct the edges
   void constructEdges();

   /// Set the duplicate handling mode
   void setDuplicateHandling(DuplicateHandling d) { duplicateHandling=d; }
   /// Get the duplicate handling mode
   DuplicateHandling getDuplicateHandling() const { return duplicateHandling; }
   /// Set the result limit
   void setLimit(unsigned l) { limit=l; }
   /// Get the result limit
   unsigned getLimit() const { return limit; }
   /// Known empty result
   void markAsKnownEmpty() { knownEmptyResult=true; }
   /// Known empty result?
   bool knownEmpty() const { return knownEmptyResult; }

   /// Get the query
   SubQuery& getQuery() { return query; }
   /// Get the query
   const SubQuery& getQuery() const { return query; }

   /// Add an entry to the output projection
   void addProjection(unsigned id) { projection.push_back(id); }
   /// Iterator over the projection
   typedef std::vector<unsigned>::const_iterator projection_iterator;
   /// Iterator over the projection
   projection_iterator projectionBegin() const { return projection.begin(); }
   /// Iterator over the projection
   projection_iterator projectionEnd() const { return projection.end(); }

   /// Add an entry to the sort order
   void addOrder(const Order& o) { order.push_back(o); }
   /// Iterator over the sort order
   typedef std::vector<Order>::const_iterator order_iterator;
   /// Iterator over the sort order
   order_iterator orderBegin() const { return order.begin(); }
   /// Iterator over the sort order
   order_iterator orderEnd() const { return order.end(); }
};
//---------------------------------------------------------------------------
#endif
