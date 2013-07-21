#ifndef H_cts_plangen_PlanGen
#define H_cts_plangen_PlanGen

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
// Added functions that construct plans for graph pattern with OPTIONAL clause.
//---------------------------------------------------------------------------

#include "cts/plangen/Plan.hpp"
#include "cts/infra/BitSet.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "rts/database/Database.hpp"
//---------------------------------------------------------------------------
/// A plan generator that construct a physical plan from a query graph
class PlanGen
{
   private:
   /// A subproblem
   struct Problem {
      /// The next problem in the DP table
      Problem* next;
      /// The known solutions to the problem
      Plan* plans;
      /// The relations involved in the problem
      BitSet relations;
   };
   /// A join description
   struct JoinDescription;
   /// The plans
   PlanContainer plans;
   /// The problems
   StructPool<Problem> problems;
   /// The database
   Database* db;
   /// The current query
   const QueryGraph* fullQuery;

   PlanGen(const PlanGen&);
   void operator=(const PlanGen&);

   /// Add a plan to a subproblem
   void addPlan(Problem* problem,Plan* plan);
   /// Generate an index scan
   void buildIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* problem,unsigned value1,unsigned value1C,unsigned value2,unsigned value2C,unsigned value3,unsigned value3C);
   /// Generate an aggregated index scan
   void buildAggregatedIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* problem,unsigned value1,unsigned value1C,unsigned value2,unsigned value2C);
   /// Generate an fully aggregated index scan
   void buildFullyAggregatedIndexScan(const QueryGraph::SubQuery& query,Database::DataOrder order,Problem* result,unsigned value1,unsigned value1C);
   /// Generate base table accesses
   Problem* buildScan(const QueryGraph::SubQuery& query,const QueryGraph::Node& node,unsigned id);
   /// Build the informaion about a join
   JoinDescription buildJoinInfo(const QueryGraph::SubQuery& query,const QueryGraph::Edge& edge);
   /// Generate an optional part
   Problem* buildOptional(const QueryGraph::SubQuery& query,unsigned id);
   /// Generate a union part
   Problem* buildUnion(const std::vector<QueryGraph::SubQuery>& query,unsigned id);
   /// Generate a gjoin part - Hancel & Giuseppe
   Problem*buildGJoin(const std::vector<QueryGraph::SubQuery>& query,unsigned id);
   /// Generate a table function access
   Problem* buildTableFunction(const QueryGraph::TableFunction& function,unsigned id);

   /// Translate a query into an operator tree only used in OPTIONAL clause
   Plan* translateForOptional(const QueryGraph::SubQuery& query);

   /// Translate a query into an operator tree
   Plan* translate(const QueryGraph::SubQuery& query);

   /// Translate a query into an operator tree. Used only when there is OPTIONAL. - Hancel & Giuseppe
   Plan* translate2(const QueryGraph::SubQuery& query);

   public:
   /// Constructor
   PlanGen();
   /// Destructor
   ~PlanGen();

   /// Translate a query into an operator tree
   Plan* translate(Database& db,const QueryGraph& query);
};
//---------------------------------------------------------------------------
#endif
