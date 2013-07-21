#include "cts/infra/QueryGraph.hpp"
#include <set>
using namespace std;

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
// Added edges in the QueryGraph for GJOIN and OPTIONAL clauses.
//---------------------------------------------------------------------------

bool QueryGraph::Node::canJoin(const Node& other) const
   // Is there an implicit join edge to another node?
{
   // Extract variables
   unsigned v11=0,v12=0,v13=0;
   if (!constSubject) v11=subject+1;
   if (!constPredicate) v12=predicate+1;
   if (!constObject) v13=object+1;
   unsigned v21=0,v22=0,v23=0;
   if (!other.constSubject) v21=other.subject+1;
   if (!other.constPredicate) v22=other.predicate+1;
   if (!other.constObject) v23=other.object+1;

   // Do they have a variable in common?
   bool canJoin=false;
   if (v11&&v21&&(v11==v21)) canJoin=true;
   if (v11&&v22&&(v11==v22)) canJoin=true;
   if (v11&&v23&&(v11==v23)) canJoin=true;
   if (v12&&v21&&(v12==v21)) canJoin=true;
   if (v12&&v22&&(v12==v22)) canJoin=true;
   if (v12&&v23&&(v12==v23)) canJoin=true;
   if (v13&&v21&&(v13==v21)) canJoin=true;
   if (v13&&v22&&(v13==v22)) canJoin=true;
      if (v13&&v23&&(v13==v23)) canJoin=true;

   return canJoin;
}
//---------------------------------------------------------------------------
QueryGraph::Edge::Edge(unsigned from,unsigned to,const vector<unsigned>& common)
   : from(from),to(to),common(common)
   // Constructor
{
}
//---------------------------------------------------------------------------
QueryGraph::Edge::~Edge()
   // Destructor
{
}
//---------------------------------------------------------------------------
QueryGraph::Filter::Filter()
   : arg1(0),arg2(0),arg3(0),id(~0u)
   // Constructor
{
}
//---------------------------------------------------------------------------
QueryGraph::Filter::Filter(const Filter& other)
   : type(other.type),arg1(other.arg1?new Filter(*other.arg1):0),arg2(other.arg2?new Filter(*other.arg2):0),arg3(other.arg3?new Filter(*other.arg3):0),id(other.id),value(other.value)
   // Copy-Constructor
{
}
//---------------------------------------------------------------------------
QueryGraph::Filter::~Filter()
   // Destructor
{
   delete arg1;
   delete arg2;
   delete arg3;
}
//---------------------------------------------------------------------------
QueryGraph::Filter& QueryGraph::Filter::operator=(const Filter& other)
   // Assignment
{
   if ((&other)!=this) {
      type=other.type;
      delete arg1;
      arg1=other.arg1?new Filter(*other.arg1):0;
      delete arg2;
      arg2=other.arg2?new Filter(*other.arg2):0;
      delete arg3;
      arg3=other.arg3?new Filter(*other.arg3):0;
      id=other.id;
      value=other.value;
   }
   return *this;
}
//---------------------------------------------------------------------------
bool QueryGraph::Filter::isApplicable(const std::set<unsigned>& variables) const
   // Could be applied?
{
   // A variable?
   if (type==Variable)
      return variables.count(id);

   // Check the input
   if (arg1&&(!arg1->isApplicable(variables))) return false;
   if (arg2&&(!arg2->isApplicable(variables))) return false;
   if (arg3&&(!arg3->isApplicable(variables))) return false;

   return true;
}
//---------------------------------------------------------------------------
QueryGraph::QueryGraph()
   : duplicateHandling(AllDuplicates),limit(~0u),knownEmptyResult(false)
   // Constructor
{
}
//---------------------------------------------------------------------------
QueryGraph::~QueryGraph()
   // Destructor
{
}
//---------------------------------------------------------------------------
void QueryGraph::clear()
   // Clear the graph
{
   query=SubQuery();
   duplicateHandling=AllDuplicates;
   knownEmptyResult=false;
}
//---------------------------------------------------------------------------
static bool intersects(const set<unsigned>& a,const set<unsigned>& b,vector<unsigned>& common)
   // Check if two sets overlap
{
   common.clear();
   set<unsigned>::const_iterator ia,la,ib,lb;
   if (a.size()<b.size()) {
      if (a.empty())
         return false;
      ia=a.begin(); la=a.end();
      ib=b.lower_bound(*ia); lb=b.end();
   } else {
      if (b.empty())
         return false;
      ib=b.begin(); lb=b.end();
      ia=a.lower_bound(*ib); la=a.end();
   }
   bool result=false;
   while ((ia!=la)&&(ib!=lb)) {
      unsigned va=*ia,vb=*ib;
      if (va<vb) {
         ++ia;
      } else if (va>vb) {
         ++ib;
      } else {
         result=true;
         common.push_back(*ia);
         ++ia; ++ib;
      }
   }
   return result;
}

//---------------------------------------------------------------------------
// Name: constructEdges
// Modified by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Construct edges for graph pattern in OPTIONAL and GJOIN.
//---------------------------------------------------------------------------
static void constructEdges(QueryGraph::SubQuery& subQuery,set<unsigned>& bindings)
   // Construct the edges for a specfic subquery
{
   // Collect all variable bindings
   vector<set<unsigned> > patternBindings,optionalBindings,unionBindings,gjoinBindings;
   patternBindings.resize(subQuery.nodes.size());
   for (unsigned index=0,limit=patternBindings.size();index<limit;++index) {
      const QueryGraph::Node& n=subQuery.nodes[index];
      if (!n.constSubject) {
         patternBindings[index].insert(n.subject);
         bindings.insert(n.subject);
      }
      if (!n.constPredicate) {
         patternBindings[index].insert(n.predicate);
         bindings.insert(n.predicate);
      }
      if (!n.constObject) {
         patternBindings[index].insert(n.object);
         bindings.insert(n.object);
      }
   }
   optionalBindings.resize(subQuery.optional.size());
   for (unsigned index=0,limit=optionalBindings.size();index<limit;++index) {
      constructEdges(subQuery.optional[index],optionalBindings[index]);
      bindings.insert(optionalBindings[index].begin(),optionalBindings[index].end());
   }
   unionBindings.resize(subQuery.unions.size());
   for (unsigned index=0,limit=unionBindings.size();index<limit;++index) {
      for (vector<QueryGraph::SubQuery>::iterator iter=subQuery.unions[index].begin(),limit=subQuery.unions[index].end();iter!=limit;++iter)
         constructEdges(*iter,unionBindings[index]);
      bindings.insert(unionBindings[index].begin(),unionBindings[index].end());
   }
   gjoinBindings.resize(subQuery.gjoins.size());
   for (unsigned index=0,limit=gjoinBindings.size();index<limit;++index) {
      for (vector<QueryGraph::SubQuery>::iterator iter=subQuery.gjoins[index].begin(),limit=subQuery.gjoins[index].end();iter!=limit;++iter)
         constructEdges(*iter,gjoinBindings[index]);
      bindings.insert(gjoinBindings[index].begin(),gjoinBindings[index].end());
   }

  // Derive all edges
   subQuery.edges.clear();
   unsigned optionalOfs=patternBindings.size(),unionOfs=optionalOfs+optionalBindings.size(),gjoinOfs=unionOfs+gjoinBindings.size();

   vector<unsigned> common;
   for (unsigned index=0,limit=patternBindings.size();index<limit;++index) {
      for (unsigned index2=index+1;index2<limit;index2++)
         if (intersects(patternBindings[index],patternBindings[index2],common))
            subQuery.edges.push_back(QueryGraph::Edge(index,index2,common));
      for (unsigned index2=0,limit2=optionalBindings.size();index2<limit2;index2++)
         if (intersects(patternBindings[index],optionalBindings[index2],common))
            subQuery.edges.push_back(QueryGraph::Edge(index,optionalOfs+index2,common));
      for (unsigned index2=0,limit2=unionBindings.size();index2<limit2;index2++)
         if (intersects(patternBindings[index],unionBindings[index2],common))
            subQuery.edges.push_back(QueryGraph::Edge(index,unionOfs+index2,common));
      for (unsigned index2=0,limit2=gjoinBindings.size();index2<limit2;index2++)
         if (intersects(patternBindings[index],gjoinBindings[index2],common))
            subQuery.edges.push_back(QueryGraph::Edge(index,gjoinOfs+index2,common));
   }
   for (unsigned index=0,limit=optionalBindings.size();index<limit;++index) {
      for (unsigned index2=index+1;index2<limit;index2++)
         if (intersects(optionalBindings[index],optionalBindings[index2],common))
            subQuery.edges.push_back(QueryGraph::Edge(optionalOfs+index,optionalOfs+index2,common));
      for (unsigned index2=0,limit2=unionBindings.size();index2<limit2;index2++)
         if (intersects(optionalBindings[index],unionBindings[index2],common))
            subQuery.edges.push_back(QueryGraph::Edge(optionalOfs+index,unionOfs+index2,common));
      for (unsigned index2=0,limit2=gjoinBindings.size();index2<limit2;index2++)
         if (intersects(optionalBindings[index],gjoinBindings[index2],common))
            subQuery.edges.push_back(QueryGraph::Edge(optionalOfs+index,gjoinOfs+index2,common));
   }
   for (unsigned index=0,limit=unionBindings.size();index<limit;++index) {
      for (unsigned index2=index+1;index2<limit;index2++)
         if (intersects(unionBindings[index],unionBindings[index2],common))
            subQuery.edges.push_back(QueryGraph::Edge(unionOfs+index,unionOfs+index2,common));
      for (unsigned index2=0,limit2=gjoinBindings.size();index2<limit2;index2++)
         if (intersects(unionBindings[index],gjoinBindings[index2],common))
            subQuery.edges.push_back(QueryGraph::Edge(unionOfs+index,gjoinOfs+index2,common));
    }
   for (unsigned index=0,limit=gjoinBindings.size();index<limit;++index) {
      for (unsigned index2=index+1;index2<limit;index2++)
         if (intersects(gjoinBindings[index],gjoinBindings[index2],common))
            subQuery.edges.push_back(QueryGraph::Edge(gjoinOfs+index,gjoinOfs+index2,common));
   }
}
//---------------------------------------------------------------------------
void QueryGraph::constructEdges()
   // Construct the edges
{
   set<unsigned> bindings;
   ::constructEdges(query,bindings);
}
//---------------------------------------------------------------------------
