#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include <iostream>
#include <vector>
#include <set>
#include <algorithm>
#include <cassert>
#include <cmath>
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
using namespace std;
//---------------------------------------------------------------------------
/// Desired star size
static const unsigned starSize = 6;
//---------------------------------------------------------------------------
static bool contains(const vector<unsigned>& allNodes,unsigned id)
   // Is the id in the list?
{
   vector<unsigned>::const_iterator pos=lower_bound(allNodes.begin(),allNodes.end(),id);
   return ((pos!=allNodes.end())&&((*pos)==id));
}
//---------------------------------------------------------------------------
static string lookupId(Database& db,unsigned id)
   // Lookup a string id
{
   const char* start=0,*stop=0; Type::ID type; unsigned subType;
   db.getDictionary().lookupById(id,start,stop,type,subType);
   return string(start,stop);
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
class LookupFilter : public Operator {
   private:
   /// The input
   Operator* input;
   /// The lookup register
   Register* reg;
   /// The filter
   const vector<unsigned>& filter;

   public:
   /// Constructor
   LookupFilter(Operator* input,Register* reg,const vector<unsigned>& filter) : Operator(0),input(input),reg(reg),filter(filter) {}
   /// Destructor
   ~LookupFilter() { delete input; }

   /// Find the first tuple
   unsigned first();
   /// Find the next tuple
   unsigned next();
   /// Print the operator
   void print(PlanPrinter& out);
   /// Handle a merge hint
   void addMergeHint(Register* l,Register* r);
   /// Register parts of the tree that can be executed asynchronous
   void getAsyncInputCandidates(Scheduler& scheduler);
};
//---------------------------------------------------------------------------
unsigned LookupFilter::first()
   // Find the first tuple
{
   unsigned count=input->first();
   if (!count)
      return false;
   if (contains(filter,reg->value))
      return count;

   return next();
}
//---------------------------------------------------------------------------
unsigned LookupFilter::next()
   // Find the next tuple
{
   while (true) {
      unsigned count=input->next();
      if (!count)
         return false;
      if (contains(filter,reg->value))
         return count;
   }
}
//---------------------------------------------------------------------------
void LookupFilter::print(PlanPrinter& out)
   // Print the operator
{
   out.beginOperator("LookupFilter",expectedOutputCardinality,observedOutputCardinality);

   std::string pred=out.formatRegister(reg);
   pred+=" in {";
   bool first=true;
   for (std::vector<unsigned>::const_iterator iter=filter.begin(),limit=filter.end();iter!=limit;++iter) {
      if (first) first=true; else pred+=" ";
      pred+=out.formatValue(*iter);
   }
   pred+="}";
   out.addGenericAnnotation(pred);

   input->print(out);

   out.endOperator();
}
//---------------------------------------------------------------------------
void LookupFilter::addMergeHint(Register* l,Register* r)
   // Handle a merge hint
{
   input->addMergeHint(l,r);
}
//---------------------------------------------------------------------------
void LookupFilter::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
template <class T> void eliminateDuplicates(vector<T>& data)
   // Eliminate duplicates in a sorted list
{
   data.resize(unique(data.begin(),data.end())-data.begin());
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
static vector<pair<unsigned,unsigned> > findLiterals(unsigned node,Database& db,const vector<unsigned>& allNodes,unsigned size)
   // Find literals connected to a node
{
   vector<pair<unsigned,unsigned> > literals;

   FactsSegment::Scan scan;
   if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object),node,0,0)) do {
      if (scan.getValue1()!=node) break;
      if (contains(allNodes,scan.getValue2())) continue;
      literals.push_back(pair<unsigned,unsigned>(scan.getValue2(),scan.getValue3()));
   } while (scan.next());
   assert(literals.size()>=size);
   for (unsigned index=0;index<size;index++)
      swap(literals[index],literals[index+(random()%(literals.size()-index))]);
   literals.resize(size);

   return literals;
}
//---------------------------------------------------------------------------
static vector<unsigned> buildStar(unsigned node,Database& db,const vector<unsigned>& allNodes,unsigned size,unsigned& relationID)
   // Build a star pattern
{
   // Pick some suitable literals
   vector<pair<unsigned,unsigned> > literals=findLiterals(node,db,allNodes,size);

   // Output base sizes
   double independentSize=1.0;
   for (unsigned index=0;index<literals.size();index++) {
      cout << "# ?n" << node << " <" << lookupId(db,literals[index].first) << "> \"" << lookupId(db,literals[index].second) << "\"." << endl;
      AggregatedFactsSegment::Scan scan;
      scan.first(db.getAggregatedFacts(Database::Order_Predicate_Object_Subject),literals[index].first,literals[index].second);
      cout << "relation r" << (relationID+index) << " " << scan.getCount() << endl;
      independentSize=independentSize*static_cast<double>(scan.getCount());
   }

   // Compute join selectivites
   vector<double> selectivities;
   vector<unsigned> first,result;
   for (unsigned index=0;index<size;index++) {
      // Collect all matches
      vector<unsigned> nodes;
      FactsSegment::Scan scan;
      scan.first(db.getFacts(Database::Order_Predicate_Object_Subject),literals[index].first,literals[index].second,0);
      do {
         if ((scan.getValue1()!=literals[index].first)||(scan.getValue2()!=literals[index].second))
            break;
         nodes.push_back(scan.getValue3());
      } while (scan.next());

      // First entry?
      if (index==0) {
         first=nodes;
         result=nodes;
      } else {
         unsigned matches=0;
         vector<unsigned> newResult;
         for (vector<unsigned>::const_iterator iter=nodes.begin(),limit=nodes.end();iter!=limit;++iter) {
            if (contains(first,*iter))
               matches++;
            if (contains(result,*iter))
               newResult.push_back(*iter);
         }
         swap(newResult,result);
         double sel=static_cast<double>(matches)/(static_cast<double>(first.size())*static_cast<double>(nodes.size()));
         selectivities.push_back(sel);
         independentSize*=sel;
      }
   }

   // Output selectivites
   double correction=pow(static_cast<double>(result.size())/independentSize,1.0/(static_cast<double>(size)-1));
   for (unsigned index=1;index<size;index++)
      cout << "join r" << relationID << " - r" << (relationID+index) << " " << (selectivities[index-1]*correction) << endl;
   relationID+=size;

   return result;
}
//---------------------------------------------------------------------------
static vector<unsigned> buildChain(unsigned from,const vector<unsigned>& fromIds,unsigned to,const vector<unsigned>& toIds,Database& db,unsigned& relationId)
   // Build a chain
{
   // Run the query to get the true result
   vector<unsigned> result;
   {
      Register ls,lo,rs,ro;
      ls.reset(); lo.reset(); rs.reset(); ro.reset();
      AggregatedIndexScan* scan1=AggregatedIndexScan::create(db,Database::Order_Object_Subject_Predicate,&ls,false,0,false,&lo,false,0);
      LookupFilter* filter1=new LookupFilter(scan1,&ls,fromIds);
      AggregatedIndexScan* scan2=AggregatedIndexScan::create(db,Database::Order_Subject_Object_Predicate,&rs,false,0,false,&ro,false,0);
      LookupFilter* filter2=new LookupFilter(scan2,&ro,toIds);
      vector<Register*> lt,rt; lt.push_back(&ls); rt.push_back(&ro);
      MergeJoin join(filter1,&lo,lt,filter2,&rs,rt,0);

      if (join.first()) do {
         result.push_back(ro.value);
      } while (join.next());

      sort(result.begin(),result.end());
      eliminateDuplicates(result);
   }

   // Make up two relations with all facts
   unsigned allFacts=db.getFacts(Database::Order_Subject_Predicate_Object).getCardinality();
   cout << "relation r" << relationId << " " << allFacts << endl;
   cout << "relation r" << (relationId+1) << " " << allFacts << endl;

   // And constructs joins
   double s1=0.5+(rand()/2.0),s2=0.5+(rand()/2.0),s3=0.5+(rand()/2.0);
   double independentSize=static_cast<double>(fromIds.size())*static_cast<double>(toIds.size())*static_cast<double>(allFacts)*static_cast<double>(allFacts)*s1*s2*s3;
   double correction=cbrt(static_cast<double>(result.size())/independentSize);
   cout << "join r" << from << " - r" << relationId << " " << (s1*correction) << endl;
   cout << "join r" << relationId << " - r" << (relationId+1) << " " << (s2*correction) << endl;
   cout << "join r" << (relationId+1) << " - r" << to << " " << (s3*correction) << endl;
   relationId+=2;

   return result;
}
//---------------------------------------------------------------------------
static string lookupURL(Database& db,unsigned id)
   // Lookup a URL
{
   const char* start=0,*end=start; Type::ID type; unsigned subType;
   db.getDictionary().lookupById(id,start,end,type,subType);
   return "<"+string(start,end)+">";
}
//---------------------------------------------------------------------------
static string lookupLiteral(Database& db,unsigned id)
   // Lookup a literal value
{
   const char* start=0,*end=start; Type::ID type; unsigned subType;
   db.getDictionary().lookupById(id,start,end,type,subType);

   if (type==Type::URI)
      return "<"+string(start,end)+">"; else
      return "\""+string(start,end)+"\"";
}
//---------------------------------------------------------------------------
static void constructQuery(unsigned id,Database& db,const vector<unsigned>& allNodes,const vector<unsigned>& central,const vector<pair<unsigned,unsigned> >& hops)
   // Build a query
{
   // Pick a central node at random
   unsigned node2=central[random()%central.size()];

   // Pick a suitable predecessor
   unsigned node1;
   {
      vector<unsigned> pre;
      for (vector<pair<unsigned,unsigned> >::const_iterator iter=hops.begin(),limit=hops.end();iter!=limit;++iter)
         if ((*iter).second==node2)
            pre.push_back((*iter).first);
      node1=pre[random()%pre.size()];
   }

   // Pick a suitable successor
   unsigned node3;
   {
      vector<unsigned> post;
      for (vector<pair<unsigned,unsigned> >::const_iterator iter=hops.begin(),limit=hops.end();iter!=limit;++iter)
         if ((*iter).first==node2)
            post.push_back((*iter).second);
      node3=post[random()%post.size()];
   }

   // Build a query
   if (getenv("JOINGRAPH")) {
      cout << "graph sparql" << id << endl;
      unsigned relationID=0;
      unsigned node1Scan=relationID;
      vector<unsigned> node1Equiv=buildStar(node1,db,allNodes,6,relationID);
      unsigned node2Scan=relationID;
      vector<unsigned> node2Equiv=buildStar(node2,db,allNodes,5,relationID);
      unsigned node3Scan=relationID;
      vector<unsigned> node3Equiv=buildStar(node3,db,allNodes,5,relationID);
      vector<unsigned> node12Equiv=buildChain(node1Scan,node1Equiv,node2Scan,node2Equiv,db,relationID);
      vector<unsigned> node123Equiv=buildChain(node2Scan,node12Equiv,node3Scan,node3Equiv,db,relationID);

      // Include statistics, could be used for corrections
      cout << "# true cardinality " << node123Equiv.size() << ", ignored for now" << endl << endl;
   } else {
      vector<pair<unsigned,unsigned> > literals1,literals2;
      literals1=findLiterals(node1,db,allNodes,6);
      literals2=findLiterals(node2,db,allNodes,5);

      // Get connection candidates
      vector<pair<unsigned,unsigned> > connections;
      {
         Register ls,lp,lo,rs,rp,ro;
         ls.reset(); lp.reset(); lo.reset(); rs.reset(); rp.reset(); ro.reset();
         ls.value=node1; ro.value=node2;
         IndexScan* scan1=IndexScan::create(db,Database::Order_Subject_Object_Predicate,&ls,true,&lp,false,&lo,false,0);
         IndexScan* scan2=IndexScan::create(db,Database::Order_Object_Subject_Predicate,&rs,false,&rp,false,&ro,true,0);
         vector<Register*> lt,rt; lt.push_back(&lp); rt.push_back(&rp);
         MergeJoin join(scan1,&lo,lt,scan2,&rs,rt,0);

         if (join.first()) do {
            connections.push_back(pair<unsigned,unsigned>(lp.value,rp.value));
         } while (join.next());
      }

      cout << "select ?a ?vo" << endl
           << "where {" << endl
           << "   ?a " << lookupURL(db,literals1[0].first) << " ?vo ." << endl;
      for (unsigned index=1;index<literals1.size();index++)
         cout << "   ?a " << lookupURL(db,literals1[index].first) << " " << lookupLiteral(db,literals1[index].second) << " ." << endl;
      for (unsigned index=0;index<literals2.size();index++)
         cout << "   ?b " << lookupURL(db,literals2[index].first) << " " << lookupLiteral(db,literals2[index].second) << " ." << endl;
      unsigned ms=random()%connections.size();
      cout << "   ?a " << lookupURL(db,connections[ms].first) << " ?ab . ?ab " << lookupURL(db,connections[ms].second) << " ?b ." << endl << "}" << endl << endl;
   }
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc<2) {
      cout << "usage: " << argv[0] << " <rdfstore> [term]" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      cout << "unable to open " << argv[1] << endl;
      return 1;
   }

   // Collect all nodes
   vector<unsigned> allNodes;
   {
      FullyAggregatedFactsSegment::Scan scan;
      if (scan.first(db.getFullyAggregatedFacts(Database::Order_Subject_Predicate_Object))) do {
         allNodes.push_back(scan.getValue1());
      } while (scan.next());
   }
   cerr << "found " << allNodes.size() << " nodes" << endl;

   // Examine candidates
   vector<unsigned> candidates;
   {
      AggregatedFactsSegment::Scan scan;
      unsigned current=~0u,literalCount=0;
      if (scan.first(db.getAggregatedFacts(Database::Order_Subject_Object_Predicate))) do {
         // A new node?
         if (scan.getValue1()!=current) {
            if (literalCount>=starSize)
               candidates.push_back(current);
            current=scan.getValue1();
            literalCount=0;
         }
         // Check if we have found a literal
         if (!contains(allNodes,scan.getValue2()))
            literalCount+=scan.getCount();
      } while (scan.next());
   }
   cerr << "found " << candidates.size() << " node candidates" << endl;

   // Compute all two hops
   vector<pair<unsigned,unsigned> > hops;
   {
      Register ls,lo,rs,ro;
      ls.reset(); lo.reset(); rs.reset(); ro.reset();
      AggregatedIndexScan* scan1=AggregatedIndexScan::create(db,Database::Order_Object_Subject_Predicate,&ls,false,0,false,&lo,false,0);
      LookupFilter* filter1=new LookupFilter(scan1,&ls,candidates);
      AggregatedIndexScan* scan2=AggregatedIndexScan::create(db,Database::Order_Subject_Object_Predicate,&rs,false,0,false,&ro,false,0);
      LookupFilter* filter2=new LookupFilter(scan2,&ro,candidates);
      vector<Register*> lt,rt; lt.push_back(&ls); rt.push_back(&ro);
      MergeJoin join(filter1,&lo,lt,filter2,&rs,rt,0);

      if (join.first()) do {
         if (ls.value!=ro.value)
            hops.push_back(pair<unsigned,unsigned>(ls.value,ro.value));
      } while (join.next());

      sort(hops.begin(),hops.end());
      eliminateDuplicates(hops);
   }
   cerr << "found " << hops.size() << " 2-hop matches" << endl;

   // Find all central nodes for a three-star query
   vector<unsigned> central;
   {
      // Collect all unique object nodes
      vector<unsigned> objects;
      for (vector<pair<unsigned,unsigned> >::const_iterator iter=hops.begin(),limit=hops.end();iter!=limit;++iter)
         objects.push_back((*iter).second);
      sort(objects.begin(),objects.end());
      eliminateDuplicates(objects);

      // Find matching subject nodes
      for (vector<pair<unsigned,unsigned> >::const_iterator iter=hops.begin(),limit=hops.end();iter!=limit;++iter)
         if (contains(objects,(*iter).first))
            central.push_back((*iter).first);
      sort(central.begin(),central.end());
      eliminateDuplicates(central);
   }
   cerr << "found " << central.size() << " central nodes" << endl;

   // Build a query
   for (unsigned index=0;index<1000;index++)
      constructQuery(index,db,allNodes,central,hops);
}
//---------------------------------------------------------------------------
