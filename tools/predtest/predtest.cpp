#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/MergeJoin.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/operator/ResultsPrinter.hpp"
#include "rts/operator/Scheduler.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/ExactStatisticsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include "rts/segment/PredicateSetSegment.hpp"
#include "infra/osdep/MemoryMappedFile.hpp"
#include <iostream>
#include <fstream>
#include <map>
#include <algorithm>
#include <cmath>
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
static double qError(double a,double b)
   // Compute the q-error
{
   if (a<b)
      swap(a,b);
   if (b<=0.0)
      return 1000000; // infty
   return a/b;
}
//---------------------------------------------------------------------------
struct PairInfo { unsigned p1,p2,count; };
static inline bool orderChunks(const pair<const PairInfo*,const PairInfo*>& ac,const pair<const PairInfo*,const PairInfo*>& bc) { const PairInfo& a=*ac.first,&b=*bc.first; return (a.p1>b.p1)||((a.p1==b.p1)&&(a.p2>b.p2)); }
//---------------------------------------------------------------------------
static void doPairs(Database& db)
   // Find common pairs
{
   Runtime runtime(db);
   runtime.allocateRegisters(6);
   Register* S1=runtime.getRegister(0),*P1=runtime.getRegister(1); //,*O1=runtime.getRegister(2);
   Register* S2=runtime.getRegister(3),*P2=runtime.getRegister(4); //,*O2=runtime.getRegister(5);

   vector<pair<const PairInfo*,const PairInfo*> > chunks;
   {
      AggregatedIndexScan* scan1=AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,S1,false,P1,false,0,false,0);
      AggregatedIndexScan* scan2=AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,S2,false,P2,false,0,false,0);
      vector<Register*> tail1,tail2; tail1.push_back(P1); tail2.push_back(P2);
      MergeJoin join(scan1,S1,tail1,scan2,S2,tail2,0);

      const unsigned maxSize = 10000000;
      unsigned matches=0,total=0;
      map<pair<unsigned,unsigned>,unsigned> classes;
      ofstream out("bin/pairs.raw");
      const PairInfo* ofs=0;
      for (unsigned count=join.first();count;count=join.next()) {
         if (P2->value<P1->value) continue;
         matches++;
         total+=count;
         classes[pair<unsigned,unsigned>(P1->value,P2->value)]++;

         if (classes.size()==maxSize) {
            const PairInfo* start=ofs;
            for (map<pair<unsigned,unsigned>,unsigned>::const_iterator iter=classes.begin(),limit=classes.end();iter!=limit;++iter) {
               PairInfo p;
               p.p1=(*iter).first.first;
               p.p2=(*iter).first.second;
               p.count=(*iter).second,
               out.write(reinterpret_cast<char*>(&p),sizeof(p));
               ofs++;
            }
            const PairInfo* stop=ofs;
            chunks.push_back(pair<const PairInfo*,const PairInfo*>(start,stop));
            classes.clear();
         }
      }
      if (!classes.empty()) {
         const PairInfo* start=ofs;
         for (map<pair<unsigned,unsigned>,unsigned>::const_iterator iter=classes.begin(),limit=classes.end();iter!=limit;++iter) {
            PairInfo p;
            p.p1=(*iter).first.first;
            p.p2=(*iter).first.second;
            p.count=(*iter).second,
            out.write(reinterpret_cast<char*>(&p),sizeof(p));
            ofs++;
         }
         const PairInfo* stop=ofs;
         chunks.push_back(pair<const PairInfo*,const PairInfo*>(start,stop));
         classes.clear();
      }
      cout << "found " << matches << " matches in " << total << " result triples." << endl;
   }

   {
      MemoryMappedFile in;
      in.open("bin/pairs.raw");
      ofstream out("bin/pairs");
      for (vector<pair<const PairInfo*,const PairInfo*> >::iterator iter=chunks.begin(),limit=chunks.end();iter!=limit;++iter) {
         (*iter).first=reinterpret_cast<const PairInfo*>(in.getBegin())+((*iter).first-static_cast<const PairInfo*>(0));
         (*iter).second=reinterpret_cast<const PairInfo*>(in.getBegin())+((*iter).second-static_cast<const PairInfo*>(0));
      }
      make_heap(chunks.begin(),chunks.end(),orderChunks);
      PairInfo current; bool hasCurrent=false;
      unsigned classCount=0;
      while (!chunks.empty()) {
         PairInfo next=*chunks.front().first;
         pop_heap(chunks.begin(),chunks.end(),orderChunks);
         if ((++chunks.back().first)==(chunks.back().second))
            chunks.pop_back(); else
            push_heap(chunks.begin(),chunks.end(),orderChunks);

         if (hasCurrent) {
            if ((current.p1==next.p1)&&(current.p2==next.p2)) {
               current.count+=next.count;
            } else {
               out.write(reinterpret_cast<char*>(&current),sizeof(current));
               classCount++;
               current=next;
            }
         } else {
            current=next;
            hasCurrent=true;
         }
      }
      if (hasCurrent) {
         out.write(reinterpret_cast<char*>(&current),sizeof(current));
         classCount++;
      }
      cout << classCount << " predicate combinations." << endl;
   }
   remove("bin/pairs.raw");
}
//---------------------------------------------------------------------------
static void doAnalyze(Database& db)
{
   map<unsigned,unsigned> predicateFrequencies;
   {
      AggregatedFactsSegment::Scan scan;
      if (scan.first(db.getAggregatedFacts(Database::Order_Predicate_Subject_Object))) do {
         predicateFrequencies[scan.getValue1()]++;
      } while (scan.next());
   }
   cout << predicateFrequencies.size() << " predicates" << endl;

   MemoryMappedFile in;
   if (!in.open("bin/pairs")) {
      cerr << "unable to open bin/pairs" << endl;
      return;
   }
   const PairInfo* begin=reinterpret_cast<const PairInfo*>(in.getBegin());
   const PairInfo* end=reinterpret_cast<const PairInfo*>(in.getEnd());
   cout << (end-begin) << " predicate pairs in total" << endl;


   map<unsigned,unsigned> predicatePartners;
   {
      unsigned count=0;
      for (const PairInfo* iter=begin;iter!=end;++iter) {
         if (iter->count>1)
            count++;

         predicatePartners[iter->p1]++;
         if (iter->p1<iter->p2)
            predicatePartners[iter->p2]++;
      }
      cout << count << " combinations occur more than once" << endl;
   }

   unsigned t1=begin[1000000].p1,t2=begin[1000000].p2,tc=begin[1000000].count;
   cout << t1 << " " << t2 << " " << tc << endl
        << predicateFrequencies[t1] << " " << predicateFrequencies[t2] << " " << predicatePartners[t1] << " " << predicatePartners[t2] << endl
        << ((predicatePartners[t2]<predicatePartners[t1])?(predicateFrequencies[t1]/predicatePartners[t2]):(predicateFrequencies[t2]/predicatePartners[t1])) << endl;

   {
      unsigned outlierCount=0;
      for (const PairInfo* iter=begin;iter!=end;++iter) {
         unsigned expected;
         if (predicatePartners[iter->p2]<predicatePartners[iter->p1])
            expected=predicateFrequencies[iter->p1]/predicatePartners[iter->p2]; else
            expected=predicateFrequencies[iter->p2]/predicatePartners[iter->p1];
         ++expected; // round up
         if (expected<1) expected=1;
         if ((iter->count>10)&&(iter->count>2*expected)) outlierCount++;
      }
      cout << outlierCount << " combinations occur more often than expected" << endl;
   }
}
//---------------------------------------------------------------------------
static double computeError(double observed,double estimated)
   // Compute the estimation error
{
   if (estimated>=observed) {
      return (estimated/observed)-1.0;
   } else {
      return -((observed/estimated)-1.0);
   }
}
//---------------------------------------------------------------------------
static void doSets(Database& db)
{
   // Precompute the predicate sets
   PredicateSetSegment ps(db.getFirstPartition());
   ps.computePredicateSets();
   { unsigned count,entries,size; ps.getStatistics(count,entries,size); cerr << count << " " << entries << " " << size << endl; }

   // Run all two-predicate queries
   Runtime runtime(db);
   runtime.allocateRegisters(6);
   Register* s1=runtime.getRegister(0),*p1=runtime.getRegister(1);
   Register* s2=runtime.getRegister(3),*p2=runtime.getRegister(4);
   vector<Register*> et,p2t;
   p2t.push_back(p2);
   FullyAggregatedFactsSegment::Scan scan1;
   map<int,unsigned> errorHistogramSubjects,errorHistogramTuples;
   if (scan1.first(db.getFullyAggregatedFacts(Database::Order_Predicate_Subject_Object))) do {
      // Run a merge join
      map<unsigned,unsigned> tupleCounts,subjectCounts;
      s1->reset();
      p1->value=scan1.getValue1();
      s2->reset();
      p2->reset();
      MergeJoin join(
         AggregatedIndexScan::create(db,Database::Order_Predicate_Subject_Object,s1,false,p1,true,0,false,0),
         s1,et,
         AggregatedIndexScan::create(db,Database::Order_Subject_Predicate_Object,s2,false,p2,false,0,false,0),
         s2,p2t,
         1);
      unsigned count;
      if ((count=join.first())!=0) do {
         tupleCounts[p2->value]+=count;
         subjectCounts[p2->value]++;
      } while ((count=join.next())!=0);

      // Compare with predicate cardinalities
      cerr << p1->value << endl;
      for (map<unsigned,unsigned>::const_iterator iter=tupleCounts.begin(),limit=tupleCounts.end(),iter2=subjectCounts.begin();iter!=limit;++iter,++iter2) {
         // Call the predicator
         vector<unsigned> predicates;
         predicates.push_back(p1->value);
         predicates.push_back((*iter).first);
         unsigned predictedSubjects; double predictedTuples;
         ps.getStarCardinality(predicates,predictedSubjects,predictedTuples);

         // Compute the errors
         double subjectError=computeError((*iter2).second,predictedSubjects),tupleError=computeError((*iter).second,predictedTuples);
         if ((tupleError>0.1)||(tupleError<-0.1))
            cerr << p1->value << "\t" << (*iter2).first << "\t" << predictedSubjects << "\t" << (*iter2).second << "\t" << subjectError << "\t" << predictedTuples << "\t" << (*iter).second << "\t" << tupleError << endl;

         // Remember the errors
         int subjectSlot=static_cast<int>(subjectError),tuplesSlot=static_cast<int>(tupleError);
         if (subjectSlot<-100) subjectSlot=-100;
         if (subjectSlot>100) subjectSlot=100;
         if (tuplesSlot<-100) tuplesSlot=-100;
         if (tuplesSlot>100) tuplesSlot=100;
         errorHistogramSubjects[subjectSlot]++;
         errorHistogramTuples[tuplesSlot]++;
      }
   } while (scan1.next());

   // Unify the histograms and show them
   for (map<int,unsigned>::const_iterator iter=errorHistogramTuples.begin(),limit=errorHistogramTuples.end();iter!=limit;++iter)
      errorHistogramSubjects[(*iter).first];
   for (map<int,unsigned>::const_iterator iter=errorHistogramSubjects.begin(),limit=errorHistogramSubjects.end();iter!=limit;++iter)
      cout << (*iter).first << "\t" << (*iter).second << "\t" << errorHistogramTuples[(*iter).first] << endl;
}
//---------------------------------------------------------------------------
static double getRealCardinality(Database& db,const QueryGraph& qg)
   // Get the real output cardinality
{
   // Run the optimizer
   PlanGen plangen;
   Plan* plan=plangen.translate(db,qg);
   if (!plan) {
      cerr << "plan generation failed" << endl;
      throw;
   }
   Operator::disableSkipping=true;

   // Build a physical plan
   Runtime runtime(db);
   Operator* operatorTree=CodeGen().translate(runtime,qg,plan,true);
   Operator* realRoot=dynamic_cast<ResultsPrinter*>(operatorTree)->getInput();

   // And execute it
   Scheduler scheduler;
   scheduler.execute(realRoot);

   // Output the counts
   double trueCard=realRoot->getObservedOutputCardinality();
   delete operatorTree;

   return trueCard;
}
//---------------------------------------------------------------------------
static string readFile(const char* name)
   // Read the input query
{
   ifstream in(name);
   if (!in.is_open()) {
      cerr << "unable to open " << name << endl;
      throw;
   }
   string result;
   while (true) {
      string s;
      getline(in,s);
      if (!in.good())
         break;
      result+=s;
      result+='\n';
   }
   return result;
}
//---------------------------------------------------------------------------
static void readQuery(Database& db,const char* file,QueryGraph& queryGraph)
   // Evaluate a query
{
   string query=readFile(file);

   // Parse the query
   SPARQLLexer lexer(query);
   SPARQLParser parser(lexer);
   try {
      parser.parse();
   } catch (const SPARQLParser::ParserException& e) {
      cout << "parse error: " << e.message << endl;
      throw;
   }

   // And perform the semantic anaylsis
   try {
      SemanticAnalysis semana(db);
      semana.transform(parser,queryGraph);
   } catch (const SemanticAnalysis::SemanticException& e) {
      cout << "semantic error: " << e.message << endl;
      throw;
   }
}
//---------------------------------------------------------------------------
class CycleChecker
{
   private:
   /// Edges
   set<pair<unsigned,unsigned> > edges;

   /// Can we reach a node
   bool canReach(unsigned from,unsigned to,set<unsigned>& visited);

   public:
   /// Should we add an edge?
   bool doEdge(unsigned from,unsigned to);
};
//---------------------------------------------------------------------------
bool CycleChecker::canReach(unsigned from,unsigned to,set<unsigned>& visited)
   // Can we reach a node
{
   if (from==to)
      return true;

   if (visited.count(from))
      return false;
   visited.insert(from);

   for (set<pair<unsigned,unsigned> >::const_iterator iter=edges.begin(),limit=edges.end();iter!=limit;++iter)
      if (((*iter).first==from)&&canReach((*iter).second,to,visited))
         return true;

   return false;
}
//---------------------------------------------------------------------------
bool CycleChecker::doEdge(unsigned from,unsigned to)
{
   if (from==to)
      return false;

   set<unsigned> visited;
   if (canReach(from,to,visited))
      return false;

   edges.insert(make_pair(from,to));
   edges.insert(make_pair(to,from));
   return true;
}
//---------------------------------------------------------------------------
static double estimateStocker(ExactStatisticsSegment& es,map<pair<unsigned,unsigned>,uint64_t>& counts2,QueryGraph& qg)
{
   QueryGraph::SubQuery& q=qg.getQuery();
   double total=es.getCardinality(~0u,~0u,~0u);

   // Base patterns
   double card=1;
   for (vector<QueryGraph::Node>::const_iterator iter=q.nodes.begin(),limit=q.nodes.end();iter!=limit;++iter) {
      double pattern=total;
      if ((*iter).constSubject)
         pattern*=static_cast<double>(es.getCardinality((*iter).subject,~0u,~0u))/total;
      if ((*iter).constPredicate)
         pattern*=static_cast<double>(es.getCardinality(~0u,(*iter).predicate,~0u))/total;
      if ((*iter).constObject)
         pattern*=static_cast<double>(es.getCardinality(~0u,~0u,(*iter).object))/total;
      card*=pattern;
   }

   // Join selectivites
   CycleChecker checker;
   for (vector<QueryGraph::Edge>::const_iterator iter=q.edges.begin(),limit=q.edges.end();iter!=limit;++iter) {
      if (q.nodes[(*iter).from].constPredicate&&q.nodes[(*iter).to].constPredicate&&(!q.nodes[(*iter).from].constSubject)&&(!q.nodes[(*iter).to].constSubject)&&(q.nodes[(*iter).from].subject==q.nodes[(*iter).to].subject))
         if (checker.doEdge((*iter).from,(*iter).to))
            card*=static_cast<double>(counts2[make_pair((*iter).from,(*iter).to)])/(total*total);
   }

   if (card<1) card=1;
   return card;
}
//---------------------------------------------------------------------------
static void doStocker(Database& db,int argc,char** argv)
{
   ExactStatisticsSegment& es=db.getExactStatistics();
   unsigned total=es.getCardinality(~0u,~0u,~0u);

   // Pre-compute counts
   map<unsigned,uint64_t> counts1;
   map<pair<unsigned,unsigned>,uint64_t> counts2;
   {
      AggregatedFactsSegment::Scan scan;
      vector<pair<unsigned,unsigned> > entries;
      unsigned current=~0u;
      if (scan.first(db.getAggregatedFacts(Database::Order_Subject_Predicate_Object))) do {
         if (scan.getValue1()!=current) {
            for (vector<pair<unsigned,unsigned> >::const_iterator iter=entries.begin(),limit=entries.end();iter!=limit;++iter) {
               counts1[(*iter).first]+=(*iter).second;
               for (vector<pair<unsigned,unsigned> >::const_iterator iter2=entries.begin(),limit2=entries.end();iter2!=limit2;++iter2) {
                  counts2[make_pair((*iter).first,(*iter2).first)]+=(*iter).second*(*iter2).second;
               }
            }
            entries.clear();
         }
         entries.push_back(make_pair(scan.getValue2(),scan.getCount()));
      } while (scan.next());
      for (vector<pair<unsigned,unsigned> >::const_iterator iter=entries.begin(),limit=entries.end();iter!=limit;++iter) {
         counts1[(*iter).first]+=(*iter).second;
         for (vector<pair<unsigned,unsigned> >::const_iterator iter2=entries.begin(),limit2=entries.end();iter2!=limit2;++iter2) {
            counts2[make_pair((*iter).first,(*iter2).first)]+=(*iter).second*(*iter2).second;
         }
      }
   }

   if (argc>0) {
      vector<double> errors;
      double prod=1;
      for (int index=0;index<argc;index++) {
         QueryGraph qg;
         readQuery(db,argv[index],qg);
         if (qg.knownEmpty()) {
            cerr << argv[index] << " has an empty result!" << endl;
            continue;
         }
         double estimate=estimateStocker(es,counts2,qg);
         double real=getRealCardinality(db,qg);
         double error=qError(estimate,real);
         prod*=estimate;
         cout << argv[index] << " " << estimate << " " << error << endl;
         errors.push_back(error);
      }
      sort(errors.begin(),errors.end());
      double sum=0;
      for (vector<double>::const_iterator iter=errors.begin(),limit=errors.end();iter!=limit;++iter) {
         sum+=(*iter);
      }
      cout << (pow(prod,1.0/static_cast<double>(errors.size()))) << " " << (errors[errors.size()/2]) << " " << (errors.back()) << " " << (sum/static_cast<double>(errors.size())) << endl;
   } else {
      // Run all two-predicate queries
      map<int,unsigned> errorHistogramTuples;
      unsigned summary[6]={0,0,0,0,0,0}; double maxQError=0;
      for (map<pair<unsigned,unsigned>,uint64_t>::const_iterator iter=counts2.begin(),limit=counts2.end();iter!=limit;++iter) {
         // Split the query
         unsigned a=(*iter).first.first;
         unsigned b=(*iter).first.second;

         // Analyze
         unsigned c1=counts1[a],c2=counts1[b];
         double joinSel=static_cast<double>((*iter).second)/(static_cast<double>(total)*static_cast<double>(total));
         double predictedTuples=joinSel*c1*c2;

         // Compute the errors
         double tupleError=computeError((*iter).second,predictedTuples);

         // Remember the errors
         int tuplesSlot=static_cast<int>(tupleError);
         if (tuplesSlot<-100) tuplesSlot=-100;
         if (tuplesSlot>100) tuplesSlot=100;
         errorHistogramTuples[tuplesSlot]++;

         double qError=(tupleError<0)?(1.0-tupleError):(1.0+tupleError);
         if (qError<=2.0) summary[0]++; else
         if (qError<=5.0) summary[1]++; else
         if (qError<=10.0) summary[2]++; else
         if (qError<=100.0) summary[3]++; else
         if (qError<=1000.0) summary[4]++; else
            summary[5]++;
         if (qError>maxQError) {
            maxQError=qError;
            cerr << maxQError << endl;
         }
      }

      // Show the histogram
      for (map<int,unsigned>::const_iterator iter=errorHistogramTuples.begin(),limit=errorHistogramTuples.end();iter!=limit;++iter)
         cout << (*iter).first << "\t" << (*iter).second << endl;
      unsigned totalSum=0;
      for (unsigned index=0;index<6;index++) {
         cout << summary[index] << " ";
         totalSum+=summary[index];
      }
      cout << maxQError << endl;
      if (!totalSum) totalSum=1;
      for (unsigned index=0;index<6;index++)
         cout << ((static_cast<double>(summary[index])*100.0)/static_cast<double>(totalSum)) << " ";
      cout << maxQError << endl;
   }
}
//---------------------------------------------------------------------------
static void doMaduko2(Database& db,Database::DataOrder order,map<unsigned,vector<pair<unsigned,unsigned> > >& lookup,const char* name,double threshold,map<unsigned,double>& baseSize,map<pair<unsigned,unsigned>,unsigned>& result)
   // Construct a subgraph of size 2
{
   result.clear();

   AggregatedFactsSegment::Scan scan;
   uint64_t count=0;
   if (scan.first(db.getAggregatedFacts(order))) {
      map<unsigned,unsigned> matches;
      uint64_t fanoutSum=0;
      unsigned current=scan.getValue1();
      while (true) {
         map<unsigned,vector<pair<unsigned,unsigned> > >::const_iterator iter=lookup.find(scan.getValue2());
         if (iter!=lookup.end()) {
            for (vector<pair<unsigned,unsigned> >::const_iterator iter2=(*iter).second.begin(),limit2=(*iter).second.end();iter2!=limit2;++iter2) {
               unsigned c=(*iter2).second,c2=scan.getCount()*c;
               matches[(*iter2).first]+=c2;
               fanoutSum+=c2;
            }
         }

         if (!scan.next()) {
            count+=matches.size();
            break;
         } else if (scan.getValue1()!=current) {
            count+=matches.size();

            double expected=static_cast<double>(fanoutSum)/static_cast<double>(matches.size());
            baseSize[current]=expected;
            for (map<unsigned,unsigned>::const_iterator iter=matches.begin(),limit=matches.end();iter!=limit;++iter)
               if (qError((*iter).second,expected)>threshold) {
                  result[make_pair(current,(*iter).first)]=(*iter).second;
               }

            matches.clear(); fanoutSum=0;
            current=scan.getValue1();
         }
      }
      double expected=static_cast<double>(fanoutSum)/static_cast<double>(matches.size());
      baseSize[current]=expected;
      for (map<unsigned,unsigned>::const_iterator iter=matches.begin(),limit=matches.end();iter!=limit;++iter)
         if (qError((*iter).second,expected)>threshold) {
            result[make_pair(current,(*iter).first)]=(*iter).second;
         }
   }
   cout << count << " " << name << " matches" << endl;
   cout << result.size() << " above error threshold" << endl;
}
//---------------------------------------------------------------------------
static double estimateMaduko(ExactStatisticsSegment& es,map<unsigned,double>& baseSizeSS,map<pair<unsigned,unsigned>,unsigned>& exactSS,QueryGraph& qg)
{
   QueryGraph::SubQuery& q=qg.getQuery();
   double total=es.getCardinality(~0u,~0u,~0u);

   // Base patterns
   double card=1;
   for (vector<QueryGraph::Node>::const_iterator iter=q.nodes.begin(),limit=q.nodes.end();iter!=limit;++iter) {
      double pattern=total;
      if ((*iter).constSubject)
         pattern*=static_cast<double>(es.getCardinality((*iter).subject,~0u,~0u))/total;
      if ((*iter).constPredicate)
         pattern*=static_cast<double>(es.getCardinality(~0u,(*iter).predicate,~0u))/total;
      if ((*iter).constObject)
         pattern*=static_cast<double>(es.getCardinality(~0u,~0u,(*iter).object))/total;
      card*=pattern;
   }

   // Join selectivites
   CycleChecker checker;
   for (vector<QueryGraph::Edge>::const_iterator iter=q.edges.begin(),limit=q.edges.end();iter!=limit;++iter) {
      if (q.nodes[(*iter).from].constPredicate&&q.nodes[(*iter).to].constPredicate)
         if (checker.doEdge((*iter).from,(*iter).to)) {
            unsigned a=q.nodes[(*iter).from].predicate,b=q.nodes[(*iter).to].predicate;
            double patterna=total;
            if (q.nodes[(*iter).from].constSubject)
               patterna*=static_cast<double>(es.getCardinality(q.nodes[(*iter).from].subject,~0u,~0u))/total;
            if (q.nodes[(*iter).from].constPredicate)
               patterna*=static_cast<double>(es.getCardinality(~0u,q.nodes[(*iter).from].predicate,~0u))/total;
            if (q.nodes[(*iter).from].constObject)
               patterna*=static_cast<double>(es.getCardinality(~0u,~0u,q.nodes[(*iter).from].object))/total;
            double patternb=total;
            if (q.nodes[(*iter).to].constSubject)
               patternb*=static_cast<double>(es.getCardinality(q.nodes[(*iter).to].subject,~0u,~0u))/total;
            if (q.nodes[(*iter).to].constPredicate)
               patternb*=static_cast<double>(es.getCardinality(~0u,q.nodes[(*iter).to].predicate,~0u))/total;
            if (q.nodes[(*iter).to].constObject)
               patternb*=static_cast<double>(es.getCardinality(~0u,~0u,q.nodes[(*iter).to].object))/total;

            if ((q.nodes[(*iter).from].subject==q.nodes[(*iter).to].subject)&&(!q.nodes[(*iter).from].constSubject)&&(!q.nodes[(*iter).to].constSubject)) {
               double sel;
               if (exactSS.count(make_pair(a,b)))
                  sel=exactSS[make_pair(a,b)]/(patterna*patternb); else
                  sel=min(baseSizeSS[a],baseSizeSS[b])/(patterna*patternb);
               card*=sel;
            }
         }
   }

   if (card<1) card=1;
   return card;
}
//---------------------------------------------------------------------------
static void doMaduko(Database& db,int argc,char** argv)
{
   if (argc<1) {
      cerr << "threshold required" << endl;
      return;
   }
   double threshold=atof(argv[0]);
   ExactStatisticsSegment& es=db.getExactStatistics();

   // Pre-compute counts
   map<unsigned,uint64_t> counts1;
   map<pair<unsigned,unsigned>,uint64_t> counts2;
   {
      AggregatedFactsSegment::Scan scan;
      vector<pair<unsigned,unsigned> > entries;
      unsigned current=~0u;
      if (scan.first(db.getAggregatedFacts(Database::Order_Subject_Predicate_Object))) do {
         if (scan.getValue1()!=current) {
            for (vector<pair<unsigned,unsigned> >::const_iterator iter=entries.begin(),limit=entries.end();iter!=limit;++iter) {
               counts1[(*iter).first]+=(*iter).second;
               for (vector<pair<unsigned,unsigned> >::const_iterator iter2=entries.begin(),limit2=entries.end();iter2!=limit2;++iter2) {
                  counts2[make_pair((*iter).first,(*iter2).first)]+=(*iter).second*(*iter2).second;
               }
            }
            entries.clear();
         }
         entries.push_back(make_pair(scan.getValue2(),scan.getCount()));
      } while (scan.next());
      for (vector<pair<unsigned,unsigned> >::const_iterator iter=entries.begin(),limit=entries.end();iter!=limit;++iter) {
         counts1[(*iter).first]+=(*iter).second;
         for (vector<pair<unsigned,unsigned> >::const_iterator iter2=entries.begin(),limit2=entries.end();iter2!=limit2;++iter2) {
            counts2[make_pair((*iter).first,(*iter2).first)]+=(*iter).second*(*iter2).second;
         }
      }
   }
   map<unsigned,vector<pair<unsigned,unsigned> > > subjectLookup;
   {
      AggregatedFactsSegment::Scan scan;
      if (scan.first(db.getAggregatedFacts(Database::Order_Subject_Predicate_Object))) do {
         subjectLookup[scan.getValue1()].push_back(make_pair(scan.getValue2(),scan.getCount()));
      } while (scan.next());
   }
   cout << subjectLookup.size() << " subjects" << endl;
   map<unsigned,vector<pair<unsigned,unsigned> > > objectLookup;
   {
      AggregatedFactsSegment::Scan scan;
      if (scan.first(db.getAggregatedFacts(Database::Order_Object_Predicate_Subject))) do {
         objectLookup[scan.getValue1()].push_back(make_pair(scan.getValue2(),scan.getCount()));
      } while (scan.next());
   }
   cout << objectLookup.size() << " objects" << endl;
   map<unsigned,double> baseSizeSS,baseSizeSO,baseSizeOS,baseSizeOO;
   map<pair<unsigned,unsigned>,unsigned> exactSS,exactSO,exactOS,exactOO;
   doMaduko2(db,Database::Order_Predicate_Subject_Object,subjectLookup,"SS",threshold,baseSizeSS,exactSS);
   doMaduko2(db,Database::Order_Predicate_Object_Subject,subjectLookup,"SO",threshold,baseSizeSO,exactSO);
   doMaduko2(db,Database::Order_Predicate_Subject_Object,objectLookup,"OS",threshold,baseSizeOS,exactOS);
   doMaduko2(db,Database::Order_Predicate_Object_Subject,objectLookup,"OO",threshold,baseSizeOO,exactOO);
   cout << "total size " << (((baseSizeSS.size()+baseSizeSO.size()+baseSizeOS.size()+baseSizeOO.size())*4)+((exactSS.size()+exactSO.size()+exactOS.size()+exactOO.size())*6)) << " words" << endl;


   if (argc>1) {
      vector<double> errors;
      double prod=1;
      for (int index=1;index<argc;index++) {
         QueryGraph qg;
         readQuery(db,argv[index],qg);
         if (qg.knownEmpty()) {
            cerr << argv[index] << " has an empty result!" << endl;
            continue;
         }
         double estimate=estimateMaduko(es,baseSizeSS,exactSS,qg);
         double real=getRealCardinality(db,qg);
         double error=qError(estimate,real);
         prod*=estimate;
         cout << argv[index] << " " << estimate << " " << error << endl;
         errors.push_back(error);
      }
      sort(errors.begin(),errors.end());
      double sum=0;
      for (vector<double>::const_iterator iter=errors.begin(),limit=errors.end();iter!=limit;++iter) {
         sum+=(*iter);
      }
      cout << (pow(prod,1.0/static_cast<double>(errors.size()))) << " " << (errors[errors.size()/2]) << " " << (errors.back()) << " " << (sum/static_cast<double>(errors.size())) << endl;
   } else {
      // Run all two-predicate queries
      map<int,unsigned> errorHistogramTuples;
      unsigned summary[6]={0,0,0,0,0,0}; double maxQError=0;
      for (map<pair<unsigned,unsigned>,uint64_t>::const_iterator iter=counts2.begin(),limit=counts2.end();iter!=limit;++iter) {
         // Split the query
         unsigned a=(*iter).first.first;
         unsigned b=(*iter).first.second;

         // Analyze
         double predictedTuples;
         if (exactSS.count(make_pair(a,b)))
            predictedTuples=exactSS[make_pair(a,b)]; else
            predictedTuples=min(baseSizeSS[a],baseSizeSS[b]);

         // Compute the errors
         double tupleError=computeError((*iter).second,predictedTuples);

         // Remember the errors
         int tuplesSlot=static_cast<int>(tupleError);
         if (tuplesSlot<-100) tuplesSlot=-100;
         if (tuplesSlot>100) tuplesSlot=100;
         errorHistogramTuples[tuplesSlot]++;

         double qError=(tupleError<0)?(1.0-tupleError):(1.0+tupleError);
         if (qError<=2.0) summary[0]++; else
         if (qError<=5.0) summary[1]++; else
         if (qError<=10.0) summary[2]++; else
         if (qError<=100.0) summary[3]++; else
         if (qError<=1000.0) summary[4]++; else
            summary[5]++;
         if (qError>maxQError) {
            maxQError=qError;
            cerr << maxQError << endl;
         }
      }

      // Show the histogram
      for (map<int,unsigned>::const_iterator iter=errorHistogramTuples.begin(),limit=errorHistogramTuples.end();iter!=limit;++iter)
         cout << (*iter).first << "\t" << (*iter).second << endl;
      unsigned totalSum=0;
      for (unsigned index=0;index<6;index++) {
         cout << summary[index] << " ";
         totalSum+=summary[index];
      }
      cout << maxQError << endl;
      if (!totalSum) totalSum=1;
      for (unsigned index=0;index<6;index++)
         cout << ((static_cast<double>(summary[index])*100.0)/static_cast<double>(totalSum)) << " ";
      cout << maxQError << endl;
   }
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc<3) {
      cout << "usage: " << argv[0] << " <database> <predpair.hist>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   if (string(argv[2])=="--pairs") {
      doPairs(db);
      return 0;
   } else if (string(argv[2])=="--analyze") {
      doAnalyze(db);
      return 0;
   } else if (string(argv[2])=="--sets") {
      doSets(db);
      return 0;
   } else if (string(argv[2])=="--stocker") {
      doStocker(db,argc-3,argv+3);
      return 0;
   } else if (string(argv[2])=="--maduko") {
      doMaduko(db,argc-3,argv+3);
      return 0;
   }

   // Open the file
   ifstream in(argv[2]);
   if (!in.is_open()) {
      cerr << "unable to open " << argv[2] << endl;
      return 1;
   }

   // Skip the header
   string s;
   getline(in,s);

   // Process the lines
   cout << "# predA\tpredB\ttrue\tinputest\test\tqerrorInput\tqerrorEst" << endl;

   while (true) {
      // Read the original Data
      unsigned preda,predb,freqa,freqb;
      double sela,selb,trueCard,estCard,e1,e2;
      if (!(in >> preda >> predb >> freqa >> freqb >> sela >> selb >> trueCard >> estCard >> e1 >> e2))
         break;

      // Build a query select ?v1 ?v2 { ?v0 preda ?v1. ?v0 predb ?v2 }
      QueryGraph qg;
      qg.addProjection(1); qg.addProjection(2);
      {
         QueryGraph::Node n1;
         n1.subject=0; n1.constSubject=false;
         n1.predicate=preda; n1.constPredicate=true;
         n1.object=1; n1.constObject=false;
         qg.getQuery().nodes.push_back(n1);
      }
      {
         QueryGraph::Node n2;
         n2.subject=0; n2.constSubject=false;
         n2.predicate=predb; n2.constPredicate=true;
         n2.object=2; n2.constObject=false;
         qg.getQuery().nodes.push_back(n2);
      }
      qg.constructEdges();

      // Run the optimizer
      PlanGen plangen;
      Plan* plan=plangen.translate(db,qg);
      if (!plan) {
         cerr << "plan generation failed" << endl;
         return 1;
      }
      Operator::disableSkipping=true;

      // Build a physical plan
      Runtime runtime(db);
      Operator* operatorTree=CodeGen().translate(runtime,qg,plan,true);
      Operator* realRoot=dynamic_cast<ResultsPrinter*>(operatorTree)->getInput();

      // And execute it
      Scheduler scheduler;
      scheduler.execute(realRoot);

      // Output the counts
      trueCard=realRoot->getObservedOutputCardinality();
      cout << preda << "\t" << predb << "\t" << trueCard << "\t" << estCard << "\t" << realRoot->getExpectedOutputCardinality() << "\t" << qError(estCard,trueCard) << "\t" << qError(realRoot->getExpectedOutputCardinality(),trueCard) << endl;

      delete operatorTree;
   }
}
//---------------------------------------------------------------------------
