#include "rts/database/Database.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <iostream>
#include <fstream>
#include <map>
#include <set>
#include <vector>
#include <algorithm>
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
namespace {
//---------------------------------------------------------------------------
struct SortBySize {
   bool operator()(const set<unsigned>* a,const set<unsigned>* b) const { return a->size()>b->size(); }
};
//---------------------------------------------------------------------------
unsigned missing(const set<unsigned>& a,const set<unsigned>& b)
{
   unsigned result=0;
   for (set<unsigned>::const_iterator iter=a.begin(),limit=a.end();iter!=limit;++iter)
      if (!b.count(*iter))
         result++;
   return result;
}
//---------------------------------------------------------------------------
struct Triple {
   unsigned v1,v2,v3;

   bool operator<(const Triple& o) const { return (v1<o.v1)||((v1==o.v1)&&((v2<o.v2)||((v2==o.v2)&&(v3<o.v3)))); }
};
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   if (argc<2) {
      std::cout << "usage: " << argv[0] << " <rdfstore>" << std::endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1])) {
      std::cout << "unable to open " << argv[1] << std::endl;
      return 1;
   }
   AggregatedFactsSegment& facts=db.getAggregatedFacts(Database::Order_Subject_Predicate_Object);

   // Collect all label classes
   map<set<unsigned>,unsigned> labelClasses;
   {
      AggregatedFactsSegment::Scan scan;
      if (scan.first(facts)) {
         unsigned current=scan.getValue1();
         set<unsigned> edges;
         edges.insert(scan.getValue2());
         while (scan.next()) {
            if (scan.getValue1()!=current) {
               if (!labelClasses.count(edges))
                  labelClasses[edges]=current;
               edges.clear();
               current=scan.getValue1();
            }
            edges.insert(scan.getValue2());
         }
         if (!labelClasses.count(edges))
            labelClasses[edges]=current;
      }
   }
   cout << "found " << labelClasses.size() << " distinct classes of outgoing edges" << endl;

   // Prune out subsets
   map<set<unsigned>,unsigned> prunedClasses;
   map<set<unsigned>,set<unsigned> > domination;
   {
      // Sort by size
      vector<const set<unsigned>*> allClasses;
      for (map<set<unsigned>,unsigned>::const_iterator iter=labelClasses.begin(),limit=labelClasses.end();iter!=limit;++iter)
         allClasses.push_back(&((*iter).first));
      sort(allClasses.begin(),allClasses.end(),SortBySize());

      // Insert if not dominated
      unsigned classId=0;
      for (vector<const set<unsigned>*>::const_iterator iter=allClasses.begin(),limit=allClasses.end();iter!=limit;++iter) {
         bool dominated=false;
         for (map<set<unsigned>,unsigned>::const_iterator iter2=prunedClasses.begin(),limit2=prunedClasses.end();iter2!=limit2;++iter2) {
            unsigned m=missing(**iter,(*iter2).first);
            if ((m==0)||(((*iter)->size()>3)&&(m==1))) {
               domination[**iter]=(*iter2).first;
               dominated=true;
               break;
            }
         }
         if (dominated)
            continue;
         prunedClasses[**iter]=classId++;
         domination[**iter]=**iter;
      }
   }
   cout << "got " << prunedClasses.size() << " after pruning" << endl;

   // Compute node classes
   map<unsigned,unsigned> nodeClasses;
   {
      AggregatedFactsSegment::Scan scan;
      if (scan.first(facts)) {
         unsigned current=scan.getValue1();
         set<unsigned> edges;
         edges.insert(scan.getValue2());
         while (scan.next()) {
            if (scan.getValue1()!=current) {
               nodeClasses[current]=prunedClasses[domination[edges]];
               edges.clear();
               current=scan.getValue1();
            }
            edges.insert(scan.getValue2());
         }
         nodeClasses[current]=prunedClasses[domination[edges]];
      }
   }
   cout << "computed node classes for " << nodeClasses.size() << " nodes" << endl;

   // Compute class edges
   set<Triple> triples;
   {
      FactsSegment::Scan scan;
      if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object))) do {
         Triple t;
         t.v1=nodeClasses[scan.getValue1()];
         t.v2=scan.getValue2();
         if (nodeClasses.count(scan.getValue3()))
            t.v3=nodeClasses[scan.getValue3()]; else
            t.v3=~0u;
         triples.insert(t);
      } while (scan.next());
   }
   cout << "derived " << triples.size() << " edges" << endl;

   // Dump a dot file
   {
      ofstream out("schema.dot");
      out << "digraph schema {" << endl;
      out << "  node [shape=point]" << endl;
      unsigned current=~0u;
      for (set<Triple>::const_iterator iter=triples.begin(),limit=triples.end();iter!=limit;++iter) {
         if ((*iter).v1!=current) {
            out << "  node" << (*iter).v1 << ";" << endl;
            current=(*iter).v1;
         }
         if (!~(*iter).v3)
            out << "  nodeliteral" << (*iter).v1 << "_" << (*iter).v2 << "[shape=none];" << endl;
      }
      for (set<Triple>::const_iterator iter=triples.begin(),limit=triples.end();iter!=limit;++iter) {
         const char* labelStart=0,*labelStop=0; Type::ID type; unsigned subType;
         db.getDictionary().lookupById((*iter).v2,labelStart,labelStop,type,subType);
         string label(labelStart,labelStop);
         if (!~(*iter).v3)
            out << "  node" << (*iter).v1 << " -> nodeliteral" << (*iter).v1 << "_" << (*iter).v2; else
            out << "  node" << (*iter).v1 << " -> node" << (*iter).v3;
         out << "[label=\"" << label << "\"];" << endl;
      }
      out << "}" << endl;
   }
   cout << "run 'dot -v -Tpdf -o schema.pdf schema.dot' or similar to view the result" << endl;
}
//---------------------------------------------------------------------------
