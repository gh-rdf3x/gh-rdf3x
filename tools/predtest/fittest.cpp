#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <map>
#include <algorithm>
#include <cstdlib>
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
namespace {
//---------------------------------------------------------------------------
/// A binding
typedef vector<int> Binding;
//---------------------------------------------------------------------------
/// An entry
struct Entry {
   /// The bindings
   Binding bindings;
   /// The selectivities
   double expected,observed;
};
//---------------------------------------------------------------------------
struct OrderByBinding { bool operator()(const Entry& a,const Entry& b) const { return a.bindings<b.bindings; } };
//---------------------------------------------------------------------------
/// A DAG node
struct Node {
   /// Parent nodes
   set<Node*> parents;
   /// Child nodes
   set<Node*> children;

   /// The bindings
   Binding bindings;
   /// The correction factor
   double value;
   /// The approximation error
   double error;
   /// Error if we would materialize
   double matError;

   /// Materialized?
   bool materialized;
};
//---------------------------------------------------------------------------
static const int boundUnknown = -1000;
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
static void generalize(map<Binding,Node*>& classes,Node* n)
   // Generalize a class
{
   for (unsigned index=0;index<n->bindings.size();index++) {
      if (n->bindings[index]>=0) {
         Binding b=n->bindings;
         b[index]=boundUnknown;

         if (classes.count(b)) {
            Node* n2=classes[b];
            n2->children.insert(n);
            n->parents.insert(n2);
         } else {
            Node* n2=new Node();
            n2->bindings=b;
            n2->value=0;
            n2->materialized=false;
            n2->children.insert(n);
            classes[b]=n2;
            n->parents.insert(n2);
            generalize(classes,n2);
         }
      }
   }
}
//---------------------------------------------------------------------------
static void findMinMax(const Node& n,double& min,double& max,bool ignoreMaterialized=false)
   // Find the minium/maximum values
{
   if (n.materialized&&ignoreMaterialized)
      return;

   if (n.children.empty()) {
      if (min>max) min=max=n.value;
      if (n.value<min) min=n.value;
      if (n.value>max) max=n.value;
   } else {
      for (set<Node*>::const_iterator iter=n.children.begin(),limit=n.children.end();iter!=limit;++iter)
         findMinMax(**iter,min,max,true);
   }
}
//---------------------------------------------------------------------------
static double error(double a,double b)
   // Compuate the error
{
   if (a<b) swap(a,b);
   if (a==b) return 0;
   if (b<=0) return 100000;
   return a/b;
}
//---------------------------------------------------------------------------
static double computeError(const Node& n,double approx,bool ignoreMaterialized=false)
   // Compute the approximation error
{
   if (n.materialized&&ignoreMaterialized)
      return 0;

   if (n.children.empty()) {
      return error(n.value,approx);
   } else {
      double max=0;
      for (set<Node*>::const_iterator iter=n.children.begin(),limit=n.children.end();iter!=limit;++iter) {
         double e=computeError(**iter,approx,true);
         if (e>max) max=e;
      }
      return max;
   }
}
//---------------------------------------------------------------------------
static void addNodes(vector<Node*>& sortedNodes,set<Node*>& added,Node* n)
   // Add nodes to a list recursively
{
   if (added.count(n))
      return;

   for (set<Node*>::const_iterator iter=n->children.begin(),limit=n->children.end();iter!=limit;++iter)
      addNodes(sortedNodes,added,(*iter));

   if (added.count(n)) throw;
   sortedNodes.push_back(n);
   added.insert(n);
}
//---------------------------------------------------------------------------
static void computeErrors(const vector<Node*>& sortedNodes)
   // Compute the estimation errors
{
   for (vector<Node*>::const_iterator iter=sortedNodes.begin(),limit=sortedNodes.end();iter!=limit;++iter) {
      Node& n=**iter;
      if (n.materialized) {
         if (n.children.empty()) {
            n.error=n.matError=0;
         } else {
            double min=1,max=0;
            findMinMax(n,min,max);
            n.value=(min==max)?min:sqrt(min*max);
            n.error=n.matError=computeError(n,n.value);
         }
      } else {
         Node* bestParent=*n.parents.begin();
         for (set<Node*>::const_iterator iter2=n.parents.begin(),limit2=n.parents.end();iter2!=limit2;++iter2)
            if ((*iter2)->error<bestParent->error)
               bestParent=*iter2;
         if (n.children.empty()) {
            n.error=error(n.value,bestParent->value);
            n.matError=0;
         } else {
            n.value=bestParent->value;
            n.error=computeError(n,n.value);

            double min=1,max=0;
            findMinMax(n,min,max);
            n.matError=computeError(n,((min==max)?min:sqrt(min*max)));
         }
      }
   }
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Check the arguments
   if (argc!=5) {
      cout << "usage: " << argv[0] << " <joins-file> <relations> <join1> <join2>" << endl;
      return 1;
   }

   // Collect all entries
   vector<Entry> entries;
   {
      ifstream in(argv[1]);
      if (!in.is_open()) {
         cerr << "unable to open " << argv[1] << endl;
         return 1;
      }
      string rel,joina,joinb;
      while (true) {
         if (!(in>>rel)) break;
         if (rel=="#") {
            getline(in,rel);
            continue;
         }
         // Read the line
         in >> joina >> joinb;
         Entry e;
         unsigned count=3*atoi(rel.c_str());
         for (unsigned index=0;index<count;index++) {
            int i;
            in >> i;
            e.bindings.push_back(i);
         }
         in >> e.expected >> e.observed;

         // Check
         if ((rel!=argv[2])||(joina!=argv[3])||(joinb!=argv[4])) continue;

         // And remember
         if ((e.expected<=0)||(e.observed<=0))
            continue; // XXX ignore this case for now
         entries.push_back(e);
      }
   }
   sort(entries.begin(),entries.end(),OrderByBinding());
   cerr << "Found " << entries.size() << " entries" << endl;

   // Generate classes
   map<Binding,Node*> classes;
   for (vector<Entry>::const_iterator iter=entries.begin(),limit=entries.end(),next;iter!=limit;iter=next) {
      // Collect all values with the same binding
      next=iter; ++next;
      while ((next!=limit)&&((*next).bindings==(*iter).bindings)) ++next;

      // Compute the average
      double min=(*iter).observed/(*iter).expected,max=min;
      for (vector<Entry>::const_iterator iter2=iter;iter2!=next;++iter2) {
         double q=(*iter2).observed/(*iter2).expected;
         if (q<min) min=q;
         if (q>max) max=q;
      }
      double value=(min==max)?min:sqrt(min*max);

      // And produce the class
      Node* n=new Node();
      n->bindings=(*iter).bindings;
      n->value=value;
      n->materialized=false;
      classes[(*iter).bindings]=n;
      generalize(classes,n);
   }
   cerr << "Found " << classes.size() << " classes" << endl;

   // Find the roots
   set<Node*> roots;
   for (map<Binding,Node*>::const_iterator iter=classes.begin(),limit=classes.end();iter!=limit;++iter)
      if ((*iter).second->parents.empty())
         roots.insert((*iter).second);
   cerr << "Found " << roots.size() << " roots" << endl;

   // Materialize them
   set<Node*> materialized,candidates;
   for (set<Node*>::const_iterator iter=roots.begin(),limit=roots.end();iter!=limit;++iter) {
      double min=1,max=0;
      findMinMax(**iter,min,max);
      (*iter)->value=(min==max)?min:sqrt(min*max);
      (*iter)->error=computeError(**iter,(*iter)->value);
      (*iter)->materialized=true;
      materialized.insert(*iter);
      candidates.insert((*iter)->children.begin(),(*iter)->children.end());
   }

   // Sort topologically
   vector<Node*> sortedNodes;
   {
      set<Node*> added;
      for (set<Node*>::const_iterator iter=roots.begin(),limit=roots.end();iter!=limit;++iter)
         addNodes(sortedNodes,added,*iter);
      reverse(sortedNodes.begin(),sortedNodes.end());
   }

   // Compute approximations and errors
   computeErrors(sortedNodes);


   // Increase
   while ((materialized.size()<1)&&(!candidates.empty())) {
      // Pick the best materialization candidates
      Node* n=*candidates.begin();
      for (set<Node*>::const_iterator iter=candidates.begin(),limit=candidates.end();iter!=limit;++iter)
         if ((*iter)->error>n->error)
         //if (((*iter)->error-((*iter)->matError))>(n->error-n->matError))
            n=*iter;
      candidates.erase(n);
      if (n->materialized) continue;

      // And materialize it
      n->materialized=true;
      materialized.insert(n);
      for (set<Node*>::const_iterator iter=n->children.begin(),limit=n->children.end();iter!=limit;++iter)
         candidates.insert(*iter);
      computeErrors(sortedNodes);
   }


   // Show errors
   {
      vector<double> errors;
      for (vector<Node*>::const_iterator iter=sortedNodes.begin(),limit=sortedNodes.end();iter!=limit;++iter)
         if ((*iter)->children.empty())
            errors.push_back((**iter).error);
      sort(errors.begin(),errors.end(),greater<double>());
      unsigned c=0;
      for (vector<double>::const_iterator iter=errors.begin(),limit=errors.end();(iter!=limit)&&(c<100);++iter,++c)
         cerr << (*iter) << " ";
      cerr << endl << errors[errors.size()/2] << endl;
   }

   // Show the materialized results
   for (set<Node*>::const_iterator iter=materialized.begin(),limit=materialized.end();iter!=limit;++iter) {
      const Node& n=**iter;
      for (Binding::const_iterator iter2=n.bindings.begin(),limit2=n.bindings.end();iter2!=limit2;++iter2)
         if ((*iter2)==boundUnknown)
            cout << "*" << " "; else
            cout << (*iter2) << " ";
      cout << n.value << " " << n.error << endl;
   }


   // Cleanup
   for (map<Binding,Node*>::const_iterator iter=classes.begin(),limit=classes.end();iter!=limit;++iter)
      delete (*iter).second;
}
//---------------------------------------------------------------------------
