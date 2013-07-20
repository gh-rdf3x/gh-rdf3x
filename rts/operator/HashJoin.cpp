#include "rts/operator/HashJoin.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
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
static inline unsigned hash1(unsigned key,unsigned hashTableSize) { return key&(hashTableSize-1); }
static inline unsigned hash2(unsigned key,unsigned hashTableSize) { return hashTableSize+((key^(key>>3))&(hashTableSize-1)); }
//---------------------------------------------------------------------------
void HashJoin::BuildHashTable::run()
   // Build the hash table
{
   if (done) return; // XXX support repeated executions under nested loop joins etc!

   // Prepare relevant domain informations
   Register* leftValue=join.leftValue;
   vector<Register*> domainRegs;
   if (leftValue->domain)
      domainRegs.push_back(leftValue);
   for (vector<Register*>::const_iterator iter=join.leftTail.begin(),limit=join.leftTail.end();iter!=limit;++iter)
      if ((*iter)->domain)
         domainRegs.push_back(*iter);
   vector<ObservedDomainDescription> observedDomains;
   observedDomains.resize(domainRegs.size());

   // Build the hash table from the left side
   unsigned hashTableSize = 1024;
   unsigned tailLength=join.leftTail.size();
   join.hashTable.clear();
   join.hashTable.resize(2*hashTableSize);
   for (unsigned leftCount=join.left->first();leftCount;leftCount=join.left->next()) {
      //cout << leftValue->value << endl;
      // Check the domain first
      bool joinCandidate=true;
      for (unsigned index=0,limit=domainRegs.size();index<limit;++index) {
         if (!domainRegs[index]->domain->couldQualify(domainRegs[index]->value)) {
            joinCandidate=false;
            break;
         }
         observedDomains[index].add(domainRegs[index]->value);
      }
      if (!joinCandidate)
         continue;
      // Compute the slots
      unsigned leftKey=leftValue->value;
      unsigned slot1=hash1(leftKey,hashTableSize),slot2=hash2(leftKey,hashTableSize);

      // Scan if the entry already exists
      Entry* e=join.hashTable[slot1];
      if ((!e)||(e->key!=leftKey))
         e=join.hashTable[slot2];
      if (e&&(e->key==leftKey)) {
         unsigned ofs=(e==join.hashTable[slot1])?slot1:slot2;
         bool match=false;
         for (Entry* iter=e;iter;iter=iter->next)
            if (leftKey==iter->key) {
               // Tuple already in the table?
               match=true;
               for (unsigned index2=0;index2<tailLength;index2++)
                  if (join.leftTail[index2]->value!=iter->values[index2]) {
                     match=false;
                     break;
                  }
               // Then aggregate
               if (match) {
                  iter->count+=leftCount;
                  break;
               }
            }
         if (match)
            continue;

         // Append to the current bucket
         e=join.entryPool.alloc();
         e->next=join.hashTable[ofs];
         join.hashTable[ofs]=e;
         e->key=leftKey;
         e->count=leftCount;
         for (unsigned index2=0;index2<tailLength;index2++)
            e->values[index2]=join.leftTail[index2]->value;
         continue;
      }
      
      // Create a new tuple
      e=join.entryPool.alloc();
      e->next=0;
      e->key=leftKey;
      e->count=leftCount;
      for (unsigned index2=0;index2<tailLength;index2++)
         e->values[index2]=join.leftTail[index2]->value;

      // And insert it
      join.insert(e);
      hashTableSize=join.hashTable.size()/2;
   }

   // Update the domains
   for (unsigned index=0,limit=domainRegs.size();index<limit;++index)
      domainRegs[index]->domain->restrictTo(observedDomains[index]);
   
   done=true;
}
//---------------------------------------------------------------------------
void HashJoin::ProbePeek::run()
   // Produce the first tuple from the probe side
{
   if (done) return; // XXX support repeated executions under nested loop joins etc!

   count=join.right->first();
   done=true;
}
//---------------------------------------------------------------------------
HashJoin::HashJoin(Operator* left,Register* leftValue,const vector<Register*>& leftTail,Operator* right,Register* rightValue,const vector<Register*>& rightTail,double hashPriority,double probePriority,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),left(left),right(right),leftValue(leftValue),rightValue(rightValue),
     leftTail(leftTail),rightTail(rightTail),entryPool(leftTail.size()*sizeof(unsigned)),
     buildHashTableTask(*this),probePeekTask(*this),hashPriority(hashPriority),probePriority(probePriority)
   // Constructor
{
}
//---------------------------------------------------------------------------
HashJoin::~HashJoin()
   // Destructor
{
   delete left;
   delete right;
}
//---------------------------------------------------------------------------
void HashJoin::insert(Entry* e)
   // Insert into the hash table
{  
   unsigned hashTableSize=hashTable.size()/2;
   // Try to insert
   bool firstTable=true;
   for (unsigned index=0;index<hashTableSize;index++) {
      unsigned slot=firstTable?hash1(e->key,hashTableSize):hash2(e->key,hashTableSize);
      swap(e,hashTable[slot]);
      if (!e)
         return;
      firstTable=!firstTable;
   }

   // No place found, rehash
   vector<Entry*> oldTable;
   oldTable.resize(4*hashTableSize);
   swap(hashTable,oldTable);
   for (vector<Entry*>::const_iterator iter=oldTable.begin(),limit=oldTable.end();iter!=limit;++iter)
      if (*iter)
         insert(*iter);
   insert(e);
}
//---------------------------------------------------------------------------
HashJoin::Entry* HashJoin::lookup(unsigned key)
   // Search an entry in the hash table
{  
   unsigned hashTableSize=hashTable.size()/2;
   Entry* e=hashTable[hash1(key,hashTableSize)];
   if (e&&(e->key==key))
      return e;
   e=hashTable[hash2(key,hashTableSize)];
   if (e&&(e->key==key))
      return e; 
    
   return 0;
}
//---------------------------------------------------------------------------
unsigned HashJoin::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   // Build the hash table if not already done
   buildHashTableTask.run();

   // Read the first tuple from the right side
   probePeekTask.run();
   if ((rightCount=probePeekTask.count)==0)
      return false;

   // Setup the lookup
   hashTableIter=lookup(rightValue->value);
      
   return next();
}
//---------------------------------------------------------------------------
unsigned HashJoin::next()
   // Produce the next tuple
{  
   // Repeat until a match is found
   while (true) {
      // Still scanning the hash table?
      for (;hashTableIter;hashTableIter=hashTableIter->next) {
         unsigned leftCount=hashTableIter->count;
         leftValue->value=hashTableIter->key;
         
         for (unsigned index=0,limit=leftTail.size();index<limit;++index)
            leftTail[index]->value=hashTableIter->values[index];
         
         hashTableIter=hashTableIter->next;

         unsigned count=leftCount*rightCount;
         observedOutputCardinality+=count;
         return count;
      }
      
      // Read the next tuple from the right
      if ((rightCount=right->next())==0)
         return false;
      
      hashTableIter=lookup(rightValue->value);
   }
}
//---------------------------------------------------------------------------
void HashJoin::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("HashJoin",expectedOutputCardinality,observedOutputCardinality);
   out.addEqualPredicateAnnotation(leftValue,rightValue);
   out.addMaterializationAnnotation(leftTail);
   out.addMaterializationAnnotation(rightTail);
   left->print(out);
   right->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
void HashJoin::addMergeHint(Register* /*reg1*/,Register* /*reg2*/)
   // Add a merge join hint
{
   // Do not propagate as we break the pipeline
}
//---------------------------------------------------------------------------
void HashJoin::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   unsigned p1=scheduler.getRegisteredPoints();
   left->getAsyncInputCandidates(scheduler);
   scheduler.registerAsyncPoint(buildHashTableTask,0,hashPriority,p1);

   unsigned p2=scheduler.getRegisteredPoints();
   right->getAsyncInputCandidates(scheduler);
   scheduler.registerAsyncPoint(probePeekTask,1,probePriority,p2);
}
//---------------------------------------------------------------------------
