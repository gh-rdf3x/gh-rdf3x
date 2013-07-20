#include "rts/runtime/DifferentialIndex.hpp"
#include "rts/database/Database.hpp"
#include "rts/operator/AggregatedIndexScan.hpp"
#include "rts/operator/FullyAggregatedIndexScan.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/runtime/Runtime.hpp"
#include <iostream>
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
/// Scan over the differential index
class DifferentialIndexScan : public Operator
{
   private:
   /// The input
   Operator* input;
   /// The latch
   Latch& latch;
   /// The triples
   set<DifferentialIndex::VersionedTriple>& triples;
   /// The timestamp
   const unsigned timestamp;
   /// The values
   Register* value1,*value2,*value3;
   /// Checks
   const unsigned check2,check3;
   /// Range bounds
   DifferentialIndex::VersionedTriple lowerBound,upperBound;
   /// Iterator over the relevant triples
   set<DifferentialIndex::VersionedTriple>::const_iterator iter,limit;
   /// Left triple
   unsigned leftCount,left1,left2,left3;
   /// Do we have a left triple?
   bool hasLeft;
   /// Currently latched?
   bool latched;

   /// Increase the iterator
   void incIter();

   public:
   /// Constructor
   DifferentialIndexScan(Operator* input,Latch& latch,unsigned timestamp,set<DifferentialIndex::VersionedTriple>& triples,Register* value1,Register* value2,Register* value3,unsigned check2,unsigned check3,const DifferentialIndex::VersionedTriple& lowerBound,const DifferentialIndex::VersionedTriple& upperBound);
   /// Destructor
   ~DifferentialIndexScan();

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
DifferentialIndexScan::DifferentialIndexScan(Operator* input,Latch& latch,unsigned timestamp,set<DifferentialIndex::VersionedTriple>& triples,Register* value1,Register* value2,Register* value3,unsigned check2,unsigned check3,const DifferentialIndex::VersionedTriple& lowerBound,const DifferentialIndex::VersionedTriple& upperBound)
   : Operator(0),input(input),latch(latch),triples(triples),timestamp(timestamp),value1(value1),value2(value2),value3(value3),check2(check2),check3(check3),lowerBound(lowerBound),upperBound(upperBound),latched(false)
   // Constructor
{
}
//---------------------------------------------------------------------------
DifferentialIndexScan::~DifferentialIndexScan()
   // Destructor
{
   if (latched) {
      latch.unlock();
      latched=false;
   }
   delete input;
}
//---------------------------------------------------------------------------
void DifferentialIndexScan::incIter()
   // Increase the iterator
{
   ++iter;
   while (iter!=limit) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      if ((~check2)&&(t.value2!=check2)) { ++iter; continue; }
      if ((~check3)&&(t.value3!=check3)) { ++iter; continue; }
      if ((t.created>timestamp)||(t.deleted<timestamp)) { ++iter; continue; }
      break;
   }
}
//---------------------------------------------------------------------------
static int compareTriple(unsigned l1,unsigned l2,unsigned l3,unsigned r1,unsigned r2,unsigned r3)
   // Compare two triples
{
   if (l1<r1) return -1;
   if (l1>r1) return 1;
   if (l2<r2) return -1;
   if (l2>r2) return 1;
   if (l3<r3) return -1;
   if (l3>r3) return 1;
   return 0;
}
//---------------------------------------------------------------------------
unsigned DifferentialIndexScan::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;

   // Latch if necessary
   if (!latched) {
      latch.lockShared();
      latched=true;
   }

   // Find the range in the differential index
   iter=triples.lower_bound(lowerBound);
   limit=triples.lower_bound(upperBound);
   while (iter!=limit) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      if ((~check2)&&(t.value2!=check2)) { ++iter; continue; }
      if ((~check3)&&(t.value3!=check3)) { ++iter; continue; }
      if ((t.created>timestamp)||(t.deleted<timestamp)) { ++iter; continue; }
      break;
   }

   // Retrieve the first DB triple
   hasLeft=false;
   leftCount=input->first();
   if (!leftCount) {
      if (iter==limit) {
         latch.unlock();
         latched=false;
         return 0;
      }
      return next();
   }

   // Compare
   if (iter==limit) {
      observedOutputCardinality+=leftCount;
      return leftCount;
   }
   const DifferentialIndex::VersionedTriple& t=(*iter);
   int cmp=compareTriple(value1->value,value2->value,value3->value,t.value1,t.value2,t.value3);
   if (cmp<0) {
      observedOutputCardinality+=leftCount;
      return leftCount;
   } else if (cmp>0) {
      left1=value1->value;
      left2=value2->value;
      left3=value3->value;
      hasLeft=true;
      value1->value=t.value1;
      value2->value=t.value2;
      value3->value=t.value3;
      incIter();
      observedOutputCardinality+=1;
      return 1;
   } else {
      incIter();
      observedOutputCardinality+=leftCount;
      return leftCount;
   }
}
//---------------------------------------------------------------------------
unsigned DifferentialIndexScan::next()
   // Produce the next tuple
{
   // Right side done?
   if (iter==limit) {
      // Both done?
      if (!leftCount) {
         if (latched) {
            latch.unlock();
            latched=false;
         }
         return 0;
      }
      // Do we have a value?
      if (hasLeft) {
         value1->value=left1;
         value2->value=left2;
         value3->value=left3;
         hasLeft=false;
         observedOutputCardinality+=leftCount;
         return leftCount;
      }
      // Read the next one
      leftCount=input->next();
      if (!leftCount) {
         if (latched) {
            latch.unlock();
            latched=false;
         }
         return 0;
      }
      observedOutputCardinality+=leftCount;
      return leftCount;
   }
   // Left side done?
   if (!leftCount) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      value1->value=t.value1;
      value2->value=t.value2;
      value3->value=t.value3;
      incIter();
      observedOutputCardinality+=1;
      return 1;
   }
   // A buffered left triple?
   if (hasLeft) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      int cmp=compareTriple(left1,left2,left3,t.value1,t.value2,t.value3);
      if (cmp<0) {
         value1->value=left1;
         value2->value=left2;
         value3->value=left3;
         hasLeft=false;
         observedOutputCardinality+=leftCount;
         return leftCount;
      } else if (cmp>0) {
         value1->value=t.value1;
         value2->value=t.value2;
         value3->value=t.value3;
         incIter();
         observedOutputCardinality+=1;
         return 1;
      } else {
         value1->value=left1;
         value2->value=left2;
         value3->value=left3;
         hasLeft=false;
         incIter();
         observedOutputCardinality+=leftCount;
         return leftCount;
      }
   }
   // Retrieve the next DB triple
   leftCount=input->next();
   if (!leftCount) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      value1->value=t.value1;
      value2->value=t.value2;
      value3->value=t.value3;
      incIter();
      observedOutputCardinality+=1;
      return 1;
   }

   // Compare
   const DifferentialIndex::VersionedTriple& t=(*iter);
   int cmp=compareTriple(value1->value,value2->value,value3->value,t.value1,t.value2,t.value3);
   if (cmp<0) {
      observedOutputCardinality+=leftCount;
      return leftCount;
   } else if (cmp>0) {
      left1=value1->value;
      left2=value2->value;
      left3=value3->value;
      hasLeft=true;
      value1->value=t.value1;
      value2->value=t.value2;
      value3->value=t.value3;
      incIter();
      observedOutputCardinality+=1;
      return 1;
   } else {
      incIter();
      observedOutputCardinality+=leftCount;
      return leftCount;
   }
}
//---------------------------------------------------------------------------
void DifferentialIndexScan::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("DifferentialIndexScan",expectedOutputCardinality,observedOutputCardinality);
   input->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
void DifferentialIndexScan::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   input->addMergeHint(reg1,reg2);
}
//---------------------------------------------------------------------------
void DifferentialIndexScan::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
/// Scan over the differential index
class AggregatedDifferentialIndexScan : public Operator
{
   private:
   /// The input
   Operator* input;
   /// The latch
   Latch& latch;
   /// The triples
   set<DifferentialIndex::VersionedTriple>& triples;
   /// The timestamp
   const unsigned timestamp;
   /// The values
   Register* value1,*value2;
   /// Checks
   const unsigned check2;
   /// Range bounds
   DifferentialIndex::VersionedTriple lowerBound,upperBound;
   /// Iterator over the relevant triples
   set<DifferentialIndex::VersionedTriple>::const_iterator iter,limit;
   /// Left triple
   unsigned leftCount,left1,left2;
   /// Do we have a left triple?
   bool hasLeft;
   /// Currently latched?
   bool latched;

   /// Increase the iterator
   unsigned incIter();

   public:
   /// Constructor
   AggregatedDifferentialIndexScan(Operator* input,Latch& latch,unsigned timestamp,set<DifferentialIndex::VersionedTriple>& triples,Register* value1,Register* value2,unsigned check2,const DifferentialIndex::VersionedTriple& lowerBound,const DifferentialIndex::VersionedTriple& upperBound);
   /// Destructor
   ~AggregatedDifferentialIndexScan();

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
AggregatedDifferentialIndexScan::AggregatedDifferentialIndexScan(Operator* input,Latch& latch,unsigned timestamp,set<DifferentialIndex::VersionedTriple>& triples,Register* value1,Register* value2,unsigned check2,const DifferentialIndex::VersionedTriple& lowerBound,const DifferentialIndex::VersionedTriple& upperBound)
   : Operator(0),input(input),latch(latch),triples(triples),timestamp(timestamp),value1(value1),value2(value2),check2(check2),lowerBound(lowerBound),upperBound(upperBound),latched(false)
   // Constructor
{
}
//---------------------------------------------------------------------------
AggregatedDifferentialIndexScan::~AggregatedDifferentialIndexScan()
   // Destructor
{
   if (latched) {
      latch.unlock();
      latched=false;
   }
   delete input;
}
//---------------------------------------------------------------------------
unsigned AggregatedDifferentialIndexScan::incIter()
   // Increase the iterator
{
   unsigned v1=(*iter).value1,v2=(*iter).value2;
   unsigned count=1;
   ++iter;
   while (iter!=limit) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      if ((~check2)&&(t.value2!=check2)) { ++iter; continue; }
      if ((t.created>timestamp)||(t.deleted<timestamp)) { ++iter; continue; }
      if (((*iter).value1!=v1)||((*iter).value2!=v2))
         break;
      ++count;
      ++iter;
   }
   return count;
}
//---------------------------------------------------------------------------
static int compareTriple2(unsigned l1,unsigned l2,unsigned r1,unsigned r2)
   // Compare two triples
{
   if (l1<r1) return -1;
   if (l1>r1) return 1;
   if (l2<r2) return -1;
   if (l2>r2) return 1;
   return 0;
}
//---------------------------------------------------------------------------
unsigned AggregatedDifferentialIndexScan::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;

   // Latch if necessary
   if (!latched) {
      latch.lockShared();
      latched=true;
   }

   // Find the range in the differential index
   iter=triples.lower_bound(lowerBound);
   limit=triples.lower_bound(upperBound);
   while (iter!=limit) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      if ((~check2)&&(t.value2!=check2)) { ++iter; continue; }
      if ((t.created>timestamp)||(t.deleted<timestamp)) { ++iter; continue; }
      break;
   }

   // Retrieve the first DB triple
   hasLeft=false;
   leftCount=input->first();
   if (!leftCount) {
      if (iter==limit) {
         latch.unlock();
         latched=false;
         return 0;
      }
      return next();
   }

   // Compare
   if (iter==limit) {
      observedOutputCardinality+=leftCount;
      return leftCount;
   }
   const DifferentialIndex::VersionedTriple& t=(*iter);
   int cmp=compareTriple2(value1->value,value2->value,t.value1,t.value2);
   if (cmp<0) {
      observedOutputCardinality+=leftCount;
      return leftCount;
   } else if (cmp>0) {
      left1=value1->value;
      left2=value2->value;
      hasLeft=true;
      value1->value=t.value1;
      value2->value=t.value2;

      unsigned count=incIter();
      observedOutputCardinality+=count;
      return count;
   } else {
      unsigned count=leftCount+incIter();
      observedOutputCardinality+=count;
      return count;
   }
}
//---------------------------------------------------------------------------
unsigned AggregatedDifferentialIndexScan::next()
   // Produce the next tuple
{
   // Right side done?
   if (iter==limit) {
      // Both done?
      if (!leftCount) {
         if (latched) {
            latch.unlock();
            latched=false;
         }
         return 0;
      }
      // Do we have a value?
      if (hasLeft) {
         value1->value=left1;
         value2->value=left2;
         hasLeft=false;
         observedOutputCardinality+=leftCount;
         return leftCount;
      }
      // Read the next one
      leftCount=input->next();
      if (!leftCount) {
         if (latched) {
            latch.unlock();
            latched=false;
         }
         return 0;
      }
      observedOutputCardinality+=leftCount;
      return leftCount;
   }
   // Left side done?
   if (!leftCount) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      value1->value=t.value1;
      value2->value=t.value2;

      unsigned count=incIter();
      observedOutputCardinality+=count;
      return count;
   }
   // A buffered left triple?
   if (hasLeft) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      int cmp=compareTriple2(left1,left2,t.value1,t.value2);
      if (cmp<0) {
         value1->value=left1;
         value2->value=left2;
         hasLeft=false;
         observedOutputCardinality+=leftCount;
         return leftCount;
      } else if (cmp>0) {
         value1->value=t.value1;
         value2->value=t.value2;

         unsigned count=incIter();
         observedOutputCardinality+=count;
         return count;
      } else {
         value1->value=left1;
         value2->value=left2;
         hasLeft=false;

         unsigned count=leftCount+incIter();
         observedOutputCardinality+=count;
         return count;
      }
   }
   // Retrieve the next DB triple
   leftCount=input->next();
   if (!leftCount) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      value1->value=t.value1;
      value2->value=t.value2;

      unsigned count=incIter();
      observedOutputCardinality+=count;
      return count;
   }

   // Compare
   const DifferentialIndex::VersionedTriple& t=(*iter);
   int cmp=compareTriple2(value1->value,value2->value,t.value1,t.value2);
   if (cmp<0) {
      observedOutputCardinality+=leftCount;
      return leftCount;
   } else if (cmp>0) {
      left1=value1->value;
      left2=value2->value;
      hasLeft=true;
      value1->value=t.value1;
      value2->value=t.value2;

      unsigned count=incIter();
      observedOutputCardinality+=count;
      return count;
   } else {
      unsigned count=leftCount+incIter();
      observedOutputCardinality+=count;
      return count;
   }
}
//---------------------------------------------------------------------------
void AggregatedDifferentialIndexScan::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("<AggregatedDifferentialIndexScan",expectedOutputCardinality,observedOutputCardinality);
   input->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
void AggregatedDifferentialIndexScan::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   input->addMergeHint(reg1,reg2);
}
//---------------------------------------------------------------------------
void AggregatedDifferentialIndexScan::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
/// Scan over the differential index
class FullyAggregatedDifferentialIndexScan : public Operator
{
   private:
   /// The input
   Operator* input;
   /// The latch
   Latch& latch;
   /// The triples
   set<DifferentialIndex::VersionedTriple>& triples;
   /// The timestamp
   const unsigned timestamp;
   /// The values
   Register* value1;
   /// Range bounds
   DifferentialIndex::VersionedTriple lowerBound,upperBound;
   /// Iterator over the relevant triples
   set<DifferentialIndex::VersionedTriple>::const_iterator iter,limit;
   /// Left triple
   unsigned leftCount,left1;
   /// Do we have a left triple?
   bool hasLeft;
   /// Currently latched?
   bool latched;

   /// Increase the iterator
   unsigned incIter();

   public:
   /// Constructor
   FullyAggregatedDifferentialIndexScan(Operator* input,Latch& latch,unsigned timestamp,set<DifferentialIndex::VersionedTriple>& triples,Register* value1,const DifferentialIndex::VersionedTriple& lowerBound,const DifferentialIndex::VersionedTriple& upperBound);
   /// Destructor
   ~FullyAggregatedDifferentialIndexScan();

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
FullyAggregatedDifferentialIndexScan::FullyAggregatedDifferentialIndexScan(Operator* input,Latch& latch,unsigned timestamp,set<DifferentialIndex::VersionedTriple>& triples,Register* value1,const DifferentialIndex::VersionedTriple& lowerBound,const DifferentialIndex::VersionedTriple& upperBound)
   : Operator(0),input(input),latch(latch),triples(triples),timestamp(timestamp),value1(value1),lowerBound(lowerBound),upperBound(upperBound),latched(false)
   // Constructor
{
}
//---------------------------------------------------------------------------
FullyAggregatedDifferentialIndexScan::~FullyAggregatedDifferentialIndexScan()
   // Destructor
{
   if (latched) {
      latch.unlock();
      latched=false;
   }
   delete input;
}
//---------------------------------------------------------------------------
unsigned FullyAggregatedDifferentialIndexScan::incIter()
   // Increase the iterator
{
   unsigned v1=(*iter).value1;
   unsigned count=1;
   ++iter;
   while (iter!=limit) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      if ((t.created>timestamp)||(t.deleted<timestamp)) { ++iter; continue; }
      if ((*iter).value1!=v1)
         break;
      ++count;
      ++iter;
   }
   return count;
}
//---------------------------------------------------------------------------
static int compareTriple1(unsigned l1,unsigned r1)
   // Compare two triples
{
   if (l1<r1) return -1;
   if (l1>r1) return 1;
   return 0;
}
//---------------------------------------------------------------------------
unsigned FullyAggregatedDifferentialIndexScan::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;

   // Latch if necessary
   if (!latched) {
      latch.lockShared();
      latched=true;
   }

   // Find the range in the differential index
   iter=triples.lower_bound(lowerBound);
   limit=triples.lower_bound(upperBound);
   while (iter!=limit) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      if ((t.created>timestamp)||(t.deleted<timestamp)) { ++iter; continue; }
      break;
   }

   // Retrieve the first DB triple
   hasLeft=false;
   leftCount=input->first();
   if (!leftCount) {
      if (iter==limit) {
         latch.unlock();
         latched=false;
         return 0;
      }
      return next();
   }

   // Compare
   if (iter==limit) {
      observedOutputCardinality+=leftCount;
      return leftCount;
   }
   const DifferentialIndex::VersionedTriple& t=(*iter);
   int cmp=compareTriple1(value1->value,t.value1);
   if (cmp<0) {
      observedOutputCardinality+=leftCount;
      return leftCount;
   } else if (cmp>0) {
      left1=value1->value;
      hasLeft=true;
      value1->value=t.value1;

      unsigned count=incIter();
      observedOutputCardinality+=count;
      return count;
   } else {
      unsigned count=leftCount+incIter();
      observedOutputCardinality+=count;
      return count;
   }
}
//---------------------------------------------------------------------------
unsigned FullyAggregatedDifferentialIndexScan::next()
   // Produce the next tuple
{
   // Right side done?
   if (iter==limit) {
      // Both done?
      if (!leftCount) {
         if (latched) {
            latch.unlock();
            latched=false;
         }
         return 0;
      }
      // Do we have a value?
      if (hasLeft) {
         value1->value=left1;
         hasLeft=false;
         observedOutputCardinality+=leftCount;
         return leftCount;
      }
      // Read the next one
      leftCount=input->next();
      if (!leftCount) {
         if (latched) {
            latch.unlock();
            latched=false;
         }
         return 0;
      }
      observedOutputCardinality+=leftCount;
      return leftCount;
   }
   // Left side done?
   if (!leftCount) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      value1->value=t.value1;

      unsigned count=incIter();
      observedOutputCardinality+=count;
      return count;
   }
   // A buffered left triple?
   if (hasLeft) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      int cmp=compareTriple1(left1,t.value1);
      if (cmp<0) {
         value1->value=left1;
         hasLeft=false;
         observedOutputCardinality+=leftCount;
         return leftCount;
      } else if (cmp>0) {
         value1->value=t.value1;

         unsigned count=incIter();
         observedOutputCardinality+=count;
         return count;
      } else {
         value1->value=left1;
         hasLeft=false;

         unsigned count=leftCount+incIter();
         observedOutputCardinality+=count;
         return count;
      }
   }
   // Retrieve the next DB triple
   leftCount=input->next();
   if (!leftCount) {
      const DifferentialIndex::VersionedTriple& t=(*iter);
      value1->value=t.value1;

      unsigned count=incIter();
      observedOutputCardinality+=count;
      return count;
   }

   // Compare
   const DifferentialIndex::VersionedTriple& t=(*iter);
   int cmp=compareTriple1(value1->value,t.value1);
   if (cmp<0) {
      observedOutputCardinality+=leftCount;
      return leftCount;
   } else if (cmp>0) {
      left1=value1->value;
      hasLeft=true;
      value1->value=t.value1;

      unsigned count=incIter();
      observedOutputCardinality+=count;
      return count;
   } else {
      unsigned count=leftCount+incIter();
      observedOutputCardinality+=count;
      return count;
   }
}
//---------------------------------------------------------------------------
void FullyAggregatedDifferentialIndexScan::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("FullyAggregatedDifferentialIndexScan",expectedOutputCardinality,observedOutputCardinality);
   input->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
void FullyAggregatedDifferentialIndexScan::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   input->addMergeHint(reg1,reg2);
}
//---------------------------------------------------------------------------
void FullyAggregatedDifferentialIndexScan::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
DifferentialIndex::DifferentialIndex(Database& db)
   : db(db),dict(db.getDictionary())
   // Constructor
{
}
//---------------------------------------------------------------------------
DifferentialIndex::~DifferentialIndex()
   // Destructor
{
}
//---------------------------------------------------------------------------
void DifferentialIndex::load(const vector<Triple>& mewTriples, bool deleteMarker)
   // Load new triples
{
   static const unsigned created = deleteMarker? ~0u:0u;
   static const unsigned deleted = deleteMarker? 0u:~0u;

   // SPO
   latches[0].lockExclusive();
   for (vector<Triple>::const_iterator iter=mewTriples.begin(),limit=mewTriples.end();iter!=limit;++iter){
      triples[0].insert(VersionedTriple((*iter).subject,(*iter).predicate,(*iter).object,created,deleted));
   }
   latches[0].unlock();
   // SOP
   latches[1].lockExclusive();
   for (vector<Triple>::const_iterator iter=mewTriples.begin(),limit=mewTriples.end();iter!=limit;++iter)
      triples[1].insert(VersionedTriple((*iter).subject,(*iter).object,(*iter).predicate,created,deleted));
   latches[1].unlock();
   // OPS
   latches[2].lockExclusive();
   for (vector<Triple>::const_iterator iter=mewTriples.begin(),limit=mewTriples.end();iter!=limit;++iter)
      triples[2].insert(VersionedTriple((*iter).object,(*iter).predicate,(*iter).subject,created,deleted));
   latches[2].unlock();
   // OSP
   latches[3].lockExclusive();
   for (vector<Triple>::const_iterator iter=mewTriples.begin(),limit=mewTriples.end();iter!=limit;++iter)
      triples[3].insert(VersionedTriple((*iter).object,(*iter).subject,(*iter).predicate,created,deleted));
   latches[3].unlock();
   // PSO
   latches[4].lockExclusive();
   for (vector<Triple>::const_iterator iter=mewTriples.begin(),limit=mewTriples.end();iter!=limit;++iter)
      triples[4].insert(VersionedTriple((*iter).predicate,(*iter).subject,(*iter).object,created,deleted));
   latches[4].unlock();
   // POS
   latches[5].lockExclusive();
   for (vector<Triple>::const_iterator iter=mewTriples.begin(),limit=mewTriples.end();iter!=limit;++iter)
      triples[5].insert(VersionedTriple((*iter).predicate,(*iter).object,(*iter).subject,created,deleted));
   latches[5].unlock();

}
//---------------------------------------------------------------------------
void DifferentialIndex::mapLiterals(const std::vector<Literal>& literals,std::vector<unsigned>& ids)
   // Map literals to ids
{
   ids.resize(literals.size());

   latches[6].lockExclusive();

   for (unsigned index=0,limit=literals.size();index<limit;index++) {
      // Check the sub-type first
      unsigned subType=0;
      if (Type::hasSubType(literals[index].type)) {
         DictionarySegment::Literal l;
         l.str=literals[index].subType;
         l.type=Type::getSubTypeType(literals[index].type);
         l.subType=0;
         if (string2id.count(l)) {
            // already known, do nothing
            subType=string2id[l];
         } else {
            // Allocate a new id for the sub-type
            unsigned id=dict.getNextId()+string2id.size();
            string2id[l]=subType=id;
            id2string.push_back(l);
         }
      }
      // Now check the literal
      DictionarySegment::Literal l;
      l.str=literals[index].value;
      l.type=literals[index].type;
      l.subType=subType;
      if (string2id.count(l)) {
         ids[index]=string2id[l];
      } else {
         unsigned id=dict.getNextId()+string2id.size();
         string2id[l]=ids[index]=id;
         id2string.push_back(l);
      }
   }

   latches[6].unlock();
}
//---------------------------------------------------------------------------
void DifferentialIndex::clear()
   // Clear the index, discarding all entries
{
   for (unsigned index=0;index<7;index++)
      latches[index].lockExclusive();

   id2string.clear();
   string2id.clear();
   for (unsigned index=0;index<6;index++) {
      triples[index].clear();
   }

   for (unsigned index=0;index<7;index++)
      latches[index].unlock();
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// Loads triples for consumption
class TriplesLoader : public FactsSegment::Source
{
   private:
   /// The range
   set<DifferentialIndex::VersionedTriple>::iterator iter,limit;

   public:
   /// Constructor
   TriplesLoader(set<DifferentialIndex::VersionedTriple>::iterator iter,set<DifferentialIndex::VersionedTriple>::iterator limit) : iter(iter),limit(limit) {}

   /// Get the next triple
   bool next(unsigned& value1,unsigned& value2,unsigned& value3,unsigned& created,unsigned& deleted);
   /// Mark the last entry as duplicate
   void markAsDuplicate();
};
//---------------------------------------------------------------------------
bool TriplesLoader::next(unsigned& value1,unsigned& value2,unsigned& value3,unsigned& created,unsigned& deleted)
   // Get the next triple
{
   if (iter==limit)
      return false;

   value1=(*iter).value1;
   value2=(*iter).value2;
   value3=(*iter).value3;
   created=(*iter).created;
   deleted=(*iter).deleted;
   ++iter;
   return true;
}
//---------------------------------------------------------------------------
void TriplesLoader::markAsDuplicate()
   // Mark the last entry as duplicate
{
   set<DifferentialIndex::VersionedTriple>::iterator last=iter;
   --last;
   const_cast<DifferentialIndex::VersionedTriple&>(*last).deleted=0;
}
//---------------------------------------------------------------------------
/// Loads triples for consumption
class AggregatedTriplesLoader : public AggregatedFactsSegment::Source
{
   private:
   /// The range
   set<DifferentialIndex::VersionedTriple>::iterator iter,limit;

   public:
   /// Constructor
   AggregatedTriplesLoader(set<DifferentialIndex::VersionedTriple>::iterator iter,set<DifferentialIndex::VersionedTriple>::iterator limit) : iter(iter),limit(limit) {}

   /// Get the next triple
   bool next(unsigned& value1,unsigned& value2,unsigned& count);
   /// Mark the last entry as duplicate
   void markAsDuplicate();
};
//---------------------------------------------------------------------------
bool AggregatedTriplesLoader::next(unsigned& value1,unsigned& value2,unsigned& count)
   // Get the next triple
{
   // Make sure the current entry is not a duplicate
   while (true) {
      if (iter==limit)
         return false;
      if ((*iter).deleted!=0)
         break;
      ++iter;
   }

   value1=(*iter).value1;
   value2=(*iter).value2;
   count=1;
   while ((iter!=limit)&&((*iter).value1==value1)&&((*iter).value2==value2)) {
      if ((*iter).deleted!=0)
         ++count;
      ++iter;
   }

   return true;
}
//---------------------------------------------------------------------------
void AggregatedTriplesLoader::markAsDuplicate()
   // Mark the last entry as duplicate
{
}
//---------------------------------------------------------------------------
/// Loads triples for consumption
class FullyAggregatedTriplesLoader : public FullyAggregatedFactsSegment::Source
{
   private:
   /// The range
   set<DifferentialIndex::VersionedTriple>::iterator iter,limit;

   public:
   /// Constructor
   FullyAggregatedTriplesLoader(set<DifferentialIndex::VersionedTriple>::iterator iter,set<DifferentialIndex::VersionedTriple>::iterator limit) : iter(iter),limit(limit) {}

   /// Get the next triple
   bool next(unsigned& value1,unsigned& count);
   /// Mark the last entry as duplicate
   void markAsDuplicate();
};
//---------------------------------------------------------------------------
bool FullyAggregatedTriplesLoader::next(unsigned& value1,unsigned& count)
   // Get the next triple
{
   // Make sure the current entry is not a duplicate
   while (true) {
      if (iter==limit)
         return false;
      if ((*iter).deleted!=0)
         break;
      ++iter;
   }

   value1=(*iter).value1;
   count=1;
   while ((iter!=limit)&&((*iter).value1==value1)) {
      if ((*iter).deleted!=0)
         ++count;
      ++iter;
   }

   return true;
}
//---------------------------------------------------------------------------
void FullyAggregatedTriplesLoader::markAsDuplicate()
   // Mark the last entry as duplicate
{
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
void DifferentialIndex::sync()
   // Synchronize with the underlying database
{
   for (unsigned index=0;index<7;index++)
      latches[index].lockExclusive();

   // Load the new strings
   if (!id2string.empty())
      dict.appendLiterals(id2string);
   id2string.clear();
   string2id.clear();

   // Load the new triples
   for (unsigned index=0;index<6;index++) {
      if (triples[index].empty())
         continue;
      {
         TriplesLoader loader(triples[index].begin(),triples[index].end());
         db.getFacts(static_cast<Database::DataOrder>(index)).update(loader);
      }
      {
         AggregatedTriplesLoader loader(triples[index].begin(),triples[index].end());
         db.getAggregatedFacts(static_cast<Database::DataOrder>(index)).update(loader);
      }
      if ((index&1)==0) {
         FullyAggregatedTriplesLoader loader(triples[index].begin(),triples[index].end());
         db.getFullyAggregatedFacts(static_cast<Database::DataOrder>(index)).update(loader);
      }
      triples[index].clear();
   }

   for (unsigned index=0;index<7;index++)
      latches[index].unlock();
}
//---------------------------------------------------------------------------
Operator* DifferentialIndex::createScan(Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality)
   // Create a suitable scan operator scanning both the DB and the differential index
{
   // Construct the database scan
   Operator* dbScan=IndexScan::create(db,order,subjectRegister,subjectBound,predicateRegister,predicateBound,objectRegister,objectBound,expectedOutputCardinality);

   // Setup the slot bindings
   Register* value1=0,*value2=0,*value3=0;
   bool bound1=false,bound2=false,bound3=false;
   switch (order) {
      case Database::Order_Subject_Predicate_Object:
         value1=subjectRegister; value2=predicateRegister; value3=objectRegister;
         bound1=subjectBound; bound2=predicateBound; bound3=objectBound;
         break;
      case Database::Order_Subject_Object_Predicate:
         value1=subjectRegister; value2=objectRegister; value3=predicateRegister;
         bound1=subjectBound; bound2=objectBound; bound3=predicateBound;
         break;
      case Database::Order_Object_Predicate_Subject:
         value1=objectRegister; value2=predicateRegister; value3=subjectRegister;
         bound1=objectBound; bound2=predicateBound; bound3=subjectBound;
         break;
      case Database::Order_Object_Subject_Predicate:
         value1=objectRegister; value2=subjectRegister; value3=predicateRegister;
         bound1=objectBound; bound2=subjectBound; bound3=predicateBound;
         break;
      case Database::Order_Predicate_Subject_Object:
         value1=predicateRegister; value2=subjectRegister; value3=objectRegister;
         bound1=predicateBound; bound2=subjectBound; bound3=objectBound;
         break;
      case Database::Order_Predicate_Object_Subject:
         value1=predicateRegister; value2=objectRegister; value3=subjectRegister;
         bound1=predicateBound; bound2=objectBound; bound3=subjectBound;
         break;
   }

   // Check if a triple exists
   VersionedTriple lowerBound,upperBound;
   lowerBound.value1=0; upperBound.value1=~0u;
   lowerBound.value2=0; upperBound.value2=~0u;
   lowerBound.value3=0; upperBound.value3=~0u;
   lowerBound.created=0; upperBound.created=~0u;
   if (bound1) {
      lowerBound.value1=upperBound.value1=value1->value;
      if (bound2) {
         lowerBound.value2=upperBound.value2=value2->value;
         if (bound3)
            lowerBound.value3=upperBound.value3=value3->value;
      }
   }
   latches[order].lockShared();
   if (triples[order].lower_bound(lowerBound)==triples[order].upper_bound(upperBound)) {
      // No relevant triples, just scan the db
      latches[order].unlock();
      return dbScan;
   }
   latches[order].unlock();

   // Produce a new scan
   const unsigned timestamp = 0;
   return new DifferentialIndexScan(dbScan,latches[order],timestamp,triples[order],value1,value2,value3,bound2?value2->value:~0u,bound3?value3->value:~0u,lowerBound,upperBound);
}
//---------------------------------------------------------------------------
Operator* DifferentialIndex::createAggregatedScan(Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality)
   // Create a suitable scan operator scanning both the DB and the differential index
{
   // Construct the database scan
   Operator* dbScan=AggregatedIndexScan::create(db,order,subjectRegister,subjectBound,predicateRegister,predicateBound,objectRegister,objectBound,expectedOutputCardinality);

   // Setup the slot bindings
   Register* value1=0,*value2=0;
   bool bound1=false,bound2=false;
   switch (order) {
      case Database::Order_Subject_Predicate_Object:
         value1=subjectRegister; value2=predicateRegister;
         bound1=subjectBound; bound2=predicateBound;
         break;
      case Database::Order_Subject_Object_Predicate:
         value1=subjectRegister; value2=objectRegister;
         bound1=subjectBound; bound2=objectBound;
         break;
      case Database::Order_Object_Predicate_Subject:
         value1=objectRegister; value2=predicateRegister;
         bound1=objectBound; bound2=predicateBound;
         break;
      case Database::Order_Object_Subject_Predicate:
         value1=objectRegister; value2=subjectRegister;
         bound1=objectBound; bound2=subjectBound;
         break;
      case Database::Order_Predicate_Subject_Object:
         value1=predicateRegister; value2=subjectRegister;
         bound1=predicateBound; bound2=subjectBound;
         break;
      case Database::Order_Predicate_Object_Subject:
         value1=predicateRegister; value2=objectRegister;
         bound1=predicateBound; bound2=objectBound;
         break;
   }

   // Check if a triple exists
   VersionedTriple lowerBound,upperBound;
   lowerBound.value1=0; upperBound.value1=~0u;
   lowerBound.value2=0; upperBound.value2=~0u;
   lowerBound.value3=0; upperBound.value3=~0u;
   lowerBound.created=0; upperBound.created=~0u;
   if (bound1) {
      lowerBound.value1=upperBound.value1=value1->value;
      if (bound2) {
         lowerBound.value2=upperBound.value2=value2->value;
      }
   }
   latches[order].lockShared();
   if (triples[order].lower_bound(lowerBound)==triples[order].upper_bound(upperBound)) {
      // No relevant triples, just scan the db
      latches[order].unlock();
      return dbScan;
   }
   latches[order].unlock();

   // Produce a new scan
   const unsigned timestamp = 0;
   return new AggregatedDifferentialIndexScan(dbScan,latches[order],timestamp,triples[order],value1,value2,bound2?value2->value:~0u,lowerBound,upperBound);
}
//---------------------------------------------------------------------------
Operator* DifferentialIndex::createFullyAggregatedScan(Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality)
   // Create a suitable scan operator scanning both the DB and the differential index
{
   // Construct the database scan
   Operator* dbScan=FullyAggregatedIndexScan::create(db,order,subjectRegister,subjectBound,predicateRegister,predicateBound,objectRegister,objectBound,expectedOutputCardinality);

   // Setup the slot bindings
   Register* value1=0;
   bool bound1=false;
   switch (order) {
      case Database::Order_Subject_Predicate_Object:
         value1=subjectRegister;
         bound1=subjectBound;
         break;
      case Database::Order_Subject_Object_Predicate:
         value1=subjectRegister;
         bound1=subjectBound;
         break;
      case Database::Order_Object_Predicate_Subject:
         value1=objectRegister;
         bound1=objectBound;
         break;
      case Database::Order_Object_Subject_Predicate:
         value1=objectRegister;
         bound1=objectBound;
         break;
      case Database::Order_Predicate_Subject_Object:
         value1=predicateRegister;
         bound1=predicateBound;
         break;
      case Database::Order_Predicate_Object_Subject:
         value1=predicateRegister;
         bound1=predicateBound;
         break;
   }

   // Check if a triple exists
   VersionedTriple lowerBound,upperBound;
   lowerBound.value1=0; upperBound.value1=~0u;
   lowerBound.value2=0; upperBound.value2=~0u;
   lowerBound.value3=0; upperBound.value3=~0u;
   lowerBound.created=0; upperBound.created=~0u;
   if (bound1) {
      lowerBound.value1=upperBound.value1=value1->value;
   }
   latches[order].lockShared();
   if (triples[order].lower_bound(lowerBound)==triples[order].upper_bound(upperBound)) {
      // No relevant triples, just scan the db
      latches[order].unlock();
      return dbScan;
   }
   latches[order].unlock();

   // Produce a new scan
   const unsigned timestamp = 0;
   return new FullyAggregatedDifferentialIndexScan(dbScan,latches[order],timestamp,triples[order],value1,lowerBound,upperBound);
}
//---------------------------------------------------------------------------
unsigned DifferentialIndex::getNextId()
   // Get the next id number
{
   unsigned result;
   latches[6].lockShared();
   result=dict.getNextId()+id2string.size();
   latches[6].unlock();
   return result;
}
//---------------------------------------------------------------------------
bool DifferentialIndex::lookup(const std::string& text,::Type::ID type,unsigned subType,unsigned& id)
   // Lookup an id for a given string
{
   // Try the regular dictionary first
   if (dict.lookup(text,type,subType,id))
      return true;

   // In the local dictionary?
   DictionarySegment::Literal l;
   l.str=text;
   l.type=type;
   l.subType=subType;
   latches[6].lockShared();
   bool result;
   if (string2id.count(l)) {
      id=string2id[l];
      result=true;
   } else {
      result=false;
   }
   latches[6].unlock();

   return result;
}
//---------------------------------------------------------------------------
bool DifferentialIndex::lookupById(unsigned id,const char*& start,const char*& stop,::Type::ID& type,unsigned& subType)
   // Lookup a string for a given id
{
   // A local string?
   if (id>=dict.getNextId()) {
      id-=dict.getNextId();
      latches[6].lockShared();
      bool result;
      if (id>=id2string.size()) {
         result=false;
      } else {
         DictionarySegment::Literal& l=id2string[id];
         start=&(l.str[0]);
         stop=start+l.str.size();
         type=l.type;
         subType=l.subType;
         result=true;
      }
      latches[6].unlock();
      return result;
   }

   // Lookup in the main dictionary
   return dict.lookupById(id,start,stop,type,subType);
}
//---------------------------------------------------------------------------
