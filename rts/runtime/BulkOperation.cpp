#include "rts/runtime/BulkOperation.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "infra/util/Type.hpp"
#include <algorithm>
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
BulkOperation::BulkOperation(DifferentialIndex& differentialIndex)
   : differentialIndex(differentialIndex),deleteMarker(false)
   // Constructor
{
}
//---------------------------------------------------------------------------
BulkOperation::~BulkOperation()
   // Destructor
{
}
//---------------------------------------------------------------------------
unsigned BulkOperation::mapString(const DifferentialIndex::Literal& value)
   // Map a string
{
   // Local?
   if (string2id.count(value))
      return string2id[value];

   // Resolve the sub-type if any
   unsigned subType=0;
   if (Type::hasSubType(value.type)) {
      if (value.type==Type::CustomType) {
         Type::ID realType=value.type;
         if (value.subType=="http://www.w3.org/2001/XMLSchema#string") {
            realType=Type::String;
         } else if (value.subType=="http://www.w3.org/2001/XMLSchema#integer") {
            realType=Type::Integer;
         } else if (value.subType=="http://www.w3.org/2001/XMLSchema#decimal") {
            realType=Type::Decimal;
         } else if (value.subType=="http://www.w3.org/2001/XMLSchema#double") {
            realType=Type::Double;
         } else if (value.subType=="http://www.w3.org/2001/XMLSchema#boolean") {
            realType=Type::Boolean;
         }
         if (realType!=value.type) {
            DifferentialIndex::Literal l;
            l.value=value.value;
            l.type=realType;
            return mapString(l);
         }
      }
      DifferentialIndex::Literal l;
      l.value=value.subType;
      l.type=Type::getSubTypeType(value.type);
      subType=mapString(l);
   };

   // Already in db?
   unsigned id;
   if (differentialIndex.lookup(value.value,value.type,subType,id))
      return id;

   // Create a temporary id
   id=(~0u)-id2string.size();
   string2id[value]=id;
   id2string.push_back(value);

   return id;
}
//---------------------------------------------------------------------------
void BulkOperation::insert(const string& subject,const string& predicate,const string& object,Type::ID objectType,const std::string& objectSubType)
   // Add a triple
{
   DifferentialIndex::Triple t;
   DifferentialIndex::Literal l;
   l.value=subject;
   l.type=Type::URI;
   t.subject=mapString(l);
   l.value=predicate;
   t.predicate=mapString(l);
   l.value=object;
   l.type=objectType;
   l.subType=objectSubType;
   t.object=mapString(l);
   triples.push_back(t);
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// Sort by subject
struct SortBySubject { bool operator()(const DifferentialIndex::Triple& a,const DifferentialIndex::Triple& b) const { return a.subject<b.subject; } };
/// Sort by predicate
struct SortByPredicate { bool operator()(const DifferentialIndex::Triple& a,const DifferentialIndex::Triple& b) const { return a.predicate<b.predicate; } };
/// Sort by object
struct SortByObject { bool operator()(const DifferentialIndex::Triple& a,const DifferentialIndex::Triple& b) const { return a.object<b.object; } };
//---------------------------------------------------------------------------
static double boxVolume(const PredicateLockManager::Box& b)
   // Compute the volume of a box
{
   return (static_cast<double>(b.subjectMax-b.subjectMin)+1.0)*
          (static_cast<double>(b.predicateMax-b.predicateMin)+1.0)*
          (static_cast<double>(b.objectMax-b.objectMin)+1.0);
}
//---------------------------------------------------------------------------
static double computeSplitVolume(vector<DifferentialIndex::Triple>::iterator iter,vector<DifferentialIndex::Triple>::iterator limit,unsigned split,unsigned slot)
   // Compute the volumes after a split
{
   PredicateLockManager::Box leftBox(0,0,0,0,0,0),rightBox(0,0,0,0,0,0);
   bool firstLeft=true,firstRight=true;

   for (;iter!=limit;++iter) {
      // Determine the partition
      bool left=false;
      switch (slot) {
         case 0: left=(*iter).subject<=split; break;
         case 1: left=(*iter).predicate<=split; break;
         case 2: left=(*iter).object<=split; break;
      }
      // Remember
      if (left) {
         if (((*iter).subject<leftBox.subjectMin)||(firstLeft))
            leftBox.subjectMin=(*iter).subject;
         if (((*iter).subject>leftBox.subjectMax)||(firstLeft))
            leftBox.subjectMax=(*iter).subject;
         if (((*iter).predicate<leftBox.predicateMin)||(firstLeft))
            leftBox.predicateMin=(*iter).predicate;
         if (((*iter).predicate>leftBox.predicateMax)||(firstLeft))
            leftBox.predicateMax=(*iter).predicate;
         if (((*iter).object<leftBox.objectMin)||(firstLeft))
            leftBox.objectMin=(*iter).object;
         if (((*iter).object>leftBox.objectMax)||(firstLeft))
            leftBox.objectMax=(*iter).object;
         firstLeft=false;
      } else {
         if (((*iter).subject<rightBox.subjectMin)||(firstRight))
            rightBox.subjectMin=(*iter).subject;
         if (((*iter).subject>rightBox.subjectMax)||(firstRight))
            rightBox.subjectMax=(*iter).subject;
         if (((*iter).predicate<rightBox.predicateMin)||(firstRight))
            rightBox.predicateMin=(*iter).predicate;
         if (((*iter).predicate>rightBox.predicateMax)||(firstRight))
            rightBox.predicateMax=(*iter).predicate;
         if (((*iter).object<rightBox.objectMin)||(firstRight))
            rightBox.objectMin=(*iter).object;
         if (((*iter).object>rightBox.objectMax)||(firstRight))
            rightBox.objectMax=(*iter).object;
         firstRight=false;
      }
   }
   return boxVolume(leftBox)+boxVolume(rightBox);
}
//---------------------------------------------------------------------------
static vector<DifferentialIndex::Triple>::iterator computeSplit(vector<DifferentialIndex::Triple>::iterator iter,vector<DifferentialIndex::Triple>::iterator limit,unsigned splitValue,unsigned slot,PredicateLockManager::Box& leftBox,PredicateLockManager::Box& rightBox)
   // Compute the volumes after a split
{
   bool firstLeft=true,firstRight=true;

   vector<DifferentialIndex::Triple>::iterator split=iter;
   for (;iter!=limit;++iter) {
      // Determine the partition
      bool left=false;
      switch (slot) {
         case 0: left=(*iter).subject<=splitValue; break;
         case 1: left=(*iter).predicate<=splitValue; break;
         case 2: left=(*iter).object<=splitValue; break;
      }
      // Remember
      if (left) {
         if (((*iter).subject<leftBox.subjectMin)||(firstLeft))
            leftBox.subjectMin=(*iter).subject;
         if (((*iter).subject>leftBox.subjectMax)||(firstLeft))
            leftBox.subjectMax=(*iter).subject;
         if (((*iter).predicate<leftBox.predicateMin)||(firstLeft))
            leftBox.predicateMin=(*iter).predicate;
         if (((*iter).predicate>leftBox.predicateMax)||(firstLeft))
            leftBox.predicateMax=(*iter).predicate;
         if (((*iter).object<leftBox.objectMin)||(firstLeft))
            leftBox.objectMin=(*iter).object;
         if (((*iter).object>leftBox.objectMax)||(firstLeft))
            leftBox.objectMax=(*iter).object;
         firstLeft=false;
         swap(*iter,*split);
         ++split;
      } else {
         if (((*iter).subject<rightBox.subjectMin)||(firstRight))
            rightBox.subjectMin=(*iter).subject;
         if (((*iter).subject>rightBox.subjectMax)||(firstRight))
            rightBox.subjectMax=(*iter).subject;
         if (((*iter).predicate<rightBox.predicateMin)||(firstRight))
            rightBox.predicateMin=(*iter).predicate;
         if (((*iter).predicate>rightBox.predicateMax)||(firstRight))
            rightBox.predicateMax=(*iter).predicate;
         if (((*iter).object<rightBox.objectMin)||(firstRight))
            rightBox.objectMin=(*iter).object;
         if (((*iter).object>rightBox.objectMax)||(firstRight))
            rightBox.objectMax=(*iter).object;
         firstRight=false;
      }
   }
   return split;
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
void BulkOperation::buildCover(unsigned maxSize,vector<PredicateLockManager::Box>& boxes)
{
   boxes.clear();

   // Trivial?
   if (triples.size()<=maxSize) {
      for (vector<DifferentialIndex::Triple>::iterator iter=triples.begin(),limit=triples.end();iter!=limit;++iter)
         boxes.push_back(PredicateLockManager::Box((*iter).subject,(*iter).subject,(*iter).predicate,(*iter).predicate,(*iter).object,(*iter).object));
      return;
   }

   // Build one big initial box
   vector<vector<DifferentialIndex::Triple>::iterator> lowerBounds,upperBounds;
   lowerBounds.push_back(triples.begin());
   upperBounds.push_back(triples.end());
   boxes.push_back(PredicateLockManager::Box(0,~0u,0,~0u,0,~0u));
   while (boxes.size()<maxSize) {
      // Find the largest box
      unsigned largestBox=0; double largestVolume=boxVolume(boxes[0]);
      for (unsigned index=1,limit=boxes.size();index<limit;index++) {
         double v=boxVolume(boxes[index]);
         if (v>largestVolume) {
            largestBox=index;
            largestVolume=v;
         }
      }
      // The the best split candidate
      unsigned lastValue,bestGap;

      sort(lowerBounds[largestBox],upperBounds[largestBox],SortBySubject());
      unsigned bestSplitSubject=(*lowerBounds[largestBox]).subject;
      lastValue=bestSplitSubject; bestGap=0;
      for (vector<DifferentialIndex::Triple>::iterator iter=lowerBounds[largestBox],limit=upperBounds[largestBox];iter!=limit;++iter) {
         unsigned gap=(*iter).subject-lastValue;
         lastValue=(*iter).subject;
         if (gap>bestGap) {
            bestSplitSubject=lastValue;
            bestGap=gap;
         }
      }
      double subjectVolume=computeSplitVolume(lowerBounds[largestBox],upperBounds[largestBox],bestSplitSubject,0);

      sort(lowerBounds[largestBox],upperBounds[largestBox],SortByPredicate());
      unsigned bestSplitPredicate=(*lowerBounds[largestBox]).predicate;
      lastValue=bestSplitPredicate; bestGap=0;
      for (vector<DifferentialIndex::Triple>::iterator iter=lowerBounds[largestBox],limit=upperBounds[largestBox];iter!=limit;++iter) {
         unsigned gap=(*iter).predicate-lastValue;
         lastValue=(*iter).predicate;
         if (gap>bestGap) {
            bestSplitPredicate=lastValue;
            bestGap=gap;
         }
      }
      double predicateVolume=computeSplitVolume(lowerBounds[largestBox],upperBounds[largestBox],bestSplitPredicate,1);

      sort(lowerBounds[largestBox],upperBounds[largestBox],SortByObject());
      unsigned bestSplitObject=(*lowerBounds[largestBox]).object;
      lastValue=bestSplitObject; bestGap=0;
      for (vector<DifferentialIndex::Triple>::iterator iter=lowerBounds[largestBox],limit=upperBounds[largestBox];iter!=limit;++iter) {
         unsigned gap=(*iter).object-lastValue;
         lastValue=(*iter).object;
         if (gap>bestGap) {
            bestSplitObject=lastValue;
            bestGap=gap;
         }
      }
      double objectVolume=computeSplitVolume(lowerBounds[largestBox],upperBounds[largestBox],bestSplitSubject,2);

      // Perform the split
      PredicateLockManager::Box leftBox(0,0,0,0,0,0),rightBox(0,0,0,0,0,0);
      vector<DifferentialIndex::Triple>::iterator split;
      if ((predicateVolume<=subjectVolume)&&(predicateVolume<=objectVolume)) {
         split=computeSplit(lowerBounds[largestBox],upperBounds[largestBox],bestSplitPredicate,1,leftBox,rightBox);
      } else if ((subjectVolume<=predicateVolume)&&(subjectVolume<=objectVolume)) {
         split=computeSplit(lowerBounds[largestBox],upperBounds[largestBox],bestSplitSubject,0,leftBox,rightBox);
      } else {
         split=computeSplit(lowerBounds[largestBox],upperBounds[largestBox],bestSplitObject,2,leftBox,rightBox);
      }

      // Update the bounds
      lowerBounds.push_back(split);
      upperBounds.push_back(upperBounds[largestBox]);
      upperBounds[largestBox]=split;
      boxes[largestBox]=leftBox;
      boxes.push_back(rightBox);
   }
}
//---------------------------------------------------------------------------
void BulkOperation::commit()
   // Commit
{
   // Resolve all temporary ids
   vector<unsigned> realIds;
   differentialIndex.mapLiterals(id2string,realIds);
   unsigned tempStart=(~0u)-realIds.size();
   for (vector<DifferentialIndex::Triple>::iterator iter=triples.begin(),limit=triples.end();iter!=limit;++iter) {
      if ((*iter).subject>tempStart) (*iter).subject=realIds[(~0u)-(*iter).subject];
      if ((*iter).predicate>tempStart) (*iter).predicate=realIds[(~0u)-(*iter).predicate];
      if ((*iter).object>tempStart) (*iter).object=realIds[(~0u)-(*iter).object];
   }
   realIds.clear();

   // Load the triples
   differentialIndex.load(triples,deleteMarker);

   // And release
   id2string.clear();
   string2id.clear();
   triples.clear();
}
//---------------------------------------------------------------------------
void BulkOperation::abort()
   // Abort
{
   id2string.clear();
   string2id.clear();
   triples.clear();
}
//---------------------------------------------------------------------------
