#include "rts/operator/IndexScan.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
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
/// Implementation
class IndexScan::Scan : public IndexScan {
   public:
   /// Constructor
   Scan(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality) : IndexScan(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality) {}

   /// First tuple
   unsigned first();
   /// Next tuple
   unsigned next();
};
//---------------------------------------------------------------------------
/// Implementation
class IndexScan::ScanFilter2 : public IndexScan {
   private:
   /// Filter
   unsigned filter2;

   public:
   /// Constructor
   ScanFilter2(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality) : IndexScan(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality) {}

   /// First tuple
   unsigned first();
   /// Next tuple
   unsigned next();
};
//---------------------------------------------------------------------------
/// Implementation
class IndexScan::ScanFilter3 : public IndexScan {
   private:
   /// Filter
   unsigned filter3;

   public:
   /// Constructor
   ScanFilter3(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality) : IndexScan(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality) {}

   /// First tuple
   unsigned first();
   /// Next tuple
   unsigned next();
};
//---------------------------------------------------------------------------
/// Implementation
class IndexScan::ScanFilter23 : public IndexScan {
   private:
   /// Filter
   unsigned filter2,filter3;

   public:
   /// Constructor
   ScanFilter23(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality) : IndexScan(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality) {}

   /// First tuple
   unsigned first();
   /// Next tuple
   unsigned next();
};
//---------------------------------------------------------------------------
/// Implementation
class IndexScan::ScanPrefix1 : public IndexScan {
   private:
   /// Stop condition
   unsigned stop1;

   public:
   /// Constructor
   ScanPrefix1(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality) : IndexScan(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality) {}

   /// First tuple
   unsigned first();
   /// Next tuple
   unsigned next();
};
//---------------------------------------------------------------------------
/// Implementation
class IndexScan::ScanPrefix1Filter3 : public IndexScan {
   private:
   /// Stop condition
   unsigned stop1;
   /// Filter
   unsigned filter3;

   public:
   /// Constructor
   ScanPrefix1Filter3(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality) : IndexScan(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality) {}

   /// First tuple
   unsigned first();
   /// Next tuple
   unsigned next();
};
//---------------------------------------------------------------------------
/// Implementation
class IndexScan::ScanPrefix12 : public IndexScan {
   private:
   /// Stop condition
   unsigned stop1,stop2;

   public:
   /// Constructor
   ScanPrefix12(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality) : IndexScan(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality) {}

   /// First tuple
   unsigned first();
   /// Next tuple
   unsigned next();
};
//---------------------------------------------------------------------------
/// Implementation
class IndexScan::ScanPrefix123 : public IndexScan {
   private:
   /// Stop condition
   unsigned stop1,stop2,stop3;

   public:
   /// Constructor
   ScanPrefix123(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality) : IndexScan(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality) {}

   /// First tuple
   unsigned first();
   /// Next tuple
   unsigned next();
};
//---------------------------------------------------------------------------
IndexScan::Hint::Hint(IndexScan& scan)
   : scan(scan)
   // Constructor
{
}
//---------------------------------------------------------------------------
IndexScan::Hint::~Hint()
   // Destructor
{
}
//---------------------------------------------------------------------------
void IndexScan::Hint::next(unsigned& value1,unsigned& value2,unsigned& value3)
   // Scanning hint
{
   // First value
   if (scan.bound1) {
      unsigned v=scan.value1->value;
      if ((~v)&&(v>value1)) {
         value1=v;
         value2=0;
         value3=0;
      }
   }
   for (std::vector<Register*>::const_iterator iter=scan.merge1.begin(),limit=scan.merge1.end();iter!=limit;++iter) {
      unsigned v=(*iter)->value;
      if ((~v)&&(v>value1)) {
         value1=v;
         value2=0;
         value3=0;
      }
   }
   if (scan.value1->domain) {
      unsigned v=scan.value1->domain->nextCandidate(value1);
      if (v>value1) {
         value1=v;
         value2=0;
         value3=0;
      }
   }

   // Second value
   if (scan.bound2) {
      unsigned v=scan.value2->value;
      if ((~v)&&(v>value2)) {
         value2=v;
         value3=0;
      }
   }
   for (std::vector<Register*>::const_iterator iter=scan.merge2.begin(),limit=scan.merge2.end();iter!=limit;++iter) {
      unsigned v=(*iter)->value;
      if ((~v)&&(v>value2)) {
         value2=v;
         value3=0;
      }
   }
   if (scan.value2->domain) {
      unsigned v=scan.value2->domain->nextCandidate(value2);
      if (v>value2) {
         value2=v;
         value3=0;
      }
   }

   // Third value
   if (scan.bound3) {
      unsigned v=scan.value3->value;
      if ((~v)&&(v>value3)) {
         value3=v;
      }
   }
   for (std::vector<Register*>::const_iterator iter=scan.merge3.begin(),limit=scan.merge3.end();iter!=limit;++iter) {
      unsigned v=(*iter)->value;
      if ((~v)&&(v>value3)) {
         value3=v;
      }
   }
   if (scan.value3->domain) {
      unsigned v=scan.value3->domain->nextCandidate(value3);
      if (v>value3) {
         value3=v;
      }
   }
}
//---------------------------------------------------------------------------
IndexScan::IndexScan(Database& db,Database::DataOrder order,Register* value1,bool bound1,Register* value2,bool bound2,Register* value3,bool bound3,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),value1(value1),value2(value2),value3(value3),bound1(bound1),bound2(bound2),bound3(bound3),facts(db.getFacts(order)),order(order),
     scan(disableSkipping?0:&hint),hint(*this)
   // Constructor
{
}
//---------------------------------------------------------------------------
IndexScan::~IndexScan()
   // Destructor
{
}
//---------------------------------------------------------------------------
void IndexScan::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   const char* scanType="";
   switch (order) {
      case Database::Order_Subject_Predicate_Object: scanType="SubjectPredicateObject"; break;
      case Database::Order_Subject_Object_Predicate: scanType="SubjectObjectPredicate"; break;
      case Database::Order_Object_Predicate_Subject: scanType="ObjectPredicateSubject"; break;
      case Database::Order_Object_Subject_Predicate: scanType="ObjectSubjectPredicate"; break;
      case Database::Order_Predicate_Subject_Object: scanType="PredicateSubjectObject"; break;
      case Database::Order_Predicate_Object_Subject: scanType="PredicateObjectSubject"; break;
   }
   out.beginOperator("IndexScan",expectedOutputCardinality,observedOutputCardinality);
   out.addArgumentAnnotation(scanType);
   out.addScanAnnotation(value1,bound1);
   out.addScanAnnotation(value2,bound2);
   out.addScanAnnotation(value3,bound3);
   out.endOperator();
}
//---------------------------------------------------------------------------
static void handleHints(Register* reg1,Register* reg2,Register* result,std::vector<Register*>& merges)
   // Add hints
{
   bool has1=false,has2=false;
   for (std::vector<Register*>::const_iterator iter=merges.begin(),limit=merges.end();iter!=limit;++iter) {
      if ((*iter)==reg1) has1=true;
      if ((*iter)==reg2) has2=true;
   }
   if (reg1==result) has1=true;
   if (reg2==result) has2=true;

   if (has1&&(!has2)) merges.push_back(reg2);
   if (has2&&(!has1)) merges.push_back(reg1);
}
//---------------------------------------------------------------------------
void IndexScan::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   handleHints(reg1,reg2,value1,merge1);
   handleHints(reg1,reg2,value2,merge2);
   handleHints(reg1,reg2,value3,merge3);
}
//---------------------------------------------------------------------------
void IndexScan::getAsyncInputCandidates(Scheduler& /*scheduler*/)
   // Register parts of the tree that can be executed asynchronous
{
}
//---------------------------------------------------------------------------
IndexScan* IndexScan::create(Database& db,Database::DataOrder order,Register* subject,bool subjectBound,Register* predicate,bool predicateBound,Register* object,bool objectBound,double expectedOutputCardinality)
   // Constructor
{
   // Setup the slot bindings
   Register* value1=0,*value2=0,*value3=0;
   bool bound1=false,bound2=false,bound3=false;
   switch (order) {
      case Database::Order_Subject_Predicate_Object:
         value1=subject; value2=predicate; value3=object;
         bound1=subjectBound; bound2=predicateBound; bound3=objectBound;
         break;
      case Database::Order_Subject_Object_Predicate:
         value1=subject; value2=object; value3=predicate;
         bound1=subjectBound; bound2=objectBound; bound3=predicateBound;
         break;
      case Database::Order_Object_Predicate_Subject:
         value1=object; value2=predicate; value3=subject;
         bound1=objectBound; bound2=predicateBound; bound3=subjectBound;
         break;
      case Database::Order_Object_Subject_Predicate:
         value1=object; value2=subject; value3=predicate;
         bound1=objectBound; bound2=subjectBound; bound3=predicateBound;
         break;
      case Database::Order_Predicate_Subject_Object:
         value1=predicate; value2=subject; value3=object;
         bound1=predicateBound; bound2=subjectBound; bound3=objectBound;
         break;
      case Database::Order_Predicate_Object_Subject:
         value1=predicate; value2=object; value3=subject;
         bound1=predicateBound; bound2=objectBound; bound3=subjectBound;
         break;
   }
   // Construct the appropriate operator
   IndexScan* result;
   if (!bound1) {
      if (!bound2) {
         if (!bound3)
            result=new Scan(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality); else
            result=new ScanFilter3(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality);
      } else {
         if (!bound3)
            result=new ScanFilter2(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality); else
            result=new ScanFilter23(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality);
      }
   } else {
      if (!bound2) {
         if (!bound3)
            result=new ScanPrefix1(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality); else
            result=new ScanPrefix1Filter3(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality);
      } else {
         if (!bound3)
            result=new ScanPrefix12(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality); else
            result=new ScanPrefix123(db,order,value1,bound1,value2,bound2,value3,bound3,expectedOutputCardinality);
      }
   }
   return result;
}
//---------------------------------------------------------------------------
unsigned IndexScan::Scan::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   if (!scan.first(facts))
      return false;
   value1->value=scan.getValue1();
   value2->value=scan.getValue2();
   value3->value=scan.getValue3();
   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
unsigned IndexScan::Scan::next()
   // Produce the next tuple
{
   if (!scan.next())
      return false;
   value1->value=scan.getValue1();
   value2->value=scan.getValue2();
   value3->value=scan.getValue3();
   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanFilter2::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   filter2=value2->value;
   if (!scan.first(facts))
      return false;
   if (scan.getValue2()!=filter2)
      return next();
   value1->value=scan.getValue1();
   value3->value=scan.getValue3();
   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanFilter2::next()
   // Produce the next tuple
{
   while (true) {
      if (!scan.next())
         return false;
      if (scan.getValue2()!=filter2)
         continue;
      value1->value=scan.getValue1();
      value3->value=scan.getValue3();
      observedOutputCardinality++;
      return 1;
   }
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanFilter3::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   filter3=value3->value;
   if (!scan.first(facts))
      return false;
   if (scan.getValue3()!=filter3)
      return next();
   value1->value=scan.getValue1();
   value2->value=scan.getValue2();
   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanFilter3::next()
   // Produce the next tuple
{
   while (true) {
      if (!scan.next())
         return false;
      if (scan.getValue3()!=filter3)
         continue;
      value1->value=scan.getValue1();
      value2->value=scan.getValue2();
      observedOutputCardinality++;
      return 1;
   }
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanFilter23::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   filter2=value2->value;
   filter3=value3->value;
   if (!scan.first(facts))
      return false;
   if ((scan.getValue2()!=filter2)||(scan.getValue3()!=filter3))
      return next();
   value1->value=scan.getValue1();
   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanFilter23::next()
   // Produce the next tuple
{
   while (true) {
      if (!scan.next())
         return false;
      if ((scan.getValue2()!=filter2)||(scan.getValue3()!=filter3))
         continue;
      value1->value=scan.getValue1();
      observedOutputCardinality++;
      return 1;
   }
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanPrefix1::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   stop1=value1->value;
   if (!scan.first(facts,stop1,0,0))
      return false;
   if (scan.getValue1()>stop1)
      return false;
   value2->value=scan.getValue2();
   value3->value=scan.getValue3();
   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanPrefix1::next()
   // Produce the next tuple
{
   if (!scan.next())
      return false;
   if (scan.getValue1()>stop1)
      return false;
   value2->value=scan.getValue2();
   value3->value=scan.getValue3();
   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanPrefix1Filter3::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   stop1=value1->value;
   filter3=value3->value;
   if (!scan.first(facts,stop1,0,0))
      return false;
   if (scan.getValue1()>stop1)
      return false;
   if (scan.getValue3()!=filter3)
      return next();
   value2->value=scan.getValue2();
   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanPrefix1Filter3::next()
   // Produce the next tuple
{
   while (true) {
      if (!scan.next())
         return false;
      if (scan.getValue1()>stop1)
         return false;
      if (scan.getValue3()!=filter3)
         continue;
      value2->value=scan.getValue2();
      observedOutputCardinality++;
      return 1;
   }
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanPrefix12::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   stop1=value1->value;
   stop2=value2->value;
   if (!scan.first(facts,stop1,stop2,0))
      return false;
   if ((scan.getValue1()>stop1)||((scan.getValue1()==stop1)&&(scan.getValue2()>stop2)))
      return false;
   value3->value=scan.getValue3();
   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanPrefix12::next()
   // Produce the next tuple
{
   if (!scan.next())
      return false;
   if ((scan.getValue1()>stop1)||((scan.getValue1()==stop1)&&(scan.getValue2()>stop2)))
      return false;
   value3->value=scan.getValue3();
   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanPrefix123::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   stop1=value1->value;
   stop2=value2->value;
   stop3=value3->value;
   if (!scan.first(facts,stop1,stop2,stop3))
      return false;
   if ((scan.getValue1()>stop1)||((scan.getValue1()==stop1)&&
       ((scan.getValue2()>stop2)||((scan.getValue2()==stop2)&&(scan.getValue3()>stop3)))))
      return false;
   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
unsigned IndexScan::ScanPrefix123::next()
   // Produce the next tuple
{
   if (!scan.next())
      return false;
   if ((scan.getValue1()>stop1)||((scan.getValue1()==stop1)&&
       ((scan.getValue2()>stop2)||((scan.getValue2()==stop2)&&(scan.getValue3()>stop3)))))
      return false;
   observedOutputCardinality++;
   return 1;
}
//---------------------------------------------------------------------------
