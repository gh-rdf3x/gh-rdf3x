#include "rts/database/Database.hpp"
#include "rts/operator/IndexScan.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/DictionarySegment.hpp"
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
static void writeURI(const char* start,const char* stop)
   // Write a URI
{
   cout << "<";
   for (const char* iter=start;iter<stop;++iter) {
      char c=*iter;
      switch (c) {
         case '\t': cout << "\\t"; break;
         case '\n': cout << "\\n"; break;
         case '\r': cout << "\\r"; break;
         case '>': cout << "\\>"; break;
         case '\\': cout << "\\\\"; break;
         default: cout << c; break;
      }
   }
   cout << ">";
}
//---------------------------------------------------------------------------
static void writeLiteral(const char* start,const char* stop)
   // Write a literal
{
   cout << "\"";
   for (const char* iter=start;iter<stop;++iter) {
      char c=*iter;
      switch (c) {
         case '\t': cout << "\\t"; break;
         case '\n': cout << "\\n"; break;
         case '\r': cout << "\\r"; break;
         case '\"': cout << "\\\""; break;
         case '\\': cout << "\\\\"; break;
         default: cout << c; break;
      }
   }
   cout << "\"";
}
//---------------------------------------------------------------------------
static void dumpSubject(DictionarySegment& dic,unsigned id)
   // Write a subject entry
{
   const char* start,*stop; Type::ID type; unsigned subType;
   if (!dic.lookupById(id,start,stop,type,subType)) {
      cerr << "consistency error: encountered unknown id " << id << endl;
      throw;
   }
   if (type!=Type::URI)
      cerr << "consistency error: subjects must be URIs" << endl;
   writeURI(start,stop);
}
//---------------------------------------------------------------------------
static void dumpPredicate(DictionarySegment& dic,unsigned id)
   // Write a predicate entry
{
   const char* start,*stop; Type::ID type; unsigned subType;
   if (!dic.lookupById(id,start,stop,type,subType)) {
      cerr << "consistency error: encountered unknown id " << id << endl;
      throw;
   }
   if (type!=Type::URI)
      cerr << "consistency error: subjects must be URIs" << endl;
   writeURI(start,stop);
}
//---------------------------------------------------------------------------
static void dumpObject(DictionarySegment& dic,unsigned id)
   // Write an object entry
{
   const char* start,*stop; Type::ID type; unsigned subType;
   if (!dic.lookupById(id,start,stop,type,subType)) {
      cerr << "consistency error: encountered unknown id " << id << endl;
      throw;
   }
   switch (type) {
      case Type::URI: writeURI(start,stop); break;
      case Type::Literal: writeLiteral(start,stop); break;
      case Type::CustomLanguage: {
         const char* start2,*stop2; Type::ID type2; unsigned subType2;
         if (!dic.lookupById(subType,start2,stop2,type2,subType2)) {
            cerr << "consistency error: encountered unknown language " << subType << endl;
            throw;
         }
         writeLiteral(start,stop);
         cout << "@";
         for (const char* iter=start2;iter!=stop2;++iter)
            cout << (*iter);
         } break;
      case Type::CustomType: {
         const char* start2,*stop2; Type::ID type2; unsigned subType2;
         if (!dic.lookupById(subType,start2,stop2,type2,subType2)) {
            cerr << "consistency error: encountered unknown type " << subType << endl;
            throw;
         }
         writeLiteral(start,stop);
         cout << "^^";
         writeURI(start2,stop2);
         } break;
      case Type::String: writeLiteral(start,stop); cout << "^^<http://www.w3.org/2001/XMLSchema#string>"; break;
      case Type::Integer: writeLiteral(start,stop); cout << "^^<http://www.w3.org/2001/XMLSchema#integer>"; break;
      case Type::Decimal: writeLiteral(start,stop); cout << "^^<http://www.w3.org/2001/XMLSchema#decimal>"; break;
      case Type::Double: writeLiteral(start,stop); cout << "^^<http://www.w3.org/2001/XMLSchema#double>"; break;
      case Type::Boolean: writeLiteral(start,stop); cout << "^^<http://www.w3.org/2001/XMLSchema#boolean>"; break;
      case Type::Date: writeLiteral(start,stop); cout << "^^<http://www.w3.org/2001/XMLSchema#dateTime>"; break;
   }
}
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   bool rawDump=false,ntriplesDump=false;
   if ((argc==3)&&(string(argv[2])=="--raw"))
     rawDump=true;
   if ((argc==3)&&(string(argv[2])=="--ntriples"))
     ntriplesDump=true;

   // Greeting
   if (!rawDump)
   cerr << "RDF-3X turtle exporter" << endl
        << "(c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x" << endl;

   // Check the arguments
   if (argc<2) {
      cerr << "usage: " << argv[0] << " <database>" << endl;
      return 1;
   }

   // Open the database
   Database db;
   if (!db.open(argv[1],true)) {
      cerr << "unable to open database " << argv[1] << endl;
      return 1;
   }

   // Raw dump?
   if (rawDump) {
      cerr.sync_with_stdio(false);
      cerr << nounitbuf;
      // Dump the facts
      unsigned maxId=0;
      {
         FactsSegment::Scan scan;
         if (scan.first(db.getFacts(Database::Order_Subject_Predicate_Object))) do {
            cout << scan.getValue1() << " " << scan.getValue2() << " " << scan.getValue3() << "\n";
            maxId=max(maxId,scan.getValue1());
            maxId=max(maxId,scan.getValue2());
            maxId=max(maxId,scan.getValue3());
         } while (scan.next());
      }
      // Dump the strings
      {
         const char* start,*stop; Type::ID type; unsigned subType;
         DictionarySegment& dic=db.getDictionary();
         for (unsigned id=0;(id<=maxId)&&dic.lookupById(id,start,stop,type,subType);++id) {
            cerr << id << " " << type << " " << subType << " ";
	    for (const char* iter=start;iter!=stop;++iter) {
	       char c=*iter;
	       if (c<=0) {
	          unsigned v=(c&0xFF);
		  const char hex[]="0123456789ABCDEF";
		  cerr << "\\x" << hex[v>>4] << hex[v&0xF];
	       } else switch (c) {
	          case '\\': cerr << "\\\\"; break;
		  case '\n': cerr << "\\n"; break;
		  case '\r': cerr << "\\r"; break;
	          default: cerr << c; break;
	       }
	    }
	    cerr << "\n";
         }
      }
      return 0;
   }

   // Dump the database
   DictionarySegment& dic=db.getDictionary();
   Register subject,predicate,object;
   subject.reset(); predicate.reset(); object.reset();
   IndexScan* scan=IndexScan::create(db,Database::Order_Subject_Predicate_Object,&subject,false,&predicate,false,&object,false,0);
   if (ntriplesDump) {
      if (scan->first()) do {
         dumpSubject(dic,subject.value);
         cout << " ";
         dumpPredicate(dic,predicate.value);
         cout << " ";
         dumpObject(dic,object.value);
         cout << "." << "\n";
      } while (scan->next());
   } else if (scan->first()) {
      // Write the first triple
      dumpSubject(dic,subject.value);
      cout << " ";
      dumpPredicate(dic,predicate.value);
      cout << " ";
      dumpObject(dic,object.value);
      unsigned lastSubject=subject.value,lastPredicate=predicate.value,lastObject=object.value;

      // And all others
      while (scan->next()) {
         if (subject.value==lastSubject) {
            if (predicate.value==lastPredicate) {
               cout << " , ";
               dumpObject(dic,object.value);
            } else {
               cout << ";\n  ";
               dumpPredicate(dic,predicate.value);
               cout << " ";
               dumpObject(dic,object.value);
            }
         } else {
            cout << ".\n";
            dumpSubject(dic,subject.value);
            cout << " ";
            dumpPredicate(dic,predicate.value);
            cout << " ";
            dumpObject(dic,object.value);
         }
         lastSubject=subject.value; lastPredicate=predicate.value; lastObject=object.value;
      }
      // Termination
      cout << "." << endl;
   }
   delete scan;
}
//---------------------------------------------------------------------------
