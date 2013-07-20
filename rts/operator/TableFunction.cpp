#include "rts/operator/TableFunction.hpp"
#include "rts/database/Database.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/runtime/TemporaryDictionary.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <iostream>
#include <sstream>
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
TableFunction::TableFunction(Operator* input,Runtime& runtime,unsigned id,const string& name,const vector<FunctionArgument>& inputArgs,const vector<Register*>& outputVars,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),input(input),runtime(runtime),id(id),name(name),inputArgs(inputArgs),outputVars(outputVars)
   // Constructor
{
}
//---------------------------------------------------------------------------
TableFunction::~TableFunction()
   // Destructor
{
   delete input;
}
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
template <class T> void escapeOutput(T start,T stop)
   // Write a string
{
   for (T iter=start;iter!=stop;++iter) {
      char c=*iter;
      if ((c=='\\')||(c==' ')||(c=='\n')||(c=='\r'))
         cout << "\\";
      cout << c;
   }
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
void TableFunction::requestTable()
   // Request new table values
{
   table.clear();
   tableIter=table.begin(); tableLimit=tableIter;

   // Request the table
   cout << "callback " << id << " ";
   escapeOutput(name.begin(),name.end());
   cout << " " << outputVars.size();
   for (unsigned index=0;index<inputArgs.size();index++) {
      cout << " ";
      if (inputArgs[index].reg) {
         unsigned v=inputArgs[index].reg->value;
         if (!~v) {
            cout << "NULL";
         } else {
            const char* start,*stop; Type::ID type; unsigned subType;
            bool ok;
            if (runtime.hasTemporaryDictionary()) {
               ok=runtime.getTemporaryDictionary().lookupById(v,start,stop,type,subType);
            } else {
               ok=runtime.getDatabase().getDictionary().lookupById(v,start,stop,type,subType);
            }
            if (!ok) {
               cout << "NULL";
            } else {
               if (type==Type::URI) {
                  cout << "<";
                  escapeOutput(start,stop);
                  cout << ">";
               } else {
                  cout << "\"";
                  escapeOutput(start,stop);
                  cout << "\"";
                  switch (type) {
                     case Type::URI: break;
                     case Type::Literal: break;
                     case Type::CustomLanguage:
                        if (runtime.hasTemporaryDictionary()) {
                           ok=runtime.getTemporaryDictionary().lookupById(subType,start,stop,type,subType);
                        } else {
                           ok=runtime.getDatabase().getDictionary().lookupById(subType,start,stop,type,subType);
                        }
                        if (ok) {
                           cout << "@";
                           escapeOutput(start,stop);
                        }
                        break;
                     case Type::CustomType:
                        if (runtime.hasTemporaryDictionary()) {
                           ok=runtime.getTemporaryDictionary().lookupById(subType,start,stop,type,subType);
                        } else {
                           ok=runtime.getDatabase().getDictionary().lookupById(subType,start,stop,type,subType);
                        }
                        if (ok) {
                           cout << "^^<";
                           escapeOutput(start,stop);
                           cout << ">";
                        }
                        break;
                     case Type::String: cout << "\"^^<http://www.w3.org/2001/XMLSchema#string>"; break;
                     case Type::Integer: cout << "\"^^<http://www.w3.org/2001/XMLSchema#integer>"; break;
                     case Type::Decimal: cout << "\"^^<http://www.w3.org/2001/XMLSchema#decimal>"; break;
                     case Type::Double: cout << "\"^^<http://www.w3.org/2001/XMLSchema#double>"; break;
                     case Type::Boolean: cout << "\"^^<http://www.w3.org/2001/XMLSchema#boolean>"; break;
                     case Type::Date: cout << "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"; break;
                  }
               }
            }
         }
      } else {
         escapeOutput(inputArgs[index].value.begin(),inputArgs[index].value.end());
      }
   }
   cout << endl;

   // Wait for the answer
   string s;
   if (!(cin >> s)) return;
   if (s!="ok") {
      cout << "unexpected answer '" << s << "'" << endl;
      return;
   }
   unsigned i;
   if (!(cin >> i)) return;
   if (i!=id) {
      cout << "unexpected answer id " << i << endl;
      return;
   }
   if (cin.get()!='\n') cin.unget();

   // Collect input
   bool done=false;
   while (!done) {
      char c;
      if (!cin.get(c)) break;

      // Ignore leading CR
      if (c=='\r') continue;

      // Prepare a new line
      vector<string> line;
      string current;
      while (true) {
         if (c=='\\') {
            if (!cin.get(c)) break;
            if ((c=='.')&&(line.empty())&&(current.length()==0)) {
               done=true;
               if (cin.get(c)) {
                  if (c!='\n')
                     cin.unget();
               }
               break;
            }
            current+=c;
         } else if (c==' ') {
            line.push_back(current);
            current="";
         } else if (c=='\n') {
            line.push_back(current);
            break;
         } else {
            current+=c;
         }
         if (!cin.get(c)) break;
      }
      if (done) break;

      // Store it if appropirate
      if (line.size()!=outputVars.size()) {
         cout << "malformed callback line, get " << line.size() << " entries, expected " << outputVars.size() << endl;
         return;
      }
      DictionarySegment& dict=runtime.getDatabase().getDictionary();
      for (vector<string>::const_iterator iter=line.begin(),limit=line.end();iter!=limit;++iter) {
         const string& s=*iter;
         string v;
         Type::ID type; unsigned subType=0;
         unsigned id;
         bool ok=true;
         if (s.length()<2) {
            ok=false;
         } else if (s[0]=='<') {
            if (s[s.length()-1]=='>')
               v=s.substr(1,s.length()-2); else
               ok=false;
            type=Type::URI;
         } else if (s[0]=='"') {
            unsigned iter=1,limit=s.length();
            for (;iter<limit;++iter)
               if (s[iter]=='\\') ++iter; else
               if (s[iter]=='"') break;
            v=s.substr(1,iter-1);
            if (iter+1==limit) {
               type=Type::Literal;
            } else if ((iter+1<limit)&&(s[iter+1]=='@')) {
               type=Type::CustomLanguage;
               if (runtime.hasTemporaryDictionary()) {
                  ok=runtime.getTemporaryDictionary().lookup(s.substr(iter+2),Type::Literal,0,subType);
               } else {
                  ok=dict.lookup(s.substr(iter+2),Type::Literal,0,subType);
               }
            } else if ((iter+2<limit)&&(s.substr(iter+1,2)=="^^")) {
               string t=s.substr(iter+3,s.length()-(iter+3)-1);
               if (t=="http://www.w3.org/2001/XMLSchema#string") type=Type::String; else
               if (t=="http://www.w3.org/2001/XMLSchema#integer") type=Type::Integer; else
               if (t=="http://www.w3.org/2001/XMLSchema#decimal") type=Type::Decimal; else
               if (t=="http://www.w3.org/2001/XMLSchema#double") type=Type::Double; else
               if (t=="http://www.w3.org/2001/XMLSchema#boolean") type=Type::Boolean; else {
                  type=Type::CustomType;
                  if (runtime.hasTemporaryDictionary()) {
                     ok=runtime.getTemporaryDictionary().lookup(t,Type::URI,0,subType);
                  } else {
                     ok=dict.lookup(t,Type::URI,0,subType);
                  }
               }
            } else ok=false;
         } else ok=false;
         if (ok) {
            if (runtime.hasTemporaryDictionary()) {
               ok=runtime.getTemporaryDictionary().lookup(v,type,subType,id);
            } else {
               ok=dict.lookup(v,type,subType,id);
            }
         }
         if (!ok) {
            table.push_back(~0u);
         } else {
            table.push_back(id);
         }
      }
   }

   // Setup the iterators
   tableIter=table.begin();
   tableLimit=table.end();
}
//---------------------------------------------------------------------------
unsigned TableFunction::first()
   // Produce the first tuple
{
   // Check for the first input tuple
   if ((count=input->first())==0)
      return false;

   // Request the table
   requestTable();

   return next();
}
//---------------------------------------------------------------------------
unsigned TableFunction::next()
   // Produce the next tuple
{
   while (true) {
      // More data?
      if (tableIter!=tableLimit) {
         for (unsigned index=0,limit=outputVars.size();index<limit;index++)
            outputVars[index]->value=*(tableIter++);
         return count;
      }

      // Examine the next input tuple
      if ((count=input->next())==0)
         return false;

      // Request the table
      requestTable();
   }
}
//---------------------------------------------------------------------------
void TableFunction::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("TableFunction",expectedOutputCardinality,observedOutputCardinality);

   stringstream a;
   a << name << " " << id << " [";
   for (vector<FunctionArgument>::const_iterator iter=inputArgs.begin(),limit=inputArgs.end();iter!=limit;++iter)
      if ((*iter).reg)
         a << out.formatRegister((*iter).reg) << " "; else
         a << (*iter).value << " ";
   a << "] [";
   for (vector<Register*>::const_iterator iter=outputVars.begin(),limit=outputVars.end();iter!=limit;++iter)
      a << out.formatRegister(*iter) << " ";
   a << "]";
   out.addGenericAnnotation(a.str());
   input->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
void TableFunction::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   input->addMergeHint(reg1,reg2);
}
//---------------------------------------------------------------------------
void TableFunction::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
