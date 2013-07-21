#include "rts/operator/Selection.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <sstream>
#include <iostream>
#include <string>
#include <cassert>
#include <cstdlib>
/*#ifdef __GNUC__
#if (__GNUC__>4)||((__GNUC__==4)&&(__GNUC_MINOR__>=5))
#define CONFIG_TR1
#endif
#endif
#ifdef CONFIG_TR1
#include <tr1/regex> 
#endif*/
#include <regex.h>
#include <time.h>
using namespace std;

//---------------------------------------------------------------------------
// RDF-3X
// Created by: 
//         Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//         (c) 2008 
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
// 
//  -----------------------------------------------------------------------
//
// Created by:
//         Giuseppe De Simone and Hancel Gonzalez
//         Advisor: Maria Esther Vidal
//         
// Universidad Simon Bolivar
// 2013,   Caracas - Venezuela.
// 
// Many bugs were fixed: lang, langMatches, bound, relational operators.
// Added comparations with between dates and selection with regular 
// expressions.
// Note: Regular expression accepted is the search of pattern.
//---------------------------------------------------------------------------

Selection::Result::~Result()
   // Destructor
{
}
//---------------------------------------------------------------------------
void Selection::Result::ensureString(Selection* selection)
   // Ensure that a string is available
{  
   if (!(flags&stringAvailable)) {
      if (flags&idAvailable) {
         const char* start,*stop;
         if ((~id)&&(selection->runtime.getDatabase().getDictionary().lookupById(id,start,stop,type,subType))) {
            value=string(start,stop);
            flags|=typeAvailable;
         } else {
            value="NULL";
         }
      } else if (flags&booleanAvailable) {
         if (boolean)
            value="true"; else
            value="false";
      } else {
         value="";
      }
      flags|=stringAvailable;
   }
}
//---------------------------------------------------------------------------
void Selection::Result::ensureType(Selection* selection)
   // Ensure that the type is available
{
   if (!(flags&typeAvailable)) {
      if (flags&idAvailable) {
         const char* start,*stop;
         if ((~id)&&(selection->runtime.getDatabase().getDictionary().lookupById(id,start,stop,type,subType))) {
            value=string(start,stop);
            flags|=stringAvailable;
         } else {
            type=Type::Literal; // XXX NULL type?
         }
      } else if (flags&booleanAvailable) {
         type=Type::Boolean;
      } else {
         type=Type::Literal;
      }
      flags|=typeAvailable;
   }
}
//---------------------------------------------------------------------------
void Selection::Result::ensureSubType(Selection* selection)
   // Ensure that the type is available
{
   ensureType(selection);
   if (!(flags&subTypeAvailable)) {
      if ((type==Type::CustomLanguage)||(type==Type::CustomType)) {
         const char* start,*stop;
         Type::ID t; unsigned st;
         if (selection->runtime.getDatabase().getDictionary().lookupById(subType,start,stop,t,st)) {
            subTypeValue=string(start,stop);
         } else {
            subTypeValue.clear();
         }
      } else {
         subTypeValue.clear();
      }
      flags|=subTypeAvailable;
   }
}
//---------------------------------------------------------------------------
void Selection::Result::ensureBoolean(Selection* runtime)
   // Ensure that a boolean interpretation is available
{
   if (!(flags&booleanAvailable)) {
      ensureString(runtime);
      boolean=(value=="true");
      flags|=booleanAvailable;
   }
}
//---------------------------------------------------------------------------
void Selection::Result::setBoolean(bool v)
   // Set to a boolean value
{
   flags=booleanAvailable|typeAvailable;
   type=Type::Boolean;
   boolean=v;
}
//---------------------------------------------------------------------------
void Selection::Result::setId(unsigned v)
   // Set to an id value
{
   flags=idAvailable;
   id=v;
}
//---------------------------------------------------------------------------
void Selection::Result::setLiteral(const std::string& v)
   // Set to a string value
{
   flags=stringAvailable|typeAvailable;
   type=Type::Literal;
   value=v;
}
//---------------------------------------------------------------------------
void Selection::Result::setIRI(const std::string& v)
   // Set to a string value
{
   flags=stringAvailable|typeAvailable;
   type=Type::URI;
   value=v;
}
//---------------------------------------------------------------------------
Selection::Predicate::Predicate()
   : selection(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
Selection::Predicate::~Predicate()
   // Destructor
{
}
//---------------------------------------------------------------------------
void Selection::Predicate::setSelection(Selection* s)
   // Set the selection
{
   selection=s;
}
//---------------------------------------------------------------------------
bool Selection::Predicate::check()
   // Check the predicate
{  
   Result r;
   eval(r);
   r.ensureBoolean(selection);
   return r.boolean;
}
//---------------------------------------------------------------------------
Selection::BinaryPredicate::~BinaryPredicate()
   // Destructor
{
   delete left;
   delete right;
}
//---------------------------------------------------------------------------
void Selection::BinaryPredicate::setSelection(Selection* s)
   // Set the selection
{
   Predicate::setSelection(s);
   left->setSelection(s);
   right->setSelection(s);
}
//---------------------------------------------------------------------------
Selection::UnaryPredicate::~UnaryPredicate()
   // Destructor
{
   delete input;
}
//---------------------------------------------------------------------------
void Selection::UnaryPredicate::setSelection(Selection* s)
   // Set the selection
{
   Predicate::setSelection(s);
   input->setSelection(s);
}
//---------------------------------------------------------------------------
void Selection::Or::eval(Result& result)
   // Evaluate the predicate
{
   result.setBoolean(left->check()||right->check());
}
//---------------------------------------------------------------------------
string Selection::Or::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "("+left->print(out)+")||("+right->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::And::eval(Result& result)
   // Evaluate the predicate
{
   result.setBoolean(left->check()&&right->check());
}
//---------------------------------------------------------------------------
string Selection::And::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "("+left->print(out)+")&&("+right->print(out)+")";
}

//---------------------------------------------------------------------------
// Name: eval
// Modified by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Evaluate the equal operator with dates. 
//---------------------------------------------------------------------------
void Selection::Equal::eval(Result& result)
   // Evaluate the predicate
{
   Result l,r;
   left->eval(l);
   right->eval(r);

   // Cheap case first
   if (l.hasId()&&r.hasId()) {
      result.setBoolean(l.id==r.id);
      return;
   }

   // Now compare for real
   l.ensureString(selection);
   r.ensureString(selection);
   
   if(((l.value.size() == 11) && (l.value[4] == 'Y' && l.value[7] == 'M' && l.value[10] == 'D')) ||
      ((r.value.size() == 11) && (r.value[4] == 'Y' && r.value[7] == 'M' && r.value[10] == 'D'))) {
     int y1, m1, d1, y2, m2, d2;
     y1 = atoi(l.value.substr(0,4).c_str()); m1 = atoi(l.value.substr(5,2).c_str()); d1 = atoi(l.value.substr(8,2).c_str());
     y2 = atoi(r.value.substr(0,4).c_str()); m2 = atoi(r.value.substr(5,2).c_str()); d2 = atoi(r.value.substr(8,2).c_str());     
     
     if(y1 == y2 && m1 == m2 && d1 == d2)
       result.setBoolean(true);
     else
       result.setBoolean(false);
     
     return;
   }

   result.setBoolean(l.value==r.value);
}
//---------------------------------------------------------------------------
string Selection::Equal::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "("+left->print(out)+")==("+right->print(out)+")";
}

//---------------------------------------------------------------------------
// Name: eval
// Modified by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Evaluate the not equal operator with dates. 
//---------------------------------------------------------------------------
void Selection::NotEqual::eval(Result& result)
   // Evaluate the predicate
{
   Result l,r;
   left->eval(l);
   right->eval(r);

   // Cheap case first
   if (l.hasId()&&r.hasId()) {
      result.setBoolean(l.id!=r.id);
      return;
   }

   if(((l.value.size() == 11) && ((l.value[4] == 'Y' || l.value[4] == '-') && l.value[7] == 'M' && l.value[10] == 'D')) ||
      ((r.value.size() == 11) && (r.value[4] == 'Y' && r.value[7] == 'M' && r.value[10] == 'D'))) {
     int y1, m1, d1, y2, m2, d2;
     y1 = atoi(l.value.substr(0,4).c_str()); m1 = atoi(l.value.substr(5,2).c_str()); d1 = atoi(l.value.substr(8,2).c_str());
     y2 = atoi(r.value.substr(0,4).c_str()); m2 = atoi(r.value.substr(5,2).c_str()); d2 = atoi(r.value.substr(8,2).c_str());     
     
     if(y1 == y2 && m1 == m2 && d1 == d2)
       result.setBoolean(false);
     else
       result.setBoolean(true);
     
     return;
   }

   // Now compare for real
   l.ensureString(selection);
   r.ensureString(selection);
   result.setBoolean(l.value!=r.value);
}
//---------------------------------------------------------------------------
string Selection::NotEqual::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "("+left->print(out)+")!=("+right->print(out)+")";
}

//---------------------------------------------------------------------------
// Name: eval
// Modified by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Evaluate the less operator with dates.
// Bug: Cast string to int.
//---------------------------------------------------------------------------
void Selection::Less::eval(Result& result)
   // Evaluate the predicate
{
   Result l,r;
   left->eval(l);
   right->eval(r);

   // XXX implement type based comparisons!
   l.ensureString(selection);
   r.ensureString(selection);
   
   if(((l.value.size() == 11) && (l.value[4] == 'Y' && l.value[7] == 'M' && l.value[10] == 'D')) ||
      ((r.value.size() == 11) && (r.value[4] == 'Y' && r.value[7] == 'M' && r.value[10] == 'D'))) {
     int y1, m1, d1, y2, m2, d2;
     y1 = atoi(l.value.substr(0,4).c_str()); m1 = atoi(l.value.substr(5,2).c_str()); d1 = atoi(l.value.substr(8,2).c_str());
     y2 = atoi(r.value.substr(0,4).c_str()); m2 = atoi(r.value.substr(5,2).c_str()); d2 = atoi(r.value.substr(8,2).c_str());     
     if (y1 < y2)
       result.setBoolean(true);
     else if (y1 > y2)
       result.setBoolean(false);
     else if (y1 == y2) {
       if (m1 < m2)
         result.setBoolean(true);
       else if (m1 > m2)
         result.setBoolean(false);
       else if (m1 == m2) {
         if (d1 < d2)
           result.setBoolean(true);
         else if (d1 > d2)
           result.setBoolean(false);
         else if (d1 == d2)
           result.setBoolean(false);
       }
     }
     return;
   }
    
   result.setBoolean(atoi(l.value.c_str()) < atoi(r.value.c_str()));
}
//---------------------------------------------------------------------------
string Selection::Less::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "("+left->print(out)+")<("+right->print(out)+")";
}

//---------------------------------------------------------------------------
// Name: eval
// Modified by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Evaluate the less or equal operator with dates.
// Bug: Cast string to int.
//---------------------------------------------------------------------------
void Selection::LessOrEqual::eval(Result& result)
   // Evaluate the predicate
{
   Result l,r;
   left->eval(l);
   right->eval(r);

   // XXX implement type based comparisons!
   l.ensureString(selection);
   r.ensureString(selection);

   if(((l.value.size() == 11) && (l.value[4] == 'Y' && l.value[7] == 'M' && l.value[10] == 'D')) ||
      ((r.value.size() == 11) && (r.value[4] == 'Y' && r.value[7] == 'M' && r.value[10] == 'D'))) {
     int y1, m1, d1, y2, m2, d2;
     y1 = atoi(l.value.substr(0,4).c_str()); m1 = atoi(l.value.substr(5,2).c_str()); d1 = atoi(l.value.substr(8,2).c_str());
     y2 = atoi(r.value.substr(0,4).c_str()); m2 = atoi(r.value.substr(5,2).c_str()); d2 = atoi(r.value.substr(8,2).c_str()); 

     if (y1 > y2)
       result.setBoolean(false);
     else if (y1 <= y2) {
       if (m1 > m2)
         result.setBoolean(false);
       else if (m1 <= m2) {
         if (d1 <= d2)
           result.setBoolean(true);
         else if (d1 > d2)
           result.setBoolean(false);
       }
     }
     return;
   }

   result.setBoolean(atoi(l.value.c_str()) <= atoi(r.value.c_str()));
}
//---------------------------------------------------------------------------
string Selection::LessOrEqual::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "("+left->print(out)+")<=("+right->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::Plus::eval(Result& result)
   // Evaluate the predicate
{
   Result l,r;
   left->eval(l);
   right->eval(r);

   l.ensureString(selection);
   r.ensureString(selection);
   stringstream s;
   s << (atof(l.value.c_str())+atof(r.value.c_str()));
   result.setLiteral(s.str());
}
//---------------------------------------------------------------------------
string Selection::Plus::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "("+left->print(out)+")+("+right->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::Minus::eval(Result& result)
   // Evaluate the predicate
{
   Result l,r;
   left->eval(l);
   right->eval(r);

   l.ensureString(selection);
   r.ensureString(selection);
   stringstream s;
   s << (atof(l.value.c_str())-atof(r.value.c_str()));
   result.setLiteral(s.str());
}
//---------------------------------------------------------------------------
string Selection::Minus::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "("+left->print(out)+")-("+right->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::Mul::eval(Result& result)
   // Evaluate the predicate
{
   Result l,r;
   left->eval(l);
   right->eval(r);

   l.ensureString(selection);
   r.ensureString(selection);
   stringstream s;
   s << (atof(l.value.c_str())*atof(r.value.c_str()));
   result.setLiteral(s.str());
}
//---------------------------------------------------------------------------
string Selection::Mul::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "("+left->print(out)+")*("+right->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::Div::eval(Result& result)
   // Evaluate the predicate
{
   Result l,r;
   left->eval(l);
   right->eval(r);

   l.ensureString(selection);
   r.ensureString(selection);
   stringstream s;
   s << (atof(l.value.c_str())/atof(r.value.c_str()));
   result.setLiteral(s.str());
}
//---------------------------------------------------------------------------
string Selection::Div::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "("+left->print(out)+")/("+right->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::Not::eval(Result& result)
   // Evaluate the predicate
{
   result.setBoolean(!input->check());
}
//---------------------------------------------------------------------------
string Selection::Not::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "!"+input->print(out);
}
//---------------------------------------------------------------------------
void Selection::Neg::eval(Result& result)
   // Evaluate the predicate
{
   Result i;
   input->eval(i);

   i.ensureString(selection);
   stringstream s;
   s << (-atof(i.value.c_str()));
   result.setLiteral(s.str());
}
//---------------------------------------------------------------------------
string Selection::Neg::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "-"+input->print(out);
}
//---------------------------------------------------------------------------
void Selection::Null::eval(Result& result)
   // Evaluate the predicate
{  
   result.setId(~0u);
}
//---------------------------------------------------------------------------
string Selection::Null::print(PlanPrinter& /*out*/)
   // Print the predicate (debugging only)
{
   return "NULL";
}
//---------------------------------------------------------------------------
void Selection::False::eval(Result& result)
   // Evaluate the predicate
{
   result.setBoolean(false);
}
//---------------------------------------------------------------------------
string Selection::False::print(PlanPrinter& /*out*/)
   // Print the predicate (debugging only)
{
   return "false";
}
//---------------------------------------------------------------------------
void Selection::Variable::eval(Result& result)
   // Evaluate the predicate
{ 
 result.setId(reg->value);
}
//---------------------------------------------------------------------------
string Selection::Variable::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return out.formatRegister(reg);
}
//---------------------------------------------------------------------------
void Selection::ConstantLiteral::eval(Result& result)
   // Evaluate the predicate
{
   result.setId(id);
}
//---------------------------------------------------------------------------
string Selection::ConstantLiteral::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return out.formatValue(id);
}
//---------------------------------------------------------------------------
void Selection::TemporaryConstantLiteral::eval(Result& result)
   // Evaluate the predicate
{
   result.setLiteral(value);
}
//---------------------------------------------------------------------------
string Selection::TemporaryConstantLiteral::print(PlanPrinter& /*out*/)
   // Print the predicate (debugging only)
{
   return value;
}
//---------------------------------------------------------------------------
void Selection::ConstantIRI::eval(Result& result)
   // Evaluate the predicate
{
   result.setId(id);
}
//---------------------------------------------------------------------------
string Selection::ConstantIRI::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return out.formatValue(id);
}
//---------------------------------------------------------------------------
void Selection::TemporaryConstantIRI::eval(Result& result)
   // Evaluate the predicate
{
   result.setIRI(value);
}
//---------------------------------------------------------------------------
string Selection::TemporaryConstantIRI::print(PlanPrinter& /*out*/)
   // Print the predicate (debugging only)
{
   return value;
}
//---------------------------------------------------------------------------
void Selection::FunctionCall::setSelection(Selection* s)
   // Set the selection
{
   Predicate::setSelection(s);
   for (vector<Predicate*>::iterator iter=args.begin(),limit=args.end();iter!=limit;++iter)
      (*iter)->setSelection(s);
}
//---------------------------------------------------------------------------
void Selection::FunctionCall::eval(Result& result)
   // Evaluate the predicate
{
   result.setId(~0u); // XXX perform the call
}
//---------------------------------------------------------------------------
string Selection::FunctionCall::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   string result="<"+func+">(";
   for (vector<Predicate*>::iterator iter=args.begin(),limit=args.end();iter!=limit;++iter) {
      if (iter!=args.begin())
         result+=",";
      result+=(*iter)->print(out);
   }
   result+=")";
   return result;
}
//---------------------------------------------------------------------------
void Selection::BuiltinStr::eval(Result& result)
   // Evaluate the predicate
{
   input->eval(result);
   result.ensureString(selection);
   result.flags|=Result::typeAvailable;
   result.type=Type::Literal;
}
//---------------------------------------------------------------------------
string Selection::BuiltinStr::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "str("+input->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinLang::eval(Result& result)
   // Evaluate the predicate
{
   Result i;
   input->eval(i);
   i.ensureType(selection);

   if (i.type!=Type::CustomLanguage) {
      result.setLiteral("");
   } else {
      i.ensureSubType(selection);
      result.setLiteral(i.subTypeValue);
   }
}
//---------------------------------------------------------------------------
string Selection::BuiltinLang::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "lang("+input->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinLangMatches::eval(Result& result)
   // Evaluate the predicate
{
   Result l,r;
   left->eval(l);

   int i = 0;
   string l_value = "";
   char c;
   while(l.value[i])
   {
     c = l.value[i];
     l_value += toupper(c);
     i++;
   }
   right->eval(r);
   r.ensureString(selection); 
   if(!r.value.compare("*") && l.value.size()) {
     result.setBoolean(true);
     return;
   }
   i = 0;
   string r_value = "";
   while(r.value[i])
   {
     c = r.value[i];
     r_value += toupper(c);
     i++;
   }
   result.setBoolean((int) (l_value.find(r_value))!=-1); // XXX implement language range checks
}
//---------------------------------------------------------------------------
string Selection::BuiltinLangMatches::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return  "langMatches("+left->print(out)+","+right->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinDatatype::eval(Result& result)
   // Evaluate the predicate
{
   Result i;
   input->eval(result);
   i.ensureType(selection);

   switch (i.type) {
      case Type::URI: result.setLiteral("http://www.w3.org/2001/XMLSchema#URI"); break;
      case Type::Literal:
      case Type::CustomLanguage:
      case Type::String: result.setLiteral("http://www.w3.org/2001/XMLSchema#string"); break;
      case Type::Integer: result.setLiteral("http://www.w3.org/2001/XMLSchema#integer"); break;
      case Type::Decimal: result.setLiteral("http://www.w3.org/2001/XMLSchema#decimal"); break;
      case Type::Double: result.setLiteral("http://www.w3.org/2001/XMLSchema#double"); break;
      case Type::Boolean: result.setLiteral("http://www.w3.org/2001/XMLSchema#boolean"); break;
      case Type::Date: result.setLiteral("http://www.w3.org/2001/XMLSchema#dateTime"); break;
      case Type::CustomType: i.ensureSubType(selection); result.setLiteral(i.subTypeValue); break;
   }
}
//---------------------------------------------------------------------------
string Selection::BuiltinDatatype::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "datatype("+input->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinBound::eval(Result& result)
   // Evaluate the predicate
{ 
  Result i;
  input->eval(result);
  result.ensureString(selection);
  
  if(!result.value.compare("NULL"))
    result.setBoolean(false);
  else
    result.setBoolean(true);
}
//---------------------------------------------------------------------------
string Selection::BuiltinBound::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "bound("+input->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinSameTerm::eval(Result& result)
   // Evaluate the predicate
{
   Result l,r;
   left->eval(l);
   right->eval(r);

   // Cheap case
   if (l.hasId()&&r.hasId()) {
      result.setBoolean(l.id==r.id);
      return;
   }

   // Expensive tests
   l.ensureType(selection);
   r.ensureType(selection);
   if ((l.type!=r.type)||(Type::hasSubType(l.type)&&(l.subType!=r.subType))) {
      result.setBoolean(false);
      return;
   }
   l.ensureString(selection);
   r.ensureString(selection);
   result.setBoolean(l.value==r.value);
}
//---------------------------------------------------------------------------
string Selection::BuiltinSameTerm::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "sameTerm("+left->print(out)+","+right->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinIsIRI::eval(Result& result)
   // Evaluate the predicate
{
   Result i;
   input->eval(i);
   i.ensureType(selection);
   result.setBoolean(i.type==Type::URI);
}
//---------------------------------------------------------------------------
string Selection::BuiltinIsIRI::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "isIRI("+input->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinIsBlank::eval(Result& result)
   // Evaluate the predicate
{
   Result i;
   input->eval(i);
   i.ensureType(selection);

   if (i.type!=Type::URI) {
      result.setBoolean(false);
   } else {
      i.ensureString(selection);
      result.setBoolean(i.value.substr(0,2)=="_:");
   }
}
//---------------------------------------------------------------------------
string Selection::BuiltinIsBlank::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "isBlanl("+input->print(out)+")";
}
//---------------------------------------------------------------------------
void Selection::BuiltinIsLiteral::eval(Result& result)
   // Evaluate the predicate
{
   Result i;
   input->eval(i);
   i.ensureType(selection);
   result.setBoolean(i.type!=Type::URI);
}
//---------------------------------------------------------------------------
string Selection::BuiltinIsLiteral::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   return "isLiteral("+input->print(out)+")";
}
//---------------------------------------------------------------------------
Selection::BuiltinRegEx::~BuiltinRegEx()
   // Destructor
{
   delete arg1;
   delete arg2;
   delete arg3;
}
//---------------------------------------------------------------------------
void Selection::BuiltinRegEx::setSelection(Selection* s)
   // Set the selection
{
   Predicate::setSelection(s);
   arg1->setSelection(s);
   arg2->setSelection(s);
   if (arg3) arg3->setSelection(s);
}

//---------------------------------------------------------------------------
// Name: eval
// Modified by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Evaluate the regex function with dates. This is not consider
//              all forms of regular expression, only pattern search.
//---------------------------------------------------------------------------
void Selection::BuiltinRegEx::eval(Result& result)
   // Evaluate the predicate
{
   Result text,pattern;
   arg1->eval(text);
   arg2->eval(pattern);
   text.ensureString(selection);
   pattern.ensureString(selection);
   int found = text.value.find(pattern.value);
   if (found != -1) {
     result.setBoolean(true);
     return;
   }
   result.setBoolean(false);
}
//---------------------------------------------------------------------------
string Selection::BuiltinRegEx::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   string result="regex("+arg1->print(out)+","+arg2->print(out);
   if (arg3) {
      result+=",";
      result+=arg3->print(out);
   }
   result+=")";
   return result;
}
//---------------------------------------------------------------------------
void Selection::BuiltinIn::setSelection(Selection* s)
   // Set the selection
{
   Predicate::setSelection(s);
   probe->setSelection(s);
   for (vector<Predicate*>::iterator iter=args.begin(),limit=args.end();iter!=limit;++iter)
      (*iter)->setSelection(s);
}
//---------------------------------------------------------------------------
void Selection::BuiltinIn::eval(Result& result)
   // Evaluate the predicate
{
   Result p,c;
   probe->eval(p);

   for (vector<Predicate*>::iterator iter=args.begin(),limit=args.end();iter!=limit;++iter) {
      (*iter)->eval(c);
      if (p.hasId()&&c.hasId()) {
         if (p.id==c.id) {
            result.setBoolean(true);
            return;
         }
      } else {
         p.ensureString(selection);
         c.ensureString(selection);
         if (p.value==c.value) {
            result.setBoolean(true);
            return;
         }
      }
   }
   result.setBoolean(false);
}
//---------------------------------------------------------------------------
string Selection::BuiltinIn::print(PlanPrinter& out)
   // Print the predicate (debugging only)
{
   string result="in("+probe->print(out);
   for (vector<Predicate*>::iterator iter=args.begin(),limit=args.end();iter!=limit;++iter) {
      result+=",";
      result+=(*iter)->print(out);
   }
   result+=")";
   return result;
}
//---------------------------------------------------------------------------
Selection::Selection(Operator* input,Runtime& runtime,Predicate* predicate,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),input(input),runtime(runtime),predicate(predicate)
   // Constructor
{
}
//---------------------------------------------------------------------------
Selection::~Selection()
   // Destructor
{
   delete predicate;
   delete input;
}
//---------------------------------------------------------------------------
unsigned Selection::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   predicate->setSelection(this);

   // Get the first tuple
   unsigned count=input->first();
   if (!count) return 0;

   // Match?
   if (predicate->check()) {
      observedOutputCardinality+=count;
      return count;
   }

   // Get the next one
   return next();
}
//---------------------------------------------------------------------------
unsigned Selection::next()
   // Produce the next tuple
{
   while (true) {
      // Retrieve the next tuple
      unsigned count=input->next();
      if (!count) return 0;

      // Match?
      if (predicate->check()) {
         observedOutputCardinality+=count;
         return count;
      }
   }
}
//---------------------------------------------------------------------------
void Selection::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("Selection",expectedOutputCardinality,observedOutputCardinality);
   out.addGenericAnnotation(predicate->print(out));
   input->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
void Selection::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   input->addMergeHint(reg1,reg2);
}
//---------------------------------------------------------------------------
void Selection::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   input->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
