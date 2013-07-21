#include "cts/parser/SPARQLLexer.hpp"
#include <iostream>
#include <cstdlib>
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
// Modified by:
//         Giuseppe De Simone and Hancel Gonzalez
//         Advisor: Maria Esther Vidal
//         
// Universidad Simon Bolivar
// 2013,   Caracas - Venezuela.
//         
// Added Token Date. This represent type "dateTime" from SPARQL.
//--------------------------------------------------------------------------

SPARQLLexer::SPARQLLexer(const std::string& input)
   : input(input),pos(this->input.begin()),tokenStart(pos),tokenEnd(pos),
     putBack(None),hasTokenEnd(false)
   // Constructor
{
}
//---------------------------------------------------------------------------
SPARQLLexer::~SPARQLLexer()
   // Destructor
{
}


//---------------------------------------------------------------------------
// Name: getNext
// Modified by: Giuseppe De Simone and Hancel Gonzalez
// Advisor: Maria Esther Vidal
// Description: Get the next token. In the modification, we added the token
//              for the type dateTime but the syntax is not like SPARQL defi-
//              nition. Syntax for this dateTime type is 0000Y00M00D where:
//              4 digits are any year.
//              Character Y for Year
//              2 digits are months in number syntax with the zero.
//              Character M for Month.
//              2 digits are days in number syntax with the zero.  
//---------------------------------------------------------------------------
SPARQLLexer::Token SPARQLLexer::getNext()
   // Get the next token
{
   // Do we have a token already?
   if (putBack!=None) {
      Token result=putBack;
      putBack=None;
      return result;
   }

   // Reset the token end
   hasTokenEnd=false;

   // Read the string
   while (pos!=input.end()) {
      tokenStart=pos;
      // Interpret the first character
      switch (*(pos++)) {
         // Whitespace
         case ' ': case '\t': case '\n': case '\r': case '\f': continue;
         // Single line comment
         case '#':
            while (pos!=input.end()) {
               if (((*pos)=='\n')||((*pos)=='\r'))
                  break;
               ++pos;
            }
            if (pos!=input.end()) ++pos;
            continue;
         // Simple tokens
         case ':': return Colon;
         case ';': return Semicolon;
         case ',': return Comma;
         case '.': return Dot;
         case '_': return Underscore;
         case '{': return LCurly;
         case '}': return RCurly;
         case '(': return LParen;
         case ')': return RParen;
         case '@': return At;
         case '+': return Plus;
         case '-': return Minus;
         case '*': return Mul;
         case '/': return Div;
         case '=': return Equal;
         // Not equal
         case '!':
            if ((pos==input.end())||((*pos)!='='))
               return Not;
            ++pos;
            return NotEqual;
         // Brackets
         case '[':
            // Skip whitespaces
            while (pos!=input.end()) {
               switch (*pos) {
                  case ' ': case '\t': case '\n': case '\r': case '\f': ++pos; continue;
               }
               break;
            }
            // Check for a closing ]
            if ((pos!=input.end())&&((*pos)==']')) {
               ++pos;
               return Anon;
            }
            return LBracket;
         case ']': return RBracket;
         // Greater
         case '>':
            if ((pos!=input.end())&&((*pos)=='=')) {
               ++pos;
               return GreaterOrEqual;
            }
            return Greater;
         // Type
         case '^':
            if ((pos==input.end())||((*pos)!='^'))
               return Error;
            ++pos;
            return Type;
         // Or
         case '|':
            if ((pos==input.end())||((*pos)!='|'))
               return Error;
            ++pos;
            return Or;
         // And
         case '&':
            if ((pos==input.end())||((*pos)!='&'))
               return Error;
            ++pos;
            return And;
         // IRI Ref
         case '<':
            tokenStart=pos;
            // Try to parse as URI
            for (;pos!=input.end();++pos) {
              char c=*pos;
              // Escape chars
              if (c=='\\') {
                 if ((++pos)==input.end()) break;
                 continue;
              }
              // Fast tests
              if ((c>='a')&&(c<='z')) continue;
              if ((c>='A')&&(c<='Z')) continue;

              // Test for invalid characters
              if ((c=='<')||(c=='>')||(c=='\"')||(c=='{')||(c=='}')||(c=='^')||(c=='|')||(c=='`')||((c&0xFF)<=0x20))
                 break;
            }

            // Successful parse?
            if ((pos!=input.end())&&((*pos)=='>')) {
               tokenEnd=pos; hasTokenEnd=true;
               ++pos;
               return IRI;
            }
            pos=tokenStart;

            // No, do we have a less-or-equal?
            if (((pos+1)!=input.end())&&((*(pos))=='=')) {
               pos++;
               return LessOrEqual;
            }
            // Just a less
            return Less;
         // String
         case '\'':
            tokenStart=pos;
            while (pos!=input.end()) {
               if ((*pos)=='\\') {
                  ++pos;
                  if (pos!=input.end()) ++pos;
                  continue;
               }
               if ((*pos)=='\'')
                  break;
               ++pos;
            }
            tokenEnd=pos; hasTokenEnd=true;
            if (pos!=input.end()) ++pos;
            return String;
         // String
         case '\"':
            tokenStart=pos;
            while (pos!=input.end()) {
               if ((*pos)=='\\') {
                  ++pos;
                  if (pos!=input.end()) ++pos;
                  continue;
               }
               if ((*pos)=='\"')
                  break;
               ++pos;
            }
            tokenEnd=pos; hasTokenEnd=true;
            if (pos!=input.end()) ++pos;
            return String;
         // Variables
         case '?': case '$':
            tokenStart=pos;
            while (pos!=input.end()) {
               char c=*pos;
               if (((c>='0')&&(c<='9'))||((c>='A')&&(c<='Z'))||((c>='a')&&(c<='z'))) {
                  ++pos;
               } else break;
            }
            tokenEnd=pos; hasTokenEnd=true;
            return Variable;
         // Number
         case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9':
           while (pos!=input.end()) {
              char c=*pos;
              if ((c>='0')&&(c<='9')) {
                 ++pos;
              }
              // Date 
              else if (c=='Y')  {
                ++pos;
                char b=*pos;
                while (pos!=input.end()) {
                  if ((b>='0')&&(b<='9')) {
                    ++pos;
                    b=*pos;
                  }
                  else if (b=='M') {
                    ++pos;
                    char a=*pos;
                    while (pos!=input.end()) {
                      if ((a>='0')&&(a<='9')) {
                        ++pos;
                        a=*pos;
                      }
                      else if(a=='D'){
                        pos = pos + 1;
                        tokenEnd=pos; hasTokenEnd=true;
			return Date;
                      }
                    }
                  }
                }  
              }
              else break;
           }
           tokenEnd=pos; hasTokenEnd=true;
           return Integer;
         // Identifier
         default:
            --pos;
            while (pos!=input.end()) {
               char c=*pos;
               if (((c>='0')&&(c<='9'))||((c>='A')&&(c<='Z'))||((c>='a')&&(c<='z'))) {
                  ++pos;
               } else break;
            }
            if (pos==tokenStart)
               return Error;
            return Identifier;
      }
   }
   return Eof;
}
//---------------------------------------------------------------------------
std::string SPARQLLexer::getTokenValue() const
   // Get the value of the current token
{
   if (hasTokenEnd)
      return std::string(tokenStart,tokenEnd); else
      return std::string(tokenStart,pos);
}
//---------------------------------------------------------------------------
std::string SPARQLLexer::getIRIValue() const
   // Get the value of the current token interpreting IRI escapes
{
   std::string::const_iterator limit=(hasTokenEnd?tokenEnd:pos);
   std::string result;
   for (std::string::const_iterator iter=tokenStart,limit=(hasTokenEnd?tokenEnd:pos);iter!=limit;++iter) {
      char c=*iter;
      if (c=='\\') {
         if ((++iter)==limit) break;
         c=*iter;
      }
      result+=c;
   }
   return result;
}
//---------------------------------------------------------------------------
std::string SPARQLLexer::getLiteralValue() const
   // Get the value of the current token interpreting literal escapes
{
   std::string::const_iterator limit=(hasTokenEnd?tokenEnd:pos);
   std::string result;
   for (std::string::const_iterator iter=tokenStart,limit=(hasTokenEnd?tokenEnd:pos);iter!=limit;++iter) {
      char c=*iter;
      if (c=='\\') {
         if ((++iter)==limit) break;
         c=*iter;
      }
      result+=c;
   }
   return result;
}
//---------------------------------------------------------------------------
bool SPARQLLexer::isKeyword(const char* keyword) const
   // Check if the current token matches a keyword
{
   std::string::const_iterator iter=tokenStart,limit=hasTokenEnd?tokenEnd:pos;

   while (iter!=limit) {
      char c=*iter;
      if ((c>='A')&&(c<='Z')) c+='a'-'A';
      char c2=*keyword;
      if ((c2>='A')&&(c2<='Z')) c2+='a'-'A';
      if (c!=c2)
         return false;
      if (!*keyword) return false;
      ++iter; ++keyword;
   }
   return !*keyword;
}
//---------------------------------------------------------------------------
bool SPARQLLexer::hasNext(Token value)
   // Peek at the next token
{
   Token peek=getNext();
   unget(peek);
   return peek==value;
}
//---------------------------------------------------------------------------
