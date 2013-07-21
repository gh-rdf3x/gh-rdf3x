#ifndef H_cts_parser_SPARQLLexer
#define H_cts_parser_SPARQLLexer

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
//---------------------------------------------------------------------------

#include <string>
//---------------------------------------------------------------------------
/// A lexer for SPARQL input
class SPARQLLexer
{
   public:
   /// Possible tokens
   enum Token { None, Error, Eof, IRI, String, Variable, Identifier, Colon, Semicolon, Comma, Dot, Underscore, LCurly, RCurly, LParen, RParen, LBracket, RBracket, Anon, Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual, At, Type, Not, Or, And, Plus, Minus, Mul, Div, Integer, Decimal, Double, Date  };

   private:
   /// The input
   std::string input;
   /// The current position
   std::string::const_iterator pos;
   /// The start of the current token
   std::string::const_iterator tokenStart;
   /// The end of the curent token. Only set if delimiters are stripped
   std::string::const_iterator tokenEnd;
   /// The token put back with unget
   Token putBack;
   /// Was the doken end set?
   bool hasTokenEnd;

   public:
   /// Constructor
   SPARQLLexer(const std::string& input);
   /// Destructor
   ~SPARQLLexer();

   /// Get the next token
   Token getNext();
   /// Get the value of the current token
   std::string getTokenValue() const;
   /// Get the value of the current token interpreting IRI escapes
   std::string getIRIValue() const;
   /// Get the value of the current token interpreting literal escapes
   std::string getLiteralValue() const;
   /// Check if the current token matches a keyword
   bool isKeyword(const char* keyword) const;
   /// Put the last token back
   void unget(Token value) { putBack=value; }
   /// Peek at the next token
   bool hasNext(Token value);

   /// Return the read pointer
   std::string::const_iterator getReader() const { return (putBack!=None)?tokenStart:pos; }
};
//---------------------------------------------------------------------------
#endif
