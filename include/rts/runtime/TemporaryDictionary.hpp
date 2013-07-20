#ifndef H_rts_runtime_TemporaryDictionary
#define H_rts_runtime_TemporaryDictionary
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
#include "infra/util/Type.hpp"
#include <vector>
#include <string>
#include <map>
//---------------------------------------------------------------------------
class DictionarySegment;
class DifferentialIndex;
//---------------------------------------------------------------------------
/// Dictionary for temporary results
class TemporaryDictionary
{
   private:
   /// A literal
   struct Literal {
      /// The string
      std::string str;
      /// The type
      Type::ID type;
      /// The subtpye
      unsigned subType;

      /// Compare
      bool operator<(const Literal& l) const { return (str<l.str)||((str==l.str)&&((type<l.type)||((type==l.type)&&(subType<l.subType)))); }
   };

   /// The underlying dictionary
   DictionarySegment& dict;
   /// The differential index (if any)
   DifferentialIndex* diffIndex;
   /// The id offset
   unsigned idBase;
   /// id to string
   std::vector<Literal> id2string;
   /// string to id
   std::map<Literal,unsigned> string2id;

   public:
   /// Constructor
   explicit TemporaryDictionary(DictionarySegment& dict);
   /// Constructor
   explicit TemporaryDictionary(DifferentialIndex& diffIndex);
   /// Destructor
   ~TemporaryDictionary();

   /// Lookup an id for a given string
   bool lookup(const std::string& text,Type::ID type,unsigned subType,unsigned& id);
   /// Lookup a string for a given id
   bool lookupById(unsigned id,const char*& start,const char*& stop,Type::ID& type,unsigned& subType);
};
//---------------------------------------------------------------------------
#endif
