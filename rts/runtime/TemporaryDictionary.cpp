#include "rts/runtime/TemporaryDictionary.hpp"
#include "rts/runtime/DifferentialIndex.hpp"
#include "rts/segment/DictionarySegment.hpp"
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
TemporaryDictionary::TemporaryDictionary(DictionarySegment& dict)
   : dict(dict),diffIndex(0),idBase(dict.getNextId())
   // Constructor
{
}
//---------------------------------------------------------------------------
TemporaryDictionary::TemporaryDictionary(DifferentialIndex& diffIndex)
   : dict(diffIndex.getDatabase().getDictionary()),diffIndex(&diffIndex),idBase(diffIndex.getNextId())
   // Constructor
{
}
//---------------------------------------------------------------------------
TemporaryDictionary::~TemporaryDictionary()
   // Destructor
{
}
//---------------------------------------------------------------------------
bool TemporaryDictionary::lookup(const std::string& text,Type::ID type,unsigned subType,unsigned& id)
   // Lookup an id for a given string
{
   Literal l;
   l.str=text;
   l.type=type;
   l.subType=subType;

   if (string2id.count(l)) {
      id=string2id[l];
      return true;
   }

   if (diffIndex) {
      if (diffIndex->lookup(text,type,subType,id))
         return true;
   } else {
      if (dict.lookup(text,type,subType,id))
         return true;
   }

   id=idBase+id2string.size();
   id2string.push_back(l);
   string2id[l]=id;

   return true;
}
//---------------------------------------------------------------------------
bool TemporaryDictionary::lookupById(unsigned id,const char*& start,const char*& stop,Type::ID& type,unsigned& subType)
   // Lookup a string for a given id
{
   if (id>=idBase) {
      id-=idBase;
      if (id>=id2string.size()) return false;
      const Literal& l=id2string[id];
      start=l.str.c_str();
      stop=start+l.str.size();
      type=l.type;
      subType=l.subType;
      return true;
   } else if (diffIndex) {
      return diffIndex->lookupById(id,start,stop,type,subType);
   } else {
      return dict.lookupById(id,start,stop,type,subType);
   }
}
//---------------------------------------------------------------------------
