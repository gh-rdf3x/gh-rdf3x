#include "StringLookup.hpp"
#include "TempFile.hpp"
#include "infra/util/Hash.hpp"
#include "infra/util/Type.hpp"
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
StringLookup::StringLookup()
   : entries(new Entry[lookupSize]),nextPredicate(0),nextNonPredicate(0)
   // Constructor
{
   for (unsigned index=0;index<lookupSize;index++)
      entries[index].id=~static_cast<uint64_t>(0);
}
//---------------------------------------------------------------------------
StringLookup::~StringLookup()
   // Destructor
{
   delete[] entries;
}
//---------------------------------------------------------------------------
unsigned StringLookup::lookupPredicate(TempFile& stringFile,const string& predicate)
   // Lookup a predicate
{
   unsigned type=Type::URI;
   unsigned subType=0;

   // Already known?
   unsigned slot=Hash::hash(predicate,(type<<24)^subType)%lookupSize;
   if ((entries[slot].value==predicate)&&(!(entries[slot].id&1)))
      return entries[slot].id;

   // No, construct a new id
   entries[slot].value=predicate;
   uint64_t id=entries[slot].id=((nextPredicate++)<<1);
   entries[slot].type=type;
   entries[slot].subType=subType;

   // And write to file
   stringFile.writeString(predicate.size(),predicate.c_str());
   stringFile.writeId((static_cast<uint64_t>(subType)<<8)|type);
   stringFile.writeId(id);

   return id;
}
//---------------------------------------------------------------------------
unsigned StringLookup::lookupValue(TempFile& stringFile,const string& value,unsigned type,unsigned subType)
   // Lookup a value
{
   // Already known?
   unsigned slot=Hash::hash(value,(type<<24)^subType)%lookupSize;
   if ((entries[slot].value==value)&&(entries[slot].type==type)&&(entries[slot].subType==subType)&&(~entries[slot].id))
      return entries[slot].id;

   // No, construct a new id
   entries[slot].value=value;
   uint64_t id=entries[slot].id=((nextNonPredicate++)<<1)|1;
   entries[slot].type=type;
   entries[slot].subType=subType;

   // And write to file
   stringFile.writeString(value.size(),value.c_str());
   stringFile.writeId((static_cast<uint64_t>(subType)<<8)|type);
   stringFile.writeId(id);

   return id;
}
//---------------------------------------------------------------------------
