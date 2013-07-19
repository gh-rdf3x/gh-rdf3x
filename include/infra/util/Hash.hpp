#ifndef H_infra_util_Hash
#define H_intra_util_Hash
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
#include <string>
//---------------------------------------------------------------------------
/// Hash functions
class Hash
{
   public:
   /// A 32bit hash
   typedef unsigned hash32_t;
   /// A 64bit hash
   typedef unsigned long long hash64_t;

   /// Hash arbitrary data
   static hash32_t hash(const void* buffer,unsigned size,hash32_t init=0);
   /// Hash a string
   static hash32_t hash(const std::string& text,hash32_t init=0);

   /// Hash arbitrary data
   static hash64_t hash64(const void* buffer,unsigned size,hash64_t init=0);
   /// Hash a string
   static hash64_t hash64(const std::string& text,hash64_t init=0);
};
//---------------------------------------------------------------------------
#endif
