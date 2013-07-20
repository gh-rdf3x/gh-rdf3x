#include "rts/runtime/DomainDescription.hpp"
#include "infra/osdep/Mutex.hpp"
#include <cstring>
//---------------------------------------------------------------------------
// Protect against messy system headers under Windows
#undef min
#undef max
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
DomainDescription::DomainDescription(const DomainDescription& other)
   : min(other.min),max(other.max)
   // Copy-Constructor
{
   memcpy(filter,other.filter,sizeof(filter));
}
//---------------------------------------------------------------------------
DomainDescription& DomainDescription::operator=(const DomainDescription& other)
   // Assignment
{
   if (this!=&other) {
      min=other.min;
      max=other.max;
      memcpy(filter,other.filter,sizeof(filter));
   }
   return *this;
}
//---------------------------------------------------------------------------
bool DomainDescription::couldQualify(unsigned value) const
   // Could this value qualify?
{
   if ((value<min)||(value>max))
      return false;

   unsigned bit=value%(filterSize*filterEntryBits);
   return filter[bit/filterEntryBits]&(filterEntry1<<(bit%filterEntryBits));
}
//---------------------------------------------------------------------------
unsigned DomainDescription::nextCandidate(unsigned value) const
   // Return the next value >= value that could qualify (or ~0u)
{
   if (value<min) value=min;
   if (value>max) return ~0u;

   // Potential value?
   unsigned bit=value%(filterSize*filterEntryBits);
   unsigned slot=bit/filterEntryBits,ofs=bit%filterEntryBits;
   FilterEntry entry=filter[slot],mask=filterEntry1<<ofs;
   if (entry&mask)
      return value;

   // No, check the next highest bit in this entry
   if (entry&(~(mask-1))) {
      while (!(entry&mask)) {
         mask<<=1;
         value++;
      }
      if (value>max) return ~0u;
      return value;
   }
   value+=filterEntryBits-ofs;

   // Scan for the next non-zero entry
   for (unsigned index=slot+1;index<filterSize;index++)
      if (filter[index]) {
         value+=filterEntryBits*(index-slot-1);
         entry=filter[index]; mask=filterEntry1;
         while (!(entry&mask)) {
            mask<<=1;
            value++;
         }
         if (value>max) return ~0u;
         return value;
      }
   value+=filterEntryBits*(filterSize-slot);
   for (unsigned index=0;index<=slot;index++)
      if (filter[index]) {
         value+=filterEntryBits*(index);
         entry=filter[index]; mask=filterEntry1;
         while (!(entry&mask)) {
            mask<<=1;
            value++;
         }
         if (value>max) return ~0u;
         return value;
      }

   // No set bit? This should not happen...
   return ~0u;
}
//---------------------------------------------------------------------------
PotentialDomainDescription::PotentialDomainDescription()
   // Constructor
{
   min=0;
   max=~0u;
   memset(filter,0xFF,sizeof(filter));
}
//---------------------------------------------------------------------------
static const unsigned lockCount = 16;
static Mutex lockTable[lockCount];
static Mutex& getLock(void* ptr) { return lockTable[reinterpret_cast<uintptr_t>(ptr)%lockCount]; }
//---------------------------------------------------------------------------
void PotentialDomainDescription::sync(PotentialDomainDescription& other)
   // Synchronize with another domain, computing the intersection. Both are modified!
{
   Mutex& lock=getLock(this);
   lock.lock();

   if (min<other.min)
      min=other.min;
   if (min>other.min)
      other.min=min;
   if (max>other.max)
      max=other.max;
   if (max<other.max)
      other.max=max;
   for (unsigned index=0;index<filterSize;index++) {
      FilterEntry n=filter[index]&other.filter[index];
      filter[index]=n;
      other.filter[index]=n;
   }

   lock.unlock();
}
//---------------------------------------------------------------------------
void PotentialDomainDescription::restrictTo(const ObservedDomainDescription& other)
   // Restrict to an observed domain
{
   Mutex& lock=getLock(this);
   lock.lock();

   if (min<other.min)
      min=other.min;
   if (max>other.max)
      max=other.max;
   for (unsigned index=0;index<filterSize;index++)
      filter[index]&=other.filter[index];

   lock.unlock();
}
//---------------------------------------------------------------------------
ObservedDomainDescription::ObservedDomainDescription()
   // Constructor
{
   min=~0u;
   max=0;
   memset(filter,0,sizeof(filter));
}
//---------------------------------------------------------------------------
void ObservedDomainDescription::add(unsigned value)
   // Add an observed value
{
   if (value<min)
      min=value;
   if (value>max)
      max=value;

   unsigned bit=value%(filterSize*filterEntryBits);
   filter[bit/filterEntryBits]|=filterEntry1<<(bit%filterEntryBits);
}
//---------------------------------------------------------------------------
