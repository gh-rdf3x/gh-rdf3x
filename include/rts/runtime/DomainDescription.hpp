#ifndef H_rts_runtime_DomainDescription
#define H_rts_runtime_DomainDescription
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
class ObservedDomainDescription;
//---------------------------------------------------------------------------
/** Domain description.
  * Base class for concrete domain descriptions.
  */
class DomainDescription
{
   protected:
   /// The desired bloom filter size
   static const unsigned filterSize = 128;
   /// An filter entry
   typedef unsigned long FilterEntry;
   /// Bits in a filter entry
   static const unsigned filterEntryBits = 8*sizeof(FilterEntry);
   /// '1' as filter entry
   static const FilterEntry filterEntry1 = 1;

   /// Value bounds
   unsigned min,max;
   /// Bloom filter of values
   FilterEntry filter[filterSize];

   /// Constructor
   DomainDescription() {}
   /// Copy-Constructor
   DomainDescription(const DomainDescription& other);

   /// Assignment
   DomainDescription& operator=(const DomainDescription& other);

   public:
   /// Could this value qualify?
   bool couldQualify(unsigned value) const;
   /// Return the next value >= value that could qualify (or ~0u)
   unsigned nextCandidate(unsigned value) const;
};
//---------------------------------------------------------------------------
/// Description of the potential domain
class PotentialDomainDescription : public DomainDescription
{
   public:
   /// Constructor
   PotentialDomainDescription();

   /// Synchronize with another domain, computing the intersection. Both are modified!
   void sync(PotentialDomainDescription& other);
   /// Restrict to an observed domain
   void restrictTo(const ObservedDomainDescription& other);
};
//---------------------------------------------------------------------------
/// Description of the observed domain
class ObservedDomainDescription : public DomainDescription
{
   friend class PotentialDomainDescription;

   public:
   /// Constructor
   ObservedDomainDescription();

   /// Add an observed value
   void add(unsigned value);
};
//---------------------------------------------------------------------------
#endif
