#ifndef H_infra_osdep_Timestamp
#define H_infra_osdep_Timestamp
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
class AvgTime;
//---------------------------------------------------------------------------
/// A high resolution timestamp
class Timestamp
{
   private:
   /// The data
   char data[64];

   friend class AvgTime;

   /// Get the raw storage space
   void* ptr() { return data; }
   /// Get the raw storage space
   const void* ptr() const { return data; }

   public:
   /// Constructor
   Timestamp();

   /// Hash
   unsigned long long getHash() const { return *static_cast<const unsigned long long*>(ptr()); }

   /// Difference in ms
   unsigned operator-(const Timestamp& other) const;
};
//---------------------------------------------------------------------------
/// Aggregate
class AvgTime {
   private:
   /// The data
   char data[64];
   /// Count
   unsigned count;

   /// Get the raw storage space
   void* ptr() { return data; }
   /// Get the raw storage space
   const void* ptr() const { return data; }

   public:
   /// Constructor
   AvgTime();

   /// Add an interval
   void add(const Timestamp& start,const Timestamp& end);
   /// The avg time in ms
   double avg() const;
};
//---------------------------------------------------------------------------
#endif
