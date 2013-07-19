#ifndef H_rts_runtime_PredicateLockManager
#define H_rts_runtime_PredicateLockManager
//---------------------------------------------------------------------------
#include "infra/osdep/Mutex.hpp"
#include <vector>
#include <set>
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
/// Lock manager based upon predicate locking
class PredicateLockManager
{
   public:
   /// A locking region
   struct Box {
      /// Bounds
      unsigned subjectMin,subjectMax,predicateMin,predicateMax,objectMin,objectMax;

      /// Constructor
      Box(unsigned subjectMin,unsigned subjectMax,unsigned predicateMin,unsigned predicateMax,unsigned objectMin,unsigned objectMan);

      /// Intersects?
      bool intersects(const Box& box) const;
   };

   private:
   /// A lock
   struct Lock {
      /// The region
      Box box;
      /// The locking transaction
      unsigned transaction;
      /// Exclusive?
      bool exclusive;

      /// Constructor
      Lock(const Box& box,unsigned transaction,bool exclusive) : box(box),transaction(transaction),exclusive(exclusive) {}
   };
   /// All active locks. XXX use a k-d tree
   std::vector<Lock> locks;
   /// All committed transactions
   std::set<unsigned> committed;
   /// The active transaction
   std::set<unsigned> active;
   /// Mutex
   Mutex mutex;

   public:
   /// Constructor
   PredicateLockManager();
   /// Destructor
   ~PredicateLockManager();

   /// Lock a region
   bool lock(unsigned transaction,const Box& box,bool exclusive);
   /// Lock multiple regions at once
   bool lockMultiple(unsigned transaction,const std::vector<std::pair<Box,bool> > regions);
   /// A transaction finished
   void finished(unsigned transaction);
};
//---------------------------------------------------------------------------
#endif
