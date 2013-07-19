#ifndef H_rts_runtime_BulkOperation
#define H_rts_runtime_BulkOperation
//---------------------------------------------------------------------------
#include "rts/runtime/DifferentialIndex.hpp"
#include "rts/runtime/PredicateLockManager.hpp"
#include <map>
#include <string>
#include <vector>
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
/// A bulk operation/transaction
class BulkOperation
{
   private:
   /// The differential index
   DifferentialIndex& differentialIndex;
   /// The triples
   std::vector<DifferentialIndex::Triple> triples;
   /// The temporary dictionary
   std::map<DifferentialIndex::Literal,unsigned> string2id;
   /// The temporary dictionary
   std::vector<DifferentialIndex::Literal> id2string;
   /// Will we delete entries?
   bool deleteMarker;

   /// Map a string
   unsigned mapString(const DifferentialIndex::Literal& value);

   public:
   /// Constructor
   explicit BulkOperation(DifferentialIndex& differentialIndex);
   /// Destructor
   ~BulkOperation();

   /// Add a triple
   void insert(const std::string& subject,const std::string& predicate,const std::string& object,Type::ID objectType,const std::string& objectSubType);
   /// Build a lock cover
   void buildCover(unsigned maxSize,std::vector<PredicateLockManager::Box>& boxes);
   /// Delete operation
   void markDeleted(){deleteMarker=true;}
   /// Commit
   void commit();
   /// Abort
   void abort();
};
//---------------------------------------------------------------------------
#endif
