#ifndef H_rts_runtime_DifferentialIndex
#define H_rts_runtime_DifferentialIndex
//---------------------------------------------------------------------------
#include "infra/osdep/Latch.hpp"
#include "rts/database/Database.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include <map>
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
class Database;
class Operator;
class Register;
//---------------------------------------------------------------------------
/// Index for all transient updates
class DifferentialIndex
{
   public:
   /// A regular triple
   struct Triple {
      /// Entries
      unsigned subject,predicate,object;
   };
   /// A versioned triple
   struct VersionedTriple {
      /// Entries
      unsigned value1,value2,value3;
      /// Versions
      unsigned created,deleted;

      /// Constructor
      VersionedTriple() : value1(0),value2(0),value3(0),created(0),deleted(0) {}
      /// Constructor
      VersionedTriple(unsigned value1,unsigned value2,unsigned value3,unsigned created,unsigned deleted) : value1(value1),value2(value2),value3(value3),created(created),deleted(deleted) {}

      /// Compare
      bool operator<(const VersionedTriple& v) const { return (value1<v.value1)||((value1==v.value1)&&((value2<v.value2)||((value2==v.value2)&&((value3<v.value3)||((value3==v.value3)&&(created<v.created)))))); }
   };
   /// A new literal
   struct Literal {
      /// The value
      std::string value;
      /// The type
      Type::ID type;
      /// The sub-type (if any)
      std::string subType;

      /// Comparison
      bool operator==(const Literal& l) const { return (type==l.type)&&(value==l.value)&&(subType==l.subType); }
      /// Comparison
      bool operator<(const Literal& l) const { return (type<l.type)||((type==l.type)&&((value<l.value)||((value==l.value)&&(subType<l.subType)))); }
   };

   private:
   /// The underlying database
   Database& db;
   /// The dictionary within the database
   DictionarySegment& dict;
   /// Triples
   std::set<VersionedTriple> triples[6];
   /// Dictionary
   std::map<DictionarySegment::Literal,unsigned> string2id;
   /// Dictionary
   std::vector<DictionarySegment::Literal> id2string;
   /// The latches
   Latch latches[7];

   public:
   /// Constructor
   explicit DifferentialIndex(Database& db);
   /// Destructor
   ~DifferentialIndex();

   /// Get the underlying database
   Database& getDatabase() { return db; }

   /// Load new triples
   void load(const std::vector<Triple>& triples, bool todelete);
   /// Map literals to ids
   void mapLiterals(const std::vector<Literal>& literals,std::vector<unsigned>& ids);

   /// Clear the index, discarding all entries
   void clear();
   /// Synchronize with the underlying database
   void sync();

   /// Create a suitable scan operator scanning both the DB and the differential index
   Operator* createScan(Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality);
   /// Create a suitable scan operator scanning both the DB and the differential index
   Operator* createAggregatedScan(Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality);
   /// Create a suitable scan operator scanning both the DB and the differential index
   Operator* createFullyAggregatedScan(Database::DataOrder order,Register* subjectRegister,bool subjectBound,Register* predicateRegister,bool predicateBound,Register* objectRegister,bool objectBound,double expectedOutputCardinality);

   /// Get the next id number
   unsigned getNextId();
   /// Lookup an id for a given string
   bool lookup(const std::string& text,::Type::ID type,unsigned subType,unsigned& id);
   /// Lookup a string for a given id
   bool lookupById(unsigned id,const char*& start,const char*& stop,::Type::ID& type,unsigned& subType);
};
//---------------------------------------------------------------------------
#endif
