#ifndef H_rts_database_Database
#define H_rts_database_Database
//---------------------------------------------------------------------------
#include "infra/Config.hpp"
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
class BufferManager;
class FactsSegment;
class AggregatedFactsSegment;
class FullyAggregatedFactsSegment;
class FilePartition;
class DatabasePartition;
class DictionarySegment;
class ExactStatisticsSegment;
//---------------------------------------------------------------------------
/// Access to the RDF database
class Database
{
   public:
   /// Supported data orders
   enum DataOrder {
      Order_Subject_Predicate_Object=0,Order_Subject_Object_Predicate,Order_Object_Predicate_Subject,
      Order_Object_Subject_Predicate,Order_Predicate_Subject_Object,Order_Predicate_Object_Subject
   };

   private:
   /// The underlying file
   FilePartition* file;
   /// The database buffer
   BufferManager* bufferManager;
   /// The partition
   DatabasePartition* partition;
   /// SN of the root page
   uint64_t rootSN;
   /// LSN offset of the current log
   uint64_t startLSN;

   Database(const Database&);
   void operator=(const Database&);

   public:
   /// Constructor
   Database();
   /// Destructor
   ~Database();

   /// Create a new database
   bool create(const char* fileName);
   /// Open a database
   bool open(const char* fileName,bool readOnly=false);
   /// Close the current database
   void close();

   /// Get a facts table
   FactsSegment& getFacts(DataOrder order);
   /// Get an aggregated facts table
   AggregatedFactsSegment& getAggregatedFacts(DataOrder order);
   /// Get fully aggregated fcats
   FullyAggregatedFactsSegment& getFullyAggregatedFacts(DataOrder order);
   /// Get the exact statistics
   ExactStatisticsSegment& getExactStatistics();
   /// Get the dictionary
   DictionarySegment& getDictionary();

   /// Get the first partition
   DatabasePartition& getFirstPartition() { return *partition; }
};
//---------------------------------------------------------------------------
#endif
