#ifndef H_rts_database_DatabaseBuilder
#define H_rts_database_DatabaseBuilder
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
#include "rts/database/Database.hpp"
#include "rts/buffer/BufferReference.hpp"
#include "infra/util/Type.hpp"
//---------------------------------------------------------------------------
class Segment;
//---------------------------------------------------------------------------
/// Builds a new RDF database from scratch
class DatabaseBuilder
{
   public:
   /// A facts reader
   class FactsReader
   {
      public:
      /// Constructor
      FactsReader();
      /// Destructor
      virtual ~FactsReader();

      /// Load a new fact
      virtual bool next(unsigned& v1,unsigned& v2,unsigned& v3) = 0;
      /// Reset the reader
      virtual void reset() = 0;
   };
   /// A reader with putback capabilities
   class PutbackReader {
      private:
      /// The real reader
      FactsReader& reader;
      /// The putback triple
      unsigned subject,predicate,object;
      /// Do we have a putback?
      bool hasPutback;

      public:
      /// Constructor
      PutbackReader(FactsReader& reader) : reader(reader),hasPutback(false) {}

      /// Get the next triple
      bool next(unsigned& subject,unsigned& predicate,unsigned& object);
      /// Put a triple back
      void putBack(unsigned subject,unsigned predicate,unsigned object);
   };
   /// A strings reader
   class StringsReader
   {
      public:
      /// Constructor
      StringsReader();
      /// Destructor
      virtual ~StringsReader();

      /// Load a new string
      virtual bool next(unsigned& len,const char*& data,Type::ID& type,unsigned& subType) = 0;
      /// Remember a string position and hash
      virtual void rememberInfo(unsigned page,unsigned ofs,unsigned hash) = 0;
   };
   /// A string info reader (reads previously remembered data)
   class StringInfoReader
   {
      public:
      /// Constructor
      StringInfoReader();
      /// Destructor
      virtual ~StringInfoReader();

      /// Load a new data item
      virtual bool next(unsigned& v1,unsigned& v2) = 0;
   };
   /// A RDF triple
   struct Triple {
      /// The values as IDs
      unsigned subject,predicate,object;
   };
   /// Helper class to automatically chain pages
   class PageChainer {
      private:
      /// Buffer references
      BufferReferenceModified lastPage,currentPage;
      /// The link offset
      unsigned ofs;
      /// The first page
      unsigned firstPage;
      /// Number of written pages
      unsigned pages;

      public:
      /// Constructor
      explicit PageChainer(unsigned ofs);
      /// Destructor
      ~PageChainer();

      /// Store a page
      void store(Segment* seg,const void* pageData);
      /// Allocate a page. Use for manual stores
      void* nextPage(Segment* seg);

      /// Get the current page number
      unsigned getPageNo() const { return currentPage.getPageNo(); }
      /// Get the first page number
      unsigned getFirstPageNo() const { return firstPage; }
      /// Get the page count
      unsigned getPages() const { return pages; }
      /// Finish chaining
      void finish();
   };

   private:
   /// The database
   Database out;
   /// The file name
   const char* dbFile;

   /// Load the triples aggregated into the database
   void loadAggregatedFacts(unsigned order,FactsReader& reader);
   /// Load the triples fully aggregated into the database
   void loadFullyAggregatedFacts(unsigned order,FactsReader& reader);

   DatabaseBuilder(const DatabaseBuilder&);
   void operator=(const DatabaseBuilder&);

   public:
   /// Constructor
   DatabaseBuilder(const char* fileName);
   /// Destructor
   ~DatabaseBuilder();

   /// Close
   void close() { out.close(); }

   /// Loads the facts in a given order
   void loadFacts(unsigned order,FactsReader& reader);
   /// Load the raw strings (must be in id order, ids 0,1,2,...)
   void loadStrings(StringsReader& reader);
   /// Load the strings mappings (must be in id order, ids 0,1,2,...)
   void loadStringMappings(StringInfoReader& reader);
   /// Load the hash->page mappings (must be in hash order)
   void loadStringHashes(StringInfoReader& reader);

   /// Compute the exact statistics (after loading)
   void computeExactStatistics(const char* tempFile);
};
//---------------------------------------------------------------------------
#endif
