#ifndef H_rts_segment_FactsSegment
#define H_rts_segment_FactsSegment
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
#include "rts/segment/Segment.hpp"
#include "rts/buffer/BufferReference.hpp"
//---------------------------------------------------------------------------
class DatabaseBuilder;
//---------------------------------------------------------------------------
/// A compressed facts table stored in a clustered B-Tree
class FactsSegment : public Segment
{
   public:
   /// The segment id
   static const Segment::Type ID = Segment::Type_Facts;
   /// Possible actions
   enum Action {
      Action_UpdateInnerPage, Action_UpdateInner,Action_InsertInner,Action_UpdateLeaf
   };
   /// A source for updates
   class Source {
      public:
      /// Destructor
      virtual ~Source();

      /// Get the next triples
      virtual bool next(unsigned& value1,unsigned& value2,unsigned& value3,unsigned& created,unsigned& deleted) = 0;
      /// Mark the last triple as duplicate
      virtual void markAsDuplicate() = 0;
   };
   /// A triple
   struct Triple {
      unsigned value1,value2,value3;
   };

   /// The index
   class IndexImplementation;
   /// The index
   class Index;

   private:
   /// The start of the raw facts table
   unsigned tableStart;
   /// The root of the index b-tree
   unsigned indexRoot;
   /// Statistics
   unsigned pages,groups1,groups2,cardinality;

   /// Refresh segment info stored in the partition
   void refreshInfo();

   /// Load the triples into the database
   void loadFullFacts(Source& reader);
   /// Load count statistics
   void loadCounts(unsigned groups1,unsigned groups2,unsigned cardinality);

   FactsSegment(const FactsSegment&);
   void operator=(const FactsSegment&);

   friend class DatabaseBuilder;

   public:
   /// Constructor
   explicit FactsSegment(DatabasePartition& partition);

   /// Get the type
   Type getType() const;

   /// Get the number of pages in the segment
   unsigned getPages() const { return pages; }
   /// Get the number of level 1 groups
   unsigned getLevel1Groups() const { return groups1; }
   /// Get the number of level 2 groups
   unsigned getLevel2Groups() const { return groups2; }
   /// Get the total cardinality
   unsigned getCardinality() const { return cardinality; }

   /// Update the segment
   void update(Source& source);

   /// A scan over the facts segment
   class Scan {
      public:
      /// Hints for skipping through the scan
      class Hint {
         public:
         /// Constructor
         Hint();
         /// Destructor
         virtual ~Hint();

         /// The hint
         virtual void next(unsigned& value1,unsigned& value2,unsigned& value3) = 0;
      };

      private:
      /// The maximum number of entries per page
      static const unsigned maxCount = BufferReference::pageSize;

      /// The current page
      BufferReference current;
      /// The segment
      FactsSegment* seg;
      /// The position on the current page
      const Triple* pos,*posLimit;
      /// The decompressed triples
      Triple triples[maxCount];
      /// The scan hint
      Hint* hint;
      /// The point in (version-)time where the scan should take place
      unsigned time;

      /// Perform a binary search
      bool find(unsigned value1,unsigned value2,unsigned value3);
      /// Read the next page
      bool readNextPage();

      Scan(const Scan&);
      void operator=(const Scan&);

      public:
      /// Constructor
      explicit Scan(Hint* hint=0,unsigned time=(~0u)-1);
      /// Destructor
      ~Scan();

      /// Start a new scan over the whole segment and reads the first entry
      bool first(FactsSegment& segment);
      /// Start a new scan starting from the first entry >= the start condition and reads the first entry
      bool first(FactsSegment& segment,unsigned start1,unsigned start2,unsigned start3);

      /// Read the next entry
      bool next() { if ((++pos)>=posLimit) return readNextPage(); else return true; }
      /// Get the first value
      unsigned getValue1() const { return (*pos).value1; }
      /// Get the second value
      unsigned getValue2() const { return (*pos).value2; }
      /// Get the third value
      unsigned getValue3() const { return (*pos).value3; }

      /// Close the scan
      void close();
   };
   friend class Scan;
};
//---------------------------------------------------------------------------
#endif
