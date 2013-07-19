#ifndef H_rts_segment_ExactStatisticsSegment
#define H_rts_segment_ExactStatisticsSegment
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
//---------------------------------------------------------------------------
class AggregatedFactsSegment;
class Database;
class DatabaseBuilder;
class FullyAggregatedFactsSegment;
class MemoryMappedFile;
//---------------------------------------------------------------------------
/// Exact cardinality and join selectivity statistics
class ExactStatisticsSegment : public Segment
{
   public:
   class Dumper2;
   class Dumper1;

   private:
   /// The position of the statistics
   unsigned c2ps,c2po,c2so,c1s,c1p,c1o;
   /// Join result sizes without constants
   unsigned long long c0ss,c0sp,c0so,c0ps,c0pp,c0po,c0os,c0op,c0oo;
   /// The total size of the database
   unsigned totalCardinality;
   /// Position of the directory
   unsigned directoryPage;

   /// Refresh segment info stored in the partition
   void refreshInfo();
   /// Lookup a segment
   AggregatedFactsSegment& getAggregatedFacts(unsigned order) const;
   /// Lookup a segment
   FullyAggregatedFactsSegment& getFullyAggregatedFacts(unsigned order) const;
   /// Lookup join cardinalities
   bool getJoinInfo(unsigned long long* joinInfo,unsigned subjectConstant,unsigned predicateConstant,unsigned objectConstant) const;
   /// Lookup join cardinalities for two constants
   bool getJoinInfo2(unsigned root,unsigned value1,unsigned value2,unsigned long long& s,unsigned long long& p,unsigned long long& o) const;
   /// Lookup join cardinalities for one constant
   bool getJoinInfo1(unsigned root,unsigned value1,unsigned long long& s1,unsigned long long& p1,unsigned long long& o1,unsigned long long& s2,unsigned long long& p2,unsigned long long& o2) const;

   /// Compute exact statistics (after loading)
   void computeExactStatistics(MemoryMappedFile& countMap);

   friend class DatabaseBuilder;

   ExactStatisticsSegment(const ExactStatisticsSegment&);
   void operator=(const ExactStatisticsSegment&);

   public:
   /// Constructor
   ExactStatisticsSegment(DatabasePartition& partition);

   /// Get the type
   Type getType() const;

   /// Compute the cardinality of a single pattern
   unsigned getCardinality(unsigned subjectConstant,unsigned predicateConstant,unsigned objectConstant) const;
   /// Compute the join selectivity
   double getJoinSelectivity(bool s1c,unsigned s1,bool p1c,unsigned p1,bool o1c,unsigned o1,bool s2c,unsigned s2,bool p2c,unsigned p2,bool o2c,unsigned o2) const;
};
//---------------------------------------------------------------------------
#endif
