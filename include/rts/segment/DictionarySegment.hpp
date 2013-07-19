#ifndef H_rts_segment_DictionarySegment
#define H_rts_segment_DictionarySegment
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
#include "infra/util/Type.hpp"
#include "rts/segment/Segment.hpp"
#include <string>
#include <vector>
//---------------------------------------------------------------------------
class DatabaseBuilder;
//---------------------------------------------------------------------------
/// A dictionary mapping strings to ids and backwards
class DictionarySegment : public Segment
{
   public:
   /// The segment id
   static const Segment::Type ID = Segment::Type_Dictionary;
   /// Possible actions
   enum Action { Action_UpdateMapping };

   /// A literal
   struct Literal {
      /// The string
      std::string str;
      /// The type
      ::Type::ID type;
      /// The sub-type (if any, otherwise 0)
      unsigned subType;

      /// Comparison
      bool operator==(const Literal& l) const { return (type==l.type)&&(subType==l.subType)&&(str==l.str); }
      /// Comparison
      bool operator<(const Literal& l) const { return (type<l.type)||((type==l.type)&&((subType<l.subType)||((subType==l.subType)&&(str<l.str)))); }
   };

   /// A source for strings
   class StringSource
   {
      public:
      /// Destructor
      virtual ~StringSource();

      /// Get a new string
      virtual bool next(unsigned& len,const char*& data,::Type::ID& type,unsigned& subType) = 0;
      /// Remember a string position and hash
      virtual void rememberInfo(unsigned page,unsigned ofs,unsigned hash) = 0;
   };
   /// A source for (id) -> hash,ofsLen updates
   class IdSource {
      public:
      /// Destructor
      virtual ~IdSource();

      /// Get the next entry
      virtual bool next(unsigned& page,unsigned& ofsLen) = 0;
   };
   /// A source for hash->page updates
   class HashSource {
      public:
      /// Destructor
      virtual ~HashSource();

      /// Get the next entry
      virtual bool next(unsigned& hash,unsigned& page) = 0;
   };

   class HashIndexImplementation;
   class HashIndex;

   private:
   /// The start of the raw string table
   unsigned tableStart;
   /// The next id after the existing ones
   unsigned nextId;
   /// The mapping table(s) (id->page)
   std::vector<std::pair<unsigned,unsigned> > mappings;
   /// The root of the index b-tree
   unsigned indexRoot;

   /// Refresh segment info stored in the partition
   void refreshInfo();
   /// Refresh the mapping table if needed
   void refreshMapping();
   /// Lookup an id for a given string on a certain page in the raw string table
   bool lookupOnPage(unsigned pageNo,const std::string& text,::Type::ID type,unsigned subType,unsigned hash,unsigned& id);

   /// Load the raw strings (must be in id order)
   void loadStrings(StringSource& source);
   /// Load the string mappings (must be in id order)
   void loadStringMappings(IdSource& source);
   /// Write the string index (must be in hash order)
   void loadStringHashes(HashSource& source);

   friend class DatabaseBuilder;

   public:
   /// Constructor
   DictionarySegment(DatabasePartition& partition);

   /// Get the type
   Segment::Type getType() const;

   /// Lookup an id for a given string
   bool lookup(const std::string& text,::Type::ID type,unsigned subType,unsigned& id);
   /// Lookup a string for a given id
   bool lookupById(unsigned id,const char*& start,const char*& stop,::Type::ID& type,unsigned& subType);

   /// Get the next id
   unsigned getNextId() const { return nextId; }

   /// Load new literals into the dictionary
   void appendLiterals(const std::vector<Literal>& strings);
};
//---------------------------------------------------------------------------
#endif
