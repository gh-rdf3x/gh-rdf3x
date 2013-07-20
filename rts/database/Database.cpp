#include "rts/database/Database.hpp"
#include "rts/buffer/BufferManager.hpp"
#include "rts/database/DatabasePartition.hpp"
#include "rts/partition/FilePartition.hpp"
#include "rts/segment/AggregatedFactsSegment.hpp"
#include "rts/segment/DictionarySegment.hpp"
#include "rts/segment/ExactStatisticsSegment.hpp"
#include "rts/segment/FactsSegment.hpp"
#include "rts/segment/FullyAggregatedFactsSegment.hpp"
#include <iostream>
#include <cassert>
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
/// Active buffer size. This is only a hint!
static const unsigned bufferSize = 16*1024*1024;
//---------------------------------------------------------------------------
Database::Database()
   : file(0),bufferManager(0),partition(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
Database::~Database()
   // Destructor
{
   close();
}
//---------------------------------------------------------------------------
static void writeUint64(unsigned char* writer,uint64_t value)
   // Write a 64bit integer value
{
   writer[0]=static_cast<unsigned char>(value>>56);
   writer[1]=static_cast<unsigned char>(value>>48);
   writer[2]=static_cast<unsigned char>(value>>40);
   writer[3]=static_cast<unsigned char>(value>>32);
   writer[4]=static_cast<unsigned char>(value>>24);
   writer[5]=static_cast<unsigned char>(value>>16);
   writer[6]=static_cast<unsigned char>(value>>8);
   writer[7]=static_cast<unsigned char>(value>>0);
}
//---------------------------------------------------------------------------
bool Database::create(const char* fileName)
   // Create a new database
{
   close();

   // Try to create the partition
   file=new FilePartition();
   if (!file->create(fileName))
      return false;
   unsigned start,len;
   if (!file->grow(4,start,len))
      return false;
   assert((start==0)&&(len==4));
   bufferManager=new BufferManager(bufferSize);
   partition=new DatabasePartition(*bufferManager,*file);

   // Create the inventory segments
   partition->create();

   // Format the root page
   {
      Partition::PageInfo pageInfo;
      unsigned char* page=static_cast<unsigned char*>(file->buildPage(0,pageInfo));

      // Magic
      page[0]='R'; page[1]='D'; page[2]='F'; page[3]=0;
      page[4]=0;   page[5]=0;   page[6]=0;   page[7]=2;

      // Root SN
      rootSN=1;
      writeUint64(page+8,rootSN);
      writeUint64(page+BufferReference::pageSize-8,rootSN);

      // Start LSN
      startLSN=0;
      writeUint64(page+16,startLSN);

      file->flushWrittenPage(pageInfo);
      file->finishWrittenPage(pageInfo);
   }
   file->flush();

   return true;
}
//---------------------------------------------------------------------------
static unsigned readUint32(const unsigned char* data) { return (data[0]<<24)|(data[1]<<16)|(data[2]<<8)|data[3]; }
static uint64_t readUint64(const unsigned char* data) { return (static_cast<uint64_t>(readUint32(data))<<32)|static_cast<uint64_t>(readUint32(data+4)); }
//---------------------------------------------------------------------------
bool Database::open(const char* fileName,bool readOnly)
   // Open a database
{
   close();

   // Try to open the file
   file=new FilePartition();
   if (!file->open(fileName,readOnly))
      return false;
   bufferManager=new BufferManager(bufferSize);

   // Check the root page
   {
      BufferReference root(BufferRequest(*bufferManager,*file,0));
      const unsigned char* page=static_cast<const unsigned char*>(root.getPage());

      // Check the magic
      if ((page[0]!='R')||(page[1]!='D')||(page[2]!='F')||(page[3]!=0)) {
         std::cerr << "invalid magic, not a RDF-3X file" << std::endl;
         return false;
      }
      if ((page[4]!=0)||(page[5]!=0)||(page[6]!=0)||(page[7]!=2)) {
         std::cerr << "unsupported file version, consider dumping and re-importing" << std::endl;
         return false;
      }
      rootSN=readUint64(page+8);
      startLSN=readUint64(page+16);
   }


   // Open the partition
   partition=new DatabasePartition(*bufferManager,*file);
   partition->open();

   return true;
}
//---------------------------------------------------------------------------
void Database::close()
   // Close the current database
{
   delete partition;
   partition=0;
   delete bufferManager;
   bufferManager=0;
   delete file;
   file=0;
}
//---------------------------------------------------------------------------
FactsSegment& Database::getFacts(DataOrder order)
   // Get the facts
{
   return *(partition->lookupSegment<FactsSegment>(DatabasePartition::Tag_SPO+order));
}
//---------------------------------------------------------------------------
AggregatedFactsSegment& Database::getAggregatedFacts(DataOrder order)
   // Get the facts
{
   return *(partition->lookupSegment<AggregatedFactsSegment>(DatabasePartition::Tag_SP+order));
}
//---------------------------------------------------------------------------
FullyAggregatedFactsSegment& Database::getFullyAggregatedFacts(DataOrder order)
   // Get fully aggregated fcats
{
   return *(partition->lookupSegment<FullyAggregatedFactsSegment>(DatabasePartition::Tag_S+(order/2)));
}
//---------------------------------------------------------------------------
ExactStatisticsSegment& Database::getExactStatistics()
   // Get the exact statistics
{
   return *(partition->lookupSegment<ExactStatisticsSegment>(DatabasePartition::Tag_ExactStatistics));
}
//---------------------------------------------------------------------------
DictionarySegment& Database::getDictionary()
   // Get the dictionary
{
   return *(partition->lookupSegment<DictionarySegment>(DatabasePartition::Tag_Dictionary));
}
//---------------------------------------------------------------------------
