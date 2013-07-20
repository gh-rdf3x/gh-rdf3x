#include "rts/partition/FilePartition.hpp"
#include <gtest/gtest.h>
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
namespace {
//---------------------------------------------------------------------------
class FilePartitionTest : public testing::Test {
   protected:
   /// Destructor
   ~FilePartitionTest();
};
//---------------------------------------------------------------------------
static const char tempFileName[]="filepartitiontest.tmp";
//---------------------------------------------------------------------------
FilePartitionTest::~FilePartitionTest()
   // Destructor
{
   remove(tempFileName);
}
//---------------------------------------------------------------------------
TEST_F(FilePartitionTest,BasicFunctionality)
   // Test the basic behavior of FilePartition
{
   // Remove the file if it exists
   remove(tempFileName);

   // Test open/create
   FilePartition p;
   EXPECT_FALSE(p.open(tempFileName,false));
   EXPECT_FALSE(p.open(tempFileName,true));
   p.close();
   ASSERT_TRUE(p.create(tempFileName));
   p.close();
   EXPECT_TRUE(p.open(tempFileName,false));

   // Grow the partition
   unsigned start,len;
   EXPECT_EQ(p.getSize(),0u);
   ASSERT_TRUE(p.grow(1,start,len));
   EXPECT_EQ(start,0u);
   EXPECT_EQ(len,1u);
   EXPECT_EQ(p.getSize(),1u);

   // Write some data
   {
      Partition::PageInfo info;
      void* writer=p.writePage(0,info);
      EXPECT_NE(writer,static_cast<void*>(0));
      static_cast<char*>(writer)[0]=42;
      EXPECT_TRUE(p.flushWrittenPage(info));
      p.finishWrittenPage(info);
   }
   EXPECT_TRUE(p.flush());
   p.close();

   // And read it brack
   ASSERT_TRUE(p.open(tempFileName,true));
   EXPECT_EQ(p.getSize(),1u);
   {
      Partition::PageInfo info;
      const void* reader=p.readPage(0,info);
      EXPECT_NE(reader,static_cast<const void*>(0));
      EXPECT_EQ(static_cast<const char*>(reader)[0],42);
      p.finishReadPage(info);
   }

   p.close();
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
