#include "infra/osdep/GrowableMappedFile.hpp"
#include <gtest/gtest.h>
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
namespace {
//---------------------------------------------------------------------------
class GrowableMappedFileTest : public testing::Test {
   protected:
   /// Destructor
   ~GrowableMappedFileTest();
};
//---------------------------------------------------------------------------
static const char tempFileName[]="growablemappedfiletest.tmp";
//---------------------------------------------------------------------------
GrowableMappedFileTest::~GrowableMappedFileTest()
   // Destructor
{
   remove(tempFileName);
}
//---------------------------------------------------------------------------
TEST_F(GrowableMappedFileTest,BasicFunctionality)
   // Test the basic behavior of GrowableMappedFile
{
   // Remove the file if it exists
   remove(tempFileName);

   // Test open/create
   char* begin,*end;
   GrowableMappedFile m;
   EXPECT_FALSE(m.open(tempFileName,begin,end,false));
   EXPECT_FALSE(m.open(tempFileName,begin,end,true));
   m.close();
   ASSERT_TRUE(m.create(tempFileName));

   // Grow
   EXPECT_TRUE(m.growPhysically(10*16*1024));
   ASSERT_TRUE(m.growMapping(5*16*1024,begin,end));
   for (unsigned index=0;index<5;index++)
      begin[index*(16*1024)]=index+1;
   for (unsigned index=5;index<10;index++) {
      char buffer[16*1024]={0};
      buffer[0]=index+1;
      EXPECT_TRUE(m.write(index*16*1024,buffer,sizeof(buffer)));
   }
   EXPECT_TRUE(m.flush());
   m.close();

   // And re-open
   ASSERT_TRUE(m.open(tempFileName,begin,end,true));
   EXPECT_EQ(end-begin,10*16*1024);
   for (unsigned index=0;index<10;index++)
      EXPECT_EQ(static_cast<char>(index+1),begin[index*(16*1024)]);
   m.close();
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
