#include "rts/segment/SpaceInventorySegment.hpp"
#include "rts/database/Database.hpp"
#include "rts/database/DatabasePartition.hpp"
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
/// Helper
class SpaceInventorySegment::TestInterface
{
   private:
   /// The real segment
   SpaceInventorySegment* seg;

   public:
   /// Constructor
   TestInterface(SpaceInventorySegment* seg) : seg(seg) {}

   /// Insert an interval. Low level primitive.
   inline void insertInterval(unsigned segmentId,unsigned from,unsigned to) { seg->insertInterval(segmentId,from,to); }
};
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
class TestSpaceInventorySegment : public testing::Test {
   protected:
   /// Destructor
   ~TestSpaceInventorySegment();
};
//---------------------------------------------------------------------------
static const char tempFileName[]="spaceinventorytest.tmp";
//---------------------------------------------------------------------------
TestSpaceInventorySegment::~TestSpaceInventorySegment()
   // Destructor
{
   remove(tempFileName);
}
//---------------------------------------------------------------------------
TEST_F(TestSpaceInventorySegment,BasicFunctionality)
   // Test the basic behavior of FilePartition
{
   // Remove the file if it exists
   remove(tempFileName);

   // Create a new database
   Database db;
   ASSERT_TRUE(db.create(tempFileName));
   SpaceInventorySegment::TestInterface seg(db.getFirstPartition().getSpaceInventory());

   // Insert some intervals
   for (unsigned index=0;index<10000;index++) {
      seg.insertInterval(index+10,10*index,10*index+9);
   }

   // Cleanup
   db.close();
   remove(tempFileName);
}
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
