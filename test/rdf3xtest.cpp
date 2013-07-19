#include <gtest/gtest.h>
#include <iostream>
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
using namespace std;
//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
   // Show help?
   if ((argc==2)&&(string(argv[1])=="--help")) {
      cout << "usage: " << argv[0] << " [option(s)]" << endl
           << "Recognized options:" << endl
           << "--gtest_list_tests       list all tests without running" << endl
           << "--gtest_filter=WildExpr  run only selected tests" << endl
           << "--gtest_print_time       show timing" << endl
           << "--gtest_repeat=X         repeat tests X times" << endl
           << "--gtest_break_on_failure break into the debugger on errors" << endl;
      return 1;
   }

   // Pass the options
   testing::InitGoogleTest(&argc,argv);

   // And run the tests
   return RUN_ALL_TESTS();
}
//---------------------------------------------------------------------------

