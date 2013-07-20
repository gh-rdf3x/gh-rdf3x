#ifndef H_tools_rdf3xload_Sorter
#define H_tools_rdf3xload_Sorter
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
class TempFile;
//---------------------------------------------------------------------------
/// Sort a temporary file
class Sorter {
   public:
   /// Sort a file
   static void sort(TempFile& in,TempFile& out,const char* (*skip)(const char*),int (*compare)(const char*,const char*),bool eliminateDuplicates=false);
};
//---------------------------------------------------------------------------
#endif
