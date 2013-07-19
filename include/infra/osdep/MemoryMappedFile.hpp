#ifndef H_infra_osdep_MemoryMappedFile
#define H_infra_osdep_MemoryMappedFile
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//----------------------------------------------------------------------------
/// Maps a file read-only into memory
class MemoryMappedFile
{
   private:
   /// os dependent data
   struct Data;

   /// os dependen tdata
   Data* data;
   /// Begin of the file
   const char* begin;
   /// End of the file
   const char* end;

   public:
   /// Constructor
   MemoryMappedFile();
   /// Destructor
   ~MemoryMappedFile();

   /// Open
   bool open(const char* name);
   /// Close
   void close();

   /// Get the begin
   const char* getBegin() const { return begin; }
   /// Get the end
   const char* getEnd() const { return end; }

   /// Ask the operating system to prefetch a part of the file
   void prefetch(const char* start,const char* end);
};
//----------------------------------------------------------------------------
#endif
