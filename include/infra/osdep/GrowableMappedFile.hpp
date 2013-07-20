#ifndef H_infra_osdep_GrowableMappedFile
#define H_infra_osdep_GrowableMappedFile
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//----------------------------------------------------------------------------
/// Maps a file read-write into memory and supports growing the file. Note
/// that mappings can be non-continuous after growth, the file might be
/// mapped into multiple chunks.
class GrowableMappedFile
{
   public:
   /// Typedef for offsets and sizes
   typedef unsigned long ofs_t;

   private:
   /// os dependent data
   struct Data;

   /// os dependen tdata
   Data* data;

   public:
   /// Constructor
   GrowableMappedFile();
   /// Destructor
   ~GrowableMappedFile();

   /// Open
   bool open(const char* name,char*& begin,char*& end,bool readOnly);
   /// Create a new file
   bool create(const char* name);
   /// Close
   void close();
   /// Flush the file
   bool flush();

   /// Grow the underlying file physically
   bool growPhysically(ofs_t increment);
   /// Grow the mapping on the underlying file
   bool growMapping(ofs_t increment,char*& begin,char*& end);

   /// Read from the unmapped part of the file
   bool read(ofs_t ofs,void* data,unsigned len);
   /// Write to the unmapped part of the file
   bool write(ofs_t ofs,const void* data,unsigned len);
};
//----------------------------------------------------------------------------
#endif
