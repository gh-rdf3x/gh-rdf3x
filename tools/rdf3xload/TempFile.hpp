#ifndef H_tools_rdf3xload_TempFile
#define H_tools_rdf3xload_TempFile
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
#include "infra/Config.hpp"
#include <fstream>
#include <string>
//---------------------------------------------------------------------------
/// A temporary file
class TempFile
{
   private:
   /// The next id
   static unsigned id;

   /// The base file name
   std::string baseName;
   /// The file name
   std::string fileName;
   /// The output
   std::ofstream out;

   /// The buffer size
   static const unsigned bufferSize = 16384;
   /// The write buffer
   char writeBuffer[bufferSize];
   /// The write pointer
   unsigned writePointer;

   /// Construct a new suffix
   static std::string newSuffix();

   TempFile(const TempFile&);
   void operator=(const TempFile&);

   public:
   /// Constructor
   TempFile(const std::string& baseName);
   /// Destructor
   ~TempFile();

   /// Swap two file references
   void swap(TempFile& other);

   /// Get the base file name
   const std::string& getBaseFile() const { return baseName; }
   /// Get the file name
   const std::string& getFile() const { return fileName; }

   /// Flush the file
   void flush();
   /// Close the file
   void close();
   /// Discard the file
   void discard();

   /// Write a string
   void writeString(unsigned len,const char* str);
   /// Write a id
   void writeId(uint64_t id);
   /// Raw write
   void write(unsigned len,const char* data);

   /// Skip an id
   static const char* skipId(const char* reader);
   /// Skip a string
   static const char* skipString(const char* reader);
   /// Read an id
   static const char* readId(const char* reader,uint64_t& id);
   /// Read a string
   static const char* readString(const char* reader,unsigned& len,const char*& str);
};
//---------------------------------------------------------------------------
#endif
