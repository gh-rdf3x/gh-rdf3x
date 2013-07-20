#include "TempFile.hpp"
#include <sstream>
#include <cassert>
#include <cstring>
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
/// The next id
unsigned TempFile::id = 0;
//---------------------------------------------------------------------------
string TempFile::newSuffix()
   // Construct a new suffix
{
   stringstream buffer;
   buffer << '.' << (id++);
   return buffer.str();
}
//---------------------------------------------------------------------------
TempFile::TempFile(const string& baseName)
   : baseName(baseName),fileName(baseName+newSuffix()),out(fileName.c_str(),ios::out|ios::binary|ios::trunc),writePointer(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
TempFile::~TempFile()
   // Destructor
{
   discard();
}
//---------------------------------------------------------------------------
void TempFile::swap(TempFile& other)
   // Swap two file references
{
   // We only support filed files
   assert(!out.is_open());
   assert(!other.out.is_open());

   // Swap the names
   baseName.swap(other.baseName);
   fileName.swap(other.fileName);
}
//---------------------------------------------------------------------------
void TempFile::flush()
   // Flush the file
{
   if (writePointer) {
      out.write(writeBuffer,writePointer);
      writePointer=0;
   }
   out.flush();
}
//---------------------------------------------------------------------------
void TempFile::close()
   // Close the file
{
   flush();
   out.close();
}
//---------------------------------------------------------------------------
void TempFile::discard()
   // Discard the file
{
   close();
   remove(fileName.c_str());
}
//---------------------------------------------------------------------------
void TempFile::writeString(unsigned len,const char* str)
   // Write a string
{
   writeId(len);
   write(len,str);
}
//---------------------------------------------------------------------------
void TempFile::writeId(uint64_t id)
   // Write a id
{
   while (id>=128) {
      unsigned char c=static_cast<unsigned char>(id|128);
      if (writePointer==bufferSize) {
         out.write(writeBuffer,writePointer);
         writePointer=0;
      }
      writeBuffer[writePointer++]=c;
      id>>=7;
   }
   if (writePointer==bufferSize) {
      out.write(writeBuffer,writePointer);
      writePointer=0;
   }
   writeBuffer[writePointer++]=static_cast<unsigned char>(id);
}
//---------------------------------------------------------------------------
void TempFile::write(unsigned len,const char* data)
   // Raw write
{
   // Fill the buffer
   if (writePointer+len>bufferSize) {
      unsigned remaining=bufferSize-writePointer;
      memcpy(writeBuffer+writePointer,data,remaining);
      out.write(writeBuffer,bufferSize);
      writePointer=0;
      len-=remaining;
      data+=remaining;
   }
   // Write big chunks if any
   if (writePointer+len>bufferSize) {
      assert(writePointer==0);
      unsigned chunks=len/bufferSize;
      out.write(data,chunks*bufferSize);
      len-=chunks*bufferSize;
      data+=chunks*bufferSize;
   }
   // And fill the rest
   memcpy(writeBuffer+writePointer,data,len);
   writePointer+=len;
}
//---------------------------------------------------------------------------
const char* TempFile::skipId(const char* reader)
   // Skip an id
{
   while ((*reinterpret_cast<const unsigned char*>(reader))&128)
      ++reader;
   return reader+1;
}
//---------------------------------------------------------------------------
const char* TempFile::skipString(const char* reader)
   // Skip a string
{
   uint64_t rawLen;
   reader=readId(reader,rawLen);
   unsigned len=static_cast<unsigned>(rawLen);
   return reader+len;
}
//---------------------------------------------------------------------------
const char* TempFile::readId(const char* reader,uint64_t& result)
   // Read an id
{
   unsigned shift=0;
   uint64_t id=0;
   while (true) {
      unsigned char c=*reinterpret_cast<const unsigned char*>(reader++);
      if (c&128) {
         id|=static_cast<uint64_t>(c&0x7F)<<shift;
         shift+=7;
      } else {
         id|=static_cast<uint64_t>(c)<<shift;
         break;
      }
   }
   result=id;
   return reader;
}
//---------------------------------------------------------------------------
const char* TempFile::readString(const char* reader,unsigned& len,const char*& str)
   // Read a string
{
   uint64_t rawLen;
   reader=readId(reader,rawLen);
   len=static_cast<unsigned>(rawLen);
   str=reader;
   return reader+len;
}
//---------------------------------------------------------------------------
