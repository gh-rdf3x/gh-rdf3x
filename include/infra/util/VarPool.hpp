#ifndef H_infra_util_VarPool
#define H_infra_util_VarPool
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
#include "infra/util/Pool.hpp"
//---------------------------------------------------------------------------
/** A pool of entries with variable sizes. This use useful for
  * allocating data types of the form
  *    struct Entry { unsigned header; unsigned data[1]; };
  * where all entries have the same length.
  * No constructors or destructors are called!
  */
template <class T> class VarPool
{
   private:
   /// Dummy container for size compuations
   struct AlignmentTest1 { char c; T a; };
   /// Dummy container for size compuations
   struct AlignmentTest2 { T a; };
   /// The alignment
   static const unsigned alignment = sizeof(AlignmentTest1)-sizeof(AlignmentTest2);

   public:
   /// Base size of an entry. Takes alignment into account.
   static const unsigned basicSize = static_cast<unsigned>((sizeof(AlignmentTest2)+alignment-1)/alignment)*alignment;

   private:
   /// Header of a memory chunk
   struct ChunkHeader {
      /// The next chunk
      ChunkHeader* next;
      union {
         /// Sizeof of an chunk
         unsigned chunkSize;
         /// Padding
         void* padding;
      };

      /// Pointer to the contained data
      T* getData() { return reinterpret_cast<T*>(this+1); }
   };

   /// List of all free entries
   T* freeList;
   /// The chunk
   ChunkHeader* chunks;
   /// Size of an entry
   unsigned size;
   /// Size of the next chunk
   unsigned chunkSize;

   /// Allocate a new chunk
   void allocChunk();

   VarPool(const VarPool&);
   void operator=(const VarPool&);

   public:
   /// Constructor
   explicit VarPool(unsigned extraSize,unsigned cs=32);
   /// Destructor
   ~VarPool() { freeAll(); }

   /// Allocate an uninitialized entry
   inline T* alloc();
   /// Release an entry
   inline void free(T* data);
   /// Release all entries
   void freeAll();

   /// Apply a function to all contained objects
   template <class F> void enumAll(F& functor);
};
//---------------------------------------------------------------------------
template <class T> VarPool<T>::VarPool(unsigned extraSize,unsigned cs)
   : freeList(0),chunks(0),size(basicSize+extraSize),chunkSize(cs)
   // Constructor
{
   if (size%sizeof(T*))
      size+=sizeof(T*)-(size%sizeof(T*));
   if (size<sizeof(T*))
      size=sizeof(T*);
   if (chunkSize&7) chunkSize=(chunkSize|7)+1;
}
//---------------------------------------------------------------------------
template <class T> void VarPool<T>::allocChunk()
   // Allocate a new chunk
{
   // Allocate the chunk
   char* newData=new char[chunkSize*size+sizeof(ChunkHeader)];
   ChunkHeader* const newChunk=reinterpret_cast<ChunkHeader*>(newData);
   newChunk->next=chunks; newChunk->chunkSize=chunkSize; chunks=newChunk;
   newData+=sizeof(ChunkHeader);

   // Initialize all entries
   for (char* iter=newData,*limit=newData+chunkSize*size;iter<limit;iter+=size)
      *reinterpret_cast<T**>(iter)=reinterpret_cast<T*>(iter+size);
   *reinterpret_cast<T**>(newData+(chunkSize-1)*size)=0;
   freeList=reinterpret_cast<T*>(newData);

   // Increase the chunk size slightly
   chunkSize+=chunkSize/4;
   if (chunkSize&7) chunkSize=(chunkSize|7)+1;
}
//---------------------------------------------------------------------------
template <class T> template <class F> void VarPool<T>::enumAll(F& functor)
   // Apply a function to all contained objects
{
   // Sort all chunks
   chunks=static_cast<ChunkHeader*>(PoolHelper::sort(chunks));

   // Sort the free list
   freeList=static_cast<T*>(PoolHelper::sort(freeList));

   // Now traverse the free list and skip all freed objects
   T* nextFree=freeList;
   for (ChunkHeader* iter=chunks;iter;iter=iter->next) {
      T* const limit=reinterpret_cast<T*>(reinterpret_cast<char*>(iter+1)+iter->chunkSize*size);
      for (T* iter2=iter->getData();iter2<limit;iter2=reinterpret_cast<T*>(reinterpret_cast<char*>(iter2)+size)) {
         while (nextFree&&(nextFree<iter2)) nextFree=*reinterpret_cast<T**>(nextFree);
         if (nextFree!=iter2)
            functor(iter2);
      }
   }
}
//---------------------------------------------------------------------------
template <class T> T* VarPool<T>::alloc()
   // Allocate an uninitialized entry
{
   if (!freeList)
      allocChunk();

   T* result=freeList;
   freeList=*reinterpret_cast<T**>(result);
   return result;
}
//---------------------------------------------------------------------------
template <class T> void VarPool<T>::free(T* data)
   // Release an entry
{
   *reinterpret_cast<T**>(data)=freeList;
   freeList=data;
}
//---------------------------------------------------------------------------
template <class T> void VarPool<T>::freeAll()
   // Release alle entries
{
   // Delete all chunks
   while (chunks) {
      ChunkHeader* next=chunks->next;
      delete[] reinterpret_cast<char*>(chunks);
      chunks=next;
   }

   // Clear the free list
   freeList=0;
}
//---------------------------------------------------------------------------
#endif
