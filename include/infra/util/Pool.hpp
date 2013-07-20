#ifndef H_infra_util_Pool
#define H_infra_util_Pool
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
#include <new>
#include <cstring>
//---------------------------------------------------------------------------
/// Helper class for Pools
class PoolHelper {
   public:
   /// Sort a single-linked list by ist pointer vailes
   static void* sort(void* data);
};
//---------------------------------------------------------------------------
/** Pool base. This class is the base class for all pools, the pool
  * impelementations change its behavior via mix-in.
  * A pool allows an efficient management of objects with a given type.
  * When a pool is deleted, all contained objects are freed automatically.
  * Depending on the implementation (i.e., the deriving class), constructors
  * or destructors are called.
  */
template <class Impl,class T> class PoolBase
{
   public:
   /// Base size of an entry. Takes alignment into account.
   static const unsigned basicSize = (sizeof(T[3])-sizeof(T[2]));
   /// Effective size of an entry
   static const unsigned size = ((sizeof(T[3])-sizeof(T[2]))<sizeof(T*))?sizeof(T*):(sizeof(T[3])-sizeof(T[2]));

   /// Initiate cleanup
   static void initCleanup(PoolBase*) {}

   private:
   /// Header of a memory chunk
   struct ChunkHeader {
      /// The next chunk
      ChunkHeader* next;
      /// Sizeof of an chunk
      unsigned chunkSize;

      /// Pointer to the contained data
      T* getData() { return reinterpret_cast<T*>(reinterpret_cast<char*>(this)-(chunkSize*PoolBase<Impl,T>::size)); }
   };

   /// List of all free entries
   T* freeList;
   /// The chunk
   ChunkHeader* chunks;
   /// Size of the next chunk
   unsigned chunkSize;

   /// Allocate a new chunk
   void allocChunk();

   PoolBase(const PoolBase&);
   void operator=(const PoolBase&);

   protected:
   /// Constructor
   PoolBase(unsigned cs=32) : freeList(0),chunks(0),chunkSize(cs) { if (chunkSize&7) chunkSize=(chunkSize|7)+1; }
   /// Destructor
   ~PoolBase() { freeAll(); }

   /// Allocate an uninitialized entry
   inline T* alloc();
   /// Release an entry
   inline void free(T* data);
   /// Release all entries
   void freeAll();

   public:
   /// Apply a function to all contained objects
   template <class F> void enumAll(F& functor);
};
//---------------------------------------------------------------------------
template <class Impl,class T> void PoolBase<Impl,T>::allocChunk()
   // Allocate a new chunk
{
   // Allocate the chunk
   char* newData=new char[chunkSize*size+sizeof(ChunkHeader)];
   ChunkHeader* const newChunk=reinterpret_cast<ChunkHeader*>(newData+chunkSize*size);
   newChunk->next=chunks; newChunk->chunkSize=chunkSize; chunks=newChunk;

   // Initialize all entries
   for (char* iter=newData;iter<reinterpret_cast<char*>(newChunk);iter+=size)
      *reinterpret_cast<T**>(iter)=reinterpret_cast<T*>(iter+size);
   *reinterpret_cast<T**>(newData+(chunkSize-1)*size)=0;
   freeList=reinterpret_cast<T*>(newData);

   // Increase the chunk size slightly
   chunkSize+=chunkSize/4;
   if (chunkSize&7) chunkSize=(chunkSize|7)+1;
}
//---------------------------------------------------------------------------
template <class Impl,class T> template <class F> void PoolBase<Impl,T>::enumAll(F& functor)
   // Apply a function to all contained objects
{
   // Sor all chunks
   chunks=static_cast<ChunkHeader*>(PoolHelper::sort(chunks));

   // Sort the free list
   freeList=static_cast<T*>(PoolHelper::sort(freeList));

   // Now traverse the free list and skip all freed objects
   T* nextFree=freeList;
   for (ChunkHeader* iter=chunks;iter;iter=iter->next) {
      T* const limit=reinterpret_cast<T*>(iter);
      for (T* iter2=iter->getData();iter2<limit;iter2=reinterpret_cast<T*>(reinterpret_cast<char*>(iter2)+size)) {
         while (nextFree&&(nextFree<iter2)) nextFree=*reinterpret_cast<T**>(nextFree);
         if (nextFree!=iter2)
            functor(iter2);
      }
   }
}
//---------------------------------------------------------------------------
template <class Impl,class T> T* PoolBase<Impl,T>::alloc()
   // Allocate an uninitialized entry
{
   if (!freeList)
      allocChunk();

   T* result=freeList;
   freeList=*reinterpret_cast<T**>(result);
   return result;
}
//---------------------------------------------------------------------------
template <class Impl,class T> void PoolBase<Impl,T>::free(T* data)
   // Release an entry
{
   *reinterpret_cast<T**>(data)=freeList;
   freeList=data;
}
//---------------------------------------------------------------------------
template <class Impl,class T> void PoolBase<Impl,T>::freeAll()
   // Release alle entries
{
   // Call destructors if desired by the implementation
   Impl::initCleanup(this);

   // Delete all chunks
   while (chunks) {
      ChunkHeader* next=chunks->next;
      delete[] reinterpret_cast<char*>(chunks->getData());
      chunks=next;
   }

   // Clear the free list
   freeList=0;
}
//---------------------------------------------------------------------------
/** Struct Pool. Manages a pool of structs, i.e., neither constructor
  * nor destructor are called. Use only if you know what you are doing at if
  * performance is critical.
  */
template <class T> class StructPool : public PoolBase<StructPool<T>,T>
{
   public:
   /// Constructor
   StructPool(unsigned chunkSize=32);

   /// Allocate an object
   inline T* alloc();
   /// Release an object
   inline void free(T* data);
   /// Release all objects
   void freeAll();
};
//---------------------------------------------------------------------------
template <class T> StructPool<T>::StructPool(unsigned chunkSize)
   : PoolBase<StructPool<T>,T>(chunkSize)
   // Constructor
{
}
//---------------------------------------------------------------------------
template <class T> T* StructPool<T>::alloc()
   // Allocate an object
{
   return PoolBase<StructPool<T>,T>::alloc();
}
//---------------------------------------------------------------------------
template <class T> void StructPool<T>::free(T* data)
   // Release an object
{
   if(data)
      PoolBase<StructPool<T>,T>::free(data);
}
//---------------------------------------------------------------------------
template <class T> void StructPool<T>::freeAll()
   // Release all objects
{
   PoolBase<StructPool<T>,T>::freeAll();
}
//---------------------------------------------------------------------------
/** Initialized Struct Pool. As struct pool, but new structs are initialized
  * with zero.
  */
template <class T> class InitZeroStructPool : public PoolBase<InitZeroStructPool<T>,T>
{
   public:
   /// Constructor
   InitZeroStructPool(unsigned chunkSize=32);

   /// Allocate
   inline T* alloc();
   /// Freigeben
   inline void free(T* data);
   /// Alles freigeben
   void freeAll();
};
//---------------------------------------------------------------------------
template <class T> InitZeroStructPool<T>::InitZeroStructPool(unsigned chunkSize)
   : PoolBase<InitZeroStructPool<T>,T>(chunkSize)
   // Constructor
{
}
//---------------------------------------------------------------------------
template <class T> T* InitZeroStructPool<T>::alloc()
   // Allocate an object
{
   T* r=PoolBase<InitZeroStructPool<T>,T>::alloc();
   std::memset(r,0,PoolBase<InitZeroStructPool<T>,T>::size);
   return r;
}
//---------------------------------------------------------------------------
template <class T> void InitZeroStructPool<T>::free(T* data)
   // Release an object
{
   if(data)
      PoolBase<InitZeroStructPool<T>,T>::free(data);
}
//---------------------------------------------------------------------------
template <class T> void InitZeroStructPool<T>::freeAll()
   // Release all objects
{
   PoolBase<InitZeroStructPool<T>,T>::freeAll();
}
//---------------------------------------------------------------------------
/** Object Pool. Calls constructors and destructors. You this if you are unsure.
  */
template <class T> class Pool : public PoolBase<Pool<T>,T>
{
   private:
   friend class PoolBase<Pool<T>,T>;
   /// Call the destructor
   struct Cleanup { void operator()(T* t) const { t->~T(); } };
   /// Call all destructors
   static void initCleanup(PoolBase<Pool<T>,T>* p);

   public:
   /// Constructor
   Pool(unsigned chunkSize=32);

   /// Allocate an object
   T* alloc();
   /// Allocate an object
   T* alloc(const T& value);
   /// Release an object
   void free(T* data);
   /// Release all objects
   void freeAll();
};
//---------------------------------------------------------------------------
template <class T> Pool<T>::Pool(unsigned chunkSize)
   : PoolBase<Pool<T>,T>(chunkSize)
   // Constructor
{
}
//---------------------------------------------------------------------------
template <class T> void Pool<T>::initCleanup(PoolBase<Pool<T>,T>* p)
   /// Call all destructors
{
   Cleanup cleanup;
   static_cast<Pool*>(p)->enumAll(cleanup);
}
//---------------------------------------------------------------------------
template <class T> T* Pool<T>::alloc()
   // Allocate an object
{
   T* result=PoolBase<Pool<T>,T>::alloc();
   try {
      new (result) T();
   } catch (...) {
      PoolBase<Pool<T>,T>::free(result);
      throw;
   }
   return result;
}
//---------------------------------------------------------------------------
template <class T> T* Pool<T>::alloc(const T& value)
   // Allocate an object
{
   T* result=PoolBase<Pool<T>,T>::alloc();
   try {
      new (result) T(value);
   } catch (...) {
      PoolBase<Pool<T>,T>::free(result);
      throw;
   }
   return result;
}
//---------------------------------------------------------------------------
template <class T> void Pool<T>::free(T* data)
   // Release an object
{
   if (data) {
      data->~T();
      PoolBase<Pool<T>,T>::free(data);
   }
}
//---------------------------------------------------------------------------
template <class T> void Pool<T>::freeAll()
   // Release all objects
{
   PoolBase<Pool<T>,T>::freeAll();
}
//---------------------------------------------------------------------------
#endif
