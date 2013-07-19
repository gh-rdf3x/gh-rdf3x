#ifndef H_rts_buffer_BufferReference
#define H_rts_buffer_BufferReference
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
class BufferManager;
class BufferFrame;
class Partition;
//---------------------------------------------------------------------------
/// A request to access a buffer page. Used by segments to "return" references.
struct BufferRequest
{
   /// The buffer manager
   BufferManager& bufferManager;
   /// The partition
   Partition& partition;
   /// The requested page
   unsigned page;

   /// Constructor
   BufferRequest(BufferManager& bufferManager,Partition& partition,unsigned page) : bufferManager(bufferManager),partition(partition),page(page) {}
};
//---------------------------------------------------------------------------
/// A request to exclusively access a buffer page. Used by segments to "return" references.
struct BufferRequestExclusive
{
   /// The buffer manager
   BufferManager& bufferManager;
   /// The partition
   Partition& partition;
   /// The requested page
   unsigned page;

   /// Constructor
   BufferRequestExclusive(BufferManager& bufferManager,Partition& partition,unsigned page) : bufferManager(bufferManager),partition(partition),page(page) {}
};
//---------------------------------------------------------------------------
/// A request to modify a buffer page. Used by segments to "return" references.
struct BufferRequestModified
{
   /// The buffer manager
   BufferManager& bufferManager;
   /// The partition
   Partition& partition;
   /// The requested page
   unsigned page;

   /// Constructor
   BufferRequestModified(BufferManager& bufferManager,Partition& partition,unsigned page) : bufferManager(bufferManager),partition(partition),page(page) {}
};
//---------------------------------------------------------------------------
/// A reference to a page in the database buffer.
/// The page remains accessible during the lifetime of the BufferReference object.
class BufferReference
{
   public:
   /// The size of a page
   static const unsigned pageSize = 16384;
   /// A page buffer
   struct PageBuffer { char data[pageSize]; };

   private:
   /// The buffer frame
   const BufferFrame* frame;

   /// No copying of references
   BufferReference(const BufferReference&);
   void operator=(const BufferReference&);

   public:
   /// Constructor
   BufferReference();
   /// Constructor from a request
   BufferReference(const BufferRequest& request);
   /// Destructor
   ~BufferReference();

   /// Null reference?
   bool operator!() const { return !frame; }

   /// Remap the reference to a different page
   BufferReference& operator=(const BufferRequest& request);
   /// Swap two reference
   void swap(BufferReference& other);
   /// Reset the reference
   void reset();

   /// Access the page
   const void* getPage() const;
   /// Get the page number
   unsigned getPageNo() const;
};
//---------------------------------------------------------------------------
class BufferReferenceModified;
//---------------------------------------------------------------------------
/// A reference to an exclusively locked page in the database buffer.
/// The page remains accessible during the lifetime of the BufferReference object.
class BufferReferenceExclusive
{
   private:
   /// The buffer frame
   const BufferFrame* frame;

   /// No copying of references
   BufferReferenceExclusive(const BufferReferenceExclusive&);
   void operator=(const BufferReferenceExclusive&);

   friend class BufferReferenceModified;

   public:
   /// Constructor
   BufferReferenceExclusive();
   /// Constructor from a request
   BufferReferenceExclusive(const BufferRequestExclusive& request);
   /// Destructor
   ~BufferReferenceExclusive();

   /// Null reference?
   bool operator!() const { return !frame; }

   /// Remap the reference to a different page
   BufferReferenceExclusive& operator=(const BufferRequestExclusive& request);
   /// Swap two reference
   void swap(BufferReferenceExclusive& other);
   /// Reset the reference
   void reset();

   /// Access the page
   const void* getPage() const;
   /// Get the page number
   unsigned getPageNo() const;
};
//---------------------------------------------------------------------------
/// A reference to an exclusively locked and modified page in the database buffer.
/// The page remains accessible during the lifetime of the BufferReference object.
class BufferReferenceModified
{
   private:
   /// The buffer frame
   BufferFrame* frame;

   /// No copying of references
   BufferReferenceModified(const BufferReferenceModified&);
   void operator=(const BufferReferenceModified&);

   public:
   /// Constructor
   BufferReferenceModified();
   /// Constructor from a request
   BufferReferenceModified(const BufferRequestModified& request);
   /// Destructor. unfix _must_ be called before the destructor!
   ~BufferReferenceModified();

   /// Null reference?
   bool operator!() const { return !frame; }

   /// Remap the reference to a different page
   BufferReferenceModified& operator=(const BufferRequestModified& request);
   /// Swap two reference
   void swap(BufferReferenceModified& other);
   /// Modify an already exclusively locked page. Transfers ownership of the page!
   void modify(BufferReferenceExclusive& ref);
   /// Unfix without logging. Logging is done by Transaction::unfix
   void unfixWithoutRecovery();
   /// Finish without logging. Logging is done by Transaction::unfix. Transfers ownership
   void finishWithoutRecovery(BufferReferenceExclusive& ref);

   /// Access the page
   void* getPage() const;
   /// Get the page number
   unsigned getPageNo() const;
};
//---------------------------------------------------------------------------
#endif
