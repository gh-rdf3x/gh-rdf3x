#include "rts/buffer/BufferReference.hpp"
#include "rts/buffer/BufferManager.hpp"
#include <cassert>
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
BufferReference::BufferReference()
   : frame(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
BufferReference::BufferReference(const BufferRequest& request)
   : frame(0)
   // Constructor from a request
{
   operator=(request);
}
//---------------------------------------------------------------------------
BufferReference::~BufferReference()
   // Destructor
{
   reset();
}
//---------------------------------------------------------------------------
BufferReference& BufferReference::operator=(const BufferRequest& request)
   // Remap the reference to a different page
{
   reset();

   frame=request.bufferManager.readPageShared(request.partition,request.page);
   return *this;
}
//---------------------------------------------------------------------------
void BufferReference::swap(BufferReference& other)
   // Swap two reference
{
   const BufferFrame* f=frame;
   frame=other.frame;
   other.frame=f;
}
//---------------------------------------------------------------------------
void BufferReference::reset()
   // Reset the reference
{
   if (frame) {
      frame->getBufferManager()->unfixPage(frame);
      frame=0;
   }
}
//---------------------------------------------------------------------------
const void* BufferReference::getPage() const
   // Access the page
{
   return frame->pageData();
}
//---------------------------------------------------------------------------
unsigned BufferReference::getPageNo() const
   // Get the page number
{
   return frame->getPageNo();
}
//---------------------------------------------------------------------------
BufferReferenceExclusive::BufferReferenceExclusive()
   : frame(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
BufferReferenceExclusive::BufferReferenceExclusive(const BufferRequestExclusive& request)
   : frame(0)
   // Constructor from a request
{
   operator=(request);
}
//---------------------------------------------------------------------------
BufferReferenceExclusive::~BufferReferenceExclusive()
   // Destructor
{
   reset();
}
//---------------------------------------------------------------------------
BufferReferenceExclusive& BufferReferenceExclusive::operator=(const BufferRequestExclusive& request)
   // Remap the reference to a different page
{
   reset();

   frame=request.bufferManager.readPageExclusive(request.partition,request.page);
   return *this;
}
//---------------------------------------------------------------------------
void BufferReferenceExclusive::swap(BufferReferenceExclusive& other)
   // Swap two reference
{
   const BufferFrame* f=frame;
   frame=other.frame;
   other.frame=f;
}
//---------------------------------------------------------------------------
void BufferReferenceExclusive::reset()
   // Reset the reference
{
   if (frame) {
      frame->getBufferManager()->unfixPage(frame);
      frame=0;
   }
}
//---------------------------------------------------------------------------
const void* BufferReferenceExclusive::getPage() const
   // Access the page
{
   return frame->pageData();
}
//---------------------------------------------------------------------------
unsigned BufferReferenceExclusive::getPageNo() const
   // Get the page number
{
   return frame->getPageNo();
}
//---------------------------------------------------------------------------
BufferReferenceModified::BufferReferenceModified()
   : frame(0)
   // Constructor
{
}
//---------------------------------------------------------------------------
BufferReferenceModified::BufferReferenceModified(const BufferRequestModified& request)
   : frame(0)
   // Constructor from a request
{
   operator=(request);
}
//---------------------------------------------------------------------------
BufferReferenceModified::~BufferReferenceModified()
   // Destructor
{
   // The page _must_ be unfixed before the constructor is called!
   assert(!frame);
}
//---------------------------------------------------------------------------
BufferReferenceModified& BufferReferenceModified::operator=(const BufferRequestModified& request)
   // Remap the reference to a different page
{
   // The page _must_ be unfixed before assigned a new page!
   assert(!frame);

   frame=request.bufferManager.readPageExclusive(request.partition,request.page)->update();
   return *this;
}
//---------------------------------------------------------------------------
void BufferReferenceModified::swap(BufferReferenceModified& other)
   // Swap two reference
{
   BufferFrame* f=frame;
   frame=other.frame;
   other.frame=f;
}
//---------------------------------------------------------------------------
void BufferReferenceModified::modify(BufferReferenceExclusive& ref)
   // Modify an already exclusively locked page. Transfers ownership of the page!
{
   // The page _must_ be unfixed before assigned a new page!
   assert(!frame);

   // Empty reference?
   if (!ref.frame)
      return;

   // Transfer ownership
   frame=ref.frame->update();
   ref.frame=0;
}
//---------------------------------------------------------------------------
void BufferReferenceModified::unfixWithoutRecovery()
   // Unfix without logging. Logging is done by Transaction::unfix
{
   // Empty reference?
   if (!frame)
      return;

   // Unfix it
   frame->getBufferManager()->unfixDirtyPageWithoutRecovery(frame);
   frame=0;
}
//---------------------------------------------------------------------------
void BufferReferenceModified::finishWithoutRecovery(BufferReferenceExclusive& ref)
   // Finish without logging. Logging is done by Transaction::unfix. Transfers ownership
{
   ref.reset();

   // Empty reference?
   if (!frame) return;

   // Mark as dirty and transfer
   frame->getBufferManager()->markDirtyWithoutRecovery(frame);
   ref.frame=frame;
   frame=0;
}
//---------------------------------------------------------------------------
void* BufferReferenceModified::getPage() const
   // Access the page
{
   return frame->pageData();
}
//---------------------------------------------------------------------------
unsigned BufferReferenceModified::getPageNo() const
   // Get the page number
{
   return frame->getPageNo();
}
//---------------------------------------------------------------------------
