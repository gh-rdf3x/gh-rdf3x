#include "rts/transaction/LogAction.hpp"
#include "rts/buffer/BufferReference.hpp"
#include "rts/segment/Segment.hpp"
#include <vector>
#include <cstring>
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
using namespace std;
//---------------------------------------------------------------------------
namespace {
//---------------------------------------------------------------------------
/// All known actions
static vector<vector<LogAction*> >* registeredActions=0;
//---------------------------------------------------------------------------
/// Small helper class for registered actions
class ActionRegistry {
   public:
   /// Constructor
   ActionRegistry();
   /// Destructor
   ~ActionRegistry();

   /// Register an action
   void registerAction(unsigned segmentId,unsigned actionId,LogAction* action);
   /// Lookup an action
   LogAction* lookupAction(unsigned segmentId,unsigned actionId);
};
//---------------------------------------------------------------------------
ActionRegistry::ActionRegistry()
   // Constructor
{
}
//---------------------------------------------------------------------------
ActionRegistry::~ActionRegistry()
   // Destructor
{
   if (registeredActions) {
      for (vector<vector<LogAction*> >::iterator iter=registeredActions->begin(),limit=registeredActions->end();iter!=limit;++iter)
          for (vector<LogAction*>::iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2)
             delete *iter2;
      delete registeredActions;
      registeredActions=0;
   }
}
//---------------------------------------------------------------------------
void ActionRegistry::registerAction(unsigned segmentId,unsigned actionId,LogAction* action)
   // Register an action
{
   if (!registeredActions)
      registeredActions=new vector<vector<LogAction*> >();

   // Enlarge as needed
   if (segmentId>=registeredActions->size())
      registeredActions->resize(segmentId+1);
   vector<LogAction*>& actions=(*registeredActions)[segmentId];
   while (actionId>=actions.size())
      actions.push_back(0);

   // And store
   actions[actionId]=action;
}
//---------------------------------------------------------------------------
LogAction* ActionRegistry::lookupAction(unsigned segmentId,unsigned actionId)
   // Lookup an action
{
   if (!registeredActions)
      return 0;
   if (segmentId>=registeredActions->size())
      return 0;
   vector<LogAction*>& actions=(*registeredActions)[segmentId];
   if (actionId>=actions.size())
      return 0;
   return actions[actionId];
}
//---------------------------------------------------------------------------
/// The helper
static ActionRegistry actionRegistry;
//---------------------------------------------------------------------------
}
//---------------------------------------------------------------------------
LogAction::~LogAction()
   // Destructor
{
}
//---------------------------------------------------------------------------
void LogAction::apply(BufferReferenceModified& page) const
   // Apply the operation to a page and unfix the page afterwards
{
   // XXX implement the logging case, too
   // log _before_ applying the change! Might reference the old data

   redo(page.getPage());
   page.unfixWithoutRecovery();
}
//---------------------------------------------------------------------------
void LogAction::applyButKeep(BufferReferenceModified& page,BufferReferenceExclusive& newPage) const
   // Apply the operation to a page and keep the page fixed
{
   // XXX implement the logging case, too
   // log _before_ applying the change! Might reference the old data

   redo(page.getPage());
   page.finishWithoutRecovery(newPage);
}
//---------------------------------------------------------------------------
void LogActionGlue::registerAction(unsigned segmentId,unsigned actionId,LogAction* singleton)
   // Register a action
{
   actionRegistry.registerAction(segmentId,actionId,singleton);
}
//---------------------------------------------------------------------------
void* LogActionGlue::Helper<uint32_t>::write(void* ptr,uint32_t value)
   // Write a value
{
   Segment::writeUint32(static_cast<unsigned char*>(ptr),value);
   return static_cast<unsigned char*>(ptr)+4;
}
//---------------------------------------------------------------------------
const void* LogActionGlue::Helper<uint32_t>::read(const void* ptr,uint32_t& value)
   // Write a value
{
   value=Segment::readUint32(static_cast<const unsigned char*>(ptr));
   return static_cast<const unsigned char*>(ptr)+4;
}
//---------------------------------------------------------------------------
void* LogActionGlue::Helper<LogData>::write(void* ptr,LogData value)
   // Write a value
{
   ptr=Helper<uint32_t>::write(ptr,value.len);
   memcpy(ptr,value.ptr,value.len);
   return static_cast<unsigned char*>(ptr)+value.len;
}
//---------------------------------------------------------------------------
const void* LogActionGlue::Helper<LogData>::read(const void* ptr,LogData& value)
   // Write a value
{
   uint32_t len;
   ptr=Helper<uint32_t>::read(ptr,len);
   value.ptr=ptr;
   value.len=len;
   return static_cast<const unsigned char*>(ptr)+len;
}
//---------------------------------------------------------------------------
