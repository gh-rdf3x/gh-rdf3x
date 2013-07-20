#ifndef H_rts_transaction_LogAction
#define H_rts_transaction_LogAction
//---------------------------------------------------------------------------
#include "infra/Config.hpp"
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
class BufferReferenceExclusive;
class BufferReferenceModified;
//---------------------------------------------------------------------------
/// Base class for all loggable operations
class LogAction
{
   public:
   /// Destructor
   virtual ~LogAction();

   /// Write a log entry
   virtual void* writeLog(void* buffer) const = 0;
   /// Read a log entry
   virtual void readLog(const void* buffer) = 0;
   /// Redo the logged operation
   virtual void redo(void* page) const = 0;
   /// Undo the logged operation
   virtual void undo(void* page) const = 0;

   /// Apply the operation to a page and unfix the page afterwards
   void apply(BufferReferenceModified& page) const;
   /// Apply the operation to a page but do not unfix
   void applyButKeep(BufferReferenceModified& page,BufferReferenceExclusive& newPage) const;
};
//---------------------------------------------------------------------------
/// A chunk of data to be stored in the log
struct LogData {
   /// Pointer to the data
   const void* ptr;
   /// Length of the data
   unsigned len;

   /// Constructor
   LogData() : ptr(0),len(0) {}
   /// Constructor
   LogData(const void* ptr,unsigned len) : ptr(ptr),len(len) {}
};
//---------------------------------------------------------------------------
/// A helper class for glueing the various log action into the system
class LogActionGlue
{
   public:
   /// I/O helper class
   template <class T> class Helper {};
   /// A hook for registering the class
   template <unsigned segmentId,unsigned actionId,class T> class Hook { public: Hook(); };

   /// Register a action
   static void registerAction(unsigned segmentId,unsigned actionId,LogAction* singleton);
};
//---------------------------------------------------------------------------
/// Specialization
template <> class LogActionGlue::Helper<uint32_t> { public: static void* write(void* ptr,uint32_t value); static const void* read(const void* ptr,uint32_t& value); };
template <> class LogActionGlue::Helper<LogData> { public: static void* write(void* ptr,LogData value); static const void* read(const void* ptr,LogData& value); };
//---------------------------------------------------------------------------
template <unsigned segmentId,unsigned actionId,class T> LogActionGlue::Hook<segmentId,actionId,T>::Hook()
   // A hook for registering the class
{
   registerAction(segmentId,actionId,new T());
}
//---------------------------------------------------------------------------
/// Construct a class name
#define LOGACTION_ID(seg,action) action
/// Construct a hook name
#define LOGACTION_HOOK(seg,action) logActionInfo_##seg##_##action
/// Common head of all log actions
#define LOGACTION_HEAD(seg,action) \
class LOGACTION_ID(seg,action) : public LogAction { public: LOGACTION_ID(seg,action)();
/// Common tail of all log actions
#define LOGACTION_TAILDECL(seg,action) \
void* writeLog(void* buffer) const;\
void readLog(const void* buffer);\
void redo(void* page) const;\
void undo(void* page) const;\
};
#define LOGACTION_TAILDEF(seg,action) \
LOGACTION_ID(seg,action)::LOGACTION_ID(seg,action)() {}\
static LogActionGlue::Hook<seg::ID,seg::Action_##action,LOGACTION_ID(seg,action)> LOGACTION_HOOK(seg,action);
#define LOGACTION_TAIL(seg,action) LOGACTION_TAILDECL(seg,action) LOGACTION_TAILDEF(seg,action)
//---------------------------------------------------------------------------
/// A log entry with 2 parameters
#define LOGACTION2DECL(seg,action,t1,v1,t2,v2) \
LOGACTION_HEAD(seg,action) \
private: t1 v1; t2 v2; public:\
LOGACTION_ID(seg,action)(t1 v1,t2 v2) : v1(v1),v2(v2) {}\
LOGACTION_TAILDECL(seg,action)
#define LOGACTION2DEF(seg,action,t1,v1,t2,v2) \
LOGACTION_TAILDEF(seg,action) \
void* LOGACTION_ID(seg,action)::writeLog(void* ptr_) const { return LogActionGlue::Helper<t2>::write(LogActionGlue::Helper<t1>::write(ptr_,v1),v2); } \
void LOGACTION_ID(seg,action)::readLog(const void* ptr_) { LogActionGlue::Helper<t2>::read(LogActionGlue::Helper<t1>::read(ptr_,v1),v2); }
#define LOGACTION2(seg,action,t1,v1,t2,v2) LOGACTION2DECL(seg,action,t1,v1,t2,v2) LOGACTION2DEF(seg,action,t1,v1,t2,v2)
//---------------------------------------------------------------------------
/// A log entry with 3 parameters
#define LOGACTION3DECL(seg,action,t1,v1,t2,v2,t3,v3) \
LOGACTION_HEAD(seg,action) \
private: t1 v1; t2 v2; t3 v3; public:\
LOGACTION_ID(seg,action)(t1 v1,t2 v2,t3 v3) : v1(v1),v2(v2),v3(v3) {}\
LOGACTION_TAILDECL(seg,action)
#define LOGACTION3DEF(seg,action,t1,v1,t2,v2,t3,v3) \
LOGACTION_TAILDEF(seg,action) \
void* LOGACTION_ID(seg,action)::writeLog(void* ptr_) const { return LogActionGlue::Helper<t3>::write(LogActionGlue::Helper<t2>::write(LogActionGlue::Helper<t1>::write(ptr_,v1),v2),v3); } \
void LOGACTION_ID(seg,action)::readLog(const void* ptr_) { LogActionGlue::Helper<t3>::read(LogActionGlue::Helper<t2>::read(LogActionGlue::Helper<t1>::read(ptr_,v1),v2),v3); }
#define LOGACTION3(seg,action,t1,v1,t2,v2,t3,v3) LOGACTION3DECL(seg,action,t1,v1,t2,v2,t3,v3) LOGACTION3DEF(seg,action,t1,v1,t2,v2,t3,v3)
//---------------------------------------------------------------------------
/// A log entry with 4 parameters
#define LOGACTION4(seg,action,t1,v1,t2,v2,t3,v3,t4,v4) \
LOGACTION_HEAD(seg,action) \
private: t1 v1; t2 v2; t3 v3; t4 v4; public: \
LOGACTION_ID(seg,action)(t1 v1,t2 v2,t3 v3,t4 v4) : v1(v1),v2(v2),v3(v3),v4(v4) {}\
LOGACTION_TAIL(seg,action) \
void* LOGACTION_ID(seg,action)::writeLog(void* ptr_) const { return LogActionGlue::Helper<t4>::write(LogActionGlue::Helper<t3>::write(LogActionGlue::Helper<t2>::write(LogActionGlue::Helper<t1>::write(ptr_,v1),v2),v3),v4); } \
void LOGACTION_ID(seg,action)::readLog(const void* ptr_) { LogActionGlue::Helper<t4>::read(LogActionGlue::Helper<t3>::read(LogActionGlue::Helper<t2>::read(LogActionGlue::Helper<t1>::read(ptr_,v1),v2),v3),v4); }
//---------------------------------------------------------------------------
/// A log entry with 5 parameters
#define LOGACTION5(seg,action,t1,v1,t2,v2,t3,v3,t4,v4,t5,v5) \
LOGACTION_HEAD(seg,action) \
private: t1 v1; t2 v2; t3 v3; t4 v4; t5 v5; public: \
LOGACTION_ID(seg,action)(t1 v1,t2 v2,t3 v3,t4 v4,t5 v5) : v1(v1),v2(v2),v3(v3),v4(v4),v5(v5) {}\
LOGACTION_TAIL(seg,action) \
void* LOGACTION_ID(seg,action)::writeLog(void* ptr_) const { return LogActionGlue::Helper<t5>::write(LogActionGlue::Helper<t4>::write(LogActionGlue::Helper<t3>::write(LogActionGlue::Helper<t2>::write(LogActionGlue::Helper<t1>::write(ptr_,v1),v2),v3),v4),v5); } \
void LOGACTION_ID(seg,action)::readLog(const void* ptr_) { LogActionGlue::Helper<t5>::read(LogActionGlue::Helper<t4>::read(LogActionGlue::Helper<t3>::read(LogActionGlue::Helper<t2>::read(LogActionGlue::Helper<t1>::read(ptr_,v1),v2),v3),v4),v5); }
//---------------------------------------------------------------------------
/// A log entry with 6 parameters
#define LOGACTION6(seg,action,t1,v1,t2,v2,t3,v3,t4,v4,t5,v5,t6,v6) \
LOGACTION_HEAD(seg,action) \
private: t1 v1; t2 v2; t3 v3; t4 v4; t5 v5; t6 v6; public: \
LOGACTION_ID(seg,action)(t1 v1,t2 v2,t3 v3,t4 v4,t5 v5,t6 v6) : v1(v1),v2(v2),v3(v3),v4(v4),v5(v5),v6(v6) {}\
LOGACTION_TAIL(seg,action) \
void* LOGACTION_ID(seg,action)::writeLog(void* ptr_) const { return LogActionGlue::Helper<t6>::write(LogActionGlue::Helper<t5>::write(LogActionGlue::Helper<t4>::write(LogActionGlue::Helper<t3>::write(LogActionGlue::Helper<t2>::write(LogActionGlue::Helper<t1>::write(ptr_,v1),v2),v3),v4),v5),v6); } \
void LOGACTION_ID(seg,action)::readLog(const void* ptr_) { LogActionGlue::Helper<t6>::read(LogActionGlue::Helper<t5>::read(LogActionGlue::Helper<t4>::read(LogActionGlue::Helper<t3>::read(LogActionGlue::Helper<t2>::read(LogActionGlue::Helper<t1>::read(ptr_,v1),v2),v3),v4),v5),v6); }
//---------------------------------------------------------------------------
/// A log entry with 7 parameters
#define LOGACTION7(seg,action,t1,v1,t2,v2,t3,v3,t4,v4,t5,v5,t6,v6,t7,v7) \
LOGACTION_HEAD(seg,action) \
private: t1 v1; t2 v2; t3 v3; t4 v4; t5 v5; t6 v6; t7 v7; public: \
LOGACTION_ID(seg,action)(t1 v1,t2 v2,t3 v3,t4 v4,t5 v5,t6 v6,t7 v7) : v1(v1),v2(v2),v3(v3),v4(v4),v5(v5),v6(v6),v7(v7) {}\
LOGACTION_TAIL(seg,action) \
void* LOGACTION_ID(seg,action)::writeLog(void* ptr_) const { return LogActionGlue::Helper<t7>::write(LogActionGlue::Helper<t6>::write(LogActionGlue::Helper<t5>::write(LogActionGlue::Helper<t4>::write(LogActionGlue::Helper<t3>::write(LogActionGlue::Helper<t2>::write(LogActionGlue::Helper<t1>::write(ptr_,v1),v2),v3),v4),v5),v6),v7); } \
void LOGACTION_ID(seg,action)::readLog(const void* ptr_) { LogActionGlue::Helper<t7>::read(LogActionGlue::Helper<t6>::read(LogActionGlue::Helper<t5>::read(LogActionGlue::Helper<t4>::read(LogActionGlue::Helper<t3>::read(LogActionGlue::Helper<t2>::read(LogActionGlue::Helper<t1>::read(ptr_,v1),v2),v3),v4),v5),v6),v7); }
//---------------------------------------------------------------------------
/// A log entry with 8 parameters
#define LOGACTION8(seg,action,t1,v1,t2,v2,t3,v3,t4,v4,t5,v5,t6,v6,t7,v7,t8,v8) \
LOGACTION_HEAD(seg,action) \
private: t1 v1; t2 v2; t3 v3; t4 v4; t5 v5; t6 v6; t7 v7; t8 v8; public: \
LOGACTION_ID(seg,action)(t1 v1,t2 v2,t3 v3,t4 v4,t5 v5,t6 v6,t7 v7,t8 v8) : v1(v1),v2(v2),v3(v3),v4(v4),v5(v5),v6(v6),v7(v7),v8(v8) {}\
LOGACTION_TAIL(seg,action) \
void* LOGACTION_ID(seg,action)::writeLog(void* ptr_) const { return LogActionGlue::Helper<t8>::write(LogActionGlue::Helper<t7>::write(LogActionGlue::Helper<t6>::write(LogActionGlue::Helper<t5>::write(LogActionGlue::Helper<t4>::write(LogActionGlue::Helper<t3>::write(LogActionGlue::Helper<t2>::write(LogActionGlue::Helper<t1>::write(ptr_,v1),v2),v3),v4),v5),v6),v7),v8); } \
void LOGACTION_ID(seg,action)::readLog(const void* ptr_) { LogActionGlue::Helper<t8>::read(LogActionGlue::Helper<t7>::read(LogActionGlue::Helper<t6>::read(LogActionGlue::Helper<t5>::read(LogActionGlue::Helper<t4>::read(LogActionGlue::Helper<t3>::read(LogActionGlue::Helper<t2>::read(LogActionGlue::Helper<t1>::read(ptr_,v1),v2),v3),v4),v5),v6),v7),v8); }
//---------------------------------------------------------------------------
#endif
