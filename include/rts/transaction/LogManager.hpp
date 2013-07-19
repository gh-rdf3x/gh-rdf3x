#ifndef H_rts_transaction_LogManager
#define H_rts_transaction_LogManager
//---------------------------------------------------------------------------
#include <stdint.h>
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
/// Log management. XXX currently not implemented!
class LogManager
{
   public:
   /// Initiate a checkpoint. Must be called by the buffer manager!
   void initiateCheckpoint();
   /// Force the log to a certain point
   void force(uint64_t lsn);
};
//---------------------------------------------------------------------------
#endif
