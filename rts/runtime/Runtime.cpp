#include "rts/runtime/Runtime.hpp"
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
void Register::reset()
   // Reset the register (both value and domain)
{
   value=~0u;
   domain=0;
}
//---------------------------------------------------------------------------
Runtime::Runtime(Database& db,DifferentialIndex* diff,TemporaryDictionary* temporaryDictionary)
   : db(db),diff(diff),temporaryDictionary(temporaryDictionary)
   // Constructor
{
}
//---------------------------------------------------------------------------
Runtime::~Runtime()
   // Destructor
{
}
//---------------------------------------------------------------------------
void Runtime::allocateRegisters(unsigned count)
   // Set the number of registers
{
   registers.clear();
   registers.resize(count);

   for (unsigned index=0;index<count;index++)
      registers[index].reset();
}
//---------------------------------------------------------------------------
void Runtime::allocateDomainDescriptions(unsigned count)
   // Set the number of domain descriptions
{
   domainDescriptions.clear();
   domainDescriptions.resize(count);
}
//---------------------------------------------------------------------------
