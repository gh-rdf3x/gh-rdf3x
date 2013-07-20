#include "infra/util/Pool.hpp"
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
void* PoolHelper::sort(void* data)
   // Sort a single-linked list using merge sort
{
#define next(x) *reinterpret_cast<void**>(x)
   // Special cases first
   if (!data)
      return data;

   // Split
   void* left=data,*leftIter=data;
   if ((data=next(data))==0) return left;
   void *right=data,*rightIter=data;
   if ((data=next(data))==0) {
      if (left<right) return left;
      next(right)=left; next(left)=0;
      return right;
   }
   for (;data;data=next(data)) {
      leftIter=next(leftIter)=data;
      if ((data=next(data))==0) break;
      rightIter=(next(rightIter)=data);
   }
   next(leftIter)=0; next(rightIter)=0;

   // Sort recursive
   left=sort(left);
   right=sort(right);

   // And merge
   void* result;
   if (left<right) {
      result=left; left=next(left);
   } else {
      result=right; right=next(right);
   }
   void* iter;
   for (iter=result;left&&right;)
      if (left<right) {
         iter=(next(iter)=left); left=next(left);
      } else {
         iter=(next(iter)=right); right=next(right);
      }
   if (left) {
      next(iter)=left;
   } else if (right) {
      next(iter)=right;
   }
   return result;
#undef next
}
//---------------------------------------------------------------------------
