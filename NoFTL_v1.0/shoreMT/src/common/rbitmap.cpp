/*<std-header orig-src='shore'>

 $Id: rbitmap.cpp,v 3.0 2000/09/18 21:04:35 natassa Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
					  Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define RBITMAP_C

#ifdef __GNUC__
#pragma implementation "rbitmap.h"
#endif

#include <stdlib.h>
#include <w_stream.h>
#include "basics.h" 
#include "rbitmap.h"
#include <w_debug.h>

// #include <iostream.h>

inline int div8(long x)         { return x >> 3; }
inline int mod8(long x)         { return x & 7; }
inline int div32(long x)        { return x >> 5; }
inline int mod32(long x)        { return x & 31; }

#define	R_OVERFLOW_MASK         0x000
	

void r_bm_zero(u_char* rbm, int size)
{
	int n = div8(size - 1) + 1;
	for (int i = 0; i < n; i++, rbm--)
		*rbm = 0;
}

void r_bm_fill(u_char* rbm, int size)
{
	int n = div8(size - 1) + 1;
	for (int i = 0; i < n; i++, rbm--)
		*rbm = ~0;
}

bool r_bm_is_set(const u_char* rbm, long offset)
{
  return (*(rbm-div8(offset)) & (128 >> mod8(offset))) != 0;
}

void r_bm_set(u_char* rbm, long offset)
{
  *(rbm-div8(offset)) |= (128 >> mod8(offset));
}

void r_bm_clr(u_char* rbm, long offset)
{
  *(rbm-div8(offset)) &= ~(128 >> mod8(offset));
}

int r_bm_first_set(const u_char* rbm, int size, int start)
{
  #ifdef W_DEBUG
  const u_char *rbm0 = rbm;
  #endif
  register int mask;
  
  dual_assert3(start >= 0 && start <= size);
  
  rbm -= div8(start);
  mask = 128 >> mod8(start);
  
  for (size -= start; size; start++, size--)  {
	if (*rbm & mask)  {
	  dual_assert3(r_bm_is_set(rbm0, start));
	  return start;
	}
	if ((mask >>= 1) == R_OVERFLOW_MASK) {
	  mask = 128;
	  rbm--;
	}
  }
	
  return -1;
}

int r_bm_first_clr(const u_char* rbm, int size, int start)
{
  dual_assert3(start >= 0 && start <= size);
  register int mask;
  #ifdef W_DEBUG
  const u_char *rbm0 = rbm;
  #endif
  
  rbm -= div8(start);
  mask = 128 >> mod8(start);
  
  for (size -= start; size; start++, size--) {
	if ((*rbm & mask) == 0)	{
	  dual_assert3(r_bm_is_clr(rbm0, start));
	  return start;
	}
	if ((mask >>= 1) == R_OVERFLOW_MASK)  {
	  mask = 128;
	  rbm--;
	}
  }
	
  return -1;
}


int r_bm_last_set(const u_char* rbm, int size, int start)
{
  register unsigned mask;
  #ifdef W_DEBUG
  const	u_char *rbm0 = rbm;
  #endif
  
  dual_assert3(start >= 0 && start < size);
  
  rbm -= div8(start);
  mask = 128 >> mod8(start);
  
  for (size = start+1; size; start--, size--)  {
	if (*rbm & mask)  {
	  dual_assert3(r_bm_is_set(rbm0, start));
	  return start;
	}
	if ((mask <<= 1) == 256) {
	  mask = 1;
	  rbm++;
	}
  }
  
  return -1;
}


int r_bm_last_clr(const u_char* rbm, int size, int start)
{
  register unsigned mask;
  #ifdef W_DEBUG
  const u_char *rbm0 = rbm;
  #endif
  
  dual_assert3(start >= 0 && start < size);
  
  rbm -= div8(start);
  mask = 128 >> mod8(start);
  
  for (size = start+1; size; start--, size--)  {
	if ((*rbm & mask) == 0)  {
	  dual_assert3(r_bm_is_clr(rbm0, start));
	  return start;
	}
	if ((mask <<= 1) == 256)  {
	  mask = 1;
	  rbm++;
	}
  }
  
  return -1;
}


int r_bm_num_set(const u_char* rbm, int size)
{
  int count;
  int mask;
  for (count = 0, mask = 128; size; size--)  {
	if (*rbm & mask)
	  count++;
	if ((mask >>= 1) == R_OVERFLOW_MASK)  {
	  mask = 128;
	  rbm--;
	}
  }
  return count;
}

bool r_bm_any_set (const u_char* rbm, int size)
{
  int mask;
  for (mask = 1; size; size--)  {
	if (*rbm & mask)
	  return true;
	if ((mask <<= 1) == R_OVERFLOW_MASK)  {
	  mask = 1;
	  rbm--;
	}
  }
  return false;
}

void r_bm_print(u_char* rbm, int size)
{  
  for (int i = 0; i < size; i++)  {
	cout << (r_bm_is_set(rbm, size-i-1) != 0);
  }
}





