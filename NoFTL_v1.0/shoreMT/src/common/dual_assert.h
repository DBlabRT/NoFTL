/*<std-header orig-src='shore' incl-file-exclusion='DUAL_ASSERT_H'>

 $Id: dual_assert.h,v 3.0 2000/09/18 21:04:34 natassa Exp $

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

#ifndef DUAL_ASSERT_H
#define DUAL_ASSERT_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
 *  This behaves like the storage manager assertions in sm.h
 *  but can be compiled for code that's shared among the storage
 * 	manager and other levels of Shore.
 */
#ifndef RPCGEN
#include <stdio.h>
#include <stdlib.h>
#endif

/* assert1 is always fatal */

#if defined(W_DEBUG) && defined(assert1)
#    define dual_assert1(_x) assert1((_x));
#else
#    define dual_assert1(_x) { \
		if (!(_x)) {\
			fprintf(stderr,\
			"Assertion failed: file \"%s\", line %d\n", __FILE__, __LINE__);\
			exit(1);\
		}\
	}
#endif /* W_DEBUG */

/* assert3 is not important enough to be fatal for non-debugging cases */

#if defined(W_DEBUG)
#	define dual_assert3(_x) dual_assert1(_x)
#else
#	define dual_assert3(_x)
#endif /* W_DEBUG */

/*<std-footer incl-file-exclusion='DUAL_ASSERT_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
