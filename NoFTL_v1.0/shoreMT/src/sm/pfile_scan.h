/*<std-header orig-src='shore' incl-file-exclusion='SCAN_H'>

 $Id: pfile_scan.h,v 3.0 2000/09/18 21:05:02 natassa Exp $

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

#ifndef PFILE_SCAN_H
#define PFILE_SCAN_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

// Look in scan.h for more comments on how scan works

#ifdef __GNUG__
#pragma interface
#endif

#ifndef XCT_DEPENDENT_H
#include <xct_dependent.h>
#endif /* XCT_DEPENDENT_H */

/*
 * Scanning a Partitioned File
 * 
 * The pfile scanning iterator class is scan_pfile_i.  The next() function
 * returns a pointer to a pin_prec_i object which "points" to the next record
 * in the pfile.  The start_offset argument to next() indicates the first
 * byte in the record to pin.
 */

#include "pfile_p.h"
#include "scan.h"

class pin_prec_i;
class pin_pfield_i;

class scan_pfile_i : public smlevel_top, public xct_dependent_t {
public:
	stid_t    			stid;
	rid_t    			curr_rid;
	
	NORET			scan_pfile_i(
	const stid_t& 		    stid,
	concurrency_t		    cc = t_cc_file,
	const flid_t		    no_ifields=0,
	flid_t*					ifields=NULL,
	bool					prefetch=false);
	NORET			~scan_pfile_i();
	// avoid compiler errors
	//NORET			W_FASTNEW_CLASS_DECL(scan_pfile_i);
	
	rc_t			next(
	pin_prec_i*&		    pin_ptr,
	// smsize_t 		    start_offset, 
	bool& 		    	    eof);

	/*
	 * The next_page function advances the scan to the first
	 * record on the next page in the file.  If next_page is
	 * called after the scan_file_i is initialized it will advance
	 * the cursor to the first record in the file, just as
	 * next() would do.
	 */
	rc_t			next_page(
	pin_prec_i*&		    pin_ptr,
	// smsize_t 		    start_offset, 
	bool& 		            eof);

	// logical serial # and volume ID of the file if created that way
	// const serial_t&		lfid() const { return _lfid; }
	// const lvid_t&		lvid() const { return _lvid; };
   
	void			finish();
	bool			eof()		{ return _eof; }
	// bool			is_logical() const{ return (_lfid != serial_t::null); }
	w_rc_t			error_code(){ return _error_occurred; }
	tid_t			xid() const { return tid; }

protected:
	tid_t			tid;
	bool			_eof;
	rc_t			_error_occurred;
	pin_prec_i*			_cursor;
	lpid_t			_next_pid;
	// serial_t			_lfid;// logical file ID if created that way
	// lvid_t			_lvid;// logical volume ID if created that way
	concurrency_t		_cc;  // concurrency control
	lock_mode_t			_page_lock_mode;
	lock_mode_t			_rec_lock_mode;

	rc_t 			_init(const flid_t no_ifields=0,
						  flid_t* ifields=NULL,
						  bool for_append=false);

	rc_t			_next(
	pin_prec_i*&			    pin_ptr,
	// smsize_t		    start_offset, 
	bool& 		            eof);

	void			xct_state_changed(
	xct_state_t		    old_state,
	xct_state_t		    new_state);

private:
	bool 	 		_do_prefetch;
	bf_prefetch_thread_t*	_prefetch;

	// disabled
	// NORET			scan_pfile_i(const scan_pfile_i&);
	// scan_pfile_i&		operator=(const scan_pfile_i&);
};

/*
 * Scanning a Partitioned File
 * 
 * The pfield scanning iterator class is scan_pfield_i.  The next() function
 * returns a pointer to a pin_prec_i object which "points" to the next record
 * in the pfield.  The start_offset argument to next() indicates the first
 * byte in the record to pin.
 */

class scan_pfield_i : public smlevel_top, public xct_dependent_t {
public:
	stid_t    			stid;
	rid_t    			curr_rid;

	NORET			scan_pfield_i(
	const stid_t& 		stid,
	const flid_t 	    	fno,
	bool			is_fixed_size,
	concurrency_t		cc = t_cc_file,
	bool			prefetch=false);
	NORET			~scan_pfield_i();
	// avoid compiler errors
	//NORET			W_FASTNEW_CLASS_DECL(scan_pfield_i);
	
	rc_t			next(
	pin_pfield_i*&		    pin_ptr,
	bool& 		    	    eof);

	/*
	 * The next_page function advances the scan to the first
	 * record on the next page in the file.  If next_page is
	 * called after the scan_file_i is initialized it will advance
	 * the cursor to the first record in the file, just as
	 * next() would do.
	 */
	rc_t			next_page(
	pin_pfield_i*&		    pin_ptr,
	bool& 		            eof);

	void			finish();
	bool			eof()		{ return _eof; }
	w_rc_t			error_code(){ return _error_occurred; }
	tid_t			xid() const { return tid; }

protected:
	tid_t			tid;
	bool			_eof;
	rc_t			_error_occurred;
	pin_pfield_i*		_cursor;
	lpid_t			_next_pid;
	concurrency_t		_cc;  // concurrency control
	lock_mode_t			_page_lock_mode;
	lock_mode_t			_rec_lock_mode;

	rc_t 			_init(const flid_t fno, bool is_fixed_size,
						bool for_append=false);

	rc_t			_next(
	pin_pfield_i*&			    pin_ptr,
	// smsize_t		    start_offset, 
	bool& 		            eof);

	void			xct_state_changed(
	xct_state_t		    old_state,
	xct_state_t		    new_state);

private:
	bool 	 		_do_prefetch;
	bf_prefetch_thread_t*	_prefetch;

	// disabled
	// NORET			scan_pfield_i(const scan_pfield_i&);
	// scan_pfield_i&		operator=(const scan_pfield_i&);
};

#include <memory.h>
#ifndef SDESC_H
#include "sdesc.h"
#endif

class append_pfile_i : public scan_pfile_i {
public:
	NORET			append_pfile_i(
	const stid_t& stid);
	NORET			~append_pfile_i();
	rc_t			next(
	pin_prec_i*&		    pin_ptr,
	smsize_t 		    start_offset, 
	bool& 		    	    eof);

	rc_t			create_rec(
	const precord_t&		prec_info,
	rid_t&				rid);
private:
	void 			_init_constructor();

	// See comments in pin.h, which does the same thing
	// file_p		        page;
	inline 
	pfile_p&     		_page() { return *(pfile_p*) _page_alias;}
	char        		_page_alias[40];
	sdesc_t			_cached_sdesc;
};

/*<std-footer incl-file-exclusion='PFILE_SCAN_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
