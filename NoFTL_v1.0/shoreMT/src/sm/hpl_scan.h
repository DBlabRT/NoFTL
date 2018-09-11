/* ------------------------------------------------------------------------------- */
/* -- Copyright (c) 2011, 2012 Databases and Distributed Systems Department,    -- */
/* -- Technical University of Darmstadt, subject to the terms and conditions    -- */
/* -- given in the file COPYRIGHT.  All Rights Reserved.                        -- */
/* ------------------------------------------------------------------------------- */
/* -- Ilia Petrov, Todor Ivanov, Veselin Marinov                                -- */
/* ------------------------------------------------------------------------------- */


#ifndef HPL_SCAN_H
#define HPL_SCAN_H

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
 * Scanning a HPL File
 * 
 * The hpl file scanning iterator class is scan_hpl_file_i.  The next() function
 * returns a pointer to a pin_hpl_rec_i object which "points" to the next record
 * in the hpl file.  The start_offset argument to next() indicates the first
 * byte in the record to pin.
 */

#include "hpl_p.h"
#include "scan.h"

class pin_hpl_rec_i;
class pin_hpl_field_i;

class scan_hpl_p_i : public smlevel_top, public xct_dependent_t
{
public:
	stid_t    			stid;
	rid_t    			curr_rid;
	
	NORET	scan_hpl_p_i(const stid_t& stid, concurrency_t cc = t_cc_file, const flid_t no_ifields=0, flid_t* ifields=NULL, bool prefetch=false);
	NORET	~scan_hpl_p_i();
	// avoid compiler errors
	//NORET			W_FASTNEW_CLASS_DECL(scan_pfile_i);
	
	rc_t			next(
	pin_hpl_rec_i*&		    pin_ptr,
	// smsize_t 		    start_offset, 
	bool& 		    	    eof);

	 /* needed for tcl scripts */
	/**\brief Return the pin_i that represents the scan state.
	 * @param[out] pin_ptr   Populate the caller's pointer.
	 * @param[out] eof  False if the pin_i points to a record,
	 * true if there were no next record to pin.
	 *
	 */
	void cursor(pin_hpl_rec_i*& pin_ptr, bool& eof)
	{
		pin_ptr = _cursor;
		eof = _eof;
	}

	/*
	 * The next_page function advances the scan to the first
	 * record on the next page in the file.  If next_page is
	 * called after the scan_file_i is initialized it will advance
	 * the cursor to the first record in the file, just as
	 * next() would do.
	 */
	rc_t			next_page(
	pin_hpl_rec_i*&		    pin_ptr,
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
	pin_hpl_rec_i*	_cursor;
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
						pin_hpl_rec_i*&		pin_ptr,
						// smsize_t		    start_offset,
						bool& 		            eof);

	void			xct_state_changed(
	xct_state_t		    old_state,
	xct_state_t		    new_state);

private:
	bool 	 		_do_prefetch;
	bf_prefetch_thread_t*	_prefetch;

	// disabled
	NORET				scan_hpl_p_i(const scan_hpl_p_i&);
	scan_hpl_p_i&		operator=(const scan_hpl_p_i&);
};

/*
 * Scanning a Partitioned File
 * 
 * The hpl_field scanning iterator class is scan_hpl_field_i.  The next() function
 * returns a pointer to a pin_hpl_rec_i object which "points" to the next record
 * in the hpl_field.  The start_offset argument to next() indicates the first
 * byte in the record to pin.
 */

class scan_hpl_field_i : public smlevel_top, public xct_dependent_t {
public:
	stid_t    			stid;
	rid_t    			curr_rid;

	NORET			scan_hpl_field_i(
	const stid_t& 	stid,
	const flid_t	fno,
	bool			is_fixed_size,
	concurrency_t	cc = t_cc_file,
	bool			prefetch=false);
	NORET			~scan_hpl_field_i();
	// avoid compiler errors
	//NORET			W_FASTNEW_CLASS_DECL(scan_pfield_i);
	
	rc_t next(
					pin_hpl_field_i*&	pin_ptr,
					bool&				eof);

	/*
	 * The next_page function advances the scan to the first
	 * record on the next page in the file.  If next_page is
	 * called after the scan_file_i is initialized it will advance
	 * the cursor to the first record in the file, just as
	 * next() would do.
	 */
	rc_t next_page(
					pin_hpl_field_i*&	pin_ptr,
					bool&				eof);

	void			finish();
	bool			eof()		{ return _eof; }
	w_rc_t			error_code(){ return _error_occurred; }
	tid_t			xid() const { return tid; }

protected:
	tid_t			tid;
	bool			_eof;
	rc_t			_error_occurred;
	pin_hpl_field_i* _cursor;
	lpid_t			_next_pid;
	concurrency_t		_cc;  // concurrency control
	lock_mode_t			_page_lock_mode;
	lock_mode_t			_rec_lock_mode;

	rc_t _init(
					const flid_t fno,
					bool is_fixed_size,
					bool for_append=false);

	rc_t _next(
					pin_hpl_field_i*&	pin_ptr,
					// smsize_t		    start_offset,
					bool&				eof);

	void xct_state_changed(
					xct_state_t		    old_state,
					xct_state_t		    new_state);

private:
	bool					_do_prefetch;
	bf_prefetch_thread_t*	_prefetch;

	// disabled
	// NORET			scan_pfield_i(const scan_pfield_i&);
	// scan_pfield_i&		operator=(const scan_pfield_i&);
};

#include <memory.h>
#ifndef SDESC_H
#include "sdesc.h"
#endif

class append_hpl_p_i : public scan_hpl_p_i {
public:
	NORET	append_hpl_p_i(const stid_t& stid);

	NORET	~append_hpl_p_i();

	rc_t	next(pin_hpl_rec_i*&	pin_ptr,
				smsize_t 		    start_offset,
				bool&				eof);

	rc_t	create_rec(const hpl_record_t&	hpl_rec_info, rid_t& rid);
private:
	void	_init_constructor();

	// See comments in pin.h, which does the same thing
	// file_p		        page;
	inline hpl_p&	_page() { return *(hpl_p*) _page_alias;}
	char			_page_alias[40];
	sdesc_t			_cached_sdesc;
};

#endif
