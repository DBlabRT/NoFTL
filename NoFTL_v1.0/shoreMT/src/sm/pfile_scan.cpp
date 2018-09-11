/*<std-header orig-src='shore'>

 $Id: pfile_scan.cpp,v 3.0 2000/09/18 21:05:02 natassa Exp $

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
																  
#define SM_SOURCE
#define SCAN_C
#ifdef __GNUG__
#   pragma implementation
#endif
#include <sm_int_4.h>
#include <sm.h>
#include <pin.h>
#include <scan.h>
#include <bf_prefetch.h>
#include <btcursor.h>
#include <rtree_p.h>
#include <new> //#include <new.h>

#include "pfile_scan.h"
#include "prec_pin.h"
#include "pfile_m.h"
#include "pfile_p.h"

//W_FASTNEW_STATIC_DECL(scan_pfile_i, 32); 

#define SCAN_METHOD_PROLOGUE1                           \
	do {                                                \
		if(_error_occurred.is_error())                  \
			return RC(_error_occurred.err_num());       \
	} while(0)

scan_pfile_i::scan_pfile_i(	const stid_t& stid_,		concurrency_t cc,
							const flid_t no_ifields,	flid_t* ifields,	bool pre)
							: xct_dependent_t(xct()),	stid(stid_),		_eof(false), 
							_error_occurred(),			_cc(cc),			_do_prefetch(pre), 
							_prefetch(0)
{
	// adding the xct_constraint_t
	INIT_SCAN_PROLOGUE_RC(scan_pfile_i::scan_file_i, cc == t_cc_append ? prologue_rc_t::read_write : prologue_rc_t::read_only,  0); 
	
	// obsolate shore version
	//w_rc_t rc = _init(no_ifields, ifields, cc == t_cc_append);
	//if (rc) return;
	/* _init sets error state */
	 W_IGNORE(_init(no_ifields, ifields,cc == t_cc_append));
}

rc_t
scan_pfile_i::next(pin_prec_i*& pin_ptr, bool& eof)
{
	// adding the xct_constraint_t and ShoreMT support
	//SM_PROLOGUE_RC(scan_pfile_i::next, in_xct, 1);
	return _next(pin_ptr, eof);
}

rc_t
scan_pfile_i::next_page(pin_prec_i*& pin_ptr, bool& eof)
{
	SCAN_METHOD_PROLOGUE1;
	// adding the xct_constraint_t and ShoreMT support
	//SM_PROLOGUE_RC(scan_pfile_i::next_page, in_xct, 1);
	SCAN_METHOD_PROLOGUE(scan_pfile_i::next_page, read_only, 1);

	/*
	 * The trick here is to make the scan think we are on the
	 * last slot on the page and then just call _next()
	 * If the _cursor is not pinned, then next will start at the
	 * first slot.  This is sufficient for our needs.
	 */
	if (_cursor->pinned()) {
	curr_rid.slot = _cursor->_page().num_slots()-1;
	}
	return _next(pin_ptr, eof);
}

scan_pfile_i::~scan_pfile_i()
{
	finish();
}

rc_t
scan_pfile_i::_init(const flid_t no_ifields, flid_t* ifields, bool for_append) 
{
	// adding the xct_constraint_t and ShoreMT support
	// Can't nest these prologues
	//SCAN_METHOD_PROLOGUE(scan_pfile_i::_init);
	this->_prefetch = 0;

	bool  eof = false;

	tid = xct()->tid();

	sdesc_t* sd = NULL;

	// determine file and record lock modes
	lock_mode_t mode = NL;

	switch(_cc) {
	case t_cc_none:
	mode = IS;
	_page_lock_mode = NL;
	_rec_lock_mode = NL;
	break;

	case t_cc_record:
	/* 
	 * This might seem weird, but it's necessary
	 * in order to prevent phantoms when another
	 * tx does delete-in-order/abort or 
	 * add-in-reverse-order/commit (using location hints)
	 * while a scan is going on. 
	 *
	 * See the note about SERIALIZABILITY, below.
	 * It turns out that this trick isn't enough.
	 * Setting page lock mode to SH keeps that code
	 * from being necessary, and it should be deleted.
	 */
	mode = IS;
	_page_lock_mode = SH;  // record lock with IS the page
	_rec_lock_mode = NL;
	break;

	case t_cc_page:
	mode = IS;
	_page_lock_mode = SH;
	_rec_lock_mode = NL;

	case t_cc_append:
	mode = IX;
	_page_lock_mode = EX;
	_rec_lock_mode = EX;
	break;

	case t_cc_file:
	mode = SH;
	_page_lock_mode = NL;
	_rec_lock_mode = NL;
	break;

	default:
	_error_occurred = RC(eBADLOCKMODE);
	return _error_occurred;
	break;
	}

	DBGTHRD(<<"scan_pfile_i calling access for stid " << stid);
	_error_occurred = dir->access(stid, sd, mode);
	if (_error_occurred.is_error())  {
	return _error_occurred;
	}
	// if (_lfid != serial_t::null && _lfid != sd->sinfo().logical_id) {
	// DBG(<<"_init: " );
	// W_FATAL(eBADLOGICALID);
	// }

	if (_error_occurred.is_error())  {
	return _error_occurred;
	}

	// see if we need to figure out the first rid in the file
	// (ie. it was not specified in the constructor)
	if (curr_rid == rid_t::null) {
	if(for_append) {
		_error_occurred = pfi->last_page(stid, curr_rid.pid); 
	} else {
		_error_occurred = pfi->first_page(stid, curr_rid.pid);
	}

	if (_error_occurred.is_error())  {
		return _error_occurred;
	}
	curr_rid.slot = -1;  // get the very first object

	} else {
	// subtract 1 slot from curr_rid so that next will advance
	// properly.  Also pin the previous slot it.
	if (curr_rid.slot > 0 && !for_append) curr_rid.slot--;
	}

	if (_page_lock_mode != NL) {
	w_assert1(curr_rid.pid.page != 0);
	_error_occurred = lm->lock(curr_rid.pid, _page_lock_mode, 
				  t_long, WAIT_SPECIFIED_BY_XCT);
	if (_error_occurred.is_error())  {
		return _error_occurred;
	}
	}

	// remember the next pid
	_next_pid = curr_rid.pid;
	_error_occurred = pfi->next_page(_next_pid, eof);
	if(for_append) {
	w_assert1(eof);
	} else if (_error_occurred.is_error())  {
	return _error_occurred;
	}
	if (eof) {
	_next_pid = lpid_t::null;
	} 

	if(smlevel_0::do_prefetch && this->_do_prefetch && !for_append) {
	// prefetch first page
	this->_prefetch = new bf_prefetch_thread_t;
	if (this->_prefetch) {
		W_COERCE( this->_prefetch->fork());
		me()->yield(); // let it run

		DBGTHRD(<<" requesting first page: " << curr_rid.pid);

		W_COERCE(this->_prefetch->request(curr_rid.pid, 
			pin_i::lock_to_latch(_page_lock_mode)));
	}
	}

	if (ifields!=NULL)
		_cursor = new pin_pprec_i (no_ifields, ifields);
	else
		_cursor = new pin_prec_i;

	return RCOK;

}

rc_t
scan_pfile_i::_next(pin_prec_i*& pin_ptr, bool& eof)
{
	// adding the xct_constraint_t and ShoreMT support
	//SCAN_METHOD_PROLOGUE(scan_pfile_i::_next);
	SCAN_METHOD_PROLOGUE1;
	pfile_p*	curr;

	w_assert1(xct()->tid() == tid);

	// scan through pages and slots looking for the next valid slot
	while (!_eof) {
	if (!_cursor->pinned()) {
		// We're getting a new page
		w_assert1(curr_rid.slot==EOP);

		if (_error_occurred = _cursor->_pin(curr_rid, _rec_lock_mode, pin_i::lock_to_latch(_rec_lock_mode), false), _error_occurred.is_error())  {
		return _error_occurred;
		}
	}
	curr = _cursor->_get_page();
	{
		slotid_t	slot;
		// BUGBUG: locking the wrong slot here!
		slot = curr->next_slot(curr_rid.slot);
		if(_rec_lock_mode != NL) {
		w_assert1(curr_rid.pid.page != 0);
		_error_occurred = lm->lock(curr_rid, _rec_lock_mode, t_long, WAIT_IMMEDIATE);
		if (_error_occurred.is_error())  
		{
			if (_error_occurred.err_num() != eLOCKTIMEOUT) {
			return _error_occurred;
			}
			//
			// re-fetch the slot if we had to wait for a lock.
			// needed for SERIALIZABILITY, since the next_slot
			// information is not protected by locks
			//
			_cursor->unpin();
			me()->yield();
			_error_occurred = RCOK;
			_error_occurred.reset();
			continue; // while loop
		}
		}
		curr_rid.slot = slot;
	}
	if (curr_rid.slot == EOP) {
		// End Of Page: last slot, so go to next page
		_cursor->unpin();
		curr_rid.pid = _next_pid;
		if (_next_pid == lpid_t::null) {
		_eof = true;
		} else {
		// if(this->_prefetch == 0) {
			if (_page_lock_mode != NL) {
			DBGTHRD(<<" locking " << curr_rid.pid);
			_error_occurred = lm->lock(curr_rid.pid, 
						  _page_lock_mode,
						  t_long,
						  WAIT_SPECIFIED_BY_XCT);
			if (_error_occurred.is_error())  {
				return _error_occurred;
			}
			}
		// } else {
			// prefetch case: we already locked & requested the next pid
			// we'll fetch it at the top of this loop
			// 
			// All we have to do in this case is locate the
			// page after that.
		// }

		DBGTHRD(<<" locating page after " << _next_pid);
		bool tmp_eof;
		_error_occurred = pfi->next_page(_next_pid, tmp_eof);
		if (_error_occurred.is_error())  {
			return _error_occurred;
		}
		if (tmp_eof) {
			_next_pid = lpid_t::null;
		} 
		DBGTHRD(<<" next page is " << _next_pid);
		}
	} else {
		W_DO(_cursor->_pin(curr_rid, _rec_lock_mode, pin_i::lock_to_latch(_rec_lock_mode), true));  // , serial_t::null));
		break;
	}
	}

	eof = _eof;

	// as a friend, set the pin_prec_i's serial number for current rec
	// if (eof) {
	// _cursor->_lrid.serial = serial_t::null;
	// } else {
	// _cursor->_lrid.serial = _cursor->_rec->tag.serial_no;
	// }

	pin_ptr = _cursor;
	return RCOK;
}

void scan_pfile_i::finish()
{
	// must finish regardless of error
	// SCAN_METHOD_PROLOGUE(scan_pfile_i::finish);

	_eof = true;

	if(_cursor->is_mine()) {
	// Don't unpin unless I'm the owner. 
	// (This function can get called from xct_state_changed(),
	// running in a non-owner thread.)
	_cursor->unpin();
		delete _cursor;
	_cursor = NULL;
	}

	if (this->_prefetch) {
	this->_prefetch->retire();
	delete this->_prefetch;
	this->_prefetch = 0;
	}

}

void 
scan_pfile_i::xct_state_changed(
	xct_state_t		/*old_state*/,
	xct_state_t		new_state)
{
	if (new_state == xct_aborting || new_state == xct_committing)  {
	finish();
	}
}

//W_FASTNEW_STATIC_DECL(scan_pfield_i, 32); 

scan_pfield_i::scan_pfield_i(const stid_t& stid_, const flid_t fno,
				 bool is_fixed_size, concurrency_t cc, bool pre) 
: xct_dependent_t(xct()), stid(stid_), _eof(false), _error_occurred(),
  _cc(cc), _do_prefetch(pre), _prefetch(0)
{
	// adding the xct_constraint_t
	INIT_SCAN_PROLOGUE_RC(scan_pfield_i::scan_field_i, cc == t_cc_append ? prologue_rc_t::read_write : prologue_rc_t::read_only, 0);

	// obsolate shore version
	//w_rc_t rc = _init(fno, is_fixed_size, cc == t_cc_append);
	//if (rc) return;
	/* _init sets error state */
	W_IGNORE(_init(fno, is_fixed_size,cc == t_cc_append));
}

rc_t
scan_pfield_i::next(pin_pfield_i*& pin_ptr, bool& eof)
{
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(scan_pfield_i::next, in_xct, read_only, 1);
	return _next(pin_ptr, eof);
}

rc_t
scan_pfield_i::next_page(pin_pfield_i*& pin_ptr, bool& eof)
{
	SCAN_METHOD_PROLOGUE1;
	// adding the xct_constraint_t and ShoreMT support
	//SM_PROLOGUE_RC(scan_pfield_i::next_page, in_xct, 1);
	SCAN_METHOD_PROLOGUE(scan_pfield_i::next_page, read_only, 1);

	/*
	 * The trick here is to make the scan think we are on the
	 * last slot on the page and then just call _next()
	 * If the _cursor is not pinned, then next will start at the
	 * first slot.  This is sufficient for our needs.
	 */
	if (_cursor->pinned()) {
	curr_rid.slot = _cursor->_page().num_slots()-1;
	}
	return _next(pin_ptr, eof);
}

scan_pfield_i::~scan_pfield_i()
{
	finish();
}

rc_t
scan_pfield_i::_init(const flid_t fno, bool is_fixed_size, bool for_append) 
{
	// Can't nest these prologues
	// adding the xct_constraint_t and ShoreMT support
	//SCAN_METHOD_PROLOGUE(scan_pfield_i::_init);
	
	this->_prefetch = 0;

	bool  eof = false;

	tid = xct()->tid();

	sdesc_t* sd = NULL;

	// determine file and record lock modes
	lock_mode_t mode = NL;

	switch(_cc) {
	case t_cc_none:
	mode = IS;
	_page_lock_mode = NL;
	_rec_lock_mode = NL;
	break;

	case t_cc_record:
	/* 
	 * This might seem weird, but it's necessary
	 * in order to prevent phantoms when another
	 * tx does delete-in-order/abort or 
	 * add-in-reverse-order/commit (using location hints)
	 * while a scan is going on. 
	 *
	 * See the note about SERIALIZABILITY, below.
	 * It turns out that this trick isn't enough.
	 * Setting page lock mode to SH keeps that code
	 * from being necessary, and it should be deleted.
	 */
	mode = IS;
	_page_lock_mode = SH;  // record lock with IS the page
	_rec_lock_mode = NL;
	break;

	case t_cc_page:
	mode = IS;
	_page_lock_mode = SH;
	_rec_lock_mode = NL;

	case t_cc_append:
	mode = IX;
	_page_lock_mode = EX;
	_rec_lock_mode = EX;
	break;

	case t_cc_file:
	mode = SH;
	_page_lock_mode = NL;
	_rec_lock_mode = NL;
	break;

	default:
	_error_occurred = RC(eBADLOCKMODE);
	return _error_occurred;
	break;
	}

	DBGTHRD(<<"scan_pfield_i calling access for stid " << stid);
	_error_occurred = dir->access(stid, sd, mode);
	if (_error_occurred.is_error())  {
	return _error_occurred;
	}

	if (_error_occurred.is_error())  {
	return _error_occurred;
	}

	// see if we need to figure out the first rid in the file
	// (ie. it was not specified in the constructor)
	if (curr_rid == rid_t::null) {
	if(for_append) {
		_error_occurred = pfi->last_page(stid, curr_rid.pid); 
	} else {
		_error_occurred = pfi->first_page(stid, curr_rid.pid);
	}

	if (_error_occurred.is_error())  {
		return _error_occurred;
	}
	curr_rid.slot = -1;  // get the very first object

	} else {
	// subtract 1 slot from curr_rid so that next will advance
	// properly.  Also pin the previous slot it.
	if (curr_rid.slot > 0 && !for_append) curr_rid.slot--;
	}

	if (_page_lock_mode != NL) {
	w_assert1(curr_rid.pid.page != 0);
	_error_occurred = lm->lock(curr_rid.pid, _page_lock_mode, 
				  t_long, WAIT_SPECIFIED_BY_XCT);
	if (_error_occurred.is_error())  {
		return _error_occurred;
	}
	}

	// remember the next pid
	_next_pid = curr_rid.pid;
	_error_occurred = pfi->next_page(_next_pid, eof);
	if(for_append) {
	w_assert1(eof);
	} else if (_error_occurred.is_error())  {
	return _error_occurred;
	}
	if (eof) {
	_next_pid = lpid_t::null;
	} 

	if(smlevel_0::do_prefetch && this->_do_prefetch && !for_append) {
	// prefetch first page
	this->_prefetch = new bf_prefetch_thread_t;
	if (this->_prefetch) {
		W_COERCE( this->_prefetch->fork());
		me()->yield(); // let it run

		DBGTHRD(<<" requesting first page: " << curr_rid.pid);

		W_COERCE(this->_prefetch->request(curr_rid.pid, pin_i::lock_to_latch(_page_lock_mode)));
	}
	}

	// as a friend, set the field number
	if (is_fixed_size)
		_cursor = new pin_ffield_i;
	else
		_cursor = new pin_vfield_i;
	_cursor->_pfield.fno = fno;

	return RCOK;
}

rc_t
scan_pfield_i::_next(pin_pfield_i*& pin_ptr, bool& eof)
{
	// adding the xct_constraint_t and ShoreMT support
	//SCAN_METHOD_PROLOGUE(scan_pfield_i::_next);
	SCAN_METHOD_PROLOGUE1;
	pfile_p*	curr;

	w_assert1(xct()->tid() == tid);

	// scan through pages and slots looking for the next valid slot
	while (!_eof) {
	if (!_cursor->pinned()) {
		// We're getting a new page
		w_assert1(curr_rid.slot==EOP);
		if (_error_occurred = _cursor->_pin(curr_rid, _rec_lock_mode, false), _error_occurred.is_error())  {
		return _error_occurred;

		}
	}
	curr = _cursor->_get_page();
	{
		slotid_t	slot;
		// BUGBUG: locking the wrong slot here!
		slot = curr->next_slot(curr_rid.slot);
		if(_rec_lock_mode != NL) {
		w_assert1(curr_rid.pid.page != 0);
		_error_occurred = lm->lock(curr_rid, _rec_lock_mode, t_long, WAIT_IMMEDIATE);
		if (_error_occurred.is_error())  
		{
			if (_error_occurred.err_num() != eLOCKTIMEOUT) {
			return _error_occurred;
			}
			//
			// re-fetch the slot if we had to wait for a lock.
			// needed for SERIALIZABILITY, since the next_slot
			// information is not protected by locks
			//
			_cursor->unpin();
			me()->yield();
			_error_occurred = RCOK;
			_error_occurred.reset();
			continue; // while loop
		}
		}
		curr_rid.slot = slot;
	}
	if (curr_rid.slot == EOP) {
		// End Of Page: last slot, so go to next page
		_cursor->unpin();
		curr_rid.pid = _next_pid;
		if (_next_pid == lpid_t::null) {
		_eof = true;
		} else {
		// if(this->_prefetch == 0) {
			if (_page_lock_mode != NL) {
			DBGTHRD(<<" locking " << curr_rid.pid);
			_error_occurred = lm->lock(curr_rid.pid, 
						  _page_lock_mode,
						  t_long,
						  WAIT_SPECIFIED_BY_XCT);
			if (_error_occurred.is_error())  {
				return _error_occurred;
			}
			}
		// } else {
			// prefetch case: we already locked & requested the next pid
			// we'll fetch it at the top of this loop
			// 
			// All we have to do in this case is locate the
			// page after that.
		// }

		DBGTHRD(<<" locating page after " << _next_pid);
		bool tmp_eof;
		_error_occurred = pfi->next_page(_next_pid, tmp_eof);
		if (_error_occurred.is_error())  {
			return _error_occurred;
		}
		if (tmp_eof) {
			_next_pid = lpid_t::null;
		} 
		DBGTHRD(<<" next page is " << _next_pid);
		}
	} else {
		if (curr_rid.slot!=0)  // not the first one
		{
		  _cursor->get_next_field();
		  _cursor->_rid = curr_rid;
		}
		else
			W_DO(_cursor->_pin(curr_rid, _rec_lock_mode, true));  // , serial_t::null));
		break;
	}
	}

	eof = _eof;

	pin_ptr = _cursor;
	return RCOK;
}

void scan_pfield_i::finish()
{
	// must finish regardless of error
	// SCAN_METHOD_PROLOGUE(scan_pfield_i::finish);

	_eof = true;

	if(_cursor->is_mine()) {
	// Don't unpin unless I'm the owner. 
	// (This function can get called from xct_state_changed(),
	// running in a non-owner thread.)
	_cursor->unpin();
	delete _cursor;
	_cursor = NULL;
	}

	if (this->_prefetch) {
	this->_prefetch->retire();
	delete this->_prefetch;
	this->_prefetch = 0;
	}
}

void 
scan_pfield_i::xct_state_changed(
	xct_state_t		/*old_state*/,
	xct_state_t		new_state)
{
	if (new_state == xct_aborting || new_state == xct_committing)  {
	finish();
	}
}

append_pfile_i::append_pfile_i(const stid_t& stid_) 
 : scan_pfile_i(stid_, t_cc_append)
{
	_init_constructor();
	// adding the xct_constraint_t
	INIT_SCAN_PROLOGUE_RC(append_pfile_i::append_pfile_i, prologue_rc_t::read_write, 0);
	W_IGNORE(_init(true));
	if(_error_occurred.is_error()) return;
	_error_occurred = lm->lock(stid, EX, t_long, WAIT_SPECIFIED_BY_XCT);
	if(_error_occurred.is_error()) return;
	sdesc_t *sd;
	_error_occurred = dir->access(stid, sd, IX); 
	// makes a copy - only because the create_rec
	// functions that we want to call require that we
	// have one to reference.
	_cached_sdesc = *sd;
	w_assert3( !_page().is_fixed() );
}

void
append_pfile_i::_init_constructor()
{
	// FUNC(append_pfile_i::_init_constructor);
	//  just make sure _page_alias is big enough
	w_assert3(sizeof(_page_alias) >= sizeof(pfile_p));

	new (_page_alias) pfile_p();
	w_assert3( !_page().is_fixed() );

}

append_pfile_i::~append_pfile_i() 
{ 
	FUNC(append_pfile_i::~append_pfile_i);
	if(_page().is_fixed()) {
	_page().unfix();
	}
	DBG( <<  (_page().is_fixed()? "IS FIXED-- ERROR" : "OK") );
	_page().destructor();

	// Must invalidate so that ref counts are adjusted
	DBGTHRD(<<"invalidating cached descr");
	_cached_sdesc.invalidate_sdesc();
	finish(); 
}

rc_t
append_pfile_i::next(pin_prec_i*&, smsize_t, bool& )
{
	w_assert1(0);
	return RCOK;
}

rc_t			
append_pfile_i::create_rec(const precord_t& prec_info, rid_t& rid)
{
	// adding the xct_constraint_t
	SCAN_METHOD_PROLOGUE(append_pfile_i::create_rec, read_write, 1);

#ifdef W_DEBUG
	if(_page().is_fixed()) {
	DBG(<<"IS FIXED! ");
	}
#endif /* W_DEBUG */

	if(_error_occurred.is_error()) { 
	return _error_occurred;
	}
	// if(is_logical()) {
		// return RC(eBADSCAN);
	// }

	W_DO( pfi->create_rec_at_end(_page(), prec_info, _cached_sdesc,  curr_rid) );

	rid = curr_rid;
	return RCOK;
}

