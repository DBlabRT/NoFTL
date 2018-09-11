/*<std-header orig-src='shore'>

 $Id: prec_pin.cpp,v 3.0 2000/09/18 21:05:03 natassa Exp $

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
#define PREC_PIN_C

#ifdef __GNUG__
#pragma implementation "pin.h"
#endif

#include <new> //#include <new.h>
#include <sm_int_4.h>
#include <pin.h>
#include <prec_pin.h>
#include <pfile_p.h>
#include <pfile_m.h>



pin_prec_i::~pin_prec_i()
{ 
	unpin();
	// unpin() actuall calls unfix on these pages and unfix does
	// everything the destructor does, but to be safe we still call
	// the destructor
	_page().destructor();
}

rc_t pin_prec_i::pin(const rid_t rid, lock_mode_t lmode, bool is_next)
{
	// no more used in shoreMT
	//// adding the xct_constraint_t
	//SM_PROLOGUE_RC(pin_prec_i::pin, in_xct, read_only, 2);
	//if (lmode != SH && lmode != UD && lmode != EX && lmode != NL)
	//return RC(eBADLOCKMODE);
	//W_DO(_pin(rid, lmode, is_next));  //, serial_t::null));

	return pin(rid, lmode, pin_i::lock_to_latch(lmode), is_next);
}

rc_t pin_prec_i::pin(const rid_t& rid, lock_mode_t lmode, latch_mode_t latch_mode, bool is_next)
{
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(pin_prec_i::pin, in_xct, read_only, 2);
	//DBG(<<"lookup lvid=" << lvid << " serial= " << lrid);
	if (lmode!=SH && lmode!=UD && lmode!=EX && lmode!=NL)
		return RC(eBADLOCKMODE);
	
	// no more used in shoreMT
	//rid_t  rid;  // physical record ID
	//lid_t  id(lvid, lrid);
	//DBG(<<"about to lookup  id= " << id);
	//LID_CACHE_RETRY_DO(id, rid_t, rid, _pin(rid, lmode)); // , id.serial));
	//_lrid = id;


	W_DO(_pin(rid, lmode, latch_mode, is_next));
	//pin_i::_set_lsn();

	return RCOK;
}

void pin_prec_i::unpin()
{
	if (pinned()) {
	_page().unfix();  
	_flags = pin_empty;

	INC_TSTAT(rec_unpin_cnt);
	}
}

bool 
pin_prec_i::is_mine() const
{
	bool	mine = false;
	bool	h_others = false;


	if( _page().is_fixed())  {
	if( _page().pinned_by_me()) {
		mine = true;
	} else {
		h_others = true;
	}
	}
	if(h_others && mine) {
	// Had better not have data page owned by me
	// and header page owned by others
	W_FATAL(eINTERNAL);
	}

	// returns false if not fixed at all
	return mine;
}

void pin_prec_i::set_ref_bit(int value)
{
	if (pinned())
		_page().set_ref_bit(value);  
}

const char* pin_prec_i::page_data()
{
	if (!pinned()) return NULL;
	return (const char*) &(_page().persistent_part());
}

void pin_prec_i::_init_constructor()
{
	//  just make sure _page_alias is big enough
	w_assert3(sizeof(_page_alias) >= sizeof(pfile_p));

	_flags = pin_empty;
	//_lrid.lvid = lvid_t::null;
	_lmode = NL; 
	new (_page_alias) pfile_p();
}

void  pin_pprec_i::new_page_info()
{
	if (_prec.no_ffields==0)
	{
		flid_t i;
	for (i=0; i<_prec.no_fields; ++i)
	{
		_prec.field_size[i]=_page().field_size(_ifields[i]);
		if (_ifields[i]<_page().no_ffields())
			_prec.no_ffields++;
		}
	}
}

void  pin_prec_i::new_page_info()
{
	if (_prec.field_size==NULL) // totally uninitialized
	{       
		flid_t i;
		_prec.no_fields = _page().no_fields();
		_prec.field_size=new Size2[_prec.no_fields];
		_prec.no_ffields = _page().no_ffields();
		for (i=0; i<_prec.no_fields; ++i)
			_prec.field_size[i] = _page().field_size(i);
		_prec.data=new char*[_prec.no_fields];
	}   
}

rc_t pin_prec_i::_pin(const rid_t rid, lock_mode_t lmode, latch_mode_t latch_mode, bool is_next)
{
	rc_t   err;	// in HPL reverted to rc_t - must use rather than rc_t due to HP_CC_BUG_2
	bool pin_page = false;	// true indicates page needs pinning

	w_assert3(lmode == SH || lmode == EX || lmode == UD ||
		lmode == NL /*for scan.cpp*/ );

	DBGTHRD(<<"enter _pin");
	if (pinned()) {
	DBG(<<"pinned");
   
	/*
	 * If the page for the new record is not the same as the
	 * old (or if we need to get a lock),
	 * then unpin the old and get the new page.
	 * If we need to get the lock, then we must unfix since
	 * we may block on the lock.  We may want to do something
	 * like repin where we try to lock with 0-timeout and
	 * only if we fail do we unlatch.
	 */
	if (rid.pid != _rid.pid || lmode != NL) {
		_page().unfix();  
		pin_page = true;
		INC_TSTAT(rec_unpin_cnt);

	}
	} else {
	DBG(<<"not pinned");
	pin_page = true;
	}

	// aquire lock only if lock is requested
	if (lmode != NL) {
	DBG(<<"acquiring lock");
	W_DO_GOTO(err, lm->lock(rid, lmode, t_long, WAIT_SPECIFIED_BY_XCT));
	DBG(<<"lock is acquired");
	_lmode = lmode;
	} else {
	// we trust the caller and therefore can do this
	if (_lmode == NL) _lmode = SH;
	}
	w_assert3(_lmode > NL); 

	if (pin_page) {
	DBGTHRD(<<"pin");
	W_DO_GOTO(err, pfi->locate_page(rid, _page(), latch_mode )); // replace lock_to_latch(_lmode) with latch_mode as in pin.cpp
	INC_TSTAT(rec_pin_cnt);
	// getting new page, so update page info
	new_page_info();
#ifdef SANITY_CHECK
	_page().sanity();
#endif
	}

	if (rid.slot!=EOP)
	   W_DO(_get_rec(rid.slot, is_next));

	_flags = pin_rec_pinned;
	_rid = rid;

/* success: */
	return RCOK;
  
failure:
	if (pin_page) {
	_page().unfix();  
	INC_TSTAT(rec_unpin_cnt);

	}
	_flags = pin_empty;
	return err;
}

rc_t pin_pprec_i::_get_rec(const slotid_t& rno, bool is_next)
{
	if (rno > 0 && is_next==true)
	{
		W_DO(_page().get_next_rec(rno, _prec, _ifields));
	}
	else
	{
		W_DO(_page().get_rec(rno, _prec, _ifields));
	}
	return RCOK;
}

rc_t pin_prec_i::_get_rec(const slotid_t& rno, bool is_next)
{
	if (rno > 0 && is_next==true)
	{
		W_DO(_page().get_next_rec(rno, _prec));
	}
	else
	{
		W_DO(_page().get_rec(rno, _prec));
	}
	return RCOK;
}

pin_pfield_i::~pin_pfield_i()
{ 
	unpin();
	// unpin() actuall calls unfix on these pages and unfix does
	// everything the destructor does, but to be safe we still call
	// the destructor
	_page().destructor();
}

rc_t pin_pfield_i::pin(const rid_t rid, lock_mode_t lmode, bool is_next)
{
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(pin_pfield_i::pin, in_xct, read_only, 2);
	if (lmode != SH && lmode != UD && lmode != EX && lmode != NL)
	return RC(eBADLOCKMODE);
	W_DO(_pin(rid, lmode, is_next));  //, serial_t::null));
	return RCOK;
}

void pin_pfield_i::unpin()
{
	if (pinned()) {
	_page().unfix();  
	_flags = pin_empty;

	INC_TSTAT(rec_unpin_cnt);

	}
}

bool 
pin_pfield_i::is_mine() const
{
	bool	mine = false;
	bool	h_others = false;


	if( _page().is_fixed())  {
	if( _page().pinned_by_me()) {
		mine = true;
	} else {
		h_others = true;
	}
	}
	if(h_others && mine) {
	// Had better not have data page owned by me
	// and header page owned by others
	W_FATAL(eINTERNAL);
	}

	// returns false if not fixed at all
	return mine;
}

void pin_pfield_i::set_ref_bit(int value)
{
	if (pinned())
		_page().set_ref_bit(value);  
}

const char* pin_pfield_i::page_data()
{
	if (!pinned()) return NULL;
	return (const char*) &(_page().persistent_part());
}

void pin_pfield_i::_init_constructor()
{
	//  just make sure _page_alias is big enough
	w_assert3(sizeof(_page_alias) >= sizeof(pfile_p));

	_flags = pin_empty;
	_lmode = NL; 
	new (_page_alias) pfile_p();
}

rc_t pin_pfield_i::_pin(const rid_t rid, lock_mode_t lmode, bool is_next)
{
	rc_t   err;	// must use rather than rc_t due to HP_CC_BUG_2
	bool pin_page = false;	// true indicates page needs pinning

	w_assert3(lmode == SH || lmode == EX || lmode == UD ||
		lmode == NL /*for scan.cpp*/ );

	DBGTHRD(<<"enter _pin");
	if (pinned()) {
	DBG(<<"pinned");
   
	/*
	 * If the page for the new record is not the same as the
	 * old (or if we need to get a lock),
	 * then unpin the old and get the new page.
	 * If we need to get the lock, then we must unfix since
	 * we may block on the lock.  We may want to do something
	 * like repin where we try to lock with 0-timeout and
	 * only if we fail do we unlatch.
	 */
	if (rid.pid != _rid.pid || lmode != NL) {
		_page().unfix();  
		pin_page = true;
		INC_TSTAT(rec_unpin_cnt);
	}
	} else {
	DBG(<<"not pinned");
	pin_page = true;
	}

	// aquire lock only if lock is requested
	if (lmode != NL) {
	DBG(<<"acquiring lock");
	W_DO_GOTO(err, lm->lock(rid, lmode, t_long, WAIT_SPECIFIED_BY_XCT));
	DBG(<<"lock is acquired");
	_lmode = lmode;
	} else {
	// we trust the caller and therefore can do this
	if (_lmode == NL) _lmode = SH;
	}
	w_assert3(_lmode > NL); 

	if (pin_page) {
	DBGTHRD(<<"pin");
	W_DO_GOTO(err, pfi->locate_page(rid, _page(), pin_i::lock_to_latch(_lmode)));
	INC_TSTAT(rec_pin_cnt);
	// getting new page, so update page info
	new_page_info();
#ifdef SANITY_CHECK
	_page().sanity();
#endif
	}

	if (rid.slot>0 && is_next==true)
	{
// cerr << "Calling get_next_field with rid.slot " << rid.slot << " for field " << _pfield.fno << " with size " << _pfield.field_size << endl;
	get_next_field();
	}
	else
	{
// cerr << "Calling get_field with rid.slot " << rid.slot << " for field " << _pfield.fno << " with size " << _pfield.field_size << endl;
	get_field();
	}

	_flags = pin_rec_pinned;
	_rid = rid;

/* success: */
	return RCOK;
  
failure:
	if (pin_page) {
	_page().unfix();  
	INC_TSTAT(rec_unpin_cnt);

	}
	_flags = pin_empty;
	return err;
}

