/* ------------------------------------------------------------------------------- */
/* -- Copyright (c) 2011, 2012 Databases and Distributed Systems Department,    -- */
/* -- Technical University of Darmstadt, subject to the terms and conditions    -- */
/* -- given in the file COPYRIGHT.  All Rights Reserved.                        -- */
/* ------------------------------------------------------------------------------- */
/* -- Ilia Petrov, Todor Ivanov, Veselin Marinov                                -- */
/* ------------------------------------------------------------------------------- */

#include "w_defines.h"
#define SM_SOURCE
#define HPL_REC_PIN_C

#ifdef __GNUG__
#pragma implementation "pin.h"
#endif

#include <new> //#include <new.h>
#include <sm_int_4.h>
#include <pin.h>
#include <hpl_rec_pin.h>
#include <hpl_p.h>
#include <hpl_m.h>



pin_hpl_rec_i::~pin_hpl_rec_i()
{ 
	unpin();
	// unpin() actuall calls unfix on these pages and unfix does
	// everything the destructor does, but to be safe we still call
	// the destructor
	_page().destructor();
}

rc_t pin_hpl_rec_i::pin(const rid_t rid,lock_mode_t lmode, bool is_next)
{
	// no more used in shoreMT
	//// adding the xct_constraint_t
	//SM_PROLOGUE_RC(pin_hpl_rec_i::pin, in_xct, read_only, 2);
	//if (lmode != SH && lmode != UD && lmode != EX && lmode != NL)
	//return RC(eBADLOCKMODE);
	//W_DO(_pin(rid, lmode, is_next));  //, serial_t::null));

	return pin(rid, lmode, pin_i::lock_to_latch(lmode), is_next);
}

rc_t pin_hpl_rec_i::pin(const rid_t& rid, lock_mode_t lmode, latch_mode_t latch_mode, bool is_next)
{
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(pin_hpl_rec_i::pin, in_xct, read_only, 2);
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

rc_t pin_hpl_rec_i::pin_get_field(const rid_t& rid, flid_t fieldNumber, lock_mode_t lmode, latch_mode_t latch_mode)
{
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(pin_hpl_rec_i::pin_get_field, in_xct, read_only, 2);

	if (lmode!=SH && lmode!=UD && lmode!=EX && lmode!=NL)
		return RC(eBADLOCKMODE);

		rc_t err; // in HPL reverted to rc_t - must use rather than rc_t due to HP_CC_BUG_2
		bool pin_page = false; // true indicates page needs pinning

		w_assert3(lmode == SH || lmode == EX || lmode == UD ||
				lmode == NL /*for scan.cpp*/);

		DBGTHRD(<<"enter _pin");
		if (pinned())
		{
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
			if (rid.pid != _rid.pid || lmode != NL)
			{
				_page().unfix();
				pin_page = true;
				INC_TSTAT(rec_unpin_cnt);

			}
		}
		else
		{
			DBG(<<"not pinned");
			pin_page = true;
		}

		// aquire lock only if lock is requested
		if (lmode != NL)
		{
			DBG(<<"acquiring lock");
			W_DO_GOTO(err, lm->lock(rid, lmode, t_long, WAIT_SPECIFIED_BY_XCT)); DBG(<<"lock is acquired");
			_lmode = lmode;
		}
		else
		{
			// we trust the caller and therefore can do this
			if (_lmode == NL)
				_lmode = SH;
		}

		w_assert3(_lmode > NL);

		if (pin_page)
		{
			DBGTHRD(<<"pin");
			W_DO_GOTO(err, hpl_man->locate_page(rid, _page(), latch_mode )); // replace lock_to_latch(_lmode) with latch_mode as in pin.cpp
			INC_TSTAT(rec_pin_cnt);
			// getting new page, so update page info
			//update_page_info();
		}

		if (rid.slot != EOP)
		{
			_hpl_field.fieldNumber = fieldNumber;
			_hpl_field.fieldSize = _page().getFieldSize(fieldNumber);

			if(_hpl_rec.numberOfFixedSizeFields > fieldNumber )
			{	// fixSizeField
				_hpl_field.isFixedSize	= true;
				_page().get_ffield(_rid.slot, _hpl_field);
			}
			else
			{	// varSizeField
				_hpl_field.isFixedSize	= false;
				_page().get_vfield(_rid.slot, _hpl_field);
			}
		}

		_flags = pin_rec_pinned;
		_rid = rid;

		/* success: */
		return RCOK;

		failure: if (pin_page)
		{
			_page().unfix();
			INC_TSTAT(rec_unpin_cnt);

		}
		_flags = pin_empty;


	return err;
}

rc_t pin_hpl_rec_i::pin_update_field(const rid_t& rid, flid_t fieldNumber, const char* data, fieldSize_t size, lock_mode_t lmode, latch_mode_t latch_mode)
{
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(pin_hpl_rec_i::pin_update_field, in_xct, read_write, 0);

	if (lmode!=SH && lmode!=UD && lmode!=EX && lmode!=NL)
		return RC(eBADLOCKMODE);

		rc_t err; // in HPL reverted to rc_t - must use rather than rc_t due to HP_CC_BUG_2
		bool pin_page = false; // true indicates page needs pinning

		w_assert3(lmode == SH || lmode == EX || lmode == UD ||
				lmode == NL /*for scan.cpp*/);

		DBGTHRD(<<"enter _pin");
		if (pinned())
		{
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
			if (rid.pid != _rid.pid || lmode != NL)
			{
				_page().unfix();
				pin_page = true;
				INC_TSTAT(rec_unpin_cnt);

			}
		}
		else
		{
			DBG(<<"not pinned");
			pin_page = true;
		}

		// aquire lock only if lock is requested
		if (lmode != NL)
		{
			DBG(<<"acquiring lock");
			W_DO_GOTO(err, lm->lock(rid, lmode, t_long, WAIT_SPECIFIED_BY_XCT)); DBG(<<"lock is acquired");
			_lmode = lmode;
		}
		else
		{
			// we trust the caller and therefore can do this
			if (_lmode == NL)
				_lmode = SH;
		}

		w_assert3(_lmode > NL);

		if (pin_page)
		{
			DBGTHRD(<<"pin");
			W_DO_GOTO(err, hpl_man->locate_page(rid, _page(), latch_mode )); // replace lock_to_latch(_lmode) with latch_mode as in pin.cpp
			INC_TSTAT(rec_pin_cnt);
			// getting new page, so update page info
			//update_page_info();
		}

		if (rid.slot != EOP)
		{
			// update the field
			W_DO(_page().update_field(rid.slot, fieldNumber, data, size));
		}

		_flags = pin_rec_pinned;
		_rid = rid;

		/* success: */
		return RCOK;

		failure: if (pin_page)
		{
			_page().unfix();
			INC_TSTAT(rec_unpin_cnt);

		}
		_flags = pin_empty;

	return err;
}

rc_t pin_hpl_rec_i::pin_update_rec(const rid_t& rid,const hpl_record_t& record, const flid_t* ifields, lock_mode_t lmode, latch_mode_t latch_mode)
{
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(pin_hpl_rec_i::pin_update_field, in_xct, read_write, 0);

	if (lmode!=SH && lmode!=UD && lmode!=EX && lmode!=NL)
		return RC(eBADLOCKMODE);

		rc_t err; // in HPL reverted to rc_t - must use rather than rc_t due to HP_CC_BUG_2
		bool pin_page = false; // true indicates page needs pinning

		w_assert3(lmode == SH || lmode == EX || lmode == UD ||
				lmode == NL /*for scan.cpp*/);

		DBGTHRD(<<"enter _pin");
		if (pinned())
		{
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
			if (rid.pid != _rid.pid || lmode != NL)
			{
				_page().unfix();
				pin_page = true;
				INC_TSTAT(rec_unpin_cnt);

			}
		}
		else
		{
			DBG(<<"not pinned");
			pin_page = true;
		}

		// aquire lock only if lock is requested
		if (lmode != NL)
		{
			DBG(<<"acquiring lock");
			W_DO_GOTO(err, lm->lock(rid, lmode, t_long, WAIT_SPECIFIED_BY_XCT)); DBG(<<"lock is acquired");
			_lmode = lmode;
		}
		else
		{
			// we trust the caller and therefore can do this
			if (_lmode == NL)
				_lmode = SH;
		}

		w_assert3(_lmode > NL);

		if (pin_page)
		{
			DBGTHRD(<<"pin");
			W_DO_GOTO(err, hpl_man->locate_page(rid, _page(), latch_mode )); // replace lock_to_latch(_lmode) with latch_mode as in pin.cpp
			INC_TSTAT(rec_pin_cnt);
			// getting new page, so update page info
			//update_page_info();
		}

		if (rid.slot != EOP)
		{
			// update record
			W_DO(_page().update_record(rid.slot, record, ifields));
		}

		_flags = pin_rec_pinned;
		_rid = rid;

		/* success: */
		return RCOK;

		failure: if (pin_page)
		{
			_page().unfix();
			INC_TSTAT(rec_unpin_cnt);

		}
		_flags = pin_empty;

	return err;
}


void pin_hpl_rec_i::unpin()
{
	if (pinned()) {
	_page().unfix();  
	_flags = pin_empty;

	INC_TSTAT(rec_unpin_cnt);
	}
}

bool pin_hpl_rec_i::is_mine() const
{
	bool mine = false;
	bool h_others = false;

	if (_page().is_fixed()) {
		if (_page().pinned_by_me()) {
			mine = true;
		} else {
			h_others = true;
		}
	}
	if (h_others && mine) {
		// Had better not have data page owned by me
		// and header page owned by others
		W_FATAL(eINTERNAL);
	}

	// returns false if not fixed at all
	return mine;
}

void pin_hpl_rec_i::set_ref_bit(int value)
{
	if (pinned())
		_page().set_ref_bit(value);  
}

const char* pin_hpl_rec_i::page_data()
{
	if (!pinned()) return NULL;
	return (const char*) &(_page().persistent_part());
}

void pin_hpl_rec_i::_init_constructor()
{
	//  just make sure _page_alias is big enough
	w_assert3(sizeof(_page_alias) >= sizeof(hpl_p));

	_flags = pin_empty;
	//_lrid.lvid = lvid_t::null;
	//_hpl_field = NULL;
	hpl_record_t _hpl_rec();
	_lmode = NL; 
	new (_page_alias) hpl_p();
}

rc_t pin_hpl_rec_i::_get_rec(const slotid_t& rno, bool is_next)
{
	if (rno > 0 && is_next == true) {
		W_DO(_page().get_next_rec(rno, _hpl_rec));
	} else {
		W_DO(_page().get_rec(rno, _hpl_rec));
	}
	return RCOK;
}

void  pin_hpl_rec_i::update_page_info()
{


//	if (_hpl_rec.fieldSize==NULL) // totally uninitialized
	if((_hpl_rec.numberOfFields != _page().getNumberOfFields()) || (_hpl_rec.numberOfFixedSizeFields != _page().getNumberOfFixedFields()))
	{

		//_hpl_rec.~hpl_record_t();
		// hpl_record_t _hpl_rec(_page().getNumberOfFields(),_page().getNumberOfFixedFields());
		_hpl_rec.numberOfFields = _page().getNumberOfFields();
		_hpl_rec.numberOfFixedSizeFields = _page().getNumberOfFixedFields();
		_hpl_rec.fieldSize = new fieldSize_t[_hpl_rec.numberOfFields];
		_hpl_rec.data = new char*[_hpl_rec.numberOfFields];
		flid_t i;
		for (i = 0; i < _hpl_rec.numberOfFields; ++i)
		{
			_hpl_rec.fieldSize[i] = _page().getFieldSize(i);
			//_hpl_rec.data[i] = new char[_page().getFieldSize(i)];
			//memcpy(data[i], record.data[i], fieldSize[i]);
			//_hpl_rec.data[i] = NULL;
		}
	}
	/*else
	{
		for (int f = 0; f < _hpl_rec.numberOfFields; ++f) {
			//delete _hpl_rec.data[f];
			_hpl_rec.data[f] = NULL;
		}
	}*/

}

rc_t pin_hpl_rec_i::_pin(const rid_t rid, lock_mode_t lmode, latch_mode_t latch_mode, bool is_next)
{
	rc_t err; // in HPL reverted to rc_t - must use rather than rc_t due to HP_CC_BUG_2
	bool pin_page = false; // true indicates page needs pinning

	w_assert3(lmode == SH || lmode == EX || lmode == UD ||
			lmode == NL /*for scan.cpp*/);

	DBGTHRD(<<"enter _pin");
	if (pinned())
	{
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
		if (rid.pid != _rid.pid || lmode != NL)
		{
			_page().unfix();
			pin_page = true;
			INC_TSTAT(rec_unpin_cnt);

		}
	}
	else
	{
		DBG(<<"not pinned");
		pin_page = true;
	}

	// aquire lock only if lock is requested
	if (lmode != NL)
	{
		DBG(<<"acquiring lock");
		W_DO_GOTO(err, lm->lock(rid, lmode, t_long, WAIT_SPECIFIED_BY_XCT));
		DBG(<<"lock is acquired");
		_lmode = lmode;
	}
	else
	{
		// we trust the caller and therefore can do this
		if (_lmode == NL)
			_lmode = SH;
	}

	w_assert3(_lmode > NL);

	if (pin_page)
	{
		DBGTHRD(<<"pin");
		W_DO_GOTO(err, hpl_man->locate_page(rid, _page(), latch_mode )); // replace lock_to_latch(_lmode) with latch_mode as in pin.cpp
#if HPL_DEBUG>1
		cout<<"Rid: "<< rid.slot <<" pageId: "<< _page().pid() <<endl;

#endif
		INC_TSTAT(rec_pin_cnt);
		// getting new page, so update page info
		update_page_info();
	}

	if (rid.slot != EOP)
	{
		W_DO(_get_rec(rid.slot, is_next));
	}

	_flags = pin_rec_pinned;
	_rid = rid;

	/* success: */
	return RCOK;

	failure: if (pin_page)
	{
		_page().unfix();
		INC_TSTAT(rec_unpin_cnt);

	}
	_flags = pin_empty;
	return err;
}

void  pin_p_hpl_rec_i::update_page_info()
{
	if (_hpl_rec.numberOfFixedSizeFields==0)
	{
		flid_t i;
	for (i=0; i<_hpl_rec.numberOfFields; ++i)
	{
		_hpl_rec.fieldSize[i]=_page().getFieldSize(_ifields[i]);
		if (_ifields[i]<_page().getNumberOfFixedFields())
			_hpl_rec.numberOfFixedSizeFields++;
		}
	}
}

rc_t pin_p_hpl_rec_i::_get_rec(const slotid_t& rno, bool is_next)
{
	if (rno > 0 && is_next==true)
	{
		//W_DO(_page().get_next_rec(rno, _hpl_rec, _ifields));
		W_DO(_page().get_rec(rno, _hpl_rec, _ifields));
	}
	else
	{
		W_DO(_page().get_rec(rno, _hpl_rec, _ifields));
	}
	return RCOK;
}



pin_hpl_field_i::~pin_hpl_field_i()
{ 
	unpin();
	// unpin() actuall calls unfix on these pages and unfix does
	// everything the destructor does, but to be safe we still call
	// the destructor
	_page().destructor();
}

rc_t pin_hpl_field_i::pin(const rid_t rid, lock_mode_t lmode, bool is_next)
{
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(pin_pfield_i::pin, in_xct, read_only, 2);
	if (lmode != SH && lmode != UD && lmode != EX && lmode != NL)
	return RC(eBADLOCKMODE);
	W_DO(_pin(rid, lmode, is_next));  //, serial_t::null));
	return RCOK;
}

rc_t pin_hpl_field_i::pin(const rid_t rid, flid_t fieldNumber, lock_mode_t lmode, bool is_next)
{
	_hpl_field.fieldNumber = fieldNumber;
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(pin_pfield_i::pin, in_xct, read_only, 2);
	if (lmode != SH && lmode != UD && lmode != EX && lmode != NL)
	return RC(eBADLOCKMODE);
	W_DO(_pin(rid, lmode, is_next));  //, serial_t::null));
	return RCOK;
}

void pin_hpl_field_i::unpin()
{
	if (pinned()) {
	_page().unfix();  
	_flags = pin_empty;

	INC_TSTAT(rec_unpin_cnt);

	}
}

bool pin_hpl_field_i::is_mine() const
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

void pin_hpl_field_i::set_ref_bit(int value)
{
	if (pinned())
		_page().set_ref_bit(value);  
}

const char* pin_hpl_field_i::page_data()
{
	if (!pinned()) return NULL;
	return (const char*) &(_page().persistent_part());
}

void pin_hpl_field_i::_init_constructor()
{
	//  just make sure _page_alias is big enough
	w_assert3(sizeof(_page_alias) >= sizeof(hpl_p));

	_flags = pin_empty;
	_lmode = NL; 
	new (_page_alias) hpl_p();
}

rc_t pin_hpl_field_i::_pin(const rid_t rid, lock_mode_t lmode, bool is_next)
{
	rc_t err; // must use rather than rc_t due to HP_CC_BUG_2
	bool pin_page = false; // true indicates page needs pinning

	w_assert3(lmode == SH || lmode == EX || lmode == UD ||
			lmode == NL /*for scan.cpp*/);

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
		W_DO_GOTO(err, lm->lock(rid, lmode, t_long, WAIT_SPECIFIED_BY_XCT)); DBG(<<"lock is acquired");
		_lmode = lmode;
	} else {
		// we trust the caller and therefore can do this
		if (_lmode == NL)
			_lmode = SH;
	} w_assert3(_lmode > NL);

	if (pin_page) {
		DBGTHRD(<<"pin");
		W_DO_GOTO(err, hpl_man->locate_page(rid, _page(), pin_i::lock_to_latch(_lmode)));
		INC_TSTAT(rec_pin_cnt);
		// getting new page, so update page info
		update_page_info();
	}

	if (rid.slot > 0 && is_next == true)
	{
		// cerr << "Calling get_next_field with rid.slot " << rid.slot << " for field " << _pfield.fno << " with size " << _pfield.field_size << endl;
		get_next_field();
	} else
	{
		// cerr << "Calling get_field with rid.slot " << rid.slot << " for field " << _pfield.fno << " with size " << _pfield.field_size << endl;
			get_field();
	}

	_flags = pin_rec_pinned;
	_rid = rid;

	/* success: */
	return RCOK;

	failure: if (pin_page) {
		_page().unfix();
		INC_TSTAT(rec_unpin_cnt);

	}
	_flags = pin_empty;
	return err;
}

