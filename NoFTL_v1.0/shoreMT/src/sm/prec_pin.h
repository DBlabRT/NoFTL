/*<std-header orig-src='shore' incl-file-exclusion='PREC_PIN_H'>

 $Id: prec_pin.h,v 3.0 2000/09/18 21:05:03 natassa Exp $

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

#ifndef PREC_PIN_H
#define PREC_PIN_H

#include "w_defines.h"
/*  -- do not edit anything above this line --   </std-header>*/

#include "sm_s.h"
#include "prec.h"
#include "pin.h"
#include "pfile_p.h"

// See comments in pin.h for details on how this works

class pin_prec_i : public smlevel_top {
	friend class scan_pfile_i;
public:
	enum flags_t { 
	pin_empty        = 0x0,
	pin_rec_pinned        = 0x01            // ,
	// pin_hdr_only        = 0x02            // , 
	// pin_separate_data    = 0x04        // ,
	// pin_lg_data_pinned    = 0x08  // large data page is pinned
	};
	
	NORET    pin_prec_i() {_init_constructor();}

	virtual NORET    ~pin_prec_i();

	// These methods pin portions of a record beginning at start
	// the actual location pinned (returned by start_byte), may
	// be <= start.
	// (They are smart enough not to unfix/refix the page
	// if the prior state has a record pinned on the same page
	// as the indicated record.)
	rc_t    pin(
			const rid_t			rid,
			lock_mode_t         lmode = SH,
			bool				is_next = true);
	rc_t    pin(
			const rid_t &		rid,
			lock_mode_t         lock_mode,
			latch_mode_t        latch_mode,
			bool				is_next);

	void       unpin();
	bool       is_mine() const; // only if owning thread

	// set_ref_bit sets the reference bit value to use for the buffer
	// frame containing the currently pinned body page when the page
	// is unpinned.  A value of 0 is a "hate" hint indicating that
	// the frame can be reused as soon as necessary.  By default,
	// a value of 1 is used indicating the page will be cached 
	// until at least 1 sweep of the buffer clock hand has passed.
	// Higher values cause the page to remain cached longer.
	void    set_ref_bit(int value);

	// repin is used to efficiently repin a record after its size
	// has been changed, or after it has been unpinned.
	// rc_t        repin(lock_mode_t lmode = SH);

	// get the next range of bytes available to be pinned
	// Parameter eof is set to true if there are no more bytes to pin.
	// When eof is reached, the previously pinned range remains pinned.
	// rc_t        next_bytes(bool& eof); 

	// is something currently pinned
	bool      pinned() const     
			{ return _flags & pin_rec_pinned; }

	// serial_no() and lvid() return the logical ID of the pinned
	// record assuming it was pinned using logical IDs. 
	// NOTE: theses IDs are the "snapped" values -- ie. they
	//       are the volume ID where the record is located and
	//       the record's serial# on that volume.  Therefore, these
	//       may be different than the ones passed in to pin the
	//       record.
	
	// not used in shoreMT
	//const serial_t&  serial_no() const { return _lrid.serial; }
	//const lvid_t&    lvid() const { return _lrid.lvid; }

	const rid_t&     rid() const {return _rid;}

	const char* page_data();

	const precord_t& precord() const {return _prec; }

	// lpid_t     page_containing(smsize_t offset, smsize_t& start_byte) const;

protected:

	void			_init_constructor(); // companion to constructor
	rc_t			_pin_data();
	// const char* _body_large();
	rc_t			_pin(const rid_t rid, lock_mode_t m, latch_mode_t latch_mode, bool is_next = true);
	virtual rc_t	_get_rec(const slotid_t& rno, bool is_next);
	pfile_p*		_get_page() const { return pinned() ? (pfile_p*)_page_alias : 0;}

	rid_t			_rid;
	// not used in shoreMT
	//lrid_t    _lrid;  // snapped logical ID if pinned this way
	precord_t    _prec;
	uint4_t     _flags;  // this cannot be flags_t since it uses
	lock_mode_t _lmode;  // current locked state

	/*
	 *    Originally pin_prec_i contained the _page and _page data
	 *    members commented out below.  This required that users #include
	 *    sm_int.h (ie. the whole world), generating large .o's.
	 *    Instead, we have the corresponding "alias" byte arrays and
	 *    member functions which cast these to the correct page type.
	 *    Only prec_pin.cpp uses these functions.  This greatly reduces the
	 *    number of .h files users need to include.
	 *
	 *  Asserts in pin_prec_i constructors verify that the _alias members
	 *  are large enough to hold pfile_p.
	 */
	char        _page_alias[40]; // see comment above 4 reason 4 alias
	pfile_p&    _page() const
					{return *(pfile_p*)_page_alias;}

	// Upon new page, readjust record info
	virtual void        new_page_info();
	// disable
	NORET    pin_prec_i(const pin_prec_i&);
	NORET    pin_prec_i& operator=(const pin_prec_i&);

};
 
class pin_pprec_i : public pin_prec_i {
	friend class scan_prec_i;
private:
	flid_t* _ifields;
public:
	pin_pprec_i(const flid_t nifi, flid_t* ifi)
		: pin_prec_i()
	{
		_prec.no_fields = (uint1_t)nifi;
	_prec.field_size = new Size2[_prec.no_fields];
	_prec.data=new char*[_prec.no_fields];
	_ifields = ifi;
	}
	void new_page_info();
	rc_t _get_rec(const slotid_t& rno, bool is_next);
};

class pin_pfield_i : public smlevel_top {
	friend class scan_pfield_i;
public:
	enum flags_t { 
	pin_empty        = 0x0,
	pin_rec_pinned        = 0x01
	};
	
	NORET    pin_pfield_i() { _init_constructor();}

	virtual NORET    ~pin_pfield_i();

	rc_t    pin(
	const rid_t        rid,
	lock_mode_t         lmode = SH, 
	bool is_next = true);

	void       unpin();
	bool       is_mine() const; // only if owning thread
	void    set_ref_bit(int value);
	bool      pinned() const     
			{ return _flags & pin_rec_pinned; }

	const rid_t&     rid() const {return _rid;}
	const char* page_data();
	const pfield_t& pfield() const {return _pfield; }

	// returns another field of the same record, yeah
	void another_pfield(pfield_t& pfield)
	{ _page().get_field(_rid.slot, pfield); }

protected:
	pfield_t    _pfield;
	rid_t    _rid;

	char        _page_alias[40]; // see comment above 4 reason 4 alias
	pfile_p&    _page() const
		{return *(pfile_p*)_page_alias;}

private:
	uint4_t     _flags;  // this cannot be flags_t since it uses
	lock_mode_t _lmode;  // current locked state

	void        _init_constructor(); // companion to constructor
	rc_t        _pin(const rid_t rid, lock_mode_t m, bool is_next = true);
	pfile_p*     _get_page() const { return pinned() ? (pfile_p*)_page_alias : 0;}
	void    new_page_info()
	{ 
		_pfield.nullable = _page().is_nullable(_pfield.fno);
		_pfield.field_size = _page().field_size(_pfield.fno);
	}

	virtual void    get_field() = 0;
	virtual void    get_next_field() = 0;

	// disable
	NORET    pin_pfield_i(const pin_pfield_i&);
	NORET    pin_pfield_i& operator=(const pin_pfield_i&);
};

class pin_ffield_i : public pin_pfield_i
{
public:
	NORET    pin_ffield_i() { _pfield.fixed_size = true; }
	NORET    ~pin_ffield_i() {}

	void    get_next_field()
	{ _pfield.data += _pfield.field_size; }

	void    get_field()
	{ _page().get_ffield(_rid.slot+1, _pfield); }
};

class pin_vfield_i : public pin_pfield_i
{
public:
	NORET    pin_vfield_i() { _pfield.fixed_size = false; }
	NORET    ~pin_vfield_i() {}

	void    get_next_field()
	{ _page().get_next_vfield(_rid.slot+1, _pfield); }

	void    get_field()
		{ _page().get_vfield(_rid.slot+1, _pfield); }
};

/*<std-footer incl-file-exclusion='PREC_PIN_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
