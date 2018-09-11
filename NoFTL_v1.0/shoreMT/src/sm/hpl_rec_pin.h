/* ------------------------------------------------------------------------------- */
/* -- Copyright (c) 2011, 2012 Databases and Distributed Systems Department,    -- */
/* -- Technical University of Darmstadt, subject to the terms and conditions    -- */
/* -- given in the file COPYRIGHT.  All Rights Reserved.                        -- */
/* ------------------------------------------------------------------------------- */
/* -- Ilia Petrov, Todor Ivanov, Veselin Marinov                                -- */
/* ------------------------------------------------------------------------------- */

#ifndef HPL_REC_PIN_H
#define HPL_REC_PIN_H

#include "w_defines.h"
#include "sm_s.h"
#include "hpl_rec.h"
#include "pin.h"
#include "hpl_p.h"

// See comments in pin.h for details on how this works

class pin_hpl_rec_i : public smlevel_top {
	friend class scan_hpl_p_i;
public:
	enum flags_t { 
	pin_empty        = 0x0,
	pin_rec_pinned        = 0x01            // ,
	// pin_hdr_only        = 0x02            // , 
	// pin_separate_data    = 0x04        // ,
	// pin_lg_data_pinned    = 0x08  // large data page is pinned
	};
	
	NORET    pin_hpl_rec_i() {_init_constructor();}

	virtual NORET    ~pin_hpl_rec_i();

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

	rc_t 	pin_get_field(
			const rid_t& 		rid,
			flid_t 				fieldNumber,
			lock_mode_t 		lmode,
			latch_mode_t 		latch_mode);

	rc_t	pin_update_field(
			const rid_t& 		rid,
			flid_t 				fieldNumber,
			const char* 		data,
			fieldSize_t 		size,
			lock_mode_t 		lmode,
			latch_mode_t 		latch_mode);

	rc_t	pin_update_rec(
			const rid_t& 		rid,
			const hpl_record_t& record,
			const flid_t* 		ifields,
			lock_mode_t 		lmode,
			latch_mode_t 		latch_mode);

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
	bool    pinned() const  { return _flags & pin_rec_pinned; }

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

	const char* 	 page_data();

	const hpl_record_t& hpl_record() const {return _hpl_rec; }

	const hpl_field_t&  hpl_field() const {return _hpl_field; }

	// lpid_t     page_containing(smsize_t offset, smsize_t& start_byte) const;
	bool			isValidRec()
	{
		//cout <<"Rec slot is: "<< rid().slot << endl;
		return _page().isSetGhostbit(rid().slot);
	}

protected:

	void			_init_constructor(); // companion to constructor
	rc_t			_pin_data();
	// const char* _body_large();
	rc_t			_pin(const rid_t rid, lock_mode_t m, latch_mode_t latch_mode, bool is_next = true);
	virtual rc_t	_get_rec(const slotid_t& rno, bool is_next);
	hpl_p*			_get_page() const { return pinned() ? (hpl_p*)_page_alias : 0;}

	rid_t			_rid;
	// not used in shoreMT
	//lrid_t    	_lrid;  // snapped logical ID if pinned this way
	hpl_record_t    _hpl_rec;
	hpl_field_t    	_hpl_field;
	uint4_t     	_flags;  // this cannot be flags_t since it uses
	lock_mode_t 	_lmode;  // current locked state

	/*
	 *    Originally pin_hpl_rec_i contained the _page and _page data
	 *    members commented out below.  This required that users #include
	 *    sm_int.h (ie. the whole world), generating large .o's.
	 *    Instead, we have the corresponding "alias" byte arrays and
	 *    member functions which cast these to the correct page type.
	 *    Only prec_pin.cpp uses these functions.  This greatly reduces the
	 *    number of .h files users need to include.
	 *
	 *  Asserts in pin_hpl_rec_i constructors verify that the _alias members
	 *  are large enough to hold pfile_p.
	 */
	char        _page_alias[40]; // see comment above 4 reason 4 alias
	hpl_p&    	_page() const  {return *(hpl_p*)_page_alias;}

	// Upon new page, readjust record info
	virtual void        update_page_info();
	// disable
	NORET    pin_hpl_rec_i(const pin_hpl_rec_i&);
	NORET    pin_hpl_rec_i& operator=(const pin_hpl_rec_i&);

};
 
class pin_p_hpl_rec_i : public pin_hpl_rec_i {
	friend class scan_hpl_rec_i;
private:
	flid_t* _ifields;
public:
	pin_p_hpl_rec_i(const flid_t nifi, flid_t* ifi): pin_hpl_rec_i()
	{
		_hpl_rec.numberOfFields = (uint1_t) nifi;
		_hpl_rec.fieldSize = new fieldSize_t[_hpl_rec.numberOfFields];
		_hpl_rec.data = new char*[_hpl_rec.numberOfFields];
		_ifields = ifi;
	}
	void update_page_info();
	rc_t _get_rec(const slotid_t& rno, bool is_next);
};

class pin_hpl_field_i : public smlevel_top {
	friend class scan_hpl_field_i;
public:
	enum flags_t { 
	pin_empty        		= 0x0,
	pin_rec_pinned        	= 0x01
	};
	
	NORET    pin_hpl_field_i() { _init_constructor();}

	virtual NORET    ~pin_hpl_field_i();

	rc_t    pin(
				const rid_t    rid,
				lock_mode_t    lmode = SH,
				bool is_next = true);

	rc_t    pin(
				const rid_t    rid,
				flid_t fieldNumber,
				lock_mode_t    lmode = SH,
				bool is_next = true);
	void    unpin();
	bool    is_mine() const; // only if owning thread
	void    set_ref_bit(int value);
	bool    pinned() const   { return _flags & pin_rec_pinned; }

	const rid_t&     	rid() const {return _rid;}
	const char* 		page_data();
	const hpl_field_t&  hpl_field() const {return _hpl_field; }
	// HPL: keep in mind that rid could be NULL
	bool			isValidField()
	{
		//cout <<"Field is (r,f):"<< rid().slot << "," << _hpl_field.fieldNumber << endl;
		return _page().isSetNullbit(rid().slot, _hpl_field.fieldNumber);
	}

	bool			isValidRec()
	{
		//cout <<"Rec slot is: "<< rid().slot << endl;
		return _page().isSetGhostbit(rid().slot);
	}

	// returns another field of the same record, yeah
	void another_hpl_field(hpl_field_t& hpl_field) { _page().get_field(_rid.slot, hpl_field); }


protected:
	hpl_field_t    	_hpl_field;
	rid_t    		_rid;

	char        	_page_alias[40]; // see comment above 4 reason 4 alias
	hpl_p&    	_page() const {return *(hpl_p*)_page_alias;}

private:
	uint4_t     	_flags;  // this cannot be flags_t since it uses
	lock_mode_t 	_lmode;  // current locked state

	void        	_init_constructor(); // companion to constructor
	rc_t        	_pin(const rid_t rid, lock_mode_t m, bool is_next = true);
	hpl_p*     		_get_page() const { return pinned() ? (hpl_p*)_page_alias : 0;}
	void    		update_page_info()
	{ 
		//_hpl_field.isNullable = _page().is_nullable(_hpl_field.fno);
		_hpl_field.fieldSize = _page().getFieldSize(_hpl_field.fieldNumber);
	}

	virtual void    get_field() = 0;
	virtual void    get_next_field() = 0;

	// disable
	NORET    pin_hpl_field_i(const pin_hpl_field_i&);
	NORET    pin_hpl_field_i& operator=(const pin_hpl_field_i&);
};

class pin_hpl_ffield_i : public pin_hpl_field_i
{
public:
	NORET    pin_hpl_ffield_i() { _hpl_field.isFixedSize = true; }
	NORET    ~pin_hpl_ffield_i() {}

	void    get_next_field()
	{
		_page().get_next_ffield(_rid.slot+1, _hpl_field);
		//_hpl_field.data += _hpl_field.fieldSize; }
	}
	void    get_field()
	{ 	_page().get_ffield(_rid.slot+1, _hpl_field); }
};

class pin_hpl_vfield_i : public pin_hpl_field_i
{
public:
	NORET    pin_hpl_vfield_i() { _hpl_field.isFixedSize = false; }
	NORET    ~pin_hpl_vfield_i() {}

	void    get_next_field()
	{ _page().get_next_vfield(_rid.slot+1, _hpl_field); }

	void    get_field()
		{ _page().get_vfield(_rid.slot+1, _hpl_field); }
};

#endif
