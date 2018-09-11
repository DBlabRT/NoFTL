/* ------------------------------------------------------------------------------- */
/* -- Copyright (c) 2011, 2012 Databases and Distributed Systems Department,    -- */
/* -- Technical University of Darmstadt, subject to the terms and conditions    -- */
/* -- given in the file COPYRIGHT.  All Rights Reserved.                        -- */
/* ------------------------------------------------------------------------------- */
/* -- Ilia Petrov, Todor Ivanov, Veselin Marinov                                -- */
/* ------------------------------------------------------------------------------- */
#include "w_defines.h"


#define SM_SOURCE
#define SM_HPL_C

#include "w.h"
#include "option.h"
#include "sm_int_4.h"
#include "btcursor.h"
#include "lgrec.h"
#include "device.h"
#include "app_support.h"
#include "sm.h"
#include "hpl_m.h"

/*--------------------------------------------------------------*
 *  ss_m::create_file()											*
 *--------------------------------------------------------------*/
rc_t
ss_m::create_file( vid_t vid, stid_t& fid, store_property_t property, const hpl_record_t& record, shpid_t cluster_hint)
{
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(ss_m::create_file, in_xct, read_write, 0);

	W_DO(_create_file(vid, fid, property, record, cluster_hint));

	return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_create_file()										*
 *--------------------------------------------------------------*/
rc_t
ss_m::_create_file(	vid_t vid, stid_t&	fid, store_property_t property, const hpl_record_t& record, shpid_t cluster_hint)
{
	FUNC(ss_m::_create_file);
	DBG( << "Attempting to create a file on volume " << vid.vol );

	store_flag_t st_flag = _make_store_flag(property);
	extnum_t first_extent = extnum_t(cluster_hint? cluster_hint / ss_m::ext_sz : 0);

	DBGTHRD(<<"about to create a store starting about extent " << first_extent);
	W_DO( io->create_store(vid, 100/*unused*/, st_flag, fid, first_extent) );

	DBGTHRD(<<"created store " << fid);

	/*
	// create a store for holding large record pages
	// Don't allocate any extents for it yet (num_exts == 0)
	// unless the cluster hint is used.
	// NB: disabled: always allocates 1 extent -- otherwise
	// asserts fail elsewhere
	*/
	stid_t lg_stid;
	W_DO( io->create_store(vid, 100/*unused*/, st_flag, lg_stid, first_extent, 1) );

	DBGTHRD(<<"created 2nd store (for lg recs) " << lg_stid);

	// Note: theoretically, some other thread could destroy
	// 	     the above stores before the following lock request
	// 	     is granted.  The only forseable way for this to
	//	     happen would be due to a bug in a vas causing
	//       it to destroy the wrong store.  We make no attempt
	//       to prevent this.
	W_DO(lm->lock(fid, EX, t_long, WAIT_SPECIFIED_BY_XCT));

	DBGTHRD(<<"locked " << fid);


	lpid_t first;
	W_DO( hpl_man->create(fid, first, record) );
	DBGTHRD(<<"locked &created -- put in store directory: " << fid);

	// logical_id obsolete in shoreMT
	sinfo_s sinfo(fid.store, t_file, 100/*unused*/, t_bad_ndx_t, t_cc_none/*not used*/, first.page, 0, 0);

	sinfo.set_large_store(lg_stid.store);
	// stpgid is obsolete in shoreMT
	//stpgid_t stpgid(fid);
	W_DO( dir->insert(fid, sinfo) );

//	sdesc_t* sd;
//	W_DO( dir->access(fid, sd, IX) );
//	sd->set_last_pid(first.page);

	DBGTHRD(<<"inserted " << fid.store);

	return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::create_rec()											*
 *--------------------------------------------------------------*/
rc_t
ss_m::create_rec( const stid_t& fid, const hpl_record_t& record, rid_t&	new_rid)
{
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(ss_m::create_rec, in_xct, read_write, 0);
	//RES_SMSCRIPT(<<"create_rec "<<fid<<" "<< record);
	W_DO(_create_rec(fid, record, new_rid));
	return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_create_rec()											*
 *--------------------------------------------------------------*/
rc_t
ss_m::_create_rec( const stid_t& fid, const hpl_record_t& record, rid_t& rid)
{
	FUNC(ss_m::_create_rec);
	sdesc_t* sd;
	W_DO( dir->access(fid, sd, IX) );

	//DBG( << "create in fid " << fid << " data.size " << data.size());

	W_DO( hpl_man->create_rec(fid, record, *sd, rid) );

	return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::destroy_hpl_rec()                                     *
 *--------------------------------------------------------------*/
rc_t
ss_m::destroy_hpl_rec(const rid_t& rid)
{
	SM_PROLOGUE_RC(ss_m::destroy_hpl_rec, in_xct,read_write, 0);
	DBG(<<"destroy_hpl_rec " <<rid);

	W_DO(_destroy_hpl_rec(rid));

	return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_destroy_hpl_rec()                                    *
 *--------------------------------------------------------------*/
rc_t
ss_m::_destroy_hpl_rec(const rid_t& rid)
{
	DBG(<<"_destroy_rec " << rid);

	W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));

	W_DO(hpl_man->destroy_rec(rid));

	return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::update_hpl_rec()                                      *
 *--------------------------------------------------------------*/
rc_t
ss_m::update_hpl_field(const rid_t& rid, const flid_t fieldNumber, const char* data, const Size2 dataSize)
{
#if FILE_LOG_COMMENT_ON
	{
		w_ostrstream s;
		s << "update_rec " << rid;
		W_DO(log_comment(s.c_str()));
	}
#endif

	SM_PROLOGUE_RC(ss_m::update_hpl_rec, in_xct,read_write, 0);
	W_DO(_update_hpl_field(rid, fieldNumber, data, dataSize));

	return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_update_hpl_rec()                                     *
 *--------------------------------------------------------------*/
rc_t
ss_m::_update_hpl_field(const rid_t& rid, const flid_t fieldNumber, const char* data, const Size2 dataSize)
{
	W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
	W_DO(hpl_man->update_field(rid, fieldNumber, data, dataSize));

	return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::read_hpl_rec()                                     	*
 *--------------------------------------------------------------*/
rc_t
ss_m::read_hpl_rec(const rid_t& rid, hpl_record_t& record)
{
	W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
	W_DO(hpl_man->read_record(rid, record));

	return RCOK;
}
