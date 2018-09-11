/*<std-header orig-src='shore'>

 $Id: smpfile.cpp,v 3.0 2000/09/18 21:05:07 natassa Exp $

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
#define SMPFILE_C

#include "w.h"
#include "option.h"
#include "sm_int_4.h"
#include "btcursor.h"
#include "lgrec.h"
#include "device.h"
#include "app_support.h"
#include "sm.h"
#include "pfile_m.h"

/*--------------------------------------------------------------*
 *  ss_m::create_file()					*
 *--------------------------------------------------------------*/
rc_t
ss_m::create_file(
	vid_t                       vid,
	stid_t&                     fid,
	store_property_t            property,
	const precord_t&    		prec_info,
	shpid_t                     cluster_hint // = 0
)
{
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(ss_m::create_file, in_xct, read_write, 0);
	//DBGTHRD(<<"create_file " <<vid << " " << property << " " << serial );
	// logical_id obsolete in shoreMT 
	W_DO(_create_file(vid, fid, property, prec_info, cluster_hint));
	//DBGTHRD(<<"create_file returns " << fid);
	return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_create_file()					*
 *--------------------------------------------------------------*/
rc_t
ss_m::_create_file(	vid_t				vid, 
					stid_t&				fid,
					store_property_t	property,
					const precord_t&    prec_info,
					shpid_t             cluster_hint // = 0
		   )
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
	W_DO( pfi->create(fid, first, prec_info) );
	DBGTHRD(<<"locked &created -- put in store directory: " << fid);

	// logical_id obsolete in shoreMT
	sinfo_s sinfo(fid.store, t_file, 100/*unused*/, 
	   t_bad_ndx_t, t_cc_none/*not used*/, first.page, 0, 0);
	sinfo.set_large_store(lg_stid.store);
	// stpgid is obsolete in shoreMT
	//stpgid_t stpgid(fid);
	W_DO( dir->insert(fid, sinfo) );

	DBGTHRD(<<"inserted " << fid.store);

	return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::create_rec()						*
 *--------------------------------------------------------------*/
rc_t
ss_m::create_rec(	const stid_t&		fid,
					const precord_t&	prec_info,
					rid_t&				new_rid)
{
	// adding the xct_constraint_t
	SM_PROLOGUE_RC(ss_m::create_rec, in_xct, read_write, 0);
	//RES_SMSCRIPT(<<"create_rec "<<fid<<" "<< prec_info);
	W_DO(_create_rec(fid, prec_info, new_rid));
	return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_create_rec()						*
 *--------------------------------------------------------------*/
rc_t
ss_m::_create_rec(	const stid_t&		fid,
					const precord_t&	prec_info, 
					rid_t&				rid
		 )
{
	FUNC(ss_m::_create_rec);
	sdesc_t* sd;
	W_DO( dir->access(fid, sd, IX) );

	//DBG( << "create in fid " << fid << " data.size " << data.size());

	W_DO( pfi->create_rec(fid, prec_info, *sd, rid) );

	return RCOK;
}

