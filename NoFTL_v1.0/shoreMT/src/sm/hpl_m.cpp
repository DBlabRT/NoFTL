
#include "w_defines.h"

#define SM_SOURCE
#define HPL_M_C

#include "sm_int_2.h"
#include "lgrec.h"
#include "w_minmax.h"
#include "sm_du_stats.h"

#include "histo.h"
#include "crash.h"

#include "hpl_p.h"
#include "hpl_m.h"

#ifdef EXPLICIT_TEMPLATE
/* Used in sort.cpp, btree_bl.cpp */
template class w_auto_delete_array_t<pfile_p>;
#endif



/*TODO HPL: page filter ??
 * Filter class to determine if we can allocate the given page from
 * the store.
 * Allocating a file page requires that we EX-latch it deep in the
 * io/volume layer so that there will be no race between allocating
 * it and formatting it.
 *
 * So the accept method here does a conditional fix.
 * The reject method unfixes it.
 */
class alloc_hpl_page_filter_t : public alloc_page_filter_t {
private:
	store_flag_t _flags;
	lpid_t       _pid;
	bool         _was_fixed;
	bool         _is_fixed;
	hpl_p&       _page;
	void  		 _reset();
public:
	NORET alloc_hpl_page_filter_t(store_flag_t flg, hpl_p &pg);
	NORET ~alloc_hpl_page_filter_t();
	bool  accept(const lpid_t&);
	void  reject();
	void  check() const;
	bool  accepted() const;
};

NORET alloc_hpl_page_filter_t::alloc_hpl_page_filter_t(
		store_flag_t flg, hpl_p &pg)
	: alloc_page_filter_t(), _flags(flg), _page(pg) { _reset(); }

NORET alloc_hpl_page_filter_t::~alloc_hpl_page_filter_t() {}

void  alloc_hpl_page_filter_t::_reset()
{
	_pid = lpid_t::null;
	_was_fixed = false;
	_is_fixed = false;
}
bool  alloc_hpl_page_filter_t::accept(const lpid_t& pid)
{
	_pid =  pid;
	_was_fixed = _page.is_latched_by_me();

#if W_DEBUG_LEVEL > 4
	{
	w_ostrstream o;
	lsn_t l = lsn_t(0, 1);
	xct_t *xd = xct();
	// Note : formatting a volume gets done outside a tx,
	// so in that case, the lsn_t(0,1) is used.  If DONT_TRUST_PAGE_LSN
	// is turned off, the raw page has lsn_t(0,1) when read from disk
	// for the first time, if, in fact, it's actually read.
	if(xd) {
		l = xd->last_lsn();
		o <<  "tid " << xd->tid() << " last_lsn " << l;
	}
	fprintf(stderr, "accept ? %d %s\n", _pid.page, o.c_str());
	}
#endif

	// Instead of doing the normal conditional_fix, we do _fix
	// so we can bypass the page format; if we do the format,
	// we'll run into problems initializing the lsn on the page, since
	// the assumption is that we have a last_lsn from allocating the
	// page!
	w_rc_t rc = _page.page_p::_fix(true, _pid, _page.t_hpl_p,
			LATCH_EX, 
			_page.t_virgin, 
			_flags,
			false /* ignore_store_id default value */,
			1 /* refbit default value */);

	if(rc.is_error()) {
		// fprintf(stderr, "NOT accept %d\n", _pid.page);
		_reset();
		check();
		INC_TSTAT(fm_alloc_page_reject);
		return false;
	}
	// fprintf(stderr, "accept %d err %d\n", _pid.page, rc.err_num());
	_is_fixed = true;
	check();
	return true;
}

bool  alloc_hpl_page_filter_t::accepted() const { return _is_fixed; }
void  alloc_hpl_page_filter_t::check() const
{
	if(accepted()) {
		/* how can I verify? Let me count the ways ... */
		// maybe not yet formatted w_assert1(_page.pid().page == _pid.page);
		// not yet formatted w_assert1(_page.rsvd_mode());
		w_assert1(_page.is_fixed());
		w_assert1(_page.is_latched_by_me());
		w_assert1(_page.is_mine());
		w_assert1(_page.my_latch()->mode() == LATCH_EX);
	}
}
void  alloc_hpl_page_filter_t::reject()
{
	check();
	if(accepted() && !_was_fixed) {
		_page.unfix();
	}
	_reset();
}

/*********************************************************************
 *
 *  hpl_m::create(  stid_t     stid,
 *                  lpid_t&    first_page
 *                  const precord_t& prec_info)
 *
 *  Create a file with partitioned records.
 *  prec_info contains # of fields, # of fixed fields, and table with sizes
 *
 *********************************************************************/
rc_t
hpl_m::create( stid_t stid, lpid_t& first_page, const hpl_record_t& record)
{

   hpl_p  page;
   DBGTHRD(<<"hpl_m::create create first page in store " << stid);
   W_DO(_alloc_page( stid, lpid_t::eof, first_page, page, true, record));

   w_assert3(page.is_fixed());
   histoid_update_t hu(page);
   hu.update();
   DBGTHRD(<<"hpl_m::create(d) first page is  " << first_page);

   return RCOK;
}

rc_t
hpl_m::create_rec( stid_t fid, const hpl_record_t& record, sdesc_t& sd, rid_t& rid)
{
	hpl_p page;
	W_DO(_create_rec(fid, record, sd, rid, page));

   return RCOK ;
}

rc_t
hpl_m::create_rec_at_end( hpl_p& page, const hpl_record_t& record, sdesc_t& sd, rid_t& rid)
{
	stid_t  fid = sd.stid();
	W_DO(_create_rec (fid, record, sd, rid, page));

	return RCOK;
}

rc_t
hpl_m::read_record( const rid_t& rid, hpl_record_t& rec_data)
{
	hpl_p 	page;
	int i, 	rec_sz=0;
	char* 	tmp;

	W_DO( _locate_page(rid, page, LATCH_SH) );
	hpl_record_t record(page.getNumberOfFields(), page.getNumberOfFixedFields());
	W_DO( page.get_rec(rid.slot, rec_data) );

/*
	for (i=0; i<record.numberOfFields; ++i)
		rec_sz += record.fieldSize[i];
	buf = new char[rec_sz];
	tmp = (char*)buf;
	for (i=0; i<record.numberOfFields; ++i)
	{
		memcpy(tmp, record.data[i], record.fieldSize[i]);
		tmp += record.fieldSize[i];
	}*/


	return RCOK;
}

rc_t
hpl_m::first_page(const stid_t& fid, lpid_t& pid, bool* allocated)
{
   return io->first_page(fid, pid, allocated);
}

rc_t
hpl_m::next_page(lpid_t& pid, bool& eof, bool* allocated)
{
	eof = false;

	rc_t rc = io->next_page(pid, allocated);
	if (rc.is_error())  {
		if (rc.err_num() == eEOF) {
			eof = true;
		} else {
			return RC_AUGMENT(rc);
		}
	}
	return RCOK;
}

rc_t
hpl_m::last_page(const stid_t& fid, lpid_t& pid, bool* allocated)
{
	return io->last_page(fid, pid, allocated, IX);
}

rc_t
hpl_m::_locate_page(const rid_t& rid, hpl_p& page, latch_mode_t mode)
{
	DBGTHRD(<<"hpl_m::_locate_page rid=" << rid);

	/*
	 * pin the page unless it's remote; even if it's remote,
	 * pin if we are using page-level concurrency
	 */
	// PAX integration removes the old shore methods
	//if (cc_alg == t_cc_page || (! rid.pid.is_remote())) {
		W_DO(page.fix(rid.pid, mode));
	//} else {
	//	W_FATAL(eINTERNAL);
	//}

	//w_assert3(page.pid().page == rid.pid.page);
	w_assert2(page.pid().page == rid.pid.page);
	// make sure page belongs to rid.pid.stid
	if (page.pid() != rid.pid) {
		page.unfix();
		//cout<<"Unfix page.pid:"<< rid.pid <<endl;
		return RC(eBADPID);
	}

#if HPL_DEBUG>1
	if (rid.slot < EOP || rid.slot > page.getNumberLastFixed())
	{

		cout<<"NumberOfRecords:" <<  page.getNumberLastFixed() << "rid:"<< rid.slot << endl;
		return RC(eBADSLOTNUMBER);
	}
#endif

	return RCOK;
}

rc_t
hpl_m::_alloc_page(	stid_t fid, const lpid_t& near_p, lpid_t&	allocPid, hpl_p	&page, bool search_file, const hpl_record_t& record)
{
	// new code for shoreMT
	store_flag_t store_flags;
	{
		/* get the store flags before we descend into the io layer */
		W_DO( io->get_store_flags(fid, store_flags));
		if (store_flags & st_insert_file)  {
			store_flags = (store_flag_t) (store_flags|st_tmp);
			// is st_tmp and st_insert_file
		}
	}

#if HPL_DEBUG>1
	 cout<<" Allocate HPL page!"<<endl;
#endif
	{
		// Filter EX-latches the page; we hold the ex latch
		// and return the page latched.
		alloc_hpl_page_filter_t ok(store_flags, page);
		W_DO(io->alloc_a_file_page(&ok, fid, near_p, allocPid, IX,search_file));
		w_assert1(page.is_mine()); // EX-latched
		// Now we format, since it couldn't be done during accept()
		W_DO(page.format(allocPid, page.t_hpl_p, page.t_virgin, store_flags));
	}

	// now put the good header in. Make sure fix does not screw up header
	hpl_p_hdr_t hdr(record, hpl_p::data_sz);
	W_DO(page.set_hdr(hdr) );

	//HPL: initialize page structure - ghost and null bits
	//  extents for each fixed field
	W_DO( page.init_page_structure());

	return RCOK;
}

// The precord_t here must contain the EXACT size of the fields
// for the record to be inserted, regardless of whether the field is
// variable size or not.

rc_t
hpl_m::_create_rec(const stid_t fid, const hpl_record_t& record, sdesc_t& sd, rid_t& rid, hpl_p& page)
{
	DBGTHRD(<<"hpl_m::_create_rec create new record in file " << fid);
	recordSize_t space_needed = 0;
	bool have_page=false;

	{ // open scope for hu
	slotid_t slot = 0;
	DBGTHRD(<<"About to copy sd");
	histoid_update_t hu(&sd);

		// compact stores become a bottleneck
	    // and they make the parallel loading of the benchmark databases
	    // unable to acquire EX latches, and those db loads don't
	    // cope with this situation.
	    uint4_t     policy = hpl_t_cache | /*t_compact |*/ hpl_t_append;

	// Check for space in the suggested page
	if(page.is_fixed()) {
		w_assert3(page.latch_mode() == LATCH_EX);

		rc_t rc = page.find_and_lock_next_slot(space_needed, slot);

		if (rc.is_error())
		{
			page.unfix();
			DBG(<<"rc=" << rc);
			if (rc.err_num() != eRECWONTFIT)
			{
				// error we can't handle
				return RC_AUGMENT(rc);
			}
			w_assert3(!page.is_fixed());
			w_assert3(!have_page);
		}
		else
		{
			if (page.fits_in_page(record, space_needed))
			{
				DBG(<<"acquired slot " << slot);
				have_page = true;
				w_assert2(page.is_fixed());
				hu.replace_page(&page);
			}
		}
	}

	if (!have_page)
	{
		W_DO(_find_slotted_page_with_space(fid, hpl_policy_t(policy), sd, space_needed, page, slot, record));
		hu.replace_page(&page);
		have_page=true;
#if HPL_DEBUG>0
		if (page.tag()!=12)
			cout<<"page tag: "<<page.tag()<<endl;
#endif

	}

	w_assert2(page.is_fixed() && page.latch_mode() == LATCH_EX);
	w_assert3 (have_page);


	rid.pid = page.pid();
	rid.slot = slot;
#if HPL_DEBUG>1
	cout<<"PageId: "<< page.pid()<<" RecordNumber: "<< slot <<endl;
#endif
	//add new HPL methods
	W_DO( page.insert_record(record, slot));

	hu.update();
	} // close scope for hu

	return RCOK ;
}

rc_t hpl_m::_find_slotted_page_with_space(
    const stid_t&        stid,
    hpl_policy_t         mask,   // might be t_append
    // and if it's exactly t_append, we had better not
    // create a record in the middle of the file.
    // mask & t_append == t_append means strict append semantics.
    sdesc_t&            sd,
    recordSize_t        space_needed,
    hpl_p&             	page,        // output
    slotid_t&           slot,        // output
    const hpl_record_t& record
)
{
    uint4_t         policy = uint4_t(mask);

    bool                    found=false;
    const histoid_t*        h = sd.store_utilization();

    /*
     * First, if policy calls for it, look in the cache
     * The cache is the histoid_t taken from the store descriptor.
     */
    if(policy & hpl_t_cache) {
        INC_TSTAT(fm_cache);
        while(!found) {
            pginfo_t        info;
            DBG(<<"looking in cache");
            W_DO(h->find_page(space_needed, found, info, &page, slot));

            if(found && page.is_hpl_p() && page.fits_in_page(record, space_needed)) {
                w_assert2(page.is_fixed());
#if HPL_DEBUG>1
                DBG(<<"found page " << info.page() << " slot " << slot);
                cout<<"Found page in hpl_t_cache "<<page.pid().page<< " slot " << slot<< endl;
#endif
                INC_TSTAT(fm_pagecache_hit);
                return RCOK;
            } else {
                // else no candidates -- drop down
                w_assert2(!page.is_fixed());
                break;
            }
        }
    }
    w_assert2(!found);
    w_assert2(!page.is_fixed());

    bool may_search_file = false;

    /*
     * Next, if policy calls for it, search the file for space.
     * We're going to be a little less aggressive here than when
     * we searched the cache.  If we read a page in from disk,
     * we want to be sure that it will have enough space.  So we
     * bump ourselves up to the next bucket.
     */
//    if(policy & t_compact)
//    {
//        INC_TSTAT(fm_compact);
//        DBG(<<"looking at file histogram");
//        smsize_t sn = page_p::bucket2free_space(
//                      page_p::free_space2bucket(space_needed)) + 1;
//        W_DO(h->exists_page(sn, found));
//        if(found) {
//            INC_TSTAT(fm_histogram_hit);
//
//            // histograms says there are
//            // some sufficiently empty pages in the file
//            // It's worthwhile to search the file for some space.
//
//            lpid_t                 lpid;
//
//            W_DO(first_page(stid, lpid, NULL/*allocated pgs only*/) );
//            DBG(<<"first allocated page of " << stid << " is " << lpid);
//
//            // scan the file for pages with adequate space
//            bool                 eof = false;
//            while (!eof) {
//				w_assert3(!page.is_fixed());
//				W_DO( page.fix(lpid, LATCH_SH, 0/*page_flags */));
//
//				INC_TSTAT(fm_search_pages);
//
//				DBG(<<"usable space=" << page.usable_space()
//						<< " needed= " << space_needed);
//
//				if (page.usable_space_for_slots() >= sizeof(file_p::slot_t)
//						&& page.usable_space() >= space_needed) {
//					W_DO(h->latch_lock_get_slot(
//									lpid.page, &page, space_needed,
//									false, // not append-only
//									found, slot));
//					if (found) {
//						w_assert3(page.is_fixed());
//						DBG(<<"found page " << page.pid().page << " slot " << slot);
//						INC_TSTAT(fm_search_hit);
//						return RCOK;
//					}
//				}
//				page.unfix(); // avoid latch-lock deadlocks.
//
//				// read the next page
//				DBG(<<"get next page after " << lpid
//						<< " for space " << space_needed);
//				W_DO(next_page_with_space(lpid, eof,
//								file_p::free_space2bucket(space_needed) + 1)); DBG(<<"next page is " << lpid);
//			}
//            // This should never happen now that we bump ourselves up
//            // to the next bucket.
//            INC_TSTAT(fm_search_failed);
//            found = false;
//        } else {
//            DBG(<<"not found in file histogram - so don't search file");
//        }
//        w_assert3(!found);
//        if(!found) {
//            // If a page exists in the allocated extents,
//            // allocate one and use it.
//            may_search_file = true;
//            // NB: we do NOT support alloc-in-file-with-search
//            // -but-don't-alloc-any-new-extents
//            // because io layer doesn't offer that option
//        }
//    } // policy & t_compact
//
//    w_assert3(!found);
//    w_assert3(!page.is_fixed());

    /*
     * Last, if policy calls for it, append (possibly strict)
     * to the file.
     * may_search_file==true indicates not strictly appending;
     * strict append is when
     *     policy is exactly t_append, in which case may_search_file
     *        had better be false
     */
    if(policy & hpl_t_append)
    {
#if   W_DEBUG_LEVEL > 1
        if(policy == t_append) {
            w_assert1(may_search_file == false);
        }
#endif

        if(may_search_file) {
            INC_TSTAT(fm_append);
        } else {
            INC_TSTAT(fm_appendonly);
        }
        lpid_t        lastpid = lpid_t(stid, sd.hog_last_pid());

        DBG(<<"try to append to file lastpid.page=" << lastpid.page);

        // might not have last pid cached
        if(lastpid.page) {
            DBG(<<"look in last page - which is " << lastpid );
            w_assert2(io->is_valid_page_of(lastpid, stid.store));

            // TODO: might get a deadlock here!!!!!

            INC_TSTAT(fm_lastpid_cached);
            sd.free_last_pid();

            W_DO(h->latch_lock_get_slot(
                        lastpid.page, &page, space_needed,
                        !may_search_file, // append-only
                        found, slot));
            if(found && page.is_hpl_p() && page.fits_in_page(record, space_needed)) {
                w_assert3(page.is_fixed());
#if HPL_DEBUG>1
                DBG(<<"found page " << page.pid().page << " slot " << slot);
                cout<<"Found page in hpl_t_append "<< page.pid().page<<" slot " << slot<< endl;
#endif
                INC_TSTAT(fm_lastpid_hit);
                w_assert2(io->is_valid_page_of(page.pid(), stid.store));
                return RCOK;
            }
            DBG(<<"no slot in last page ");
        } else {
            sd.free_last_pid();
        }

        DBGTHRD(<<"allocate new page may_search_file="<< may_search_file  );

        /* Allocate a new page */
        lpid_t        newpid;
        /*
         * Argument may_search_file determines behavior of _alloc_page:
         * if true, it searches existing extents in the file besides
         * the one indicated by the near_pid (lastpid here) argument,
         * It appends extents if it can't satisfy the request
         * with the first extent inspected.
         *
         * Furthermore, if may_search_file determines the treatment
         * of that first extent inspected: if may_search_file is true,
         * it looks for ANY free page in that extent; else it only
         * looks for free pages at the "end" of the extent, preserving
         * append_t policy.
         */
        //W_DO(_alloc_page( fid, lpid_t::eof, lastpid, page, false, record));
        W_DO(_alloc_page(stid,  lastpid, newpid,  page, may_search_file, record));
        w_assert3(page.is_fixed());
        w_assert3(page.latch_mode() == LATCH_EX);
        // Now have long-term IX lock on the page

        if(may_search_file) {
            sd.set_last_pid(0); // don't know last page
                // and don't want to look for it now
        } else {
            sd.set_last_pid(newpid.page);
        }

        // Page is already latched, but we don't have a
        // lock on a slot yet.  (Doesn't get doubly-latched by
        // calling latch_lock_get_slot, by the way.)
        W_DO(h->latch_lock_get_slot(
                newpid.page, &page, space_needed,
                !may_search_file,
                found, slot));

        if(found && page.is_hpl_p() && page.fits_in_page(record, space_needed)) {
            w_assert3(page.is_fixed());
#if HPL_DEBUG>1
                DBG(<<"found page " << page.pid().page << " slot " << slot);
                cout<<"Found page in hpl_t_append 2part: "<< page.pid().page<<" slot " << slot<< endl;
#endif
            INC_TSTAT(fm_alloc_pg);
            w_assert2(io->is_valid_page_of(page.pid(), stid.store));
            return RCOK;
        }
        page.unfix();
    }
    w_assert3(!found);
    w_assert3(!page.is_fixed());

    INC_TSTAT(fm_nospace);
    DBG(<<"not found");
    return RC(eSPACENOTFOUND);
}


/*--------------------------------------------------------------*
 *  hpl_m::destroy_rec()                                     	*
 *--------------------------------------------------------------*/
rc_t
hpl_m::destroy_rec(const rid_t& rid)
{
    hpl_p       	 page;
    //hpl_record_t*    rec;

    DBGTHRD(<<"destroy_rec");
    W_DO(_locate_page(rid, page, LATCH_EX));

    /*
     * Find or create a histoid for this store.
     */
    w_assert2(page.is_fixed());
    w_assert2(page.is_latched_by_me());
    w_assert2(page.is_mine());

    //cout<<"Locating the page!"<<endl;

    W_DO( page.delete_record(rid));


//TODO HPL: implement destroy_rec
//    W_DO( page.get_rec(rid.slot, rec) );
//    DBGTHRD(<<"got rec for rid " << rid);
//
//    if (rec->is_small()) {
//        // nothing special
//        DBG(<<"small");
//    } else {
//        DBG(<<"large -- truncate " << rid << " size is " << rec->body_size());
//        W_DO(_truncate_large(page, rid.slot, rec->body_size()));
//    }
//
//    W_DO( page.destroy_rec(rid.slot) ); // does a page_mark for the slot
//
//    if (page.rec_count() == 0) {
//        DBG(<<"Now free page");
//        w_assert2(page.is_fixed());
//        W_DO(_free_page(page));
//        return RCOK;
//    }
//
    DBG(<<"Update page utilization");
    /*
     *  Update the page's utilization info in the
     *  cache.
     *  (page_p::unfix updates the extent's histogram info)
     */
    histoid_update_t hu(page);
    hu.update();

    //cout<<"Done deleteing!"<<endl;

    return RCOK;
}

/*--------------------------------------------------------------*
 *  hpl_m::update_field()                                     	*
 *--------------------------------------------------------------*/
rc_t
hpl_m::update_field(const rid_t& rid, const flid_t fieldNumber, const char* data, const Size2 dataSize)
{
    hpl_p    		page;
    hpl_record_t*   rec;

    latch_mode_t page_latch_mode = LATCH_EX;

#if HPL_DEBUG>1
    cout<<"update field"<<endl;
#endif
    W_DO(_locate_page(rid, page, page_latch_mode));

    bool enoughSpace = false;
    if ( !page.getIsFixedSize(fieldNumber) )
    {
    	if ( page.fits_in_page( dataSize ) )
    		enoughSpace = true;
    	else
    		enoughSpace = false;
    }
    else
    	enoughSpace = true;

    if (enoughSpace)
    	W_DO( page.update_field(rid.slot, fieldNumber, data, dataSize));
    else
    {
    	// TODO: HPL: get new page + get_rec + insert_rec
#if HPL_DEBUG>0
	cout << "Error: Unimplemented feature called! (Relocate record)"<< endl;
#endif
    }



//TODO HPL: implement update_rec
    /*
     *        Do some parameter checking
     */
//    if (start > rec->body_size()) {
//        return RC(eBADSTART);
//    }
//    if (data.size() > (rec->body_size()-start)) {
//        return RC(eRECUPDATESIZE);
//    }
//
//    if (rec->is_small()) {
//        W_DO( page.splice_data(rid.slot, u4i(start), data.size(), data) );
//    } else {
//        if (rec->tag.flags & t_large_0) {
//            lg_tag_chunks_h lg_hdl(page, *(lg_tag_chunks_s*)rec->body());
//            W_DO(lg_hdl.update(start, data));
//        } else {
//            lg_tag_indirect_h lg_hdl(page, *(lg_tag_indirect_s*)rec->body(), rec->page_count());
//            W_DO(lg_hdl.update(start, data));
//        }
//    }

    return RCOK;
}
