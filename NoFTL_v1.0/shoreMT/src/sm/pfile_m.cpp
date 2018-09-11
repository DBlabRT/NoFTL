
#include "w_defines.h"                                                          

#define SM_SOURCE
#define PFILE_M_C

#include "sm_int_2.h"
#include "lgrec.h"
#include "w_minmax.h"
#include "sm_du_stats.h"
 
#include "histo.h"
#include "crash.h"

#include "pfile_p.h"
#include "pfile_m.h"
 
#ifdef EXPLICIT_TEMPLATE
/* Used in sort.cpp, btree_bl.cpp */
template class w_auto_delete_array_t<pfile_p>;
#endif



/**\brief   For PAX page format
 * Filter class to determine if we can allocate the given page from
 * the store.
 * Allocating a file page requires that we EX-latch it deep in the
 * io/volume layer so that there will be no race between allocating
 * it and formatting it.
 *
 * So the accept method here does a conditional fix.
 * The reject method unfixes it.
 */
class alloc_pfile_page_filter_t : public alloc_page_filter_t {
private:
	store_flag_t _flags;
	lpid_t       _pid;
	bool         _was_fixed;
	bool         _is_fixed;
	pfile_p&      _page;
	void  _reset();
public:
	NORET alloc_pfile_page_filter_t(store_flag_t flg, pfile_p &pg);
	NORET ~alloc_pfile_page_filter_t();
	bool  accept(const lpid_t&);
	void  reject();
	void  check() const;
	bool  accepted() const;
};

NORET alloc_pfile_page_filter_t::alloc_pfile_page_filter_t(
		store_flag_t flg, pfile_p &pg) 
	: alloc_page_filter_t(), _flags(flg), _page(pg) { _reset(); }

NORET alloc_pfile_page_filter_t::~alloc_pfile_page_filter_t() {}

void  alloc_pfile_page_filter_t::_reset()
{
	_pid = lpid_t::null;
	_was_fixed = false;
	_is_fixed = false;
}
bool  alloc_pfile_page_filter_t::accept(const lpid_t& pid) 
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
	w_rc_t rc = _page.page_p::_fix(true, _pid, _page.t_pfile_p,
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

bool  alloc_pfile_page_filter_t::accepted() const { return _is_fixed; }
void  alloc_pfile_page_filter_t::check() const
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
void  alloc_pfile_page_filter_t::reject() 
{
	check();
	if(accepted() && !_was_fixed) {
		_page.unfix();
	}
	_reset();
}

/*********************************************************************
 *
 *  pfile_m::create(  stid_t     stid,
 *                    lpid_t&    first_page
 *                      const precord_t& prec_info)
 *
 *  Create a file with partitioned records. 
 *  prec_info contains # of fields, # of fixed fields, and table with sizes
 * 
 *********************************************************************/
rc_t
pfile_m::create( stid_t stid, lpid_t& first_page, const precord_t& prec_info)
{

   pfile_p  page;
   DBGTHRD(<<"pfile_m::create create first page in store " << stid);
   W_DO(_alloc_page( stid, lpid_t::eof, first_page, page, true, prec_info));
   w_assert3(page.is_fixed());
   DBGTHRD(<<"pfile_m::create(d) first page is  " << first_page);
   return RCOK;
}

rc_t 
pfile_m::create_rec( stid_t fid, const precord_t& prec_info,
					// pg_policy_t policy, 
					sdesc_t& sd,
					rid_t& rid)
{
   // w_assert3(policy==t_append);
   pfile_p page;
   _create_rec (fid, prec_info, sd, rid, page);
   return RCOK ; 
}

rc_t
pfile_m::create_rec_at_end(
	pfile_p&         page, // in-out -- caller might have it fixed
	const precord_t& prec_info,
	sdesc_t& sd,
	rid_t& rid)
{
	// stpgid is obsolete in shoreMT
	//W_DO(_create_rec (sd.stpgid().stid(), prec_info, sd, rid, page));
	stid_t        fid = sd.stid();
	W_DO(_create_rec (fid, prec_info, sd, rid, page));

	return RCOK;
}

rc_t
pfile_m::read_rec( rid_t& s_rid, void*& buf)
{
	rid_t rid(s_rid);
	pfile_p page;
	int i, rec_sz=0;
	char* tmp;

	DBGTHRD(<<"read_rec");
	W_DO( _locate_page(rid, page, LATCH_SH) );
	precord_t prec_info(page.no_fields(), page.no_ffields());
	W_DO( page.get_rec(rid.slot, prec_info) );
	for (i=0; i<prec_info.no_fields; ++i)
		rec_sz += prec_info.field_size[i];
	buf = new char[rec_sz];
	tmp = (char*)buf;
	for (i=0; i<prec_info.no_fields; ++i)
	{
		memcpy(tmp, prec_info.data[i], prec_info.field_size[i]);
		tmp += prec_info.field_size[i];
	}
	return RCOK;
}

rc_t 
pfile_m::first_page(const stid_t& fid, lpid_t& pid, bool* allocated) 
{
   return io->first_page(fid, pid, allocated);
}

rc_t
pfile_m::next_page(lpid_t& pid, bool& eof, bool* allocated)
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
pfile_m::last_page(const stid_t& fid, lpid_t& pid, bool* allocated)
{
	return io->last_page(fid, pid, allocated, IX);
}

rc_t
pfile_m::_locate_page(const rid_t& rid, pfile_p& page, latch_mode_t mode)
{
	DBGTHRD(<<"pfile_m::_locate_page rid=" << rid);

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
		return RC(eBADPID);
	}

	if (rid.slot < EOP || rid.slot >= page.num_slots())  {
	return RC(eBADSLOTNUMBER);
	}

	return RCOK;
}

rc_t
pfile_m::_alloc_page(	stid_t				fid,
						const lpid_t&		near_p,
						lpid_t&				allocPid, 
						pfile_p				&page,
						bool				search_file,
						const precord_t&	prec_info)
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

	{
		// Filter EX-latches the page; we hold the ex latch
		// and return the page latched.
		alloc_pfile_page_filter_t ok(store_flags, page);
		W_DO(io->alloc_a_file_page(&ok, fid, near_p, allocPid, IX,search_file));
		w_assert1(page.is_mine()); // EX-latched
		// Now we format, since it couldn't be done during accept()
		W_DO(page.format(allocPid, page.t_pfile_p, page.t_virgin, store_flags));
	}

	// now put the good header in. Make sure fix does not screw up header
	pfile_p_hdr_t hdr(prec_info, pfile_p::data_sz);
	W_DO(page.set_hdr(hdr) );


	// obsolete version of code in shore
	// w_assert3(search_file==false);
	//w_assert3(near_p==lpid_t::eof);
	//W_DO(io->alloc_pages(fid,near_p,1,&allocPid,false,IX,search_file));

	//store_flag_t store_flags;
	//W_DO(io->get_store_flags(allocPid.stid(), store_flags));
	//if (store_flags & st_insert_file)
	//	store_flags = (store_flag_t) (store_flags|st_tmp);
	//W_DO(page.fix(allocPid, LATCH_EX, page.t_virgin, store_flags) );
	
	// now put the good header in. Make sure fix does not screw up header
	//pfile_p_hdr_t hdr(prec_info, pfile_p::data_sz);
	//W_DO(page.set_hdr(hdr) );
	
	
	return RCOK;
}

// The precord_t here must contain the EXACT size of the fields
// for the record to be inserted, regardless of whether the field is
// variable size or not.

rc_t
pfile_m::_create_rec(const stid_t           fid,
					 const precord_t&		prec_info,
					 // pg_policy_t         policy,
					 sdesc_t&               sd,
					 rid_t&                 rid,
					 pfile_p&				page)                 // in-output
{
	DBGTHRD(<<"file_m::_create_rec create new record in file " << fid);
	Size2 space_needed = 0;
	bool have_page=false;

	// Check for space in the suggested page 

	if(page.is_fixed()) { 
		w_assert3(page.latch_mode() == LATCH_EX);  
		if (page.fits_in_page(prec_info, space_needed))
			have_page=true;
	}

	if (!have_page)
	{
		// go to end of file
		lpid_t  lastpid = lpid_t(fid, sd.hog_last_pid());
	bool check_last=true;

	// won't need that anymore
		if (page.is_fixed())	// rec didn't fit
		{
		if (page.pid()==lastpid)
			check_last=false;
			page.unfix();
			w_assert3(!page.is_fixed());
		}

	if (check_last==true)
	{
			// fix the page
			W_DO (page.fix(lastpid, LATCH_EX));
			if (page.fits_in_page(prec_info, space_needed))
				have_page=true;
	}

		if (!have_page)
		{
		// pfile_p_hdr_t* old_hdr = ((pfile_p_hdr_t *)(page._pp->data));
			// allocate new page
			W_DO(_alloc_page( fid, lpid_t::eof, lastpid, page, false, prec_info));
		// pfile_p_hdr_t* new_hdr = ((pfile_p_hdr_t *)(page._pp->data));
			w_assert3(page.is_fixed()); 
			w_assert3(page.latch_mode() == LATCH_EX);
			have_page = true;
			// I know last page (because _alloc_page is called with 
			// may_search_file=false) and am setting it here
			sd.set_last_pid(lastpid.page);
		// copy variable size field sizes
		// flid_t i;
		// transfer sizes
		// for (i=old_hdr->no_ffields; i< old_hdr->no_fields; ++i)
		// 	new_hdr->field_size[i] = old_hdr->field_size[i];
		}
	}

	rid.pid = page.pid();
	w_assert3 (have_page);
	W_DO( page.append_rec(prec_info, rid.slot));

	// print internal PAX page infrastructure
	//page.sanity();

	return RCOK ; 
}

