/* --------------------------------------------------------------- */
/* -- Copyright (c) 1994, 1995 Computer Sciences Department,    -- */
/* -- University of Wisconsin-Madison, subject to the terms     -- */
/* -- and conditions given in the file COPYRIGHT.  All Rights   -- */
/* -- Reserved.                                 -- */
/* --------------------------------------------------------------- */

/*
 *  $Id: pfile_m.h,v 3.0 2000/09/18 21:05:02 natassa Exp $
 */
#ifndef PFILE_M_H
#define PFILE_M_H

#include "sdesc.h"
#include "prec.h"

class pfile_p;

class pfile_m : public smlevel_2 {

    public:

    NORET pfile_m() { } 
    NORET ~pfile_m() { } 

    rc_t create(
                stid_t          stid, 
                lpid_t&         first_page,
                const precord_t& prec_info);

    rc_t create_rec( 
			stid_t    fid,
			const precord_t& prec_info,
			// pg_policy_t     policy,
        		sdesc_t&    sd,
        		rid_t&    rid // out
			); // out

    rc_t create_rec_at_end(
    			pfile_p& page, // in-out -- caller might have it fixed
			const precord_t& prec_info,
			sdesc_t& sd,
			rid_t& rid);

    rc_t read_rec( rid_t& s_rid, void*& buf);

    rc_t       first_page(
      const stid_t&     fid,
      lpid_t&         pid,
      bool*           allocated = NULL);
    rc_t       next_page(
      lpid_t&         pid,
      bool&           eof,
      bool*           allocated = NULL);
    rc_t       last_page(
      const stid_t&     fid,
      lpid_t&         pid,
      bool*           allocated = NULL);                        

    static rc_t locate_page(const rid_t& rid, pfile_p& page,
           latch_mode_t mode)
           {return _locate_page(rid, page, mode); }

    protected :

    static rc_t _locate_page(const rid_t& rid, pfile_p& page, latch_mode_t mode);

    rc_t		_alloc_page(
				stid_t fid, 
				const lpid_t& near_p, 
				lpid_t& allocPid,
				pfile_p &page, 
				bool search_file, 
				const precord_t& prec_info);

    rc_t		_create_rec (
				const stid_t        fid,
				const precord_t&	prec_info,
				// pg_policy_t 		policy,
				sdesc_t&		sd,
				rid_t&			rid,
				// serial_t&		serial_no,
				pfile_p& 		page);

    private:

} ;

#endif 
