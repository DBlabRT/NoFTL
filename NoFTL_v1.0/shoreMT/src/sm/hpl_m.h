/* ------------------------------------------------------------------------------- */
/* -- Copyright (c) 2011, 2012 Databases and Distributed Systems Department,    -- */
/* -- Technical University of Darmstadt, subject to the terms and conditions    -- */
/* -- given in the file COPYRIGHT.  All Rights Reserved.                        -- */
/* ------------------------------------------------------------------------------- */
/* -- Ilia Petrov, Todor Ivanov, Veselin Marinov                                -- */
/* ------------------------------------------------------------------------------- */


#ifndef HPL_M_H
#define HPL_M_H

#include "sdesc.h"
#include "hpl_rec.h"

class hpl_p;

enum hpl_policy_t {
    hpl_t_append        = 0x01, // retain sort order (cache 0 pages)
    hpl_t_cache        = 0x02, // look in n cached pgs
    hpl_t_compact        = 0x04 // scan file for space in pages
    /* These are masks - the following combinations are supported:
     * t_append    -- preserve sort order
     * t_cache  -- look only in cache - error if no luck (not really sensible)
     * t_compact  -- don't bother with cache (bad idea)
     * t_cache | t_append -- check the cache first, append if no luck
     * t_cache | t_compact -- check the cache first, if no luck,
     *                     search the file if histogram if apropos;
     *                     error if no luck
     * t_cache | t_compact | t_append  -- like above, but append to
     *                file as a last resort
     * t_compact | t_append  -- don't bother with cache (bad idea)
     *
     * Of course, not all combos are sensible.
     */

};

class hpl_m : public smlevel_2 {

public:

	NORET hpl_m() { }
	NORET ~hpl_m() { }

	static rc_t create(
			stid_t          		stid,
			lpid_t&         		first_page,
			const hpl_record_t& 	record);

	static rc_t create_rec(
			stid_t    fid,
			const hpl_record_t& 	record,
			sdesc_t&    			sd,
			rid_t&    				rid); // out

	static rc_t create_rec_at_end(
			hpl_p& 					page, // in-out -- caller might have it fixed
			const hpl_record_t& 	record,
			sdesc_t& 				sd,
			rid_t& 					rid);

	static rc_t destroy_rec(const rid_t& rid);

	static rc_t update_field(
			const rid_t&            rid,
			const flid_t 			fieldNumber,
			const char* 			data,
			const Size2					dataSize);

	static rc_t read_record(
			const rid_t& 			 rid,
			hpl_record_t&      rec_data);

	static rc_t first_page(
			const stid_t&     	fid,
			lpid_t&         	pid,
			bool*           	allocated = NULL);

	static rc_t  next_page(
			lpid_t&         	pid,
			bool&           	eof,
			bool*           	allocated = NULL);

	static rc_t  last_page(
			const stid_t&     	fid,
			lpid_t&         	pid,
			bool*           	allocated = NULL);

	static rc_t locate_page(
			const rid_t& 		rid,
			hpl_p& 				page,
			latch_mode_t 		mode)
	{
		return _locate_page(rid, page, mode);
	}

protected :

	static rc_t _find_slotted_page_with_space(
	            const stid_t&   		fid,
	            hpl_policy_t     		mask,
	            sdesc_t&        		sd,
	            recordSize_t        	space_needed,
	            hpl_p&         			page,       // output
	            slotid_t&       		slot,        // output
	            const hpl_record_t& 	record);

	static rc_t _locate_page(const rid_t& rid, hpl_p& page, latch_mode_t mode);

	static rc_t	_alloc_page(
			stid_t 					fid,
			const lpid_t& 			near_p,
			lpid_t& 				allocPid,
			hpl_p 					&page,
			bool 					search_file,
			const hpl_record_t& 	record);

	static rc_t	_create_rec (
			const stid_t        	fid,
			const hpl_record_t&		record,
			sdesc_t&				sd,
			rid_t&					rid,
			hpl_p& 					page);


private:

} ;

#endif
