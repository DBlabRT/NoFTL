/* --------------------------------------------------------------- */
/* -- Copyright (c) 1994, 1995 Computer Sciences Department,    -- */
/* -- University of Wisconsin-Madison, subject to the terms     -- */
/* -- and conditions given in the file COPYRIGHT.  All Rights   -- */
/* -- Reserved.                                                 -- */
/* --------------------------------------------------------------- */
/* -- Anastassia Ailamaki                                       -- */
/* --------------------------------------------------------------- */

#ifndef PFILE_HDR_H
#define PFILE_HDR_H

#include <stddef.h>
#include <math.h>

#include "prec.h"
#include "bitmap.h"

#define MAX_BM_SIZE (MAX_FIELDS/8)

#define    FREC_OVH    0.125
typedef uint2_t		Offset2 ;

#define    PPageHeaderSize        (align(sizeof(pfile_p_hdr_t)))

// Offset to the start of the nullable bitmap
#define NBOffset    (offsetof(pfile_p_hdr_t,bm_null) )
// offset to the start of the field size array (FROM START OF HEADER)
#define FSOffset    (offsetof(pfile_p_hdr_t,field_size))
// offset to the start of the field start array (FROM START OF HEADER)
#define FVOffset    (offsetof(pfile_p_hdr_t,field_start))

/*--------------------------------------------------------------*
 *  struct pfile_p_hdr_t                                        *
 *--------------------------------------------------------------*/

struct pfile_p_hdr_t {
	// 0 for varsize
	Size2   field_size[MAX_FIELDS];     // Field size array
										// move to file header
	Offset2  field_start[MAX_FIELDS];   // table of offsets to start of each field
										// from beginning of data field (right
										// after end of header)
	u_char  bm_null[MAX_BM_SIZE];       // nullable bitmap
										// move to file header
	Size2  data_size;			// in bytes
	Size2  free_space;                	// in bytes
	uint2_t  max_records;               // maximum number of records
	uint2_t  no_records;                // number of records in page
	uint1_t  no_fields;                 // number of fields 
										// move to file header
	uint1_t  no_ffields;            	// number of fields 
										// move to file header
	fill4    filler;			// filler for 64-bit

	// fno starts from 0 and goes up to no-fields-1  here.
	void set_nullable(flid_t fno) { bm_set(bm_null, fno); }
	void set_non_nullable(flid_t fno) { bm_clr(bm_null, fno); }
	// method bm_any_set is not implemented
	//inline bool has_nullable() { return bm_any_set(bm_null, no_fields); }
	bool  has_varsize() { return (no_fields>no_ffields); }
	bool  is_nullable(flid_t fno) { return bm_is_set(bm_null, fno); }
	bool  is_non_nullable(flid_t fno) { return bm_is_clr(bm_null, fno); }
	bool  is_varsize(flid_t fno) { return (fno>=no_ffields); }
	bool  is_fixed_size(flid_t fno) { return (fno < no_ffields); }

	// Returns MINIMUM record size
	Size2 min_rec_size() { int i=0; Size2 sz=0; 
			for (;i<no_fields;++i) sz+=field_size[i]; 
			return sz; }

	// Returns MINIMUM record size with overhead 
	// 1 bit per fixed field, 12 bytes per var size field
	double add_min_rsize_ovh (const int rec_data_size) { 
		return (rec_data_size + no_ffields*FREC_OVH + 
						// here I am adding 
						// 1/8 (1 presence bit) per fixed-size record
				((no_fields-no_ffields)*sizeof(Offset2))); }
	
	rc_t calculate_max_records()
	{
	flid_t i;
		// initially, just get the theoretical max (be optimistic)
		max_records = (int)floor(data_size/add_min_rsize_ovh(min_rec_size()));
	int space_needed;
	while (max_records>0)
	{
		space_needed = 0;
		for (i=0; i<no_fields; ++i) 
		space_needed += align(max_records * field_size[i]); 
		space_needed +=
				(align((Size2)ceil(max_records*FREC_OVH))*no_ffields +
			 align(max_records*sizeof(Offset2))*(no_fields-no_ffields));
			if (space_needed > data_size)
				max_records--;
		else break;
	}

	if (max_records==0)
	   return RC(smlevel_0::eRECWONTFIT);
		return RCOK;
	}

	// Psilo-constructor
	pfile_p_hdr_t() // dummy for pfile_p::format()
	{
		int i;

		free_space = 0;
		no_records = 0;
		no_fields = 0;
		no_ffields = 0;
		for (i=no_fields; i<MAX_FIELDS; ++i)
		{
			field_start[i]=0 ; // NULL
			field_size[i]=INV_FIELD ;
		}
		// nothing is nullable in the beginning
		bm_zero(bm_null,MAX_FIELDS);
	}

	pfile_p_hdr_t(const precord_t& prec_info, Size2 data_sz)
	{ 
		int i; 
		no_records = 0;
		no_fields = prec_info.no_fields;
		no_ffields = prec_info.no_ffields;
		free_space=data_size=data_sz;
	// # of minipages which are precedd by an ffield
	int have_ff_before = no_ffields + ((no_ffields<no_fields)?1:0);


		w_assert3(no_fields>0 && no_fields<=MAX_FIELDS);
		w_assert3(no_ffields<=no_fields);
		w_assert3(prec_info.field_size);

		for (i=0; i<no_fields; ++i) 
		{
			w_assert3(prec_info.field_size[i] > 0);
			field_size[i]=prec_info.field_size[i];
		}

		// For the field starts
		w_assert3(free_space);
		W_COERCE(calculate_max_records());

		// do for i=0
		field_start[0]=0;
	
	// do up to (and including) the first vfield
	Size2 std_fovh = (Size2)align((Size2)ceil(max_records*FREC_OVH));
		for (i=1; i<have_ff_before; ++i) 
		{
			field_start[i]= field_start[i-1] + 
				align(max_records * field_size[i-1]) + std_fovh;
		}

	// the rest are preseded by a vfield minipage
	Size2 std_vovh = (Size2)align((Size2)max_records*sizeof(Offset2));
	for (i=have_ff_before; i < no_fields; ++i)
	{
		field_start[i]= field_start[i-1] +
		align(max_records * field_size[i-1]) + std_vovh;
	}
		for (i=no_fields; i<MAX_FIELDS; ++i)
		{
			field_start[i]=0 ; // NULL
			field_size[i]=INV_FIELD ;
		}
		// nothing is nullable in the beginning
		bm_zero(bm_null,MAX_FIELDS);
	}

};

#endif 

