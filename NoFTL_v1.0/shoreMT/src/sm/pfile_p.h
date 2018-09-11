/* --------------------------------------------------------------- */
/* -- Copyright (c) 1994, 1995 Computer Sciences Department,    -- */
/* -- University of Wisconsin-Madison, subject to the terms     -- */
/* -- and conditions given in the file COPYRIGHT.  All Rights   -- */
/* -- Reserved.                                                 -- */
/* --------------------------------------------------------------- */
/* -- Anastassia Ailamaki                                       -- */
/* --------------------------------------------------------------- */

#ifndef PFILE_P_H
#define PFILE_P_H

#include "page.h"
#include "pfile_hdr.h"
#include "rbitmap.h"

// Cast to header start
#define    pheader ((pfile_p_hdr_t *)(_pp->data))

#define EOP    -1        // End of page

// These macros define header elements in the page for faster access

#define DATA_SIZE (*(uint2_t*)(_pp->data+offsetof(pfile_p_hdr_t, data_size)))
#define FREE_SPACE (*(uint2_t*)(_pp->data+offsetof(pfile_p_hdr_t, free_space)))
#define MAX_RECORDS (*(uint2_t*)(_pp->data+offsetof(pfile_p_hdr_t, max_records)))
#define NO_RECORDS (*(uint2_t*)(_pp->data+offsetof(pfile_p_hdr_t, no_records)))
#define NO_FIELDS  (*(uint1_t*)(_pp->data+offsetof(pfile_p_hdr_t, no_fields)))
#define NO_FFIELDS  (*(uint1_t*)(_pp->data+offsetof(pfile_p_hdr_t, no_ffields)))

// fno (field number) ranges from 0 to field_no-1)
// rno (record number) ranges from 0 to no_records-1)

// Current Nullmap size for rno records
#define NullmapSize(rno)	((Size2)align((Size2)ceil((rno)*FREC_OVH)))
// Current Offset array size for rno records
#define OffsetArrSize(rno)	((Size2)align((rno)*sizeof(Offset2)))
// How much will the Nullmap overhead be if I add the rno-th record (starting from 0)
#define NullmapOvh(rno)	(((rno)%(ALIGNON*8))?0:ALIGNON)
// How much will the Offset overhead be if I add the rno-th record (starting from 0)
#define OffsetOvh(rno)	(((rno)%(ALIGNON/sizeof(Offset2)))?0:ALIGNON)

#define DataSizeOffset(fno)        (FSOffset+(fno)*sizeof(Size2))
#define DataSizeAddr(fno)        (_pp->data+(Offset2)DataSizeOffset(fno))
#define FieldSize(fno)            (*(Size2*)(DataSizeAddr(fno)))

// Offset to data start[fno] from start of page
#define DataStartOffsetOffset(fno)  (FVOffset+(fno)*sizeof(Offset2))
#define DataStartOffsetAddr(fno)    (_pp->data+(Offset2)DataStartOffsetOffset(fno))
#define DataStartOffset(fno)    (PPageHeaderSize+*(Offset2*)DataStartOffsetAddr(fno))
#define DataStartAddr(fno)    (_pp->data+DataStartOffset(fno))

// Returns the byte that starts the tail (good for the fixed-size
// attributes *or* for size calculations
#define TailStartOffset(fno)    ((fno==NO_FIELDS-1) ?   \
								  page_p::data_sz-1 :  \
								  DataStartOffset(fno+1)-1)
#define TailStartAddr(fno)    (_pp->data+(Offset2)TailStartOffset(fno))

#define VTailStartOffset(fno)    ((fno==NO_FIELDS-1) ?   \
								 page_p::data_sz-sizeof(Offset2) :  \
								 DataStartOffset(fno+1)-sizeof(Offset2))
#define VTailStartAddr(fno)    (_pp->data+(Offset2)VTailStartOffset(fno))

// Fixed value field size is same as above if not null
#define FFieldPresent(fno,rno)    (r_bm_is_set((u_char*)TailStartOffset(fno),rno))
#define FFieldSize(fno,rno)        (FFieldPresent(fno,rno) ? FieldSize(fno) : 0)

#define VarFieldEndPtr(fno,rno) (VTailStartAddr(fno)-((rno)*sizeof(Offset2)))
#define VarFieldEndOffset(fno,rno) (*(Offset2*)VarFieldEndPtr(fno,rno))

// variable value field size differs
#define VFieldSize(fno,rno)        (VarFieldEndOffset(fno,rno)-(rno==0 ? 0 : \
					VarFieldEndOffset(fno,rno-1)))

#define FTotalValueSize(fno)	(NO_RECORDS*FieldSize(fno))
#define VTotalValueSize(fno)	(NO_RECORDS==0 ? 0 : \
					VarFieldEndOffset(fno, NO_RECORDS-1))

#define MinipageSize(fno) ((fno==NO_FIELDS-1) ? \
			   (page_p::data_sz - DataStartOffset(fno)) : \
			   (DataStartOffset(fno+1) - DataStartOffset(fno)))

/*
#define VMinipageBusySize(fno)	(NO_RECORDS*sizeof(Offset2) + \
						VTotalValueSize(fno))

#define VMinipageFreeSize(fno)	(MinipageSize(fno) - VMinipageBusySize(fno))
*/

// Non-Nullable fixed-size field start address
// (no need to check for presence)
#define FFieldStartAddr(fno,rno) ( DataStartAddr(fno)+ rno*FieldSize(fno) )
// Nullable fixed-size field start address
// (check for presence)
#define FNFieldStartAddr(fno,rno) ( \
	(r_bm_is_set((u_char*)TailStartAddr(fno), rno)) ? \
		((char*)DataStartAddr(fno)+ \
			r_bm_num_set((u_char*)TailStartAddr(fno), rno)*FieldSize(fno)) \
		: NULL)

/*--------------------------------------------------------------*
 *  class pfile_p                                               *
 *--------------------------------------------------------------*/

class pfile_p : public page_p {

  friend class pfile_m;
  
   private:
	 // Header size (My header, not Shore's)
	 // Depends on number of fields, to it's to be set in the constructor
	 enum { header_sz = PPageHeaderSize };

	 // Free space on pfile_p is page_p minus the header size.
	 // To be set in the constructor.
	 // data_sz includes data and the null/offset maps
	 enum { data_sz = page_p::data_sz - PPageHeaderSize };

   public:

	MAKEPAGE(pfile_p, page_p, 1) ;// Macro to install basic funcs from page_p 

	int         free_space()     { return pheader->free_space; }
	int         no_records()     { return pheader->no_records; }
	int         no_fields()     { return pheader->no_fields; }
	int         no_ffields()     { return pheader->no_ffields; }
	int         is_fixed_size(flid_t fno)     { return pheader->is_fixed_size(fno); }
	int         is_nullable(flid_t fno)     { return pheader->is_nullable(fno); }

	Size2        field_size(flid_t fno)    { return FieldSize(fno) ; }

	// for whoever wants it this way
	int         num_slots()     { return pheader->no_records; }

	uint2_t        header_size() const { return header_sz; }
	uint2_t        data_size() const { return data_sz; }

	// reeturns true if there is space in the page for the record.
	// I subtract a number of bytes equal to the number of variable-sized
	// fields in the page, because of the 1-byte dicrepancy per field that
	// may result from the averaging during reorganization.
	// Better be safe than sorry...
	bool         fits_in_page(const precord_t& prec_info, Size2& sz);

	// Returns minimum record size assuming zero size of variable fields
	smsize_t    min_rec_size() const
					{ return pheader->min_rec_size() ; }

	rc_t        link_up( shpid_t ppid, shpid_t npid )
					{ return page_p::link_up(ppid, npid) ; }

	rc_t        write_header( const pfile_p_hdr_t& hdr ) 
					{ memcpy( _pp->data, (void *)&hdr, header_sz) ; 
					  return RCOK ;  }

	rc_t        set_hdr(const pfile_p_hdr_t& new_hdr)
					{return write_header (new_hdr); }

	bool	fit_fields_in_page(const precord_t& prec_info);
	void	append_fixed_size_value(const flid_t fno, const char* data);
	void	append_var_size_value(const flid_t fno, const char* data, 
									const Size2 sz);

	rc_t	reorganize_page(const precord_t& prec_info);
	rc_t        append_rec(const precord_t& prec_info, 
						   slotid_t& slot);

	void*        fixed_value_addr(const int rno, const flid_t fno) const;

	slotid_t    next_slot(slotid_t curr)
					{ w_assert3(curr >=-1);
					  return (curr < pheader->no_records-1) ? curr+1 : EOP ; }
	void         sanity();

	rc_t        get_rec(slotid_t rno,
				precord_t& prec_info);
	rc_t        get_rec(slotid_t rno,
				precord_t& prec_info,
			const flid_t* ifields);
	rc_t        get_next_rec(slotid_t rno,
					 precord_t& prec_info,
				 const flid_t* ifields=NULL);
	bool        is_rec_valid(slotid_t rno) 
					{ return (rno >= 0 && rno < NO_RECORDS); }

	// inline functions for fast retrieval of data in fields

	void get_vfield(slotid_t rno, pfield_t& pfield)
	{
		w_assert3 (pheader->is_varsize(pfield.fno));
	w_assert3 (pfield.fno < NO_FIELDS);
	pfield.data = ((pfield.field_size=VFieldSize(pfield.fno,rno)) > 0 ? 
		DataStartAddr(pfield.fno) +
		(rno>0 ? VarFieldEndOffset(pfield.fno, rno-1) : 0): NULL);
	}

// rno is the DESIRED record slot id, whereas pfield contains rno-1 data
	void get_next_vfield (slotid_t rno, pfield_t& pfield)
	{ pfield.data += pfield.field_size;
	  pfield.field_size=VFieldSize(pfield.fno,rno); }

	void get_ffield (slotid_t rno, pfield_t& pfield)
	{
// cerr << "In page.get_ffield with rno " << rno << ", fno " << pfield.fno <<", with size " << pfield.field_size << endl;
		pfield.data = FFieldStartAddr(pfield.fno,rno);
// cerr << "Returning pfield.data " << pfield.data << endl;
	// ((!pfield.nullable) ? FFieldStartAddr(pfield.fno,rno)
	// :  FNFieldStartAddr(pfield.fno,rno))
	}

	void get_field (slotid_t rno, pfield_t& pfield)
	{ 
	if (pfield.fixed_size)
		pfield.data = FFieldStartAddr(pfield.fno,rno);
		// ((!pfield.nullable) ? FFieldStartAddr(pfield.fno,rno)
		// :  FNFieldStartAddr(pfield.fno,rno)) 
	else
	   pfield.data = ((pfield.field_size=VFieldSize(pfield.fno,rno)) > 0 ? 
			DataStartAddr(pfield.fno) +
			   (rno>0 ? VarFieldEndOffset(pfield.fno, rno-1) : 0): NULL);
	}
} ;

#endif
