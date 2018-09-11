/* --------------------------------------------------------------- */
/* -- Copyright (c) 1994, 1995 Computer Sciences Department,    -- */
/* -- University of Wisconsin-Madison, subject to the terms     -- */
/* -- and conditions given in the file COPYRIGHT.  All Rights   -- */
/* -- Reserved.                                                 -- */
/* --------------------------------------------------------------- */
/* -- Anastassia Ailamaki                                       -- */
/* --------------------------------------------------------------- */

#define SM_SOURCE

#define PFILE_P_C
#include "sm_int_2.h"
#include "lgrec.h"
#include "w_minmax.h"
#include "sm_du_stats.h"

#include "bitmap.h"
#include "rbitmap.h"
#include "pfile_p.h"

#include "histo.h"
#include "crash.h"

MAKEPAGECODE(pfile_p, page_p) ;  

// not used anymore
//void pfile_p::ntoh()
//{
//	// vid_t vid = pid().vol();
//	// TODO: byteswap the records on the page
//}

//
// format the data page
//
rc_t
pfile_p::format(const lpid_t& pid, tag_t tag, uint4_t flags, store_flag_t store_flags)
{
   w_assert3(tag == t_pfile_p);

// Natassa: This is a dummy header
// I'll put a good header together later
   pfile_p_hdr_t ctrl;
   vec_t vec;
   vec.put(&ctrl, sizeof(ctrl));

   // method format is replaced by _format
	W_DO( page_p::_format(pid, tag, flags, store_flags) );

	// changes in shoreMT
	//persistent_part().store_flags = store_flags;
	
	// always set the store_flag here -- see comments 
	// in bf::fix(), which sets the store flags to st_regular
	// for all pages, and lets the type-specific store manager
	// override (because only file pages can be insert_file)
	//
	// persistent_part().set_page_storeflags(store_flags);
	this->set_store_flags(store_flags); // through the page_p, through the bfcb_t
	
// Natassa: Commenting this out -- puts header in slot 0
//    W_COERCE(page_p::reclaim(0, vec, false));

// Natassa: The only reason I see this is necessary is to set the dirty bit
	rc_t rc = log_page_format(*this, 0, 0, &vec);
	return rc;

	// return RCOK;
}

bool
pfile_p::fits_in_page(const precord_t& prec_info, Size2& sz)
{


	flid_t i;

	// Compute clean overhead of record in page
	sz=0;
	for (i=0; i<NO_FFIELDS; ++i)
	{
	sz += (align(FTotalValueSize(i)+prec_info.field_size[i]) -
		align(FTotalValueSize(i)) + NullmapOvh(NO_RECORDS));
	}
	for (i=NO_FFIELDS; i<NO_FIELDS; ++i)
	{
	sz += (align(VTotalValueSize(i)+prec_info.field_size[i]) -
		align(VTotalValueSize(i)) + OffsetOvh(NO_RECORDS));
	}
// cerr << "Overhead of Record\t" << NO_RECORDS << "\tin page is\t" << sz << "\tbytes, remaining free space is\t" << FREE_SPACE << ".\n";

/*	if (prec_info.no_ffields==13  && NO_RECORDS==204) //table lineitem
		return false;
	else if(prec_info.no_ffields==6  && NO_RECORDS==256) //table orders
		return false;
	else if(prec_info.no_ffields==3  && NO_RECORDS==199) //table part
		return false;*/


	return (sz <= FREE_SPACE) ;
			// - (pheader->no_fields-pheader->no_ffields)); 
	}

bool
pfile_p::fit_fields_in_page(const precord_t& prec_info)
{
	uint1_t i;
	// Overhead is computed with the new one included
	Size2 nullmap_size = NullmapSize(NO_RECORDS+1);
	Size2 offset_table_size = OffsetArrSize(NO_RECORDS+1);

	for (i=0; i<NO_FFIELDS; ++i)
	{
	if (prec_info.field_size[i] <= (Size2)(MinipageSize(i) -
					 (FTotalValueSize(i) + nullmap_size)))
		append_fixed_size_value(i, prec_info.data[i]);
	else 
		break;
	}

	if (i==NO_FFIELDS) // Fixed-size will fit
	{
		for (; i<NO_FIELDS; ++i)
	{
// cerr << "MinipageSize(" << (int)i << ") = " << MinipageSize(i) << endl;
// cerr << "I need " << prec_info.field_size[i] << " bytes " << endl;
// cerr << "I have " << VTotalValueSize(i) << " bytes in value and " << offset_table_size << "(=" << NO_RECORDS << "*" << sizeof(Offset2) << ") " << " in offset space "<< endl;
// cerr << "Free space in minipage is " << (Size2)(MinipageSize(i) - (VTotalValueSize(i) + offset_table_size)) << endl;
		if (prec_info.field_size[i] <= (Size2)(MinipageSize(i) -
						(VTotalValueSize(i) + offset_table_size)))
			append_var_size_value(i, prec_info.data[i], 
						prec_info.field_size[i]);
		else
			break;
	}

		if (i==NO_FIELDS) // Everything will fit
			return true;
	}
// cerr << "Field " << (int)i << " did not fit " << endl;
	return false;
}

void
_move_page_boundary(const int no_fields, flid_t fno, 
				bool *moved, char** source, 
				char** target, const int* len)
{
	int pos;
	if (moved[fno]) 
	{
	return;
	}
	else if (source[fno] == target[fno]) /* no action */
	{
	moved[fno]=true;
		return;
	}
	else if (source[fno] > target[fno]) /* move backwards */
	{
// cerr << "Moving " << (int)fno << " back by " << source[fno]-target[fno] << " bytes (from " << (void*)source[fno] << " to " << (void*)target[fno] << "\n";
		for (pos=0; pos<len[fno]; pos++)
			target[fno][pos] = source[fno][pos];
	moved[fno]=true;
	return;
	}
	else    /* move forward */
	{
		if (fno < no_fields-1)    /* not last field */
	{
		/* move next one first so I don't step on it */
		_move_page_boundary(no_fields, fno+1, moved, source, target, len);
	}
// cerr << "Moving " << (int)fno << " forward by " << target[fno]-source[fno] << " bytes (from " << (void*)source[fno] << " to " << (void*)target[fno] << "\n";
	for (pos=0; -pos<len[fno]; pos--)
		target[fno][pos] = source[fno][pos];
	moved[fno]=true;
	return;
	}
}

rc_t 
pfile_p::reorganize_page(const precord_t& prec_info)
{
// cerr << "REORGANIZING to accomodate record " << NO_RECORDS << endl;
	// if I'm here, something went wrong (a field didn't fit)
	w_assert3(NO_FIELDS > NO_FFIELDS);
	// Recalculate space needs for all varsize fields

	int i;
	// I will only use the positions for the vfields
	double *avg_field_size=new double[NO_FIELDS];

// cerr << "New record field sizes: " ; for (i=0; i<NO_FIELDS; ++i) cerr << prec_info.field_size[i] << ", "; cerr << endl;

	for (i=NO_FFIELDS; i<NO_FIELDS; ++i)
	{
	avg_field_size[i] = (double)(VTotalValueSize(i)+prec_info.field_size[i]) / 
						(double)(NO_RECORDS+1);
		pheader->field_size[i] = (Size2)ceil(avg_field_size[i]);
	}

	// with the new values
	W_DO(pheader->calculate_max_records());

	// as a result of rounding up the average variable field sizes in the
	// header, this may happen once per page
	if (MAX_RECORDS<=NO_RECORDS)
	{
		MAX_RECORDS=NO_RECORDS+1;
	}

	// now recalculate starts one by one and reorganize
	w_assert3(pheader->field_start[0]==0);

	Size2 std_fovh = NullmapSize(MAX_RECORDS);
	Size2 std_vovh = OffsetArrSize(MAX_RECORDS);
	Offset2 *new_field_start=new Offset2[NO_FIELDS];
	bool *moved=new bool[NO_FIELDS];

	new_field_start[0] = DataStartOffset(0); // start doesn't change
	moved[0]=true;

	// First do all fixed-size fields
	char** source=new char*[NO_FIELDS];
	char** target=new char*[NO_FIELDS];
	int *len=new int[NO_FIELDS];

	// i=NO_FFIELDS the first varsize that has a fsize behind it
	// i is varsize, i-1 is fsize
	for (i=1; i<=NO_FFIELDS; ++i) 
	{
		new_field_start[i] = new_field_start[i-1] + 
				 align(MAX_RECORDS * FieldSize(i-1)) + std_fovh;
	}
	for (i=NO_FFIELDS+1; i< NO_FIELDS; ++i)
	{
		new_field_start[i] = new_field_start[i-1] +
				   align((Size2)ceil(MAX_RECORDS*avg_field_size[i-1])) +
				   std_vovh;
// cerr << "avg_field_size["<<i-1<<"]="<<avg_field_size[i-1]<<", MAX_RECORDS="<<MAX_RECORDS<<", (MAX_RECORDS*avg_field_size["<<i-1<<"])="<<(MAX_RECORDS*avg_field_size[i-1])<<endl;
// cerr << "align((Size2)ceil(MAX_RECORDS*avg_field_size["<<i-1<<"]="<<align((Size2)ceil(MAX_RECORDS*avg_field_size[i-1]))<<endl;
// cerr << "new_field_start["<<i<<"]="<<new_field_start[i]<<endl;
	}

	/* NOW compute source and targets */
	/* This is byte-by-byte-copy, doesn't have to be aligned */
	Size2 real_fovh = (Size2)ceil(NO_RECORDS*FREC_OVH);
	Size2 real_vovh = NO_RECORDS * sizeof(Offset2);
// cerr << "real_fovh=" << real_fovh << ", real_vovh=" << real_vovh << endl;
	for (i=1; i<NO_FFIELDS; ++i) 
	{
	len[i] = real_fovh + FTotalValueSize(i);
		if (new_field_start[i] < DataStartOffset(i))   // needs go back
	{
		source[i] = DataStartAddr(i)-real_fovh;
		target[i] = _pp->data+new_field_start[i]-real_fovh;
		}
	else // I have to move it forward, so I'll do it from the end backwards
	{
		source[i] = DataStartAddr(i)+FTotalValueSize(i)-1;
		target[i] = _pp->data+new_field_start[i]+FTotalValueSize(i)-1;
	}
	moved[i]=false;
	}
	
	// Now do the first varsize that has a fsize behind it
	// i is varsize, i-1 is fsize
	len[i] = real_fovh + VTotalValueSize(i);   // fixed overhead + varsize values
	if (new_field_start[i] < DataStartOffset(i))   // needs go back
	{
	// source = where I start - fixed-size overhead from the one before me
		source[i] = DataStartAddr(i)-real_fovh;   
	target[i] = _pp->data+new_field_start[i]-real_fovh;
	}
	else   // I have to move it forward, so I'll do it from the end backwards
	{
		source[i] = DataStartAddr(i)+VTotalValueSize(i)-1;  // my last byte
	target[i] = _pp->data+new_field_start[i]+VTotalValueSize(i)-1;
	}
	moved[i]=false;

	// Now do the rest of the varsize

	for (i=NO_FFIELDS+1; i< NO_FIELDS; ++i)
	{
	len[i] = real_vovh + VTotalValueSize(i);
		if (new_field_start[i] < DataStartOffset(i))   // needs go back
	{
		source[i] = DataStartAddr(i)-real_vovh;
		target[i] = _pp->data+new_field_start[i]-real_vovh;
		}
	else // I have to move it forward, so I'll do it from the end backwards
	{
		source[i] = DataStartAddr(i)+VTotalValueSize(i)-1;
		target[i] = _pp->data+new_field_start[i]+VTotalValueSize(i)-1;
	}
	moved[i]=false;
	}

	// Now do the moving and reset start addresses
	for (i=1; i<NO_FIELDS; ++i)
	{
	_move_page_boundary(NO_FIELDS, i, moved, source, target, len);
	*(Offset2*)DataStartOffsetAddr(i) = new_field_start[i]-PPageHeaderSize;
	}

	delete [] avg_field_size;
	delete [] new_field_start;
	delete [] moved;
	delete [] source;
	delete [] target;
	delete [] len;
// cerr << "***********End of reorganizing\n";
// sanity();
	return RCOK;
}

void 
pfile_p::append_fixed_size_value(const flid_t fno, const char* data)
{

#ifdef CHECK_FOR_NULLS
	// See if it is null
	if (data!=NULL) {
		// Go put the value in (use the macro for nullable)
		memcpy((char*)(DataStartAddr(fno)+
		   r_bm_num_set((u_char*)TailStartAddr(fno), NO_RECORDS)*FieldSize(fno)),
		   data, FieldSize(fno));
		r_bm_set((u_char*)TailStartAddr(fno), NO_RECORDS);
	} else {
		// nullable as far as I'm concerned
		pheader->set_nullable(fno);
		r_bm_clr((u_char*)TailStartAddr(fno), NO_RECORDS);
	}
#else
	r_bm_set((u_char*)TailStartAddr(fno), NO_RECORDS);
	memcpy((char*)(DataStartAddr(fno)+NO_RECORDS*FieldSize(fno)), data, FieldSize(fno));
#endif
}

void 
pfile_p::append_var_size_value(const flid_t fno, const char* data, const Size2 sz)
{
	int nr=NO_RECORDS;
	Offset2 new_offset;
#ifdef CHECK_FOR_NULLS
	// See if it is null
	if (data != NULL) {
		// Go put the value in
#endif
	if (nr>0)  // not the first record
	{
			memcpy(DataStartAddr(fno) + VarFieldEndOffset(fno,nr-1), data, sz);
		// I cannot do this for alignment reasons
		// VarFieldEndOffset(fno,nr) = VarFieldEndOffset(fno,nr-1)+sz;
		memcpy((char*)&new_offset, VarFieldEndPtr(fno,nr-1), sizeof(Offset2));
		new_offset += sz;
		memcpy((char*)(VarFieldEndPtr(fno,nr)), (char*)&new_offset, sizeof(Offset2));
	}
	else   // this is the 0th record
	{
		memcpy(DataStartAddr(fno), data, sz);
		VarFieldEndOffset(fno,nr) = sz;  // It will always be aligned
	}
#ifdef CHECK_FOR_NULLS
	} else { // null 
		// nullable as far as I'm concerned
		pheader->set_nullable(fno);
		// Write the same as the one before that
		VarFieldEndOffset(fno,nr) = VarFieldEndOffset(fno,nr-1);
	}
#endif
}

//Needs the prec_info and the effective size as returned by fits_in_page()
rc_t
pfile_p::append_rec(const precord_t& prec_info, slotid_t& slot) 
{
	// cerr << "Appending record " << recno++ << endl;
	flid_t i;
#ifdef W_DEBUG
	Size2 sz;
	w_assert3 (fits_in_page(prec_info,sz));
	Size2 pre_free = pheader->free_space;
#endif

	if (!fit_fields_in_page(prec_info)) 
	{
		W_DO(reorganize_page(prec_info));
		// now it should fit
		if (!fit_fields_in_page(prec_info))
		{
			cerr << "Internal error! Even after reorganizing the page:" << endl;
			sanity();
			return RC(eRECWONTFIT);
		}
	}
	slot=pheader->no_records;

	pheader->no_records++;
	// recalculate free space
	// cerr << "Free space was\t" << FREE_SPACE;
	pheader->free_space = pheader->data_size;
	for (i=0; i<NO_FFIELDS; ++i)
		pheader->free_space -= align(FTotalValueSize(i));
	for (i=NO_FFIELDS; i<NO_FIELDS; ++i)
		pheader->free_space -= align(VTotalValueSize(i));

	pheader->free_space -= NullmapSize(NO_RECORDS)*NO_FFIELDS +
			OffsetArrSize(NO_RECORDS)*(NO_FIELDS-NO_FFIELDS);
	w_assert3(pre_free-sz==pheader->free_space);
	// cerr << "\tand now is\t" << FREE_SPACE <<".\n";
	w_assert1(pheader->free_space>=0);

	return RCOK;
}                            

// Find column fno for record rno (rno ranges from 0 to record_no-1
// fno ranges from 0 to field_no-1 )

void* pfile_p::fixed_value_addr(const int rno, const flid_t fno) const
{
	w_assert3 (pheader->is_fixed_size(fno));
	w_assert3 (fno < NO_FFIELDS);
	// get the easy case done with

#ifdef CHECK_FOR_NULLS 
	if (pheader->is_non_nullable(fno))
#endif
		return DataStartAddr(fno) + rno * FieldSize(fno);

#ifdef CHECK_FOR_NULLS    
	// r_bm_* handles bitmaps that grow backwards
	// only take into account the non-nulls
	if (r_bm_is_set((u_char*)TailStartAddr(fno), rno))
		return DataStartAddr(fno) +
			rno * r_bm_num_set((u_char*)TailStartAddr(fno), rno);
	return NULL;
#endif
}

// for solidarity purposes
void pfile_p::sanity()
{
	int i;
	bool ff;

	cerr << "Checking page sanity...\n";
	cerr << "\tPage id " << _pp->pid << endl;
	cerr << "\tPage starts at address: " << (void*)_pp->data << "\n";
	cerr << "\tHeader size is: " << PPageHeaderSize << "\n";
	cerr << "\tHeader contents:\n";
	w_assert3(pheader->free_space == FREE_SPACE);
	w_assert3(pheader->data_size == DATA_SIZE);
	cerr << "\t\tAvailable space: " << DATA_SIZE << " bytes \n";
	cerr << "\t\tFree space: " << FREE_SPACE << " bytes \n";
	w_assert3(pheader->max_records == MAX_RECORDS);
	w_assert3(pheader->no_records == NO_RECORDS);
	cerr << "\t\tThere are " << NO_RECORDS << " (max " << MAX_RECORDS << 
							") record(s) in the page.\n";
	w_assert3(pheader->no_fields == NO_FIELDS);
	w_assert3(pheader->no_ffields == NO_FFIELDS);
	cerr << "\t\tThere are " << (int) pheader->no_fields << " fields, " 
			<< (int) pheader->no_ffields << " fixed-size.\n";
	cerr << "\t\tNullable bitmap: " ;
	bm_print((u_char*)(_pp->data+NBOffset), NO_FIELDS);
	cerr << "\n";
	cerr << "\n";
	for (i=0; i<NO_FIELDS; ++i)
	{
		cerr << "\tField " << i+1 ;
		cerr << "\t\tSize: " ;
		w_assert3(FieldSize(i)>=0);
	ff = (i < NO_FFIELDS);
		cerr << FieldSize(i) << "\t\tMinipage size " << MinipageSize(i) << "\n";
		cerr << "\t\tStarts at offset: " << DataStartOffset(i) << 
				" Address: " << (void*)(DataStartAddr(i)) << "\n" ;
		cerr << "\t\tTail starts at offset: ";
		if (ff==true)
		{
		cerr << TailStartOffset(i) << " Address: " << 
							(void*)(TailStartAddr(i));
			cerr << "\t\tPresence bits (" << 
			r_bm_num_set((u_char*)TailStartAddr(i), NO_RECORDS) << " set): ";
			r_bm_print((u_char*)(TailStartAddr(i)), NO_RECORDS);
		}
		else
		{
		cerr << VTailStartOffset(i) << " Address: " << 
							(void*)(VTailStartAddr(i));
			cerr << "\t\tOffsets to value ends: ";
			if (NO_RECORDS>0) cerr << *(Offset2*)VarFieldEndPtr(i,0);
		int j;
			for (j=1; j<NO_RECORDS; ++j)
				cerr << ", " << *(Offset2*)VarFieldEndPtr(i,j);
		}
		cerr << "\n";
	}
}

// rno is the DESIRED record slot id, whereas prec contains rno-1 data
rc_t pfile_p::get_next_rec(slotid_t rno, precord_t& prec_info, const flid_t* ifields)
{
	if (!is_rec_valid(rno))
		return RC(eBADSLOTNUMBER);

	int i;
	prec_info.rec_size = 0;
	for (i=0; i<prec_info.no_ffields;++i)
	{
		prec_info.data[i] += prec_info.field_size[i];
	prec_info.rec_size += prec_info.field_size[i];
	}
	for (i=prec_info.no_ffields; i<prec_info.no_fields; ++i)
	{
	prec_info.data[i] += prec_info.field_size[i];
		prec_info.rec_size += (prec_info.field_size[i] = 
				(ifields!=NULL  ? VFieldSize(ifields[i], rno) 
					: VFieldSize(i, rno)));
	}
	return RCOK;
}

rc_t pfile_p::get_rec(slotid_t rno, precord_t& prec_info)
{
	if (!is_rec_valid(rno))
		return RC(eBADSLOTNUMBER);

	// prec_info must have everything allocated
	w_assert3( prec_info.field_size != NULL && prec_info.data != NULL);
	prec_info.rec_size = 0;

	int i;

	for (i=0; i<prec_info.no_ffields;++i)
	{
			prec_info.data[i] = (char*)FFieldStartAddr(i,rno);
		prec_info.rec_size += prec_info.field_size[i];
	}
	// Set the sizes and addresses of variable fields
	for (i=prec_info.no_ffields; i<prec_info.no_fields;++i)
	{
		prec_info.rec_size += (prec_info.field_size[i] = VFieldSize(i, rno));
		prec_info.data[i] = (prec_info.field_size[i] > 0 ? 
			(rno==0 ? DataStartAddr(i) : 
			DataStartAddr(i) + VarFieldEndOffset(i,rno-1)) : NULL);
	}
	return RCOK;
}

rc_t pfile_p::get_rec(slotid_t rno, precord_t& prec_info, const flid_t *ifields)
{
	if (!is_rec_valid(rno))
		return RC(eBADSLOTNUMBER);

	// prec_info must have everything allocated
	w_assert3(prec_info.field_size != NULL && prec_info.data != NULL);
	w_assert1(ifields!= NULL);

	flid_t i;
	prec_info.rec_size = 0;

	// just in case they are mixed up
	for (i=0; i<prec_info.no_ffields;++i)
	{
#ifdef CHECK_FOR_NULLS
		if (pheader->is_non_nullable(ifields[i]))
	{
#endif
			prec_info.data[i] = (char*)FFieldStartAddr(ifields[i],rno);
		prec_info.rec_size += prec_info.field_size[i];
#ifdef CHECK_FOR_NULLS
	}
		else
	{
			prec_info.data[i] = (char*)FNFieldStartAddr(ifields[i],rno);
			if (prec_info.data[i]!=NULL)
		prec_info.rec_size += prec_info.field_size[i];
		}
#endif
	}
	// Set the sizes and addresses of variable fields
	for (i=prec_info.no_ffields;i<prec_info.no_fields;++i)
	{
		prec_info.rec_size += 
		(prec_info.field_size[i] = VFieldSize(ifields[i], rno));
		prec_info.data[i] = (prec_info.field_size[i] > 0 ? 
			  (rno==0 ? DataStartAddr(ifields[i]) : 
		  DataStartAddr(ifields[i]) + VarFieldEndOffset(ifields[i],rno-1))
		  : NULL);
	}
	return RCOK;
}


