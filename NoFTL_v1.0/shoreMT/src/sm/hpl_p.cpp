/* ------------------------------------------------------------------------------- */
/* -- Copyright (c) 2011, 2012 Databases and Distributed Systems Department,    -- */
/* -- Technical University of Darmstadt, subject to the terms and conditions    -- */
/* -- given in the file COPYRIGHT.  All Rights Reserved.                        -- */
/* ------------------------------------------------------------------------------- */
/* -- Ilia Petrov, Todor Ivanov, Veselin Marinov                                -- */
/* ------------------------------------------------------------------------------- */

#define SM_SOURCE

#define HPL_P_C
#include "sm_int_2.h"
#include "lgrec.h"
#include "w_minmax.h"
#include "sm_du_stats.h"
#include <math.h>

#include "bitmap.h"
#include "rbitmap.h"
#include "hpl_p.h"

#include "histo.h"
#include "crash.h"

#include <TimeMeasure.h>

#include <emmintrin.h>

#define PREFETCH_T0(addr,nrOfBytesAhead) _mm_prefetch((char *)(addr+nrOfBytesAhead),_MM_HINT_T0)

MAKEPAGECODE(hpl_p, page_p);

inline uint2_t hpl_gcd(uint2_t u, uint2_t v)
{
	// simple cases (termination)
	if (u == v)
		return u;
	if (u == 0)
		return v;
	if (v == 0)
		return u;

	// look for factors of 2
	if (~u & 1) // u is even
	{
		if (v & 1) // v is odd
			return hpl_gcd(u >> 1, v);
		else // both u and v are even
			return hpl_gcd(u >> 1, v >> 1) << 1;
	}
	if (~v & 1) // u is odd, v is even
		return hpl_gcd(u, v >> 1);

	// reduce larger argument
	if (u > v)
		return hpl_gcd((u - v) >> 1, v);
	return hpl_gcd((v - u) >> 1, u);
}

//#pragma GCC optimize (0)

inline int2_t hpl_div(int2_t x,int2_t  devisor)
{

/*
	switch (devisor)
	{
	case 1:
		return x;
		break;
	case 2:
		return x >> 1;
		break;
	case 4:
		return x >> 2;
		break;
	case 8:
		return x >> 3;
		break;
	case 16:
		return x >> 4;
		break;
	case 32:
		return x >> 5;
		break;
	case 64:
		return x >> 6;
		break;
	case 128:
		return x >> 7;
		break;
	case 256:
		return x >> 8;
		break;
	case 512:
		return x >> 9;
		break;
	case 1024:
		return x >> 10;
		break;
	default:
		cout << "hpl_div: "<<x<< " / " << devisor << endl;
		return x / devisor;
		break;
	}
*/

	return x / devisor;
}

//#pragma GCC optimize (3)

inline int2_t hpl_func1(int2_t x,int2_t  devisor)
{

/*	switch (devisor)
	{
	case 1:
		return x;
		break;
	case 2:
		return (x >> 1) << 1;
		break;
	case 4:
		return (x >> 2) << 2;
		break;
	case 8:
		return (x >> 3) << 3;
		break;
	case 16:
		return (x >> 4) << 4;
		break;
	case 32:
		return (x >> 5) << 5;
		break;
	case 64:
		return (x >> 6) << 6;
		break;
	case 128:
		return (x >> 7) << 7;
		break;
	case 256:
		return (x >> 8) << 8;
		break;
	case 512:
		return (x >> 9) << 9;
		break;
	case 1024:
		return (x >> 10) << 10;
		break;
	default:
		cout << "hpl_func: "<< devisor << " * ( "<<x<< " / " << devisor << " )"<< endl;
		return devisor * (x / devisor);
		break;
	}*/
	return devisor * (x / devisor);
}

inline int2_t hpl_mod(int2_t x,int2_t mod)
{
	//cout<< "Mod is: "<< mod <<endl;
	//const unsigned int d = 1U << mod; 	// So d will be one of: 1, 2, 4, 8, 16, 32, ...
	//cout<< "Mod is: "<< (int2_t)(x & (mod - 1)) <<endl;
	//return 	(int2_t)(x & (mod - 1));  		// m will be n % d
	return x & (mod-1);
	//return x % mod;
}


//
// format the data page
//
rc_t hpl_p::format(const lpid_t& pid, tag_t tag, uint4_t flags, store_flag_t store_flags)
{
	w_assert3(tag == t_hpl_p);

	hpl_p_hdr_t ctrl;
	vec_t vec;
	vec.put(&ctrl, sizeof(ctrl));

	// method format is replaced by _format
	W_DO( page_p::_format(pid, tag, flags, store_flags) );

	this->set_store_flags(store_flags); // through the page_p, through the bfcb_t

	rc_t rc = log_page_format(*this, 0, 0, &vec);

	return rc;
}

/**
 * check if there is enough place in page to store a field
 */
bool hpl_p::fits_in_page(fieldSize_t dataSize)
{
	if (getSizeOfVarFieldInVarArea(dataSize) < FREE_SPACE)
	{
		return true;
	}
	else
	{
		// try to find space in the holes
		if (get_free_var_field_offset(dataSize) > 0)
		{
			return true;
		}
		else
		{
			//TODO: HPL: implement reorganize
			//reorganize_page();
			return false;
		}
	}
}

/**
 * checks if record fits in page
 * size is set to the size of record
 */
bool hpl_p::fits_in_page(const hpl_record_t& record, recordSize_t& recordSize)
{
	recordSize = 0;

	// compute size of var fields
	recordSize_t sizeVarFields = 0;

	flid_t fieldNumber = NO_FFIELDS;
	for (; fieldNumber < NO_FIELDS; ++fieldNumber)
	{
		// check for varFields
		//fieldSize_t currentFieldSize =( IsFixedSize(fieldNumber)? FieldSize(fieldNumber) : varFixedPartSize);
		//if (!IsFixedSize(fieldNumber))
		sizeVarFields += record.fieldSize[fieldNumber] + varFixedPartSize + sizeof(fieldSize_t);
	}


	offset_t sizeOfBlocks = numberOfNewBlocksNeeded(get_free_record_slot()) * CacheBlockSize;

	if (FREE_RECORDS > 0)
	{
		if (sizeOfBlocks + sizeVarFields < FREE_SPACE - (offset_t)thresholdInsert)//getInsertThresholdInBytes()
		{
			return true;
		}
		else
		{
			reorganize_page(record);
			if (sizeOfBlocks + sizeVarFields < FREE_SPACE - (offset_t)thresholdInsert)
				return true;
			else
				return false;
		}
	}
	else
	{
		bitmapSize_t ghostAndNullVectorSizePerSegment = SizeOfGhostBitmapPerSegment+SizeOfNullBitmapPerSegment;
		// try for new segment
		if (ghostAndNullVectorSizePerSegment + sizeOfBlocks + sizeVarFields < FREE_SPACE - (offset_t)thresholdInsert)
		{
			init_new_segment_structure();
			return true;
		}
		else
		{
			reorganize_page(record);

			// try for new segment
			if (ghostAndNullVectorSizePerSegment + sizeOfBlocks + sizeVarFields	< FREE_SPACE - (offset_t)thresholdInsert)
			{
				init_new_segment_structure();
				return true;
			}
			else
			{
				return false;
			}
		}
	}
}

//***********************************************************************************
//PAX-only  functions
//***********************************************************************************
//bool
//hpl_p::fit_fields_in_page(const hpl_record_t& record)
//{
////	uint1_t i;
////	// Overhead is computed with the new one included
////	Size2 nullmap_size = NullmapSize(NO_RECORDS+1);
////	Size2 offset_table_size = OffsetArrSize(NO_RECORDS+1);
////
////	for (i=0; i<NO_FFIELDS; ++i)
////	{
////		if (record.fieldSize[i] <= (Size2)(MinipageSize(i) - (FTotalValueSize(i) + nullmap_size)))
////		{
////			append_fixed_size_value(i, record.data[i]);
////
////			//test HPL offset
////			int offset = get_field_offset(NO_RECORDS+1,i);
////			//cout << "Get_field_offset= "<< offset <<" for recNmbr: " << NO_RECORDS+1 << " and field: "<< (int)i << endl;
////
////		}else
////			break;
////	}
////
////	if (i==NO_FFIELDS) // Fixed-size will fit
////	{
////		for (; i<NO_FIELDS; ++i)
////		{
////			if (record.fieldSize[i] <= (Size2)(MinipageSize(i) - (VTotalValueSize(i) + offset_table_size)))
////			{
////				append_var_size_value(i, record.data[i],record.fieldSize[i]);
////			}else
////				break;
////		}
////
////		if (i==NO_FIELDS) // Everything will fit
////			return true;
////	}
//
//	return false;
//}

void _move_hpl_page_boundary(const int no_fields, flid_t fno, bool *moved, char** source,
		char** target, const int* len)
{
	int pos;
	if (moved[fno])
	{
		return;
	}
	else if (source[fno] == target[fno]) /* no action */
	{
		moved[fno] = true;
		return;
	}
	else if (source[fno] > target[fno]) /* move backwards */
	{
		// cerr << "Moving " << (int)fno << " back by " << source[fno]-target[fno] << " bytes (from " << (void*)source[fno] << " to " << (void*)target[fno] << "\n";
		for (pos = 0; pos < len[fno]; pos++)
			target[fno][pos] = source[fno][pos];
		moved[fno] = true;
		return;
	}
	else /* move forward */
	{
		if (fno < no_fields - 1) /* not last field */
		{
			/* move next one first so I don't step on it */
			_move_hpl_page_boundary(no_fields, fno + 1, moved, source, target, len);
		}
		// cerr << "Moving " << (int)fno << " forward by " << target[fno]-source[fno] << " bytes (from " << (void*)source[fno] << " to " << (void*)target[fno] << "\n";
		for (pos = 0; -pos < len[fno]; pos--)
			target[fno][pos] = source[fno][pos];
		moved[fno] = true;
		return;
	}
}

rc_t hpl_p::reorganize_page(const hpl_record_t& record)
{
	//	// cerr << "REORGANIZING to accomodate record " << NO_RECORDS << endl;
	//	// if I'm here, something went wrong (a field didn't fit)
	//	w_assert3(NO_FIELDS > NO_FFIELDS);
	//	// Recalculate space needs for all varsize fields
	//
	//	int i;
	//	// I will only use the positions for the vfields
	//	double *avg_field_size=new double[NO_FIELDS];
	//
	//	for (i=NO_FFIELDS; i<NO_FIELDS; ++i)
	//	{
	//		avg_field_size[i] = (double)(VTotalValueSize(i)+record.fieldSize[i]) /
	//				(double)(NO_RECORDS+1);
	//		hpl_header->fieldSize[i] = (Size2)ceil(avg_field_size[i]);
	//	}
	//
	//
	//	// as a result of rounding up the average variable field sizes in the
	//	// header, this may happen once per page
	//	if (MAX_RECORDS<=NO_RECORDS)
	//	{
	//		MAX_RECORDS=NO_RECORDS+1;
	//	}
	//
	//	// now recalculate starts one by one and reorganize
	//	w_assert3(hpl_header->fieldStart[0]==0);
	//
	//	Size2 std_fovh = NullmapSize(MAX_RECORDS);
	//	Size2 std_vovh = OffsetArrSize(MAX_RECORDS);
	//	Offset2 *new_field_start=new Offset2[NO_FIELDS];
	//	bool *moved=new bool[NO_FIELDS];
	//
	//	new_field_start[0] = DataStartOffset(0); // start doesn't change
	//	moved[0]=true;
	//
	//	// First do all fixed-size fields
	//	char** source=new char*[NO_FIELDS];
	//	char** target=new char*[NO_FIELDS];
	//	int *len=new int[NO_FIELDS];
	//
	//	// i=NO_FFIELDS the first varsize that has a fsize behind it
	//	// i is varsize, i-1 is fsize
	//	for (i=1; i<=NO_FFIELDS; ++i)
	//	{
	//		new_field_start[i] = new_field_start[i-1] +
	//				align(MAX_RECORDS * FieldSize(i-1)) + std_fovh;
	//	}
	//	for (i=NO_FFIELDS+1; i< NO_FIELDS; ++i)
	//	{
	//		new_field_start[i] = new_field_start[i-1] +
	//				align((Size2)ceil(MAX_RECORDS*avg_field_size[i-1])) +
	//				std_vovh;
	//	}
	//
	//	/* NOW compute source and targets */
	//	/* This is byte-by-byte-copy, doesn't have to be aligned */
	//	Size2 real_fovh = (Size2)ceil(NO_RECORDS*FREC_OVH);
	//	Size2 real_vovh = NO_RECORDS * sizeof(Offset2);
	//	// cerr << "real_fovh=" << real_fovh << ", real_vovh=" << real_vovh << endl;
	//	for (i=1; i<NO_FFIELDS; ++i)
	//	{
	//		len[i] = real_fovh + FTotalValueSize(i);
	//		if (new_field_start[i] < DataStartOffset(i))   // needs go back
	//		{
	//			source[i] = DataStartAddr(i)-real_fovh;
	//			target[i] = _pp->data+new_field_start[i]-real_fovh;
	//		}
	//		else // I have to move it forward, so I'll do it from the end backwards
	//		{
	//			source[i] = DataStartAddr(i)+FTotalValueSize(i)-1;
	//			target[i] = _pp->data+new_field_start[i]+FTotalValueSize(i)-1;
	//		}
	//		moved[i]=false;
	//	}
	//
	//	// Now do the first varsize that has a fsize behind it
	//	// i is varsize, i-1 is fsize
	//	len[i] = real_fovh + VTotalValueSize(i);   // fixed overhead + varsize values
	//	if (new_field_start[i] < DataStartOffset(i))   // needs go back
	//	{
	//		// source = where I start - fixed-size overhead from the one before me
	//		source[i] = DataStartAddr(i)-real_fovh;
	//		target[i] = _pp->data+new_field_start[i]-real_fovh;
	//	}
	//	else   // I have to move it forward, so I'll do it from the end backwards
	//	{
	//		source[i] = DataStartAddr(i)+VTotalValueSize(i)-1;  // my last byte
	//		target[i] = _pp->data+new_field_start[i]+VTotalValueSize(i)-1;
	//	}
	//	moved[i]=false;
	//
	//	// Now do the rest of the varsize
	//
	//	for (i=NO_FFIELDS+1; i< NO_FIELDS; ++i)
	//	{
	//		len[i] = real_vovh + VTotalValueSize(i);
	//		if (new_field_start[i] < DataStartOffset(i))   // needs go back
	//		{
	//			source[i] = DataStartAddr(i)-real_vovh;
	//			target[i] = _pp->data+new_field_start[i]-real_vovh;
	//		}
	//		else // I have to move it forward, so I'll do it from the end backwards
	//		{
	//			source[i] = DataStartAddr(i)+VTotalValueSize(i)-1;
	//			target[i] = _pp->data+new_field_start[i]+VTotalValueSize(i)-1;
	//		}
	//		moved[i]=false;
	//	}
	//
	//	// Now do the moving and reset start addresses
	//	for (i=1; i<NO_FIELDS; ++i)
	//	{
	//		_move_hpl_page_boundary(NO_FIELDS, i, moved, source, target, len);
	//		*(Offset2*)DataStartOffsetAddr(i) = new_field_start[i]-HPLPageHeaderSize;
	//	}
	//
	//	delete [] avg_field_size;
	//	delete [] new_field_start;
	//	delete [] moved;
	//	delete [] source;
	//	delete [] target;
	//	delete [] len;
	//	// cerr << "***********End of reorganizing\n";
	//	// sanity();
	return RCOK;
}

void hpl_p::append_fixed_size_value(const flid_t fno, const char* data)
{

	//#ifdef CHECK_FOR_NULLS
	//	// See if it is null
	//	if (data!=NULL) {
	//		// Go put the value in (use the macro for nullable)
	//		memcpy((char*)(DataStartAddr(fno)+r_bm_num_set((u_char*)TailStartAddr(fno), NO_RECORDS)*FieldSize(fno)),data, FieldSize(fno));
	//		r_bm_set((u_char*)TailStartAddr(fno), NO_RECORDS);
	//	} else {
	//		// nullable as far as I'm concerned
	//		hpl_header->set_nullable(fno);
	//		r_bm_clr((u_char*)TailStartAddr(fno), NO_RECORDS);
	//	}
	//#else
	//	r_bm_set((u_char*)TailStartAddr(fno), NO_RECORDS);
	//	memcpy((char*)(DataStartAddr(fno)+NO_RECORDS*FieldSize(fno)), data, FieldSize(fno));
	//#endif
	//}
	//
	//void
	//hpl_p::append_var_size_value(const flid_t fno, const char* data, const Size2 sz)
	//{
	//	int nr=NO_RECORDS;
	//	Offset2 new_offset;
	//#ifdef CHECK_FOR_NULLS
	//	// See if it is null
	//	if (data != NULL) {
	//		// Go put the value in
	//#endif
	//		if (nr>0)  // not the first record
	//		{
	//			memcpy(DataStartAddr(fno) + VarFieldEndOffset(fno,nr-1), data, sz);
	//			// I cannot do this for alignment reasons
	//			// VarFieldEndOffset(fno,nr) = VarFieldEndOffset(fno,nr-1)+sz;
	//			memcpy((char*)&new_offset, VarFieldEndPtr(fno,nr-1), sizeof(Offset2));
	//			new_offset += sz;
	//			memcpy((char*)(VarFieldEndPtr(fno,nr)), (char*)&new_offset, sizeof(Offset2));
	//		}
	//		else   // this is the 0th record
	//		{
	//			memcpy(DataStartAddr(fno), data, sz);
	//			VarFieldEndOffset(fno,nr) = sz;  // It will always be aligned
	//		}
	//#ifdef CHECK_FOR_NULLS
	//	} else { // null
	//		// nullable as far as I'm concerned
	//		hpl_header->set_nullable(fno);
	//		// Write the same as the one before that
	//		VarFieldEndOffset(fno,nr) = VarFieldEndOffset(fno,nr-1);
	//	}
	//#endif
}

//Needs the record and the effective size as returned by fits_in_page()
rc_t hpl_p::append_rec(const hpl_record_t& record, recordID_t& slot)
{

	//	flid_t i;
	//	/*#ifdef W_DEBUG
	//		Size2 sz;
	//		w_assert3 (fits_in_page(record,sz));
	//		Size2 pre_free = hpl_header->freeSpace;
	//	#endif*/
	//
	//
	//	fit_fields_in_page(record);
	//
	//	// no need of reorganization
	//	// store the fields in the mini-pages and return if they fit
	//	/*	if (!fit_fields_in_page(record))
	//	{
	//		W_DO(reorganize_page(record));
	//		// now it should fit
	//		if (!fit_fields_in_page(record))
	//		{
	//			cerr << "Internal error! Even after reorganizing the page:" << endl;
	//			sanity();
	//			return RC(eRECWONTFIT);
	//		}
	//	} */
	//
	//	// update administrative structures in header
	//	slot=hpl_header->numberOfRecordsInPage;
	//	hpl_header->numberOfRecordsInPage++;
	//	///////////////////////////////////
	//	// recalculate free space
	//	///////////////////////////////////
	//	hpl_header->freeSpace = hpl_header->dataSize;
	//	for (i=0; i<NO_FFIELDS; ++i)
	//	{
	//		hpl_header->freeSpace -= align(FTotalValueSize(i));
	//	}
	//	for (i=NO_FFIELDS; i<NO_FIELDS; ++i)
	//	{
	//		hpl_header->freeSpace -= align(VTotalValueSize(i));
	//	}
	//	hpl_header->freeSpace -= NullmapSize(NO_RECORDS)*NO_FFIELDS +
	//			OffsetArrSize(NO_RECORDS)*(NO_FIELDS-NO_FFIELDS);
	//
	//	w_assert3(pre_free-sz==hpl_header->freeSpace);
	//	w_assert1(hpl_header->freeSpace>=0);

	return RCOK;
}

// Find column fno for record rno (rno ranges from 0 to record_no-1
// fno ranges from 0 to field_no-1 )

void* hpl_p::fixed_value_addr(const recordID_t rno, const flid_t fno) const
{
	//	w_assert3 (hpl_header->getIsFixedSize(fno));
	//	w_assert3 (fno < NO_FFIELDS);
	//	// get the easy case done with
	//
	//#ifdef CHECK_FOR_NULLS
	//	if (hpl_header->is_non_nullable(fno))
	//#endif
	//		return DataStartAddr(fno) + rno * FieldSize(fno);
	//
	//#ifdef CHECK_FOR_NULLS
	//	// r_bm_* handles bitmaps that grow backwards
	//	// only take into account the non-nulls
	//	if (r_bm_is_set((u_char*)TailStartAddr(fno), rno))
	//		return DataStartAddr(fno) +
	//		rno * r_bm_num_set((u_char*)TailStartAddr(fno), rno);

	//	return NULL;
	//#endif
}

// for solidarity purposes
void hpl_p::sanity()
{
	/*int i;
	 bool ff;

	 cerr << "Checking page sanity...\n";
	 cerr << "\tPage id " << _pp->pid << endl;
	 cerr << "\tPage starts at address: " << (void*)_pp->data << "\n";
	 cerr << "\tHeader size is: " << HPLPageHeaderSize << "\n";
	 cerr << "\tHeader contents:\n";
	 w_assert3(hpl_header->freeSpace == FREE_SPACE);
	 w_assert3(hpl_header->dataSize == DATA_SIZE);
	 cerr << "\t\tAvailable space: " << DATA_SIZE << " bytes \n";
	 cerr << "\t\tFree space: " << FREE_SPACE << " bytes \n";
	 w_assert3(hpl_header->max_records == MAX_RECORDS);
	 w_assert3(hpl_header->getNumberOfRecords == NO_RECORDS);
	 cerr << "\t\tThere are " << NO_RECORDS << " (max " << MAX_RECORDS <<
	 ") record(s) in the page.\n";
	 w_assert3(hpl_header->getNumberOfFields == NO_FIELDS);
	 w_assert3(hpl_header->getNumberOfFixedFields == NO_FFIELDS);
	 cerr << "\t\tThere are " << (int) hpl_header->numberOfFields << " fields, "
	 << (int) hpl_header->numberOfFixedSizeFields << " fixed-size.\n";
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
	 */
}

//***********************************************************************************
//PAX-only  end
//***********************************************************************************


//***********************************************************************************
//PAX&HPL  start
//***********************************************************************************

rc_t hpl_p::get_next_rec(recordID_t recordNumber, hpl_record_t& record)
{
#if HPL_DEBUG>0
	if (!is_rec_valid(recordNumber))
		return RC(eBADSLOTNUMBER);

	// record must have everything allocated
	w_assert3(record.getFieldSize != NULL && record.data != NULL);
#endif

	record.dataSize = 0;

	// check the ghost bit of the record
	if (isSetGhostbit(recordNumber))
	{
		flid_t fieldNumber;
		for (fieldNumber = 0; fieldNumber < record.numberOfFixedSizeFields; ++fieldNumber)
		{
			if (isSetNullbit(recordNumber, fieldNumber))
			{
				if (isFirstInBlock(recordNumber, fieldNumber))
				{
					//get the offset for this field
					offset_t offset = get_offset_field(recordNumber, fieldNumber);
					record.data[fieldNumber] = (_pp->data + HPLPageHeaderSize + offset);
#if HPL_DEBUG>1
					printf("Reading fixField(%.d): \"%d\" (%.d,%.d) at %.d\n", record.fieldSize[fieldNumber], record.data[fieldNumber], recordNumber, fieldNumber, offset);
					cout << "Reading \"" << *(Offset2*)record.data[fieldNumber] << "\" (" << recordNumber << "," << fieldNumber << ") at " << offset << "\n";
#endif
				}
				else
				{
					record.data[fieldNumber] += record.fieldSize[fieldNumber];
				}
				record.dataSize += record.fieldSize[fieldNumber];
			}
			else
			{
#if HPL_DEBUG>0
				cout << "You should never have seen this!" << endl;
#endif
				//data is not valid
				record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
				record.data[fieldNumber][0] = '\0';
				//record.fieldSize[fieldNumber] = 0;
			}

		}
		for (; fieldNumber < record.numberOfFields; ++fieldNumber)
		{
			//check if field is valid in the nullbitmap
			if (isSetNullbit(recordNumber, fieldNumber))
			{
				//get the offset for this field
				offset_t offset = get_offset_field(recordNumber, fieldNumber);

				// get the real varData
				if (strncmp((_pp->data + HPLPageHeaderSize + offset + sizeof(offset_t)), "\0", (varFixedPartSize - sizeof(offset_t))) != 0)
				{
					size_t sizeVarField = (*(Size2*) (_pp->data + HPLPageHeaderSize + (*(offset_t*) (_pp->data + HPLPageHeaderSize + offset))));

					if (sizeVarField > 0 && sizeVarField < 256)
					{
						record.fieldSize[fieldNumber] = sizeVarField + (varFixedPartSize - sizeof(offset_t));
						record.data[fieldNumber] = (char*) (_pp->data + HPLPageHeaderSize + offset + (varFixedPartSize - sizeof(offset_t)));
//						record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
//						memcpy(record.data[fieldNumber], (char*) (_pp->data + HPLPageHeaderSize + offset + sizeof(offset_t)), 2);
//						memcpy(record.data[fieldNumber] + 2, (char*) (_pp->data + HPLPageHeaderSize + (*(offset_t*) (_pp->data + HPLPageHeaderSize + offset)) + sizeof(Size2)), (num - 1));
//						record.data[fieldNumber][record.fieldSize[fieldNumber] - 1] = '\0';
						record.dataSize += record.fieldSize[fieldNumber];
#if HPL_DEBUG>1
								cout<<"("<< recordNumber << "," << fieldNumber << ") Size: "<< sizeVarField << "\\ OffsetFix: "<< offset <<" OffsetVar: "<<(*(offset_t*)(_pp->data+HPLPageHeaderSize+offset)) << " FieldValue(c)["<< record.fieldSize[fieldNumber] << "]: ";
								cout << record.data[fieldNumber]<<endl;
								//cout << "Reading \"" << *(Offset2*)record.data[fieldNumber] << "\" (" << recordNumber << "," << fieldNumber << ") at " << offset << "\n";
#endif

					}
					else
					{
						//data is not valid
#if HPL_DEBUG>0
						cout << "Invalid length of varField data! recordNumber= " << recordNumber << "; record.fieldSize[" << fieldNumber << "]="
								<< record.fieldSize[fieldNumber] << "; sizeVarField=" << sizeVarField << endl;
#endif
						record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
						record.data[fieldNumber][0] = '\0';
					}
				}
				else
				{
					//data is not valid
					record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
					record.data[fieldNumber][0] = '\0';
					//record.fieldSize[fieldNumber] = 0;
				}
			}
			else
			{
				//data is not valid
				record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
				record.data[fieldNumber][0] = '\0';
				//record.fieldSize[fieldNumber] = 0;
			}

			/*if (isFirstInBlock(recordNumber, fieldNumber))
			 {
			 //get the offset for this field
			 offset_t offset = get_offset_field(recordNumber, fieldNumber);
			 // used to get only poorman
			 record.data[fieldNumber] = (_pp->data + HPLPageHeaderSize + offset + sizeof(offset_t));
			 #if HPL_DEBUG>1
			 printf("Reading varField(%.d): \"%d\" (%.d,%.d) at %.d\n", record.fieldSize[fieldNumber], record.data[fieldNumber], recordNumber, fieldNumber, offset);
			 cout << "Reading \"" << *(Offset2*)record.data[fieldNumber] << "\" (" << recordNumber << "," << fieldNumber << ") at " << offset << "\n";
			 #endif
			 }
			 else
			 {
			 // get the var fixed field
			 record.data[fieldNumber] += varFixedPartSize; //record.fieldSize[fieldNumber];
			 }
			 record.dataSize += varFixedPartSize;*/
		}
	}
	else
	{
		// record not valid
		for (int f = 0; f < record.numberOfFields; ++f)
		{
			record.data[f] = NULL;
		}
		//record.numberOfFields=0;
	}

	return RCOK;
}

// rno is the DESIRED record slot id, whereas prec contains rno-1 data
rc_t hpl_p::get_next_rec(recordID_t recordNumber, hpl_record_t& record, const flid_t* ifields)
{
#if HPL_DEBUG>0
	if (!is_rec_valid(recordNumber))
		return RC(eBADSLOTNUMBER);
	// record must have everything allocated
	w_assert3(record.getFieldSize != NULL && record.data != NULL);w_assert1(ifields!= NULL);
#endif

	record.dataSize = 0;

	// check the ghost bit of the record
	if (isSetGhostbit(recordNumber))
	{
		// fix size fields
		flid_t fieldNumber;
		for (fieldNumber = 0; fieldNumber < record.numberOfFixedSizeFields; ++fieldNumber)
		{
			//check if field is valid in the nullbitmap
			if (isSetNullbit(recordNumber, ifields[fieldNumber]))
			{

				if (isFirstInBlock(recordNumber, ifields[fieldNumber]))
				{
					//get the offset for this field
					offset_t offset = get_offset_field(recordNumber, ifields[fieldNumber]);
					record.data[fieldNumber] = (_pp->data + HPLPageHeaderSize + offset);
				}
				else
				{
					record.data[fieldNumber] += record.fieldSize[fieldNumber];
				}
				record.dataSize += record.fieldSize[fieldNumber];

			}
			else
			{
				//data is not valid
				record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
				record.data[fieldNumber][0] = '\0';
				//record.fieldSize[fieldNumber] = 0;
			}

#if HPL_DEBUG>1
			printf("Reading fixField(%.d): \"%d\" (%.d,%.d)\n", record.fieldSize[fieldNumber], record.data[fieldNumber], recordNumber, ifields[fieldNumber]);
			cout << "Reading fix \"" << *(Offset2*)record.data[fieldNumber] << "\" (" << recordNumber << "," << ifields[fieldNumber] << ")" << "\n";
#endif
		}
		// var fields
		for (; fieldNumber < record.numberOfFields; ++fieldNumber)
		{

			//check if field is valid in the nullbitmap
			if (isSetNullbit(recordNumber, fieldNumber))
			{
				//get the offset for this field
				offset_t offset = get_offset_field(recordNumber, ifields[fieldNumber]);

				// get the real varData
				if (strncmp((_pp->data + HPLPageHeaderSize + offset	+ sizeof(offset_t)), "\0", (varFixedPartSize - sizeof(offset_t))) != 0)
				{
					size_t sizeVarField = (*(Size2*) (_pp->data + HPLPageHeaderSize	+ (*(offset_t*) (_pp->data + HPLPageHeaderSize + offset))));

					if (sizeVarField > 0 && sizeVarField < 256)
					{
						record.fieldSize[fieldNumber] = sizeVarField + varFixedPartSize - sizeof(offset_t);
						record.data[fieldNumber] = (_pp->data + HPLPageHeaderSize + offset + (varFixedPartSize - sizeof(offset_t)));
						// poor man disabled
						//record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
						//memcpy(record.data[fieldNumber], (char*) (_pp->data + HPLPageHeaderSize + offset + sizeof(offset_t)), 2);
						//memcpy(record.data[fieldNumber] + 2, (char*) (_pp->data + HPLPageHeaderSize + (*(offset_t*) (_pp->data + HPLPageHeaderSize + offset)) + sizeof(Size2)), (sizeVarField - 1));
						//record.data[fieldNumber][record.fieldSize[fieldNumber] - 1] = '\0';
						record.dataSize += record.fieldSize[fieldNumber]; // varFixedPartSize - sizeof(offset_t) for data in poorman
#if HPL_DEBUG>1
						cout<<"("<< recordNumber << "," << fieldNumber << ") Size: "<< sizeVarField << "\\ OffsetFix: "<< offset <<" OffsetVar: "<<(*(offset_t*)(_pp->data+HPLPageHeaderSize+offset)) << " FieldValue(c)["<< record.fieldSize[fieldNumber] << "]: ";
						cout << record.data[fieldNumber]<<endl;
						//cout << "Reading \"" << *(Offset2*)record.data[fieldNumber] << "\" (" << recordNumber << "," << fieldNumber << ") at " << offset << "\n";
#endif
					}
					else
					{
						//data is not valid
#if HPL_DEBUG>0
						cout
								<< "Invalid length of varField data! recordNumber= "
								<< recordNumber << "; record.fieldSize["
								<< fieldNumber << "]="
								<< record.fieldSize[fieldNumber]
								<< "; sizeVarField=" << sizeVarField << endl;
#endif
						record.data[fieldNumber] =	new char[record.fieldSize[fieldNumber]];
						record.data[fieldNumber][0] = '\0';
					}
				}
				else
				{
					//data is not valid
					record.data[fieldNumber] =	new char[record.fieldSize[fieldNumber]];
					record.data[fieldNumber][0] = '\0';
					//record.fieldSize[fieldNumber] = 0;
				}
			}
			else
			{
				//data is not valid
				record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
				record.data[fieldNumber][0] = '\0';
				//record.fieldSize[fieldNumber] = 0;
			}

//			if (isFirstInBlock(recordNumber, ifields[fieldNumber]))
//			{
//				//get the offset for this field
//				offset_t offset = get_offset_field(recordNumber,ifields[fieldNumber]);
//				record.data[fieldNumber] = (_pp->data + HPLPageHeaderSize + offset + sizeof(offset_t));
//			}
//			else
//			{
//				record.data[fieldNumber] += varFixedPartSize;
//			}
//			record.dataSize += varFixedPartSize;
//#if HPL_DEBUG>1
//			printf("Reading varField(%.d): \"%d\" (%.d,%.d)\n", record.fieldSize[fieldNumber], record.data[fieldNumber], recordNumber, ifields[fieldNumber]);
//			cout << "Reading var \"" << *(Offset2*)record.data[fieldNumber] << "\" (" << recordNumber << "," << ifields[fieldNumber] << ")" << "\n";
//#endif
		}
	}
	else
	{
		// record not valid
		for (int f = 0; f < record.numberOfFields; ++f)
		{
			record.data[f] = NULL;
		}
		//record.numberOfFields=0;
	}

	return RCOK;
}

/*rc_t hpl_p::get_rec(recordID_t recordNumber, hpl_record_t& record, const flid_t *ifields)
 {
 #if HPL_DEBUG>0
 if (!is_rec_valid(recordNumber))
 return RC(eBADSLOTNUMBER);
 // record must have everything allocated
 w_assert3(record.getFieldSize != NULL && record.data != NULL); w_assert1(ifields!= NULL);
 #endif


 record.dataSize = 0;

 flid_t fieldNumber;
 for (fieldNumber = 0; fieldNumber < record.numberOfFixedSizeFields; ++fieldNumber)
 {
 //check if field is valid in the nullbitmap

 //get the offset for this field
 offset_t offset = get_offset_field(recordNumber, ifields[fieldNumber]);

 // read the field value
 record.data[fieldNumber] = (_pp->data + HPLPageHeaderSize + offset);
 record.dataSize += record.fieldSize[fieldNumber];
 #if HPL_DEBUG>1
 printf("Reading fixField(%.d): \"%d\" (%.d,%.d) at %.d\n", record.fieldSize[fieldNumber], record.data[fieldNumber], recordNumber, ifields[fieldNumber], offset);
 cout << "Reading \"" << *(Offset2*)record.data[fieldNumber] << "\" (" << recordNumber << "," << ifields[fieldNumber] << ") at " << offset << "\n";
 #endif
 }
 for (; fieldNumber < record.numberOfFields; ++fieldNumber)
 {
 //get the offset for this field
 offset_t offset = get_offset_field(recordNumber, ifields[fieldNumber]);

 record.data[fieldNumber] = (_pp->data + HPLPageHeaderSize + offset + sizeof(offset_t));
 record.dataSize += varFixedPartSize;
 #if HPL_DEBUG>1
 printf("Reading fixField(%.d): \"%d\" (%.d,%.d) at %.d\n", record.fieldSize[fieldNumber], record.data[fieldNumber], recordNumber, ifields[fieldNumber], offset);
 cout << "Reading \"" << *(Offset2*)record.data[fieldNumber] << "\" (" << recordNumber << "," << ifields[fieldNumber] << ") at " << offset << "\n";
 #endif
 }
 return RCOK;
 }*/

rc_t hpl_p::get_rec(recordID_t recordNumber, hpl_record_t& record, const flid_t *ifields)
{
#if HPL_DEBUG>0
	if (!is_rec_valid(recordNumber))
		return RC(eBADSLOTNUMBER);
	// record must have everything allocated
	w_assert3(record.getFieldSize != NULL && record.data != NULL);w_assert1(ifields!= NULL);
#endif

	record.dataSize = 0;

	// check the ghost bit of the record
	if (isSetGhostbit(recordNumber))
	{

		flid_t fieldNumber;
		for (fieldNumber = 0; fieldNumber < record.numberOfFixedSizeFields; ++fieldNumber)
		{
			//check if field is valid in the nullbitmap
			if (isSetNullbit(recordNumber, fieldNumber))
			{
				//get the offset for this field
				offset_t offset = get_offset_field(recordNumber, ifields[fieldNumber]);
#if HPL_PREFETCH>0
			//cout << "Prefetch Test!"<< endl;
			PREFETCH_T0(_pp->data + HPLPageHeaderSize + offset,0);
#endif
#if HPL_DEBUG>1
				cout<<endl;
				cout<<"FieldNumber: "<< fieldNumber <<" FixOffset: " << offset << " FieldValue: "<< *(int*)(_pp->data + HPLPageHeaderSize + offset) << " FieldValue(d): "<< *(double*)(_pp->data + HPLPageHeaderSize + offset) << " FieldValue(c)["<< record.fieldSize[fieldNumber] << "]: ";
				for (int bla=0; bla < record.fieldSize[fieldNumber]; bla++)
				{
					cout << *(char*)(_pp->data + HPLPageHeaderSize + offset + bla);
				}
				cout <<endl;
#endif
				record.data[fieldNumber] = (char*) (_pp->data + HPLPageHeaderSize + offset);
				record.dataSize += record.fieldSize[fieldNumber];
			}
			else
			{
				//data is not valid
				record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
				record.data[fieldNumber][0] = '\0';
				//record.fieldSize[fieldNumber] = 0;
			}

#if HPL_DEBUG>1
			//cout << "Reading \"" << *(Offset2*)record.data[fieldNumber] << "\" (" << recordNumber << "," << fieldNumber << ") at " << offset << "\n";
#endif
		}
		for (; fieldNumber < record.numberOfFields; ++fieldNumber)
		{
			//check if field is valid in the nullbitmap
			if (isSetNullbit(recordNumber, fieldNumber))
			{
				//get the offset for this field
				offset_t offset = get_offset_field(recordNumber, ifields[fieldNumber]);
#if HPL_PREFETCH>0
			//cout << "Prefetch Test!"<< endl;
			PREFETCH_T0(_pp->data + HPLPageHeaderSize + offset,0);
#endif

				// get the real varData
				if (strncmp((_pp->data + HPLPageHeaderSize + offset + sizeof(offset_t)), "\0", (varFixedPartSize - sizeof(offset_t))) != 0)
				{
					size_t num = (*(Size2*) (_pp->data + HPLPageHeaderSize + (*(offset_t*) (_pp->data + HPLPageHeaderSize + offset))));

					if (num > 0 && num < 256)
					{
						record.fieldSize[fieldNumber] = num + (varFixedPartSize - sizeof(offset_t));
						record.data[fieldNumber] = (char*) (_pp->data + HPLPageHeaderSize + offset + (varFixedPartSize - sizeof(offset_t)));
//						record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
//						memcpy(record.data[fieldNumber], (char*) (_pp->data + HPLPageHeaderSize + offset + sizeof(offset_t)), 2);
//						memcpy(record.data[fieldNumber] + 2, (char*) (_pp->data + HPLPageHeaderSize + (*(offset_t*) (_pp->data + HPLPageHeaderSize + offset)) + sizeof(Size2)), (num - 1));
//						record.data[fieldNumber][record.fieldSize[fieldNumber] - 1] = '\0';
						record.dataSize += record.fieldSize[fieldNumber];
#if HPL_DEBUG>1
						cout<<"("<< recordNumber << "," << fieldNumber << ") Size: "<< num << "\\ OffsetFix: "<< offset <<" OffsetVar: "<<(*(offset_t*)(_pp->data+HPLPageHeaderSize+offset)) << " FieldValue(c)["<< record.fieldSize[fieldNumber] << "]: ";
						cout << record.data[fieldNumber]<<endl;
						//cout << "Reading \"" << *(Offset2*)record.data[fieldNumber] << "\" (" << recordNumber << "," << fieldNumber << ") at " << offset << "\n";
#endif

					}
					else
					{
						//data is not valid
#if HPL_DEBUG>0
						cout << "Invalid length of varField data! RecordNumber: " << recordNumber << " FieldNumber: " << fieldNumber << " FieldSize: "
								<< record.fieldSize[fieldNumber] << " ReadNum: " << num << endl;
#endif
						record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
						record.data[fieldNumber][0] = '\0';
					}
				}
				else
				{
					//data is not valid
					record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
					record.data[fieldNumber][0] = '\0';
					//record.fieldSize[fieldNumber] = 0;
				}
			}
			else
			{
				//data is not valid
				record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
				record.data[fieldNumber][0] = '\0';
				//record.fieldSize[fieldNumber] = 0;
			}
		}
	}
	else
	{
		// record not valid
		for (int f = 0; f < record.numberOfFields; ++f)
		{
			record.data[f] = NULL;
		}
		//record.numberOfFields=0;
	}

	return RCOK;
}

/*--------------------------------------------------------------*
 *  hpl_p::get_record() - get hpl record
 *--------------------------------------------------------------*/
rc_t hpl_p::get_rec(recordID_t recordNumber, hpl_record_t& record)
{
#if HPL_DEBUG>0
	if (!is_rec_valid(recordNumber))
		return RC(eBADSLOTNUMBER);
#endif

	record.dataSize = 0;

	// check the ghost bit of the record
	if(isSetGhostbit(recordNumber))
	{

		flid_t fieldNumber;
		for (fieldNumber = 0; fieldNumber < record.numberOfFixedSizeFields; ++fieldNumber)
		{
			//check if field is valid in the nullbitmap
			if (isSetNullbit(recordNumber, fieldNumber))
			{
				//get the offset for this field
				offset_t offset = get_offset_field(recordNumber, fieldNumber);

#if HPL_DEBUG>1
					cout<<endl;
					cout<<"FieldNumber: "<< fieldNumber <<" FixOffset: " << offset  << " FieldValue: "<< *(int*)(_pp->data + HPLPageHeaderSize + offset) << " FieldValue(d): "<< *(double*)(_pp->data + HPLPageHeaderSize + offset) << " FieldValue(c)["<< record.fieldSize[fieldNumber] << "]: ";
					for (int bla=0; bla < record.fieldSize[fieldNumber]; bla++)
					{
						cout << *(char*)(_pp->data + HPLPageHeaderSize + offset + bla);
					}
					cout <<endl;
#endif
				record.data[fieldNumber] = (char*)(_pp->data + HPLPageHeaderSize + offset);
				record.dataSize += record.fieldSize[fieldNumber];
			}
			else
			{
				//data is not valid
				record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
				record.data[fieldNumber][0] = '\0';
				//record.fieldSize[fieldNumber] = 0;
			}

	#if HPL_DEBUG>1
				//cout << "Reading \"" << *(Offset2*)record.data[fieldNumber] << "\" (" << recordNumber << "," << fieldNumber << ") at " << offset << "\n";
	#endif
		}
		for (; fieldNumber < record.numberOfFields; ++fieldNumber)
		{
			//check if field is valid in the nullbitmap
			if (isSetNullbit(recordNumber, fieldNumber))
			{
				//get the offset for this field
				offset_t offset = get_offset_field(recordNumber, fieldNumber);

				// get the real varData
				if(strncmp((_pp->data + HPLPageHeaderSize + offset + sizeof(offset_t)),"\0",(varFixedPartSize - sizeof(offset_t))) != 0)
				{
				size_t sizeVarField = (*(Size2*)(_pp->data+HPLPageHeaderSize+(*(offset_t*)(_pp->data+HPLPageHeaderSize+offset))));

					if(sizeVarField > 0 && sizeVarField < 256)
					{
						record.fieldSize[fieldNumber] = sizeVarField + (varFixedPartSize - sizeof(offset_t));
						record.data[fieldNumber] = (char*) (_pp->data + HPLPageHeaderSize + offset + (varFixedPartSize - sizeof(offset_t)));
//						record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
//						memcpy(record.data[fieldNumber], (char*) (_pp->data + HPLPageHeaderSize + offset + sizeof(offset_t)), 2);
//						memcpy(record.data[fieldNumber] + 2, (char*) (_pp->data + HPLPageHeaderSize + (*(offset_t*) (_pp->data + HPLPageHeaderSize + offset)) + sizeof(Size2)), (num - 1));
//						record.data[fieldNumber][record.fieldSize[fieldNumber] - 1] = '\0';
						record.dataSize += record.fieldSize[fieldNumber];
	#if HPL_DEBUG>1
			cout<<"("<< recordNumber << "," << fieldNumber << ") Size: "<< sizeVarField << "\\ OffsetFix: "<< offset <<" OffsetVar: "<<(*(offset_t*)(_pp->data+HPLPageHeaderSize+offset)) << " FieldValue(c)["<< record.fieldSize[fieldNumber] << "]: ";
			cout << record.data[fieldNumber]<<endl;
			//cout << "Reading \"" << *(Offset2*)record.data[fieldNumber] << "\" (" << recordNumber << "," << fieldNumber << ") at " << offset << "\n";
	#endif


					}
					else
					{
						//data is not valid
		#if HPL_DEBUG>0
						cout<<"Invalid length of varField data! recordNumber= "<<recordNumber<<"; record.fieldSize["<<fieldNumber<<"]="<<record.fieldSize[fieldNumber]<<"; sizeVarField="<<sizeVarField<<endl;
		#endif
						record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
						record.data[fieldNumber][0] = '\0';
					}
				}
				else
				{
					//data is not valid
					record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
					record.data[fieldNumber][0] = '\0';
					//record.fieldSize[fieldNumber] = 0;
				}
			}
			else
			{
				//data is not valid
				record.data[fieldNumber] = new char[record.fieldSize[fieldNumber]];
				record.data[fieldNumber][0] = '\0';
				//record.fieldSize[fieldNumber] = 0;
			}
		}
	}
	else
	{
		// record not valid
		for (int f = 0; f < record.numberOfFields; ++f)
		{
			record.data[f] = NULL;
		}
		//record.numberOfFields=0;
	}
	return RCOK;
}


//***********************************************************************************
//HPL&PAX  end
//***********************************************************************************


/////////////////////////////////////////////////////////////////////////
// HPL  specific
/////////////////////////////////////////////////////////////////////////

recordID_t hpl_p::getNextFreeRecord(bool append, recordID_t recordNumber)
{
	//cout << "append = " << append << "record# = " << recordNumber << endl;
	recordID_t currentRecordNumber = (append?NO_LAST_FIXED:recordNumber);

	recordID_t numberOfSlots = NO_RECORDS + FREE_RECORDS;

	while (currentRecordNumber < numberOfSlots)
	{
		currentRecordNumber++;
		if (!isSetGhostbit(currentRecordNumber))
		{
			//cout << "return " << currentRecordNumber << endl;
			return currentRecordNumber;
		}
	}
	return -1;
}

recordID_t hpl_p::getNextValidRecord(recordID_t recordNumber)
{
	recordID_t currentRecordNumber = recordNumber;

	while (currentRecordNumber < NO_LAST_FIXED)
	{
#if HPL_DEBUG>1
			cout<<"Looking for valid record in "<< currentRecordNumber <<endl;
#endif
		currentRecordNumber++;
//		if (isSetGhostbit(currentRecordNumber))
//		{
#if HPL_DEBUG>1
			cout<<"Next valid record is "<< currentRecordNumber <<endl;
#endif
			return currentRecordNumber;
//		}
	}
	return -1;
}

/*--------------------------------------------------------------*
 *  hpl_p::get_offset_field() - find the field offset           *
 *--------------------------------------------------------------*/
void hpl_p::init_cacheBlockMap()
{
	// find the smallest number of fields per cache block
	// because all field sizes are powers of two it will be also gcd of all other
	recordID_t smallestNumberOfFieldsPerCacheBlock = FieldsPerCacheline(0);
	for(int2_t i=1; i<NO_FIELDS; ++i)
	{
		if (FieldsPerCacheline(i) < smallestNumberOfFieldsPerCacheBlock)
		{
			smallestNumberOfFieldsPerCacheBlock = FieldsPerCacheline(i);
		}
	}

	// store here insert position
	int2_t last[NO_FIELDS];
	for (int fieldIterrator=0; fieldIterrator<NO_FIELDS; fieldIterrator++)
		last[fieldIterrator]=0;


	// init cacheBlockMap
	for (int fieldIterrator=0; fieldIterrator<NO_FIELDS; fieldIterrator++)
		for (int j=0; j<cacheBlockMapSize; j++)
			hpl_header->cacheBlockMap[fieldIterrator][j]=0;

	offset_t offsetIterrator = 0;
	int2_t ghostAndNullVectorSizePerSegment = SizeOfGhostBitmapPerSegment + SizeOfNullBitmapPerSegment;

	offset_t recordsItterator=0;
	//recordsItterator < maxNumberOfRecordsPerSegment &&
	while (offsetIterrator < DATA_SIZE && offsetIterrator >=0)
	{
		// Size of ghostbits and nullbits
		if (hpl_mod(recordsItterator, maxNumberOfRecordsPerSegment)==0)
			offsetIterrator+=ghostAndNullVectorSizePerSegment;

		for (int fieldIterrator=0; fieldIterrator<NO_FIELDS; fieldIterrator++)
		{
			if ( hpl_mod(recordsItterator, FieldsPerCacheline(fieldIterrator))==0 )
			{
				hpl_header->cacheBlockMap[fieldIterrator][last[fieldIterrator]]=offsetIterrator;
				offsetIterrator+=CacheBlockSize;
				if (offsetIterrator > DATA_SIZE && offsetIterrator < 0)
					break;
				last[fieldIterrator]+=1;
#if HPL_DEBUG>0
				if (last[fieldIterrator]>=cacheBlockMapSize)
					cout << "Error: Too many records in page to fit in cache block map for field " << fieldIterrator << endl;
#endif
			}
		}
		recordsItterator+=smallestNumberOfFieldsPerCacheBlock;
	}

#if HPL_DEBUG > 1
	for (int fieldIterrator=0; fieldIterrator<NO_FIELDS; fieldIterrator++)
	{
		for (int j=0; j<cacheBlockMapSize; j++)
		{
			cout << hpl_header->cacheBlockMap[fieldIterrator][j] << ", ";
		}
		cout << endl;
	}
#endif



}

offset_t hpl_p::get_offset_field(recordID_t recordNumber, const flid_t fieldNumber)
{
	blockID_t blockNumber = hpl_div(recordNumber, FieldsPerCacheline(fieldNumber));

	//Find the exact offset of the field within the block containing the field.
	offset_t lastBlockOffset = hpl_mod(recordNumber , FieldsPerCacheline(fieldNumber));

	offset_t offset = hpl_header->cacheBlockMap[fieldNumber][blockNumber] +  ( IsFixedSize(fieldNumber)? FieldSize(fieldNumber) : varFixedPartSize)*lastBlockOffset;
	return offset;
}

/**
 * return offsets of next record to be inserted after NO_LAST_FIXED
 */
offset_t hpl_p::get_next_offset_field(recordID_t recordNumber, const flid_t fieldNumber)
{
	if (isFirstInBlock(recordNumber, fieldNumber))
	{
		blockID_t blockNumber = hpl_div(recordNumber, FieldsPerCacheline(fieldNumber));
		return hpl_header->cacheBlockMap[fieldNumber][blockNumber];
	}
	else
	{
		return OffsetLastFixed(fieldNumber)+FieldSize(fieldNumber);
	}
}

/*--------------------------------------------------------------*
 *  hpl_p::get_offset_field() - find the field offset           *
 *--------------------------------------------------------------*/
offset_t hpl_p::get_offset_field_slow(recordID_t recordNumber, const flid_t fieldNumber)
{
	// find the offset for given recordNumber and fieldNumber

	//TODO HPL: update the Offset formula to work with incremental segments

	// ***************************************************************
	//Step #1: Find which is the biggest k such that k*M<recordNumber.
	//		   Compute how many blocks are used to store S=k*M records.
	// ***************************************************************

	// if block numbering starts from 0
	//blockID_t k_numberOfBlockContainingRecord = hpl_div(recordNumber, hpl_header->fieldsPerCacheline[fieldNumber]); // ROUNDDOWN the result
	recordID_t S_numberOfRecordsInPreviousBlocks = hpl_func1(recordNumber, FieldsPerCacheline(fieldNumber)); //k_numberOfBlockContainingRecord;
	blockID_t numberOfBlocksUsed = 0;

	for (flid_t fieldIterrator = 0; fieldIterrator < NO_FIELDS; fieldIterrator++)
	{
		//int2_t fieldsPerCacheBlockForCurrentField = FieldsPerCacheline(fieldIterrator);

		if ((fieldNumber < fieldIterrator) || (recordNumber + 1	> FieldsPerCacheline(fieldNumber)))
		{
			//roundup
			// HPL: hack involved here: if x and y are int we want ceil(x/y), we do (x + y - 1) / y;
			//numberOfBlocksUsed += (S_numberOfRecordsInPreviousBlocks + fieldsPerCacheBlockForCurrentField - 1) / fieldsPerCacheBlockForCurrentField;
			numberOfBlocksUsed += hpl_div((S_numberOfRecordsInPreviousBlocks + FieldsPerCacheline(fieldIterrator) - 1) , FieldsPerCacheline(fieldIterrator));
		}
		else
		{
			//rounddown
			//numberOfBlocksUsed += floor(S_numberOfRecordsInPreviousBlocks / fieldsPerCacheBlockForCurrentField);
			numberOfBlocksUsed += hpl_div( S_numberOfRecordsInPreviousBlocks , FieldsPerCacheline(fieldIterrator));
		}

		// ********************************************************************************************
		//Step #2: Find how many blocks are there before the block containing(recordNumber,fieldNumber)
		//		   and after point computed in Step #1.
		//
		// ********************************************************************************************

		if ((fieldNumber > 0) && (fieldIterrator > 0) && (fieldIterrator <= fieldNumber))
		{
			if ((hpl_mod(S_numberOfRecordsInPreviousBlocks , FieldsPerCacheline(fieldIterrator - 1))) == 0)
			{
				numberOfBlocksUsed++;
			}
		}
	}

	// ********************************************************************************************
	//Step #3: Find the exact offset of the field within the block containing the field.
	//
	// ********************************************************************************************
	offset_t lastBlockOffset = hpl_mod(recordNumber , FieldsPerCacheline(fieldNumber));

	// ********************************************************************************************
	//Step #4: Size of ghostbits and nullbits
	//
	// ********************************************************************************************
	int2_t ghostAndNullVectorSizePerSegment = SizeOfGhostBitmapPerSegment + SizeOfNullBitmapPerSegment;
	// HPL: hack involved here: if x and y are int we want ceil(x/y), we do (x + y - 1) / y;
	//int numberOfSegmentContainingRecord = (recordNumber + 1 + maxNumberOfRecordsPerSegment - 1)	/maxNumberOfRecordsPerSegment;
	int2_t numberOfSegmentContainingRecord = hpl_div((recordNumber + 1 + maxNumberOfRecordsPerSegment - 1)	, maxNumberOfRecordsPerSegment);

	// ********************************************************************************************
	//Step #5: Sum the offsets from the previous Steps.
	//
	// ********************************************************************************************
	offset_t offset = numberOfBlocksUsed * CacheBlockSize + FieldSize(fieldNumber) * (lastBlockOffset)	+ numberOfSegmentContainingRecord * ghostAndNullVectorSizePerSegment;

	return offset;
}

offset_t hpl_p::getSizeOfSingleSegment()
{
	offset_t ghostAndNullVectorSizePerSegment = SizeOfGhostBitmapPerSegment
			+SizeOfNullBitmapPerSegment;

	offset_t sizeOfSingleSegment = maxNumberOfRecordsPerSegment * RECORD_SIZE_FIXED_PART + ghostAndNullVectorSizePerSegment;

	return sizeOfSingleSegment;
}

/**
 * return the offset of the segment containing the given record
 * offset does not include HPLHeader
 */
offset_t hpl_p::get_offset_segment(recordID_t recordNumber)
{
	offset_t offset = 0;

	// HPL: hack involved here: if x and y are int we want floor(x/y), we do ceil(x/y)-1;
	offset_t numberOfSegmentsBeforeTheSegmentContainingRecord = (recordNumber + 1 + (maxNumberOfRecordsPerSegment - 1)) / maxNumberOfRecordsPerSegment - 1;

	offset = numberOfSegmentsBeforeTheSegmentContainingRecord * hpl_p::getSizeOfSingleSegment();

	return offset;
}

/**
 * return the offset of ghost bitmap for given record number
 * offset does not include HPLHeader
 */
offset_t hpl_p::get_offset_ghostbitmap(recordID_t recordNumber)
{
	return get_offset_segment(recordNumber);
}

/**
 * return the offset of the nullbitmap for given field of a record
 * offset does not include HPLHeader
 */
offset_t hpl_p::get_offset_nullbitmap(recordID_t recordNumber, flid_t fieldNumber)
{
	offset_t offset = 0;

	// nullbitmap has the same size as the ghost bitmap
	offset = (fieldNumber) * SizeOfGhostBitmapPerSegment;

	return get_offset_segment(recordNumber) + SizeOfGhostBitmapPerSegment + offset;
}

/**
 * return offset where there is enough space to write field with given size
 *
 * return -1 if no space found
 */
offset_t hpl_p::get_free_var_field_offset(fieldSize_t varFieldSize)
{
	//TODO: look in var holes

	if (varFieldSize < FREE_SPACE)
		return hpl_header->offsetLastVar - varFieldSize;
	else
		return -1;
}

/**
 * check if nullbit for record, field
 */
bool hpl_p::isSetGhostbit(recordID_t recordNumber)
{
	char* offset = _pp->data + HPLPageHeaderSize + get_offset_ghostbitmap(recordNumber);
	offset_t localOffset = (recordNumber) % maxNumberOfRecordsPerSegment;
#if HPL_DEBUG>2
	cout<<" Ghostbit for (r) = ("<< recordNumber <<") is " << bm_is_set((u_char*)(offset),localOffset) <<endl;
#endif
	return bm_is_set((u_char*) (offset), localOffset);
}

/**
 * set ghostbit for record
 */
void hpl_p::set_ghostbit(recordID_t recordNumber)
{
	//cout << recordNumber << endl;
	char* offset = _pp->data + HPLPageHeaderSize + get_offset_ghostbitmap(recordNumber);
	offset_t localOffset = (recordNumber) % maxNumberOfRecordsPerSegment;
	//if (localOffset != 0)
		bm_set((u_char*) (offset), localOffset);

#if HPL_DEBUG>2
	cout << "Ghostbitmap:\n";
	bm_print((u_char*)(offset), maxNumberOfRecordsPerSegment);
	cout << "\n";
#endif
}

/**
 * unset ghostbit for record
 */
void hpl_p::unset_ghostbit(recordID_t recordNumber)
{
	char* offset = _pp->data + HPLPageHeaderSize + get_offset_ghostbitmap(recordNumber);
	offset_t localOffset = (recordNumber) % maxNumberOfRecordsPerSegment;
	bm_clr((u_char*) (offset), localOffset);

#if HPL_DEBUG>2
	cout << "Ghostbitmap:\n";
	bm_print((u_char*)(offset), maxNumberOfRecordsPerSegment);
	cout << "\n";
#endif
}

/**
 * check if nullbit for record, field
 */
bool hpl_p::isSetNullbit(recordID_t recordNumber, flid_t fieldNumber)
{
	char* offset = _pp->data + HPLPageHeaderSize + get_offset_nullbitmap(recordNumber, fieldNumber);
	offset_t localOffset = (recordNumber) % maxNumberOfRecordsPerSegment;
#if HPL_DEBUG>2
	cout<<" Nullfield for (r,f) = ("<< recordNumber <<", "<< fieldNumber << ") is " << bm_is_set((u_char*)(offset),localOffset) <<endl;
#endif
	return bm_is_set((u_char*) (offset), localOffset);
}

/**
 * set nullbit for record, field
 */
void hpl_p::set_nullbit(recordID_t recordNumber, flid_t fieldNumber)
{
	//cout << recordNumber << "," << fieldNumber << endl;
	char* offset = _pp->data + HPLPageHeaderSize + get_offset_nullbitmap(recordNumber, fieldNumber);
	offset_t localOffset = (recordNumber) % maxNumberOfRecordsPerSegment;
	bm_set((u_char*) (offset), localOffset);

#if HPL_DEBUG>1
	cout << "Nullbitmap:\n";
	bm_print((u_char*)(offset), maxNumberOfRecordsPerSegment);
	cout << "\n";
#endif
}

/**
 * unset nullbit for record, field
 */
void hpl_p::unset_nullbit(recordID_t recordNumber, flid_t fieldNumber)
{
	char* offset = _pp->data + HPLPageHeaderSize + get_offset_nullbitmap(recordNumber, fieldNumber);
	offset_t localOffset = (recordNumber) % maxNumberOfRecordsPerSegment;
	bm_clr((u_char*) (offset), localOffset);

#if HPL_DEBUG>1
	cout << "Nullbitmap:\n";
	bm_print((u_char*)(offset), maxNumberOfRecordsPerSegment);
	cout << "\n";
#endif
}

/**
 * checks if field is first in block of cache lines
 */
inline bool hpl_p::isFirstInBlock(recordID_t recordNumber, flid_t fieldNumber)
{
	return (hpl_mod(recordNumber , FieldsPerCacheline(fieldNumber)) == 0 ? true : false);
}

/**
 * returns number of new blocks needed to be allocated in order to insert this record with given number
 * Note: keep in mind that it could be inserted in a hole and then we don't need to allocate
 */
blockID_t hpl_p::numberOfNewBlocksNeeded(recordID_t  recordNumber)
{
	int2_t result = 0;

	// if we are filling hole we don't need to allocate new blocks of cache lines
	if (recordNumber < NO_LAST_FIXED)
		return 0;

	for (flid_t fieldNumber = 0; fieldNumber < NO_FIELDS; fieldNumber++)
	{
		if (isFirstInBlock(recordNumber, fieldNumber))
			result++;
	}
	return result;
}

/**
 * return free slot to insert record
 */
recordID_t hpl_p::get_free_record_slot()
{
	// this will ignore free spaces that come from deletes
	recordID_t slot = NO_LAST_FIXED + 1;
	return slot;
}

void hpl_p::generatePoorManAndOffset(const offset_t offsetVar, const char* data, const fieldSize_t dataSize, char* returnValue)
{
	//TODO: HPL: check if field is < poormansize
	// copy offset
	memcpy(returnValue, static_cast<const char*> (static_cast<const void*> (&offsetVar)), sizeof(offsetVar));

#if HPL_DEBUG>0
	if (dataSize>255)
		cout << "Error: Field with size bigger than 255 has arrived.\n";
#endif

	// memcpy(returnValue + sizeof(offsetVar),	static_cast<const char*> (static_cast<const void*> (&dataSize)), sizeof(dataSize));

	// add poor man
	// keep space for the offset
	//memcpy(returnValue + sizeof(offsetVar), data, (varFixedPartSize - sizeof(offsetVar)));
}

/*--------------------------------------------------------------*
 *  hpl_p::init_page_structure() - initialize the page structure
 *--------------------------------------------------------------*/
rc_t hpl_p::init_page_structure()
{

	//init_new_segment_structure();

	hpl_header->offsetLastVar = getDataSize();
	FREE_SPACE = getDataSize();

	init_cacheBlockMap();

	return RCOK;
}

/**--------------------------------------------------------------*
 *  hpl_p::init_page_structure() - initialize the page structure
 *--------------------------------------------------------------*/
rc_t hpl_p::init_new_segment_structure()
{
	// get starting offset of the new segment
	// numberLastFixed points always to a record residing in the last segment
	offset_t offsetNewSegment = get_offset_segment(NO_LAST_FIXED)	+ hpl_p::getSizeOfSingleSegment();

	// allocate ghost bits for each record
	u_char ghost_bitmap[SizeOfGhostBitmapPerSegment];
	// initialize the bitmap with zeros
	bm_zero(ghost_bitmap, maxNumberOfRecordsPerSegment);
	// store bitmap
	memcpy((char*) (_pp->data + HPLPageHeaderSize + offsetNewSegment), (char*) ghost_bitmap, SizeOfGhostBitmapPerSegment);

#if HPL_DEBUG>1
	cout << "\nInitialized ghost bitmap: \n";
	bm_print((u_char*)(_pp->data+HPLPageHeaderSize+offsetNewSegment), SizeOfGhostBitmapPerSegment*8);
	cout << "\n";
#endif

	// allocate null bits for each field
	u_char null_bitmap[SizeOfNullBitmapPerSegment];
	// initialize the bitmap with zeros
	bm_zero(null_bitmap, maxNumberOfRecordsPerSegment * NO_FIELDS);
	// store bitmaps
	memcpy(
			(char*) (_pp->data + HPLPageHeaderSize + SizeOfGhostBitmapPerSegment + offsetNewSegment),
			(char*) null_bitmap, SizeOfNullBitmapPerSegment);

#if HPL_DEBUG>1
	cout << "\nInitialized null bitmap for "<< (int)NO_FIELDS <<" fields: \n";
	for (int i = 0; i< NO_FIELDS; i++)
	{
		bm_print((u_char*)(_pp->data+HPLPageHeaderSize+SizeOfGhostBitmapPerSegment*i+offsetNewSegment), SizeOfGhostBitmapPerSegment*8);
		cout << "\n";
	}
	cout << "\n";
#endif

	// update available free space
	FREE_SPACE = hpl_header->offsetLastVar - (HPLPageHeaderSize + offsetNewSegment
			+ SizeOfGhostBitmapPerSegment + SizeOfGhostBitmapPerSegment * NO_FIELDS);
	FREE_RECORDS += maxNumberOfRecordsPerSegment;
	_pp->nvacant = FREE_RECORDS;


	return RCOK;
}

/*--------------------------------------------------------------*
 *  hpl_p::insert_record() - insert hpl record
 *--------------------------------------------------------------*/
rc_t hpl_p::insert_record(const hpl_record_t& record, recordID_t freeSlot)
{
	//TODO HPL: check if enough space available and allocate segment
	offset_t	tempOffsetLastFixed[16];

	recordID_t recordNumber = freeSlot;
	// get free record slot for the record
	if (freeSlot==-1)
		recordID_t recordNumber = get_free_record_slot();

	//record ID = NO_RECORDS
	flid_t counterField = 0;
	// loop fixed size fields first
	for (counterField = 0; counterField < NO_FFIELDS; ++counterField)
	{
		//get the offset for this field
		offset_t offset = get_next_offset_field(recordNumber, counterField);

		//insert the fixed field
		insert_fixed_size_value(recordNumber, counterField, offset, record.data[counterField]);

		//OffsetLastFixed(counterField) = offset;
		tempOffsetLastFixed[counterField] = offset;
	}
	memcpy(hpl_header->offsetLastFixed,(char*)(tempOffsetLastFixed) , 16*sizeof(offset_t));


#if HPL_DEBUG>10
	cout << "Last fixed offsets: " << OffsetLastFixed(0);
	for (counterField = 1; counterField < NO_FFIELDS; ++counterField)
	{
		cout  << " - " << OffsetLastFixed(counterField);
	}
	cout << endl;
#endif

	// loop var size fields next
	for (; counterField < NO_FIELDS; ++counterField)
	{

			// get offset to insert var field value
			offset_t offsetVar = get_free_var_field_offset(getSizeOfVarFieldInVarArea(record.fieldSize[counterField]));

			// get the offset for the fixed part of the var field
			offset_t offsetFixed = get_offset_field(recordNumber, counterField);

			// generate fixed part of var field
			char* valueVarFixed = new char[varFixedPartSize];
			generatePoorManAndOffset(offsetVar, record.data[counterField], record.fieldSize[counterField], valueVarFixed);

			// insert the fixed field
			insert_fixed_size_value(recordNumber, counterField, offsetFixed, valueVarFixed);

			// insert var field
			insert_var_size_value(offsetVar, record.data[counterField], record.fieldSize[counterField], recordNumber, counterField);

			// update the freeSpace in header
			FREE_SPACE -= getSizeOfVarFieldInVarArea(record.fieldSize[counterField]);

			// garbage collection
			delete[] valueVarFixed;

	}

	//*********************************************** print test
	//		for (int i=0; i<record.numberOfFixedSizeFields;++i)
	//		{
	//				//get the offset for this field
	//				char *data[record.fieldSize[i]];
	//				int offset = get_offset_field(recordNumber,i);
	//				memcpy(data,(char*)(_pp->data+HPLPageHeaderSize+offset) , record.fieldSize[i]);
	//				cout<<" Field: "<<i<<" field value: "<< *(Offset2*)data <<endl;
	//		}
	//*********************************************** print test

	//cout << recordNumber << endl;
	//set ghost bit for this record
	set_ghostbit(recordNumber);

	//TODO HPL: update administrative structures in header , freeSpace
	NO_RECORDS++;
	FREE_RECORDS--;
	_pp->nvacant = FREE_RECORDS;
	_pp->nslots = FREE_RECORDS+NO_RECORDS;

	// update last record if we have inserted such
	if (recordNumber > NO_LAST_FIXED)
	{
		NO_LAST_FIXED = recordNumber;
		FREE_SPACE -= numberOfNewBlocksNeeded(recordNumber) * CacheBlockSize;
	}

#if HPL_DEBUG>1
	hpl_header->printHeaderSummary();

	hpl_record_t test_record = hpl_record_t(record);
	for (int i=0; i<test_record.numberOfFields; i++)
	{
		test_record.data[i] = NULL;
	}
	get_rec(recordNumber, test_record);
	for (int i=0; i<test_record.numberOfFields; i++)
	{
	if (*test_record.data[i] != *record.data[i])
		cout << "ERROR: different values!\n";
	}
#endif



	return RCOK;
}

/*--------------------------------------------------------------*
 *  hpl_p::insert_fixed_size_value() - insert fixed field
 *--------------------------------------------------------------*/
void hpl_p::insert_fixed_size_value(recordID_t recordNumber, const flid_t fieldNumber, offset_t offset, const char* data)
{


#if HPL_DEBUG>1
	if (recordNumber==0)
	{
		cout << "New page\n";
	}
	cout << "Inserting int \"" << *(int*)data <<"\" double \"" <<*(double*)data<< "\" (" << recordNumber << "," << fieldNumber << ") at " << offset << "; header size: " << HPLPageHeaderSize << "\n";
#endif

	if (data != NULL)
	{
		// insert the field value
		if (IsFixedSize(fieldNumber))
		{
			memcpy((char*) (_pp->data + HPLPageHeaderSize + offset), data, FieldSize(fieldNumber));
#if HPL_DEBUG>1
		//if (fieldNumber==0)
		//	cout << "Insert at address: " <<  _pp << " + " << (HPLPageHeaderSize + offset) << " = " << (void*) (_pp->data + HPLPageHeaderSize + offset) << "; (void*) _pp = " << (void*) _pp << endl;
#endif
		}
		else
			memcpy((char*) (_pp->data + HPLPageHeaderSize + offset), data, varFixedPartSize);

		// set null_bitmap bit for this field
		set_nullbit(recordNumber, fieldNumber);
	}
	else
	{
		// no data => clear null_bitmap bit for this field
		unset_nullbit(recordNumber, fieldNumber);
	}
}

/**
 * return the size of a field in var area
 */
fieldSize_t hpl_p::getSizeOfVarFieldInVarArea(fieldSize_t dataSize)
{
	//TODO: HPL: remove poor man from this
	// actual string - poorman + size
	return dataSize + sizeof(dataSize)-(varFixedPartSize-sizeof(offset_t));
}

/*--------------------------------------------------------------*
 *  hpl_p::insert_var_size_value() - insert var field
 *--------------------------------------------------------------*/
void hpl_p::insert_var_size_value(const offset_t offset, const char* data, const fieldSize_t dataSize, const recordID_t recordNumber, flid_t fieldNumber)
{
	//TODO: HPL: remove first 4 bytes of the var value and fix size of memcpy
	// (remove hardcoded data)
	// PoorMan = 4 bytes (Size4)
	// Size of Field = 2 bytes (Size2)

#if HPL_DEBUG>1
		//char* data2 = new char[dataSize + 1];
		//memcpy(data2, data, dataSize);
		//data2[dataSize] = '\0';
		cout << "Inserting var(" << dataSize << "): \"" << data << "\" (" << recordNumber << "," << fieldNumber << ") at " << offset << "\n";
#endif

	if ((data != NULL))// && (dataSize > 0))
	{
		fieldSize_t poorManSize = varFixedPartSize-sizeof(offset_t);
		fieldSize_t realDataSize = dataSize-poorManSize;
		memcpy((char*) (_pp->data + HPLPageHeaderSize + offset),
				static_cast<const char*> (static_cast<const void*> (&realDataSize)),
				sizeof(realDataSize));
		//memcpy((char*) (_pp->data + HPLPageHeaderSize + offset + sizeof(realDataSize)), data+poorManSize, realDataSize);

		// update offset if var added at the end
		if (offset < hpl_header->offsetLastVar)
			hpl_header->offsetLastVar = offset;
	}
	else
	{
		// no data => clear null_bitmap bit for this field
		unset_nullbit(recordNumber, fieldNumber);
	}
}

/*--------------------------------------------------------------*
 *  hpl_p::update_field() - update hpl field
 *--------------------------------------------------------------*/
rc_t hpl_p::update_field(const recordID_t recordNumber, const flid_t fieldNumber, const char* data, const fieldSize_t dataSize)
{
	//TODO:HPL: add free space to the structure of free var space

	//TODO:HPL: check if the record is valid

	//TODO HPL: update variable field + fixed_field offset
	//			if updated value <= the current field then update in place
	//			if updated value > the current field  then append at the end

	//update fixed-size field
	if ((data != NULL)) //&& (dataSize > 0))
	{
		if (!isSetNullbit(recordNumber, fieldNumber))
		{
			set_nullbit(recordNumber, fieldNumber);
		}

		if (IsFixedSize(fieldNumber))
		{
			//get the offset for this field
			offset_t offset = get_offset_field(recordNumber, fieldNumber);

			//overwrite the field value
			memcpy((char*) (_pp->data + HPLPageHeaderSize + offset), data, FieldSize(fieldNumber));
		}
		else
		{
			// get the offset for the fixed part of the var field
			offset_t offsetFixed = get_offset_field(recordNumber, fieldNumber);

			// get offset to insert var field value (update-in-place)
			offset_t offsetVar = *(offset_t*)(_pp->data+HPLPageHeaderSize+offsetFixed);

			if ((varFixedPartSize - sizeof(offsetVar))== 0)
			{
				// generate fixed part of var field
				char* valueVarFixed = new char[varFixedPartSize];
				generatePoorManAndOffset(offsetVar, data, dataSize, valueVarFixed);

				// insert the fixed field
				insert_fixed_size_value(recordNumber, fieldNumber, offsetFixed, valueVarFixed);

				// garbage collection
				delete[] valueVarFixed;
			}

			// insert var field
			insert_var_size_value(offsetVar, data, dataSize, recordNumber, fieldNumber);
		}
	}
	else
	{
		//clear NullBitmap bit for this field
		unset_nullbit(recordNumber, fieldNumber);
	}

	return RCOK;
}

/*--------------------------------------------------------------*
 *  hpl_p::update_record() - update hpl field
 *--------------------------------------------------------------*/
rc_t hpl_p::update_record(const recordID_t recordNumber,const hpl_record_t& record, const flid_t* ifields)
{
	//TODO:HPL: add free space to the structure of free var space

	//TODO:HPL: check if the record is valid

	//TODO:HPL: implement deletes of fields

	//TODO HPL: update variable field + fixed_field offset
	//			if updated value <= the current field then update in place
	//			if updated value > the current field  then append at the end

	// fixed fields
	flid_t fieldNumber;
	for (fieldNumber = 0; fieldNumber < record.numberOfFixedSizeFields; ++fieldNumber)
	{
		if (!isSetNullbit(recordNumber, ifields[fieldNumber]))
		{
			set_nullbit(recordNumber, ifields[fieldNumber]);
		}

		//get the offset for this field
		offset_t offset = get_offset_field(recordNumber, ifields[fieldNumber]);

		//overwrite the field value
		memcpy((char*) (_pp->data + HPLPageHeaderSize + offset), record.data[fieldNumber], FieldSize(ifields[fieldNumber]));
	}

	// var fields
	for (; fieldNumber < record.numberOfFields; ++fieldNumber)
	{
		if ((record.data[fieldNumber] != NULL))
		{
			if (!isSetNullbit(recordNumber, ifields[fieldNumber]))
			{
				set_nullbit(recordNumber, ifields[fieldNumber]);
			}

			// get the offset for the fixed part of the var field
			offset_t offsetFixed = get_offset_field(recordNumber, ifields[fieldNumber]);

			// get offset to insert var field value (update-in-place)
			offset_t offsetVar = *(offset_t*)(_pp->data+HPLPageHeaderSize+offsetFixed);

			if ((varFixedPartSize - sizeof(offsetVar))== 0)
			{
				// generate fixed part of var field
				char* valueVarFixed = new char[varFixedPartSize];
				generatePoorManAndOffset(offsetVar, record.data[fieldNumber], record.fieldSize[fieldNumber], valueVarFixed);

				// insert the fixed field
				insert_fixed_size_value(recordNumber, ifields[fieldNumber], offsetFixed, valueVarFixed);

				// garbage collection
				delete[] valueVarFixed;
			}

			// insert var field
			insert_var_size_value(offsetVar, record.data[fieldNumber], record.fieldSize[fieldNumber], recordNumber, ifields[fieldNumber]);
		}
	}
	return RCOK;
}

/*--------------------------------------------------------------*
 *  hpl_p::delete_record() - delete hpl record
 *--------------------------------------------------------------*/
rc_t hpl_p::delete_record(const rid_t& rid)
{
	//TODO:HPL: add free space to the structure of free var space

	//TODO HPL: update freeSpace and freeRecords stuctures


#if HPL_DEBUG>1
	cout << "Deleting "<< rid.slot << endl;
	bm_print((u_char*)(_pp->data+HPLPageHeaderSize), maxNumberOfRecordsPerSegment);
	cout << "\n";
#endif

	//clear GhostBitmap bit for this record
	unset_ghostbit(rid.slot);

#if HPL_DEBUG>1
	cout << "Deleted "<< rid.slot << endl;
	bm_print((u_char*)(_pp->data+HPLPageHeaderSize), maxNumberOfRecordsPerSegment);
	cout << "\n";
#endif

	//TODO HPL: update varFreeSpaceStructure

	//hpl_header->freeSpaceVar+=
	NO_RECORDS--;
	FREE_RECORDS++;
	_pp->nvacant = FREE_RECORDS;
	_pp->nslots = FREE_RECORDS+NO_RECORDS;


	return RCOK;
}


void hpl_p::get_ffield(recordID_t recordNumber, hpl_field_t& field)
	{
		if (isSetGhostbit(recordNumber))
		{
			//get the offset for this field
			offset_t offset = get_offset_field(recordNumber, field.fieldNumber);
#if HPL_PREFETCH>0
			//cout << "Prefetch Test!"<< endl;
			PREFETCH_T0(_pp->data + HPLPageHeaderSize + offset,0);
#endif
			field.data = (_pp->data + HPLPageHeaderSize + offset);
		}
		else
		{
			field.data = NULL;
		}
	}

void hpl_p::get_next_ffield(recordID_t recordNumber, hpl_field_t& field)
	{
		if (isSetGhostbit(recordNumber))
		{

			if (isFirstInBlock(recordNumber, field.fieldNumber))
			{ //get the offset for this field
				offset_t offset = get_offset_field(recordNumber, field.fieldNumber);
#if HPL_PREFETCH>0
			//cout << "Prefetch Test!"<< endl;
			PREFETCH_T0(_pp->data + HPLPageHeaderSize + offset,0);
#endif
				field.data = (_pp->data + HPLPageHeaderSize + offset);
			}
			else
			{
				field.data += field.fieldSize;
#if HPL_PREFETCH>0
			//cout << "Prefetch Test!"<< endl;
			PREFETCH_T0(field.data,0);
#endif
			}
		}
		else
		{
			field.data = NULL;
		}
	}

void hpl_p::get_field(recordID_t recordNumber, hpl_field_t& field)
	{
		// read the field value
		if (IsFixedSize(field.fieldNumber))
		{
			//get the offset for this field
			offset_t offset = get_offset_field(recordNumber, field.fieldNumber);
#if HPL_PREFETCH>0
			//cout << "Prefetch Test!"<< endl;
			PREFETCH_T0(_pp->data + HPLPageHeaderSize + offset,0);
#endif
			field.data = (_pp->data + HPLPageHeaderSize + offset);
		}
		else
		{
			//get the offset for this field
			//				Size4 offset = get_offset_field(recordNumber,ifields[fieldNumber]);
			//				record.fieldSize[fieldNumber] = *(_pp->data+HPLPageHeaderSize+offset+sizeof(Size4));
			//				record.poorMan[fieldNumber] = (_pp->data+HPLPageHeaderSize+offset+sizeof(Size4)+sizeof(Size1));
			//				record.data[fieldNumber] = (char*)(_pp->data+HPLPageHeaderSize+(*(Size4*)(_pp->data+HPLPageHeaderSize+offset))+sizeof(Size2));

		}

		//		if (field.isFixedSize)
		//		{
		//			field.data = FFieldStartAddr(field.fieldNumber,rno);
		//		}
		//		else
		//		{
		//			field.data = ((field.fieldSize=VFieldSize(field.fieldNumber,rno)) > 0 ?
		//					DataStartAddr(field.fieldNumber) +
		//					(rno>0 ? VarFieldEndOffset(field.fieldNumber, rno-1) : 0): NULL);
		//		}
	}

void hpl_p::get_vfield(recordID_t recordNumber, hpl_field_t& field)
{

	//		w_assert3 (hpl_header->is_varsize(field.fieldNumber));
	//		w_assert3 (field.fieldNumber < NO_FIELDS);
	//		field.data = ((field.fieldSize=VFieldSize(field.fieldNumber,rno)) > 0 ?
	//				DataStartAddr(field.fieldNumber) +
	//				(rno>0 ? VarFieldEndOffset(field.fieldNumber, rno-1) : 0): NULL);
}

// rno is the DESIRED record slot id, whereas field contains rno-1 data
void hpl_p::get_next_vfield(recordID_t rno, hpl_field_t& field)
{
	//		field.data += field.fieldSize;
	//		field.fieldSize=VFieldSize(field.fieldNumber,rno);
}


rc_t hpl_p::find_and_lock_next_slot(uint4_t space_needed, slotid_t& idx)
{
	return _find_and_lock_free_slot(true, space_needed, idx);
}
rc_t hpl_p::find_and_lock_free_slot(uint4_t space_needed, slotid_t& idx)
{
	return _find_and_lock_free_slot(false, space_needed, idx);
}
rc_t hpl_p::_find_and_lock_free_slot(bool append_only, uint4_t space_needed, slotid_t& idx)
{
	FUNC(find_and_lock_free_slot);
	recordID_t start_slot = -1; // first slot to check if free
	// _find_and_lock_free_slot should never be using slot 0 on
	// a file page because that's the header slot; even though
	// the header is unused.  We will hit assertions if we use slot 0.
	rc_t rc;

	DBG(<< "space_needed " << space_needed);
	for (int slotIterrator=0;;slotIterrator++)
	{
		idx=getNextFreeRecord(append_only, start_slot);


		// try to lock the slot, but do not block
		rid_t rid(pid(), idx);

		// IP: For DORA it may ignore to acquire other locks than the RID
		rc = lm->lock(rid, EX, t_long, WAIT_IMMEDIATE);

		if (rc.is_error())
		{
			if (rc.err_num() == eLOCKTIMEOUT)
			{
				// slot is locked by another transaction, so find
				// another slot.  One choice is to start searching
				// for a new slot after the one just found.
				// An alternative is to force the allocation of a
				// new slot.  We choose the alternative potentially
				// attempting to get many locks on a page where
				// we've already noticed contention.

				DBG(<< __LINE__ << " rc=" << rc);
				if (slotIterrator==FREE_RECORDS-1)
				{
					return RC(eRECWONTFIT);
				}
				else
				{
					start_slot = idx; // force new slot allocation
				}
			}
			else
			{
				// error we can't handle
				DBG(<< __LINE__ << " rc=" << rc);
				return RC_AUGMENT(rc);
			}
		}
		else
		{
			// found and locked the slot
			DBG(<< "tid " << xct()->tid() <<" found and locked slot " << idx << " for rid " << rid);
			break;
		}
	}
	return RCOK;
}
