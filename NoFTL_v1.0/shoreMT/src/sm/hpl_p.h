/* ------------------------------------------------------------------------------- */
/* -- Copyright (c) 2011, 2012 Databases and Distributed Systems Department,    -- */
/* -- Technical University of Darmstadt, subject to the terms and conditions    -- */
/* -- given in the file COPYRIGHT.  All Rights Reserved.                        -- */
/* ------------------------------------------------------------------------------- */
/* -- Ilia Petrov, Todor Ivanov, Veselin Marinov                                -- */
/* ------------------------------------------------------------------------------- */

#ifndef HPL_P_H
#define HPL_P_H

#include "page.h"
#include "hpl_hdr.h"

// Cast to header start
#define	hpl_header	((hpl_p_hdr_t *)(_pp->data))

#define	EOP			-1		// End of page
// TODO: HPL: fix this according to the new page header
// These macros define header elements in the page for faster access
#define DATA_SIZE	(*(offset_t*)(_pp->data+offsetof(hpl_p_hdr_t, dataSize)))
#define FREE_SPACE	(*(offset_t*)(_pp->data+offsetof(hpl_p_hdr_t, freeSpace)))
#define NO_RECORDS	(*(recordID_t*)(_pp->data+offsetof(hpl_p_hdr_t, numberOfRecordsInPage)))
#define NO_LAST_FIXED	(*(recordID_t*)(_pp->data+offsetof(hpl_p_hdr_t, numberLastFixed)))
#define FREE_RECORDS (*(recordID_t*)(_pp->data+offsetof(hpl_p_hdr_t, freeRecords)))
#define NO_FIELDS	(*(uint1_t*)(_pp->data+offsetof(hpl_p_hdr_t, numberOfFields)))
#define NO_FFIELDS	(*(uint1_t*)(_pp->data+offsetof(hpl_p_hdr_t, numberOfFixedSizeFields)))
#define RECORD_SIZE_FIXED_PART (*(fieldSize_t*)(_pp->data+offsetof(hpl_p_hdr_t, recordSizeFixedPart)))
#define OFFSET_LAST_FIXED_ARRAY	(*(_pp->data+offsetof(hpl_p_hdr_t, offsetLastFixed)))


// fno (field number) ranges from 0 to field_no-1)
// rno (record number) ranges from 0 to no_records-1)
#define DataSizeOffset(fno)		(FieldSizeOffset+(fno)*sizeof(fieldSize_t))
#define DataSizeAddr(fno)		(_pp->data+(fieldSize_t)DataSizeOffset(fno))
#define FieldSize(fno)			(*(fieldSize_t*)(DataSizeAddr(fno)))


// ffno (fixed field number)
#define OffsetLastFixedOffset(ffno)	(OffsetLastFixedOffset2+(ffno)*sizeof(offset_t))
#define OffsetLastFixedAddr(ffno)	(_pp->data+(offset_t)OffsetLastFixedOffset(ffno))
#define OffsetLastFixed(ffno)		(*(offset_t*)(OffsetLastFixedAddr(ffno)))

#define FieldsPerCachelineDataOffset(fno)	(FieldsPerCachelineOffset+(fno)*sizeof(recordID_t))
#define FieldsPerCachelineAddr(fno)			(_pp->data+(recordID_t)FieldsPerCachelineDataOffset(fno))
#define FieldsPerCacheline(fno)				(*(recordID_t*)(FieldsPerCachelineAddr(fno)))

#define IsFixedSize(fno)		( (fno<NO_FFIELDS) ?	(true) : (false) )


// TODO:HPL: we don't need this (after the offset method is working)
//HPL params
#define SizeOfGhostBitmapPerSegment CacheLineSize
#define	SizeOfNullBitmapPerSegment  (CacheLineSize*NO_FIELDS)

//***********************************************************************************
//PAX-only  functions
//***********************************************************************************
//// Current Nullmap size for rno records
//#define	NullmapSize(rno)	((Size2)align((Size2)ceil((rno)*FREC_OVH)))
//// Current Offset array size for rno records
//#define	OffsetArrSize(rno)	((Size2)align((rno)*sizeof(Offset2)))
//// How much will the Nullmap overhead be if I add the rno-th record (starting from 0)
//#define NullmapOvh(rno)		(((rno)%(ALIGNON*8))?0:ALIGNON)
//// How much will the Offset overhead be if I add the rno-th record (starting from 0)
//#define OffsetOvh(rno)		(((rno)%(ALIGNON/sizeof(Offset2)))?0:ALIGNON)

//#define DataSizeOffset(fno)	(FSOffset+(fno)*sizeof(Size2))
//#define DataSizeAddr(fno)	(_pp->data+(Offset2)DataSizeOffset(fno))
//#define FieldSize(fno)		(*(Size2*)(DataSizeAddr(fno)))
//
//// Offset to data start[fno] from start of page
//#define DataStartOffsetOffset(fno)	(FVOffset+(fno)*sizeof(Offset2))
//#define DataStartOffsetAddr(fno)	(_pp->data+(Offset2)DataStartOffsetOffset(fno))
//#define DataStartOffset(fno)		(HPLPageHeaderSize+*(Offset2*)DataStartOffsetAddr(fno))
//#define DataStartAddr(fno)			(_pp->data+DataStartOffset(fno))
//
//// Returns the byte that starts the tail (good for the fixed-size
//// attributes *or* for size calculations
//#define TailStartOffset(fno)		((fno==NO_FIELDS-1) ?		\
//									page_p::data_sz-1 :			\
//									DataStartOffset(fno+1)-1)
//#define TailStartAddr(fno)			(_pp->data+(Offset2)TailStartOffset(fno))
//
//#define VTailStartOffset(fno)		((fno==NO_FIELDS-1) ?				\
//									page_p::data_sz-sizeof(Offset2) :	\
//									DataStartOffset(fno+1)-sizeof(Offset2))
//#define VTailStartAddr(fno)			(_pp->data+(Offset2)VTailStartOffset(fno))
//
//// Fixed value field size is same as above if not null
//#define FFieldPresent(fno,rno)		(r_bm_is_set((u_char*)TailStartOffset(fno),rno))
//#define FFieldSize(fno,rno)			(FFieldPresent(fno,rno) ? FieldSize(fno) : 0)
//
//#define VarFieldEndPtr(fno,rno)		(VTailStartAddr(fno)-((rno)*sizeof(Offset2)))
//#define VarFieldEndOffset(fno,rno)	(*(Offset2*)VarFieldEndPtr(fno,rno))
//
//// variable value field size differs
//#define VFieldSize(fno,rno)			(VarFieldEndOffset(fno,rno)-(rno==0 ? 0 :			\
//									VarFieldEndOffset(fno,rno-1)))
//
//#define FTotalValueSize(fno)		(NO_RECORDS*FieldSize(fno))
//#define VTotalValueSize(fno)		(NO_RECORDS==0 ? 0 : 								\
//									VarFieldEndOffset(fno, NO_RECORDS-1))
//
//#define MinipageSize(fno)			((fno==NO_FIELDS-1) ?								\
//									(page_p::data_sz - DataStartOffset(fno)) :			\
//									(DataStartOffset(fno+1) - DataStartOffset(fno)))
//
///*
//#define VMinipageBusySize(fno)	(NO_RECORDS*sizeof(Offset2) + \
//						VTotalValueSize(fno))
//
//#define VMinipageFreeSize(fno)	(MinipageSize(fno) - VMinipageBusySize(fno))
//*/
//
//// Non-Nullable fixed-size field start address
//// (no need to check for presence)
//#define FFieldStartAddr(fno,rno)	(DataStartAddr(fno)+ rno*FieldSize(fno))
//
//// Nullable fixed-size field start address
//// (check for presence)
//#define FNFieldStartAddr(fno,rno)	( 																\
//									(r_bm_is_set((u_char*)TailStartAddr(fno), rno)) ?				\
//									((char*)DataStartAddr(fno) +									\
//									r_bm_num_set((u_char*)TailStartAddr(fno), rno)*FieldSize(fno))	\
//									: NULL)
//***********************************************************************************
//PAX end
//***********************************************************************************


/*--------------------------------------------------------------*
 *  class hpl_p                                                 *
 *--------------------------------------------------------------*/

class hpl_p: public page_p
{

	friend class hpl_m;

private:

	// Header size (HPL header, not Shore's)
	// Depends on number of fields, to it's to be set in the constructor
	enum
	{
		headerSize = HPLPageHeaderSize
	};

	// Free space on hpl_p is page_p minus the HPL header size.
	// To be set in the constructor.
	// dataSize includes data and the null/offset maps
	enum
	{
		dataSize = page_p::data_sz - HPLPageHeaderSize
	};

public:

	// TODO: HPL: what does this macro?
	// Macro to install basic funcs from page_p
	MAKEPAGE(hpl_p, page_p, 1);

	offset_t getFreeSpace()
	{
		return hpl_header->freeSpace;
	}
	recordID_t getNumberOfRecords()
	{
		return hpl_header->numberOfRecordsInPage;
	}
	uint1_t getNumberOfFields()
	{
		return hpl_header->numberOfFields;
	}
	uint1_t getNumberOfFixedFields()
	{
		return hpl_header->numberOfFixedSizeFields;
	}
	bool getIsFixedSize(flid_t fieldNumber)
	{
		return IsFixedSize(fieldNumber);
	}
	fieldSize_t getFieldSize(flid_t fieldNumber)
	{
		return hpl_header->fieldSize[fieldNumber];
	}
	recordID_t getNumberLastFixed()
	{
		return hpl_header->numberLastFixed;
	}
/*	*
	 * returns size of the field in the fixed part area (N bytes for vars)

	fieldSize_t getFixedFieldSize(flid_t fieldNumber)
	{
		return (getIsFixedSize(fieldNumber) ? FieldSize(fieldNumber) : varFixedPartSize);
	}*/
	inline fieldSize_t getVarOverhead()
	{
		return sizeof(fieldSize_t);
	}

	offset_t getHeaderSize() const
	{
		return headerSize;
	}
	offset_t getDataSize() const
	{
		return dataSize;
	}
	offset_t getInsertThresholdInBytes()
	{
		return (offset_t) getDataSize() * thresholdInsert / 100;
	}

	rc_t link_up(shpid_t ppid, shpid_t npid)
	{
		return page_p::link_up(ppid, npid);
	}

	rc_t write_header(const hpl_p_hdr_t& hdr)
	{
		memcpy(_pp->data, (void *) &hdr, headerSize);
		return RCOK;
	}

	rc_t set_hdr(const hpl_p_hdr_t& new_hdr)
	{
		return write_header(new_hdr);
	}

	bool is_rec_valid(recordID_t recordNumber)
	{
		return (recordNumber >= 0 && recordNumber < _pp->nslots);
	}

	inline bool is_hpl_p()
	{
	    // check if page is hpl format
	   // w_assert3(tag()&(t_hpl_p));
	    return (tag() == t_hpl_p);
	}

	//***********************************************************************************
	//PAX-only  functions
	//***********************************************************************************

	//	bool		fit_fields_in_page(const hpl_record_t& record);

	// Returns minimum record size assuming zero size of variable fields
	//	smsize_t	min_rec_size() const
	//	{
	//		return hpl_header->min_rec_size();
	//	}

	void
	append_fixed_size_value(const flid_t fno, const char* data);
	void
	append_var_size_value(const flid_t fno, const char* data, const fieldSize_t sz);
	rc_t
	append_rec(const hpl_record_t& record, recordID_t& slot);

	rc_t
	reorganize_page(const hpl_record_t& record);

	void*
	fixed_value_addr(const recordID_t rno, const flid_t fno) const;

	void
	sanity();

	// inline functions for fast retrieval of data in fields


	//***********************************************************************************
	//PAX functions end
	//***********************************************************************************

	//////////////////////////////////////////////
	//  HPL & PAX functions
	//////////////////////////////////////////////

	// reeturns true if there is space in the page for the record.
	bool fits_in_page(const hpl_record_t& record, fieldSize_t& sz);

	rc_t get_rec(recordID_t recordNumber, hpl_record_t& record, const flid_t* ifields);
	rc_t get_rec(recordID_t recordNumber, hpl_record_t& record);
	rc_t get_next_rec(recordID_t recordNumber, hpl_record_t& record, const flid_t* ifields);
	rc_t get_next_rec(recordID_t recordNumber, hpl_record_t& record);


	//////////////////////////////////////////////
	//  HPL structures
	//////////////////////////////////////////////


	struct varField
	{
		fieldSize_t size; // size of field value
		char* data; // field value


		// constructor
		varField(char* data) :
			data(data)
		{
			const char* varFieldValuePtr = data;
			size = strlen(varFieldValuePtr);
#if HPL_DEBUG>1
			cout << "Initialized var field structure with size: " << size << "and value (" << data << ")" << endl;
#endif
		}
	};


	bool 			fits_in_page(fieldSize_t dataSize);

	offset_t 		get_offset_field(recordID_t recordNumber, const flid_t fieldNumber);
	offset_t 		get_next_offset_field(recordID_t recordNumber, const flid_t fieldNumber);
	offset_t 		get_offset_field_slow(recordID_t recordNumber, const flid_t fieldNumber);
	void 			get_ffield(recordID_t recordNumber, hpl_field_t& field);
	void 			get_next_ffield(recordID_t recordNumber, hpl_field_t& field);
	void 			get_field(recordID_t recordNumber, hpl_field_t& field);
	void			get_vfield(recordID_t recordNumber, hpl_field_t& field);
	void			get_next_vfield(recordID_t rno, hpl_field_t& field);

	offset_t 		get_offset_segment(recordID_t recordNumber);
	offset_t 		get_offset_ghostbitmap(recordID_t recordNumber);
	offset_t		get_offset_nullbitmap(recordID_t recordNumber, flid_t fieldNumber);
	bool 			isFirstInBlock(recordID_t recordNumber, flid_t fieldNumber);
	blockID_t 		numberOfNewBlocksNeeded(recordID_t recordNumber);

	offset_t		getSizeOfSingleSegment();
	fieldSize_t 	getSizeOfVarFieldInVarArea(fieldSize_t dataSize);

	void 			set_ghostbit(recordID_t recordNumber);
	void 			unset_ghostbit(recordID_t recordNumber);
	bool 			isSetGhostbit(recordID_t recordNumber);
	void 			set_nullbit(recordID_t recordNumber, flid_t fieldNumber);
	void 			unset_nullbit(recordID_t recordNumber, flid_t fieldNumber);
	bool 			isSetNullbit(recordID_t recordNumber, flid_t fieldNumber);
	recordID_t 		getNextFreeRecord(bool append, recordID_t recordNumber=0);
	recordID_t		getNextValidRecord(recordID_t recordNumber);
	inline recordID_t 	next_slot(recordID_t curr)
	{
		//w_assert3(curr >=-1);
		//return (curr < (hpl_header->numberLastFixed)) ? curr+1 : EOP;
		return getNextValidRecord(curr);
	}


	offset_t 		get_free_var_field_offset(fieldSize_t fieldSize);
	recordID_t 		get_free_record_slot();
	void 			generatePoorManAndOffset(const offset_t offsetVar, const char* data, const fieldSize_t dataSize, char* returnValue);
	rc_t 			init_page_structure();
	rc_t 			init_new_segment_structure();
	void 			init_cacheBlockMap();
	rc_t 			insert_record(const hpl_record_t& record, recordID_t freeSlot = -1);
	void 			insert_fixed_size_value(recordID_t recordNumber, const flid_t fieldNumber, offset_t offset, const char* data);
	void			insert_var_size_value(const offset_t offset, const char* data, const fieldSize_t dataSize, const recordID_t recordNumber, flid_t fieldNumber);
	rc_t 			get_record(const recordID_t& recordNumber, hpl_record_t& record);
	rc_t 			update_field(const recordID_t recordNumber, const flid_t fieldNumber, const char* data, const fieldSize_t dataSize);
	rc_t 			update_record(const recordID_t recordNumber,const hpl_record_t& record, const flid_t* ifields);
	rc_t			delete_record(const rid_t& rid);

	rc_t           	find_and_lock_next_slot(uint4_t space_needed, slotid_t& idx);
	rc_t            find_and_lock_free_slot(uint4_t space_needed, slotid_t& idx);
	rc_t            _find_and_lock_free_slot(bool append_only, uint4_t space_needed, slotid_t& idx);
};

#endif
