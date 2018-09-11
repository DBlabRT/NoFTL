/* ------------------------------------------------------------------------------- */
/* -- Copyright (c) 2011, 2012 Databases and Distributed Systems Department,    -- */
/* -- Technical University of Darmstadt, subject to the terms and conditions    -- */
/* -- given in the file COPYRIGHT.  All Rights Reserved.                        -- */
/* ------------------------------------------------------------------------------- */
/* -- Ilia Petrov, Todor Ivanov, Veselin Marinov                                -- */
/* ------------------------------------------------------------------------------- */

#ifndef HPL_HDR_H
#define HPL_HDR_H

#include <stddef.h>
#include <math.h>

#include "hpl_rec.h"

typedef	uint2_t		Offset2 ;

#define	HPLPageHeaderSize	(align(sizeof(hpl_p_hdr_t)))

// HPL params
#define CacheLineSize 					64
#define CacheLinesInBlock 				4
#define CacheBlockSize					(CacheLineSize*CacheLinesInBlock)
#define maxNumberOfRecordsPerSegment	(CacheLineSize*8)
#define varFixedPartSize				2
#define	thresholdInsert					100	// 7000 = 15%  --> percent free space to trigger stop of inserts
#define	cacheBlockMapSize				88 		// 64 for TPCC / 88 for TPC-B

// offset to the start of the field size array (FROM START OF HEADER)
#define FieldSizeOffset					(offsetof(hpl_p_hdr_t,fieldSize))
// offset to the start of the field per cacheline array (FROM START OF HEADER)

// offset to the start of the offsetLastFixed array (FROM START OF HEADER)
#define OffsetLastFixedOffset2			(offsetof(hpl_p_hdr_t,offsetLastFixed))

#define FieldsPerCachelineOffset		(offsetof(hpl_p_hdr_t,fieldsPerCacheline))

// fno (field number) ranges from 0 to field_no-1)
// rno (record number) ranges from 0 to no_records-1)

/*--------------------------------------------------------------*
 *  struct hpl_p_hdr_t                                        *
 *--------------------------------------------------------------*/

struct hpl_p_hdr_t {
	// 0 for varsize
	fieldSize_t	fieldSize[HPL_MAX_FIELDS];			// 00-31	Field size array

	offset_t	freeSpaceVar;					// 32,33	sum of free holes in the var space
	recordID_t	freeRecords;					// 34,35
	recordID_t	numberLastFixed;				// 36,37	number of the last (the one with the biggest offset) record
	recordID_t 	numberOfRecordsInPage;			// 38,39	number of records in page

	uint1_t		numberOfFields;					// 40		number of fields
	uint1_t		numberOfFixedSizeFields;		// 41		number of fixed size fields (fields must be ordered and var sized fields must reside at the end)
	offset_t	dataSize;						// 42,43	in bytes
	offset_t	freeSpace;						// 44,45	space between last allocated block of cache lines in the fix field area and offsetLastVar
	offset_t	offsetLastVar;					// 46,47	offset of last (the one with the smallest offset) inserted var field

	fieldSize_t recordSizeFixedPart;			// 48,49	size of record in the fixed part area

	fill14    	filler;							// 50-63	filler for 64-byte

	recordID_t	fieldsPerCacheline[HPL_MAX_FIELDS];	// 64-95	Field size array

	offset_t	cacheBlockMap[HPL_MAX_FIELDS][cacheBlockMapSize];	// 96-352	256 byte map of cache blocks

	offset_t	offsetLastFixed[16];


	//inline bool has_nullable() { return bm_any_set(bm_null, no_fields); }
	bool  has_varsize() { return (numberOfFields>numberOfFixedSizeFields); }
	bool  is_varsize(flid_t fieldNumber) { return (fieldNumber>=numberOfFixedSizeFields); }
	bool  is_fixed_size(flid_t fieldNumber) { return (fieldNumber < numberOfFixedSizeFields); }


	/**
	 * returns MINIMUM record size
	 * var fields are counted as 8 bytes
	 * (you can think of it as the size of the record in the fixed part of the page)
	 */
/*	Size2 min_rec_size()
	{
		int i=0;
		Size2 sz=0;
		for (;i<numberOfFixedSizeFields;++i)
		{
			sz+=fieldSize[i];
		}
		sz += (numberOfFields-numberOfFixedSizeFields)*varFixedPartSize;
		return sz;
	}*/

/*
	// Returns MINIMUM record size with overhead
	// 1 bit per fixed field, 12 bytes per var size field
	double add_min_rsize_ovh (const int rec_data_size) {
		return (rec_data_size + numberOfFixedSizeFields*FREC_OVH +
				// here I am adding
				// 1/8 (1 presence bit) per fixed-size record
				((numberOfFields-numberOfFixedSizeFields)*sizeof(Offset2))); }

*/

	// constructor
	hpl_p_hdr_t() // dummy for pfile_p::format()
	{
		int i;

		dataSize 				= 0;
		freeSpace 				= 0;
		freeSpaceVar 			= 0;
		freeRecords 			= 0;
		numberOfRecordsInPage 	= 0;
		numberOfFields 			= 0;
		numberOfFixedSizeFields = 0;
		numberLastFixed 		= -1;
		offsetLastVar 			= 0;
		recordSizeFixedPart		= 0;
		for (i=numberOfFields; i<HPL_MAX_FIELDS; ++i)
		{
			fieldSize[i]=INV_FIELD ;
			fieldsPerCacheline[i]=0;
		}
	}

	// constructor
	hpl_p_hdr_t(const hpl_record_t& record, Size2 data_sz)
		: numberOfFields (record.numberOfFields),
		  numberOfFixedSizeFields (record.numberOfFixedSizeFields),
		  dataSize (data_sz),
		  freeSpace (data_sz),
		  offsetLastVar (data_sz)
	{
		freeRecords 			= 0;
		numberOfRecordsInPage 	= 0;
		numberLastFixed 		= -1;
		freeSpaceVar 			= 0;
		recordSizeFixedPart 	= 0;

		w_assert3(numberOfFields>0 && numberOfFields<=HPL_MAX_FIELDS);
		w_assert3(numberOfFixedSizeFields<=numberOfFields);
		w_assert3(record.fieldSize);

		int i;

		for (i=0; i<numberOfFixedSizeFields; ++i)
		{
			w_assert3(record.fieldSize[i] > 0);
			fieldSize[i]=record.fieldSize[i];
			recordSizeFixedPart+=record.fieldSize[i];
			fieldsPerCacheline[i]=CacheBlockSize/record.fieldSize[i];
		}
		for (; i<numberOfFields; ++i)
		{
			w_assert3(record.fieldSize[i] > 0);
			// set real size of varField
			fieldSize[i]=record.fieldSize[i]; //varFixedPartSize;
			fieldsPerCacheline[i]=CacheBlockSize/varFixedPartSize;
		}
		recordSizeFixedPart += (numberOfFields-numberOfFixedSizeFields) * varFixedPartSize;

#if HPL_DEBUG>1
		//cout << "Number of fixed size fields: " << numberOfFixedSizeFields << "; number of fields: " << numberOfFields << endl;
		printf("Number of fixed size fields: %d; number of fields: %d\n", numberOfFixedSizeFields, numberOfFields);
#endif
	}

	void printHeaderSummary()
	{
		cout << "free space: " << freeSpace << "(" << freeSpaceVar << ") of " << dataSize << " / " << "records: " << numberOfRecordsInPage << " of " << freeRecords  << " / NLFR: " << numberLastFixed << " / OLVF: " << offsetLastVar << "\n";
	}
};

#endif

