/* ------------------------------------------------------------------------------- */
/* -- Copyright (c) 2011, 2012 Databases and Distributed Systems Department,    -- */
/* -- Technical University of Darmstadt, subject to the terms and conditions    -- */
/* -- given in the file COPYRIGHT.  All Rights Reserved.                        -- */
/* ------------------------------------------------------------------------------- */
/* -- Ilia Petrov, Todor Ivanov, Veselin Marinov                                -- */
/* ------------------------------------------------------------------------------- */

/**
 * Intermediate structure to hold tuple/logical row information
 */

#ifndef HPL_REC_H
#define HPL_REC_H
#include <new>

typedef uint2_t		flid_t;			// unsigned 2 bytes (16 bits)
typedef uint1_t		Size1;			// unsigned 1 bytes
typedef int2_t		Size2;			// signed 2 bytes
typedef int4_t		Size4;			// unsigned 4 bytes
typedef uint8_t		Size8;			// unsigned 8 bytes

typedef uint2_t		fieldSize_t;
typedef uint2_t		offset_t;
typedef Size2		recordID_t;
typedef uint2_t		blockID_t;
typedef	uint2_t		recordSize_t;
typedef	uint2_t		bitmapSize_t;


#define HPL_MAX_FIELDS	32				// maximum number of fields in a record
#define INV_FIELD	HPL_MAX_FIELDS		// TODO: HPL: what is this???


struct hpl_record_t
{
	uint1_t			numberOfFields;
	uint1_t			numberOfFixedSizeFields;
	Size2			dataSize;				// this the size of the data ONLY
	fieldSize_t*	fieldSize;
	char**			data;

	// Constructors
	hpl_record_t()  // dummy for pfile_p::format()
	{
		numberOfFields 			= 0;
		numberOfFixedSizeFields	= 0;
		dataSize				= 0;
		fieldSize				= NULL;
		data					= NULL;
	}

	hpl_record_t(int nf, int nff):
		numberOfFields(nf),
		numberOfFixedSizeFields(nff)
	{
		int i;
		fieldSize = new fieldSize_t[nf];
		dataSize = 0;
		data = new char*[nf];
		for (i=0; i<nf; ++i)
		{
			fieldSize[i] = INV_FIELD;
			data[i] = NULL;
		}
	}

	// copy constructor
	hpl_record_t(const struct hpl_record_t& record):
		numberOfFields(record.numberOfFields),
		numberOfFixedSizeFields(record.numberOfFixedSizeFields),
		dataSize(record.dataSize)
	{
		int i;
		if (record.fieldSize!=NULL)
		{
			fieldSize = new fieldSize_t[numberOfFields];
			for (i=0; i<numberOfFields; ++i)
				fieldSize[i] = record.fieldSize[i];
		}
		if (record.data!=NULL)
		{
			data = new char*[numberOfFields];
			for (i=0; i<numberOfFields; ++i)
			{
				if (record.data[i]!=NULL)
				{
					data[i] = new char[fieldSize[i]];
					memcpy(data[i], record.data[i], fieldSize[i]);
				}
			}
		}
	}

	/**
	 * returns the size of the whole structure
	 */
	recordSize_t size()
	{
		recordSize_t sizeOfRecord = sizeof(hpl_record_t);
		if (fieldSize!=NULL)
		{
			sizeOfRecord += numberOfFields*sizeof(fieldSize_t);
		}
		sizeOfRecord += dataSize;
		return sizeOfRecord;
	}

	// Destructor
	~hpl_record_t()
	{
		if (fieldSize!=NULL)
			delete[] fieldSize;
		if (data!= NULL)
		{
			/*int i;
			for (i=0; i<numberOfFields; ++i)
			{
				delete[] data[i];
			} */
		delete [] data;
		}
	}
} ;

inline ostream& operator<< (ostream& o, const hpl_record_t& record)
{
	o << "Partitioned record info: " ;
	int i;
	for (i = 0; i<record.numberOfFixedSizeFields; ++i)
	{
		o << i << ": " << record.fieldSize[i] << " bytes | ";
	}
	if (i==record.numberOfFields)
		o << "\n";
	else
		o << i << "-" << record.numberOfFields << ": varsize" << "\n";

	return o;
}

struct hpl_field_t
{
	flid_t		fieldNumber;                // from 0 to numberOfFields-1
	fieldSize_t	fieldSize;
	bool		isNullable;
	bool		isFixedSize;
	char*		data;

	// Constructors
/*	hpl_field_t()  // dummy format()
	{
		fieldNumber	= 0;
		fieldSize	= 0;
		isNullable	= false;
		isFixedSize	= true;
		data		= NULL;
	}*/

	hpl_field_t(flid_t fn=0, fieldSize_t fs=INV_FIELD)
	{
		fieldNumber	= fn;
		fieldSize	= fs;
		isNullable	= false;
		isFixedSize	= true;
		data		= NULL;
	}

	// Destructor
	~hpl_field_t()
	{
		delete [] data;
	}
};

#endif 

