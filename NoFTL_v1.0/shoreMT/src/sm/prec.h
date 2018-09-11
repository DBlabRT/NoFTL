/* --------------------------------------------------------------- */
/* -- Copyright (c) 1994, 1995 Computer Sciences Department,    -- */
/* -- University of Wisconsin-Madison, subject to the terms     -- */
/* -- and conditions given in the file COPYRIGHT.  All Rights   -- */
/* -- Reserved.                                                 -- */
/* --------------------------------------------------------------- */
/* -- Anastassia Ailamaki                                       -- */
/* --------------------------------------------------------------- */

#ifndef PREC_H
#define PREC_H

typedef uint2_t     flid_t ;
typedef int2_t      Size2 ;

#define MAX_FIELDS    16    // maximum number of fields n a record
#define INV_FIELD   MAX_FIELDS

// User supplies this structure
struct precord_t {
	uint1_t  no_fields;
	uint1_t  no_ffields;
	Size2   rec_size;
	Size2   *field_size;
	char**   data;

	// Constructors
	precord_t()  // dummy for pfile_p::format()
	{
		no_fields = 0;
		no_ffields = 0;
		rec_size = 0;
		field_size = NULL;
		data = NULL;
	}

	precord_t(int nf, int nff):
		no_fields(nf), no_ffields(nff)
	{
		int i;
		field_size = new Size2[nf];
	rec_size = 0; // this the size of the data ONLY
		data = new char*[nf];
		for (i=0; i<nf; ++i) { field_size[i] = INV_FIELD; data[i] = NULL;  }
	}

	// copy constructor, no data
	precord_t(const struct precord_t& prec_info):
		no_fields(prec_info.no_fields), no_ffields(prec_info.no_ffields),
	rec_size(prec_info.rec_size)
	{
		int i;
		if (prec_info.field_size!=NULL)
		{
			field_size = new Size2[no_fields];
			for (i=0; i<no_fields; ++i)
				field_size[i] = prec_info.field_size[i];
		}
	if (prec_info.data!=NULL)
	{
		data = new char*[no_fields];
		// ouch
		for (i=0; i<no_fields; ++i)
		{
			if (prec_info.data[i]!=NULL)
		{
			data[i] = new char[field_size[i]];
			memcpy(data[i], prec_info.data[i], field_size[i]);
			}
		}
	}
	}

	// returns the size of the whole structure.
	Size2 size()
	{
		Size2 sz = sizeof(precord_t);
	if (field_size!=NULL)
		sz += no_fields*sizeof(Size2);
	sz += rec_size;
		return sz;
	}

	// Destructor
	~precord_t()
	{
		delete [] field_size;
		if (data!=NULL) 
		delete [] data;
	// DO NOT DELETE data[i]
	}
} ;

inline ostream& operator<< (ostream& o, const precord_t& prec_info)
{
	o << "Partitioned record info: " ;
	int i;
	for (i = 0; i<prec_info.no_ffields; ++i)
	  o << i << ": " << prec_info.field_size[i] << " bytes | ";
	if (i==prec_info.no_fields)
	  o << "\n";
	else
	  o << i << "-" << prec_info.no_fields << ": varsize" << "\n";
	return o;
}

struct pfield_t {
	flid_t fno;                // from 0 to no_fields-1
	Size2 field_size;
	bool nullable;
	bool fixed_size;
	char* data;

	pfield_t(flid_t f=0, Size2 fs=INV_FIELD)
	{
		fno=f;
		field_size=fs;
		nullable=false;
		fixed_size=true;
		data=NULL;
	}

};

#endif 

