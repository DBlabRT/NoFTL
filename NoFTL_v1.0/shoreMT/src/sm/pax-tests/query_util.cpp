#include "query_util.h"

// This function reads fields from line into prec, which is a 
// partitioned structure that holds each field separately. 
// In char* line, fields are separated 
// with | and line must end with | as well.
// returns real record size (sum of field sizes it read)

w_rc_t get_prec_from_line(const file_info_t& finfo, precord_t& prec, char* line)
{
	rc_t rc;
	uint4_t i = 0;
	Size2 j;
	char *sptr, *tptr;
	sptr = tptr = line;

	prec.rec_size = 0;
	int ignoredFields = 0;
	do
	{
		if (*tptr == '|') // Field value is NULL
		{
			prec.field_size[i] = 0;
			prec.data[i] = NULL;
		}
		else
		{
			// Otherwise, advance to the
			while ((*tptr) != '|')
				tptr++;
			*tptr = '\0'; // to terminate strings and numbers wisely

			int actualRecordField = i - ignoredFields;

			switch (finfo.field_type[i])
			{
			case SM_IGNORE:
				ignoredFields++;
				break;
			case SM_INTEGER:
				prec.data[i] = (char*) new int;
				*(int*) prec.data[actualRecordField] = atoi(sptr);
				prec.field_size[actualRecordField] = finfo.field_size[i];
				prec.rec_size += prec.field_size[actualRecordField]; // real record size
				break;
			case SM_FLOAT:
				prec.data[actualRecordField] = (char*) new double;
				*(double*) prec.data[actualRecordField] = atof(sptr);
				prec.field_size[actualRecordField] = finfo.field_size[i];
				prec.rec_size += prec.field_size[actualRecordField]; // real record size
				break;
			case SM_DATE8:
				prec.field_size[actualRecordField] = finfo.field_size[i];
				time_t timestamp;
				timestamp = convertYYMMDDtoTimestamp(sptr);
				prec.data[actualRecordField] = new char[prec.field_size[actualRecordField]];
				memcpy( prec.data[actualRecordField],static_cast<const char*> (static_cast<const void*> (&timestamp)),
						prec.field_size[actualRecordField]);
				prec.rec_size += prec.field_size[actualRecordField]; // real record size
				break;
			case SM_DATE:
				w_assert3(finfo.field_size[i]==DATE_SIZE); // date
			case SM_CHAR:
				prec.data[actualRecordField] = new char[finfo.field_size[i]];
				memcpy(prec.data[actualRecordField], sptr, tptr - sptr); // first copy string
				for (j = tptr - sptr; j < finfo.field_size[i]; ++j)
					prec.data[actualRecordField][j] = '\0'; // add trailing \0's
				prec.field_size[actualRecordField] = finfo.field_size[i];
				prec.rec_size += prec.field_size[actualRecordField]; // real record size
				break;
			case SM_VARCHAR:
				prec.field_size[actualRecordField] = tptr - sptr + 1;
				prec.data[actualRecordField]
						= new char[prec.field_size[actualRecordField]];
				memcpy(prec.data[actualRecordField], sptr, tptr - sptr); // first copy string
				prec.data[actualRecordField][tptr - sptr] = '\0'; // add trailing \0
				prec.rec_size += prec.field_size[i]; // real record size
				break;
			default:
				cerr << "Bad type found in file information" << endl;
				return RC_AUGMENT(rc);
			}
		}
		sptr = ++tptr; // set sptr to start of next field
	} while (++i < finfo.no_fields + ignoredFields);
	return RCOK;
}

// Same as above, but for HPL page format.
w_rc_t get_hpl_rec_from_line(const file_info_t& finfo, hpl_record_t& hpl_rec,
		char* line)
{
	rc_t rc;
	Size4 i = 0;
	Size2 j;
	char *sptr, *tptr;
	sptr = tptr = line;

	hpl_rec.dataSize = 0;

	int ignoredFields = 0;
	do
	{
		if (*tptr == '|') // Field value is NULL
		{
			hpl_rec.fieldSize[i] = 0;
			hpl_rec.data[i] = NULL;
		}
		else
		{
			// Otherwise, advance to the
			while ((*tptr) != '|')
				tptr++;
			*tptr = '\0'; // to terminate strings and numbers wisely

			int actualRecordField = i - ignoredFields;

			switch (finfo.field_type[i])
			{
			// HPL: if we want to ignore a column in the load data wi set ignore flag in the schema
			case SM_IGNORE:
				ignoredFields++;
				break;
			case SM_INTEGER:
				hpl_rec.data[actualRecordField] = (char*) new int;
				*(int*) hpl_rec.data[actualRecordField] = atoi(sptr);
				hpl_rec.fieldSize[actualRecordField] = finfo.field_size[i];
				hpl_rec.dataSize += hpl_rec.fieldSize[actualRecordField]; // real record size
				break;
			case SM_FLOAT:
				hpl_rec.data[actualRecordField] = (char*) new double;
				*(double*) hpl_rec.data[actualRecordField] = atof(sptr);
				hpl_rec.fieldSize[actualRecordField] = finfo.field_size[i];
				hpl_rec.dataSize += hpl_rec.fieldSize[actualRecordField]; // real record size
				break;
			case SM_DATE:
				w_assert3(finfo.field_size[i]==DATE_SIZE); // date
			case SM_CHAR:
				hpl_rec.data[actualRecordField] = new char[finfo.field_size[i]];
				memcpy(hpl_rec.data[actualRecordField], sptr, tptr - sptr); // first copy string
				for (j = tptr - sptr; j < finfo.field_size[i]; ++j)
					hpl_rec.data[actualRecordField][j] = '\0'; // add trailing \0's
				hpl_rec.fieldSize[actualRecordField] = finfo.field_size[i];
				hpl_rec.dataSize += hpl_rec.fieldSize[actualRecordField]; // real record size
				break;
			case SM_DATE8:
				hpl_rec.fieldSize[actualRecordField] = finfo.field_size[i];
				time_t timestamp;
				timestamp = convertYYMMDDtoTimestamp(sptr);
				hpl_rec.data[actualRecordField]
						= new char[hpl_rec.fieldSize[actualRecordField]];
				memcpy(
						hpl_rec.data[actualRecordField],
						static_cast<const char*> (static_cast<const void*> (&timestamp)),
						hpl_rec.fieldSize[actualRecordField]);
				hpl_rec.dataSize += hpl_rec.fieldSize[actualRecordField]; // real record size
				break;
			case SM_VARCHAR:
				hpl_rec.fieldSize[actualRecordField] = tptr - sptr + 1;
				hpl_rec.data[actualRecordField]
						= new char[hpl_rec.fieldSize[actualRecordField]];
				memcpy(hpl_rec.data[actualRecordField], sptr, tptr - sptr); // first copy string
				hpl_rec.data[actualRecordField][tptr - sptr] = '\0'; // add trailing \0
				hpl_rec.dataSize += hpl_rec.fieldSize[actualRecordField]; // real record size
				break;
			default:
				cerr << "Bad type found in file information" << endl;
				return RC_AUGMENT(rc);
			}
		}
		sptr = ++tptr; // set sptr to start of next field
	} while (++i < finfo.no_fields + ignoredFields);

	return RCOK;
}

// Same as above, but flattens record in a string the traditional NSM way.
w_rc_t get_frec_from_line(const file_info_t& finfo, char* record, char* line,
		u_int& record_size)
{
	rc_t rc;

	// sptr = field start ptr, tptr = taisptr
	char *sptr = line, *tptr = line;
	uint4_t i = 0;
	int j;
	int rs = 0;

	int int_value = 0;
	double double_value = 0;
	int offset_to_next_var_start = finfo.fixed_rsize; // from beginning of rec
	int ignoredFields = 0;
	do
	{
		// FIXME -- Nulls are not handled in NSM
		// if (*tptr=='|') // Field value is NULL
		// {
		// prec.field_size[i]=0;
		// prec.data[i] = NULL;
		// }
		// else
		// {
		while ((*tptr) != '|')
			tptr++;
		*tptr = '\0';

		int actualRecordField = i - ignoredFields;

		switch (finfo.field_type[i])
		{
		case SM_IGNORE:
			//ignoredFields++;
			break;
		case SM_INTEGER:
			w_assert3(finfo.field_size[i]==sizeof(int));
			int_value = atoi(sptr);
			memcpy(record + rs, &int_value, sizeof(int));
			rs += sizeof(int);
			break;
		case SM_FLOAT:
			w_assert3(finfo.field_size[i]==sizeof(double));
			double_value = atof(sptr);
			memcpy(record + rs, &double_value, sizeof(double));
			rs += sizeof(double);
			break;
		case SM_DATE8:
			//prec.field_size[actualRecordField] = finfo.field_size[i];
			time_t timestamp;
			timestamp = convertYYMMDDtoTimestamp(sptr);
			//prec.data[actualRecordField] = new char[prec.field_size[actualRecordField]];
			memcpy(record + rs, static_cast<const char*> (static_cast<const void*> (&timestamp)),finfo.field_size[i]);
			rs += (finfo.field_size[i]);; // real record size
			break;
		case SM_DATE:
			w_assert3(finfo.field_size[i]==DATE_SIZE);
		case SM_CHAR:
			w_assert3(finfo.field_size[i]>=tptr-sptr+1);
			memcpy(record + rs, sptr, tptr - sptr); // first copy the string
			for (j = tptr - sptr; j < finfo.field_size[i]; ++j)
				record[rs + j] = '\0'; // add trailing \0's
			rs += (finfo.field_size[i]);
			break;
		case SM_VARCHAR:
			memcpy(record + offset_to_next_var_start, sptr, tptr - sptr);
			// set offset from beginning of record to start of next varchar
			offset_to_next_var_start += (tptr - sptr + 1);
			record[offset_to_next_var_start - 1] = '\0'; // add trailing \0
			*(Offset2*) (record + rs) = offset_to_next_var_start;
			rs += sizeof(Offset2);
			break;
		default:
			cerr << "Bad type found in file information" << endl;
			return RC_AUGMENT(rc);
		}
		// }
		sptr = ++tptr; // set sptr to start of next field
	} while (++i < finfo.no_fields + ignoredFields);
	w_assert3(finfo.fixed_rsize == rs);
	record_size = offset_to_next_var_start; // this is the real record size
	return RCOK;
}

// reads schema information from table_name.schema, 
// puts no_fields, no_ffields, and field types in finfo
rc_t read_schema(const char* table_name, file_info_t& finfo)
{
	rc_t rc;

	char schema_file[FILE_NAME_SIZE];
	sprintf(schema_file, "%s/%s.schema", SCHEMA_DIR, table_name);

	// read schema
	FILE* fp;
	if ((fp = fopen(schema_file, "r")) == NULL)
	{
		cerr << "Error reading " << schema_file << "." << endl;
		return RC_AUGMENT(rc);
	}

	// schema file contains: # of fields, # of fixed-size fields,
	// and a series of sizes (exc. lines starting with #)

	uint4_t i = 0, k;
	char *line = new char[LINE_SIZE];

	// pass comments and empty lines
	while ((line = fgets(line, LINE_SIZE, fp)) != NULL && ((*line) == '#'
			|| (*line) == '\n'))
		;
	// read no_fields
	finfo.no_fields = atoi(line);

	// pass comments, read no_ffields
	while ((line = fgets(line, LINE_SIZE, fp)) != NULL && ((*line) == '#'
			|| (*line) == '\n'))
		;
	finfo.no_ffields = atoi(line);
	finfo.field_size = new Size2[finfo.no_fields];
	finfo.field_type = new FieldType[finfo.no_fields];
	finfo.fixed_rsize = 0;

	int numberOfFields = finfo.no_fields;

	// read field types
	while (i < numberOfFields && (line = fgets(line, LINE_SIZE, fp)) != NULL)
	{
		if ((*line) == '#' || (*line) == '\n')
			continue;
		switch (line[0])
		{
		case 'Z': // SM_IGNORE HPL: if we want ingore a column we set this flag
			finfo.field_type[i] = SM_IGNORE;
			if(finfo.page_layout != 0) finfo.no_fields--;
			break;
		case 'I': // SM_INTEGER
			finfo.field_type[i] = SM_INTEGER;
			finfo.fixed_rsize += (finfo.field_size[i] = sizeof(int));
			break;
		case 'F': // SM_FLOAT
			finfo.field_type[i] = SM_FLOAT;
			finfo.fixed_rsize += (finfo.field_size[i] = sizeof(double));
			break;
		case 'D': // SM_DATE
			finfo.field_type[i] = SM_DATE;
			finfo.fixed_rsize += (finfo.field_size[i] = DATE_SIZE);
			break;
		case 'Y': // SM_DATE8 HPL: date to be loaded w/o "-"
			finfo.field_type[i] = SM_DATE8;
			finfo.fixed_rsize += (finfo.field_size[i] = DATE_SIZE8);
			break;
		case 'C': // SM_CHAR(x)
			finfo.field_type[i] = SM_CHAR;
			for (k = 0; !isdigit(line[k]); k++)
				;
			finfo.fixed_rsize += (finfo.field_size[i] = atoi(line + k) + 1);
			break;
		case 'V': // SM_VARCHAR
			finfo.field_type[i] = SM_VARCHAR;
			for (k = 0; !isdigit(line[k]); k++)
				;
			if (isdigit(line[k]))
				finfo.field_size[i] = atoi(line + k) + 1;
			else
				finfo.field_size[i] = VARCHAR_LEN_HINT;
			finfo.fixed_rsize += sizeof(Offset2);
			break;
		default:
			cerr << "Bad type found in" << schema_file << endl;
			return RC_AUGMENT(rc);
		}
		i++;
	}

	delete[] line;
	fclose(fp);
	return RCOK;
}

void print_field(const file_info_t& finfo, const flid_t fno, const char* value,
		FILE* output_to)
{
	rc_t rc;
	char fmt[10];

	w_assert3(fno < finfo.no_fields);
	if (value == NULL)
	{
		value = "\0\0\0\0\0\0\0\0";
	}
	switch (finfo.field_type[fno])
	{
	case SM_DATE8:
	case SM_INTEGER:
		fprintf(output_to, "%7d", *(int*) value);
		break;
	case SM_FLOAT:
		fprintf(output_to, "%10.3f", *(double*) value);
		break;
	case SM_DATE:
	case SM_CHAR:
		sprintf(fmt, "%%%ds", finfo.field_size[fno] + 2);
		fprintf(output_to, fmt, value); // value has trailing \0
		break;
	case SM_VARCHAR:
		fprintf(output_to, " %20s", value); // value has trailing \0
		break;
	default:
		cerr << "Bad type found in file information" << endl;
		return;
	}
}

static void _print_frec(const file_info_t& finfo, const char* frec,
		FILE* output_to)
{
	uint4_t rs = 0;
	flid_t i;
	int next_vstart_offset = finfo.fixed_rsize;
	for (i = 0; i < finfo.no_fields; i++)
	{
		if (finfo.field_type[i] != SM_VARCHAR)
		{
			print_field(finfo, i, frec + rs, output_to);
			rs += finfo.field_size[i];
		}
		else
		{
			print_field(finfo, i, frec + next_vstart_offset, output_to);
			next_vstart_offset = *(Offset2*) (frec + rs);
			rs += sizeof(Offset2);
		}
	}
}

static void _print_prec(const file_info_t& finfo, const precord_t& prec,
		FILE* output_to)
{
	flid_t f;
	for (f = 0; f < finfo.no_fields; f++)
		print_field(finfo, f, prec.data[f], output_to);
}

static void _print_hpl_rec(const file_info_t& finfo,
		const hpl_record_t& hpl_rec, FILE* output_to)
{
	flid_t f;
	for (f = 0; f < finfo.no_fields; f++)
		print_field(finfo, f, hpl_rec.data[f], output_to);
}

void print_prec(const file_info_t& finfo, const precord_t& prec,
		FILE* output_to)
{
	_print_prec(finfo, prec, output_to);
	fprintf(output_to, "\n");
}

void print_hpl_rec(const file_info_t& finfo, const hpl_record_t& hpl_rec,
		FILE* output_to)
{
	_print_hpl_rec(finfo, hpl_rec, output_to);
	fprintf(output_to, "\n");
}

void print_two_precs(const file_info_t& finfo1, const precord_t& prec1,
		const file_info_t& finfo2, const precord_t& prec2, FILE* output_to)
{
	_print_prec(finfo1, prec1, output_to);
	_print_prec(finfo2, prec2, output_to);
	fprintf(output_to, "\n");
}

void print_two_hpl_recs(const file_info_t& finfo1, const hpl_record_t& hpl_rec1,
		const file_info_t& finfo2, const hpl_record_t& hpl_rec2, FILE* output_to)
{
	_print_hpl_rec(finfo1, hpl_rec1, output_to);
	_print_hpl_rec(finfo2, hpl_rec2, output_to);
	fprintf(output_to, "\n");
}

void print_frec(const file_info_t& finfo, const char* frec, FILE* output_to)
{
	_print_frec(finfo, frec, output_to);
	fprintf(output_to, "\n");
}

void print_two_frecs(const file_info_t& finfo1, const char* frec1,
		const file_info_t& finfo2, const char* frec2, FILE* output_to)
{
	_print_frec(finfo1, frec1, output_to);
	_print_frec(finfo2, frec2, output_to);
	cout << endl;
}

int compare(const char* left, const char* right, FieldType type)
{
	switch (type)
	{
	case SM_DATE8:
		return COMPARE_NO(*(time_t*)left, *(time_t*)right);
	case SM_INTEGER:
		return COMPARE_NO(*(int*)left, *(int*)right);
	case SM_FLOAT:
		return COMPARE_NO(*(double*)left, *(double*)right);
	case SM_DATE:
		return COMPARE_DATE(left, right);
	case SM_CHAR:
	case SM_VARCHAR:
		return COMPARE_STR(left, right);
	default:
		cerr << "Wrong type!" << endl;
		w_assert3(0);
		return 0;
	}
}

int compare_date(const char* left, const char* right)
{
	w_assert3(strlen(left)==DATE_SIZE && strlen(right)==DATE_SIZE);w_assert3(left[4]=='-' && left[7]=='-' && right[4]=='-' && right[7]=='-');
	int res;

	if ((res = COMPARE_NO(atoi(left+DATE_Y_START), atoi(right+DATE_Y_START)))
			!= 0)
		return res;
	else if ((res
			= COMPARE_NO(atoi(left+DATE_M_START), atoi(right+DATE_M_START)))
			!= 0)
		return res;
	else
		return COMPARE_NO(atoi(left+DATE_D_START), atoi(right+DATE_D_START));
}

const char* toString(const file_info_t& finfo, const precord_t& prec, uint4_t col, char* buf)
{
	switch (finfo.field_type[col])
	{
	case SM_INTEGER:
		sprintf(buf, "%d", *(int*) prec.data[col]);
		return buf;

	case SM_DATE8:
	case SM_FLOAT:
		sprintf(buf, "%f", *(double*) prec.data[col]);
		return buf;

	case SM_DATE:
	case SM_CHAR:
	case SM_VARCHAR:
		return prec.data[col];
	default:
		cerr << "Bad type found in column " << col << endl;
		w_assert3(0);
		return NULL;
	}
}

const char* toString(const file_info_t& finfo, const hpl_record_t& hpl_rec, uint4_t col, char* buf)
{
	switch (finfo.field_type[col])
	{
	case SM_INTEGER:
		sprintf(buf, "%d", *(int*) hpl_rec.data[col]);
		return buf;

	case SM_DATE8:
	case SM_FLOAT:
		sprintf(buf, "%f", *(double*) hpl_rec.data[col]);
		return buf;

	case SM_DATE:
	case SM_CHAR:
	case SM_VARCHAR:
		return hpl_rec.data[col];
	default:
		cerr << "Bad type found in column " << col << endl;
		w_assert3(0);
		return NULL;
	}
}

// returns offset to field value from beginning of record
Offset2 find_offset(const file_info_t& finfo, const char* frec, uint4_t col)
{
	Offset2 start_offset; // from beginning of record
	uint4_t i;

	if (finfo.field_type[col] != SM_VARCHAR)
	{
		start_offset = 0;
		for (i = 0; i < col; ++i)
			start_offset += finfo.field_size[i];
	}
	else
	{
		if (col == finfo.no_ffields)
		{
			start_offset = finfo.fixed_rsize;
		}
		else
		{
			start_offset = *(Offset2*) (frec + finfo.fixed_rsize
					- (finfo.no_fields - col + 1) * sizeof(Offset2));
		}
	}
	return start_offset;
}

Offset2 find_offset101(const file_info_t& finfo, const char* frec, uint4_t col)
{
	Offset2 start_offset; // from beginning of record
	uint4_t i;

	if (finfo.field_type[col] != SM_VARCHAR)
	{
		start_offset = 0;
		for (i = 0; i < col; ++i)
		{
			if(finfo.field_type[i] != SM_IGNORE)
			{
				start_offset += finfo.field_size[i];
			}
		}
	}
	else
	{
		if (col == finfo.no_ffields)
		{
			start_offset = finfo.fixed_rsize;
		}
		else
		{
			start_offset = *(Offset2*) (frec + finfo.fixed_rsize - (finfo.no_fields - col + 1) * sizeof(Offset2));
		}
	}
	return start_offset;
}

const char* toString(const file_info_t& finfo, const char* frec, uint4_t col, char* buf)
{
	const char* field_ptr = FIELD_START(finfo,frec,col);

	switch (finfo.field_type[col])
	{
	case SM_INTEGER:
		sprintf(buf, "%d", *(int*) field_ptr);
		return buf;

	case SM_DATE8:
	case SM_FLOAT:
		sprintf(buf, "%f", *(double*) field_ptr);
		return buf;

	case SM_DATE:
	case SM_CHAR:
	case SM_VARCHAR:
		// Offset2 end_offset = *(Offset2*)(frec + finfo.fixed_rsize -
		// (finfo.no_fields-col)*sizeof(Offset2));
		// string is terminated
		return field_ptr;

	default:
		cerr << "Bad type found in column " << col << endl;
		w_assert3(0);
		return NULL;
	}
}

void frec_to_minirec(const file_info_t& finfo, const char* frec,
		const file_info_t& mini_finfo, const flid_t* ifields, char** mini_rec,
		Size2& mini_rec_size)
{
	flid_t v;
	Size2 fsz;
	Offset2 *offset_array, *mini_offset_array;
	Size2 size_so_far = 0;

	v = 0;
	flid_t f;
	Offset2 fsoffset;

	// first get size
	mini_rec_size = mini_finfo.fixed_rsize;
	offset_array = (Offset2*) (frec + finfo.fixed_rsize - (finfo.no_fields
			- finfo.no_ffields) * sizeof(Offset2));

	for (f = mini_finfo.no_ffields; f < mini_finfo.no_fields; ++f)
	{
		fsoffset = (ifields[f] > finfo.no_ffields ? offset_array[ifields[f]
				- finfo.no_ffields - 1] : finfo.fixed_rsize); // field start offset
		mini_rec_size += (offset_array[ifields[f] - finfo.no_ffields]
				- fsoffset);
	}

	// now allocate
	*mini_rec = new char[mini_rec_size];

	// now copy
	for (f = 0; f < mini_finfo.no_ffields; ++f)
	{
		memcpy(*mini_rec + size_so_far, FIELD_START(finfo, frec, ifields[f]),
				mini_finfo.field_size[f]);
		size_so_far += mini_finfo.field_size[f];
	}

	w_assert1(mini_finfo.fixed_rsize - size_so_far -
			(mini_finfo.no_fields-mini_finfo.no_ffields)*sizeof(Offset2)== 0);

	mini_offset_array = (Offset2*) (*mini_rec + size_so_far);

	size_so_far = mini_finfo.fixed_rsize;

	for (f = mini_finfo.no_ffields; f < mini_finfo.no_fields; ++f)
	{
		fsoffset = (ifields[f] > finfo.no_ffields ? offset_array[ifields[f]
				- finfo.no_ffields - 1] : finfo.fixed_rsize); // field start offset
		fsz = offset_array[ifields[f] - finfo.no_ffields] - fsoffset;
		memcpy(*mini_rec + size_so_far, frec + fsoffset, fsz);
		size_so_far += fsz;
		*(Offset2*) (mini_offset_array + v) = size_so_far;
		v++;
	}
}

time_t convertYYMMDDtoTimestamp(char* dateString)
{
	struct tm tm;
	time_t timestamp;
	if (strptime(dateString, "%Y-%m-%d", &tm) != NULL)
	{
		tm.tm_hour = 0;
		tm.tm_min = 0;
		tm.tm_sec = 0;
		tm.tm_gmtoff = 0;
		tm.tm_isdst = 0;
		timestamp = timegm(&tm);
		return timestamp;
	}
#if HPL_DEBUG>0
	cout << "Error: got wrong date to convert: " << dateString << endl;
#endif
	return 0;
}


/**
 * compare poorMan with const string and return varField value if string matches
*/
bool compareVarField(hpl_record_t *hpl_rec, const flid_t fieldNumber, const char* constStr)
{
	if(strncmp(hpl_rec->data[fieldNumber],constStr,(varFixedPartSize - sizeof(offset_t))) == 0)
	{
		// get varField value
		//data[col] = _pp->data;  // data + HPLPageHeaderSize + offset + sizeof(offset_t));
		return true;
	}
	else
		return false;
}
