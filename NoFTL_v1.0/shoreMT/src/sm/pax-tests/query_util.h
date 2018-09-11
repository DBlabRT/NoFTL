#ifndef QUERY_UTIL_H
#define QUERY_UTIL_H

#include "sm_vas.h"

#define LINE_SIZE       501
#define FILE_NAME_SIZE  101
#define DATE_SIZE       10+2
// HPL: date w/o "-"
#define DATE_SIZE8      8
#define MAX_REC_SIZE    1024
#define MAX_LD_TABLES   10
#define MAX_USE_TABLES  5
#define MAX_TABLE_NAME_SIZE 30
#define DSM_KEY_SIZE    100


#define VARCHAR_LEN_HINT        25

#define SCHEMA_DIR "tpcd/schema"
#define TABLE_DATA_DIR "tpcd/data"

#ifdef _WINDOWS
#define PRINT_DATE system("time /T")
#else
#define PRINT_DATE system("date")
#endif

#define PRINT_PAGE_STATS(msg)   \
{               \
    cerr << msg << ":" << endl; \
    cerr << "\tpage_alloc_cnt: " << GET_TSTAT(page_alloc_cnt) << endl;  \
    cerr << "\tpage_dealloc_cnt: " << GET_TSTAT(page_dealloc_cnt) << endl;  \
    cerr << "\tpage_fix_cnt:" << GET_TSTAT(page_fix_cnt) << endl;       \
    cerr << "\tpage_refix_cnt:" << GET_TSTAT(page_refix_cnt) << endl;   \
    cerr << "\tpage_unfix_cnt:" << GET_TSTAT(page_unfix_cnt) << endl;   \
}

typedef enum {NSM=0, PAX, HPL, DSM, NO_LAYOUT} PageLayout;

typedef enum {SM_USER=0, SM_SYSTEM, SM_ALL, NO_MEASURE} MeasureMode;

typedef enum {SM_INTEGER=0, SM_FLOAT, SM_DATE, SM_CHAR, SM_VARCHAR, SM_IGNORE, SM_DATE8} FieldType;

//typedef uint2_t		Offset2 ;

/* 
 * Date format is YYYY-MM-DD
 */

#define DATE_Y_START 0       
#define DATE_M_START 5
#define DATE_D_START 8

extern int compare(const char* left, const char* right, FieldType type);
extern int compare_date(const char* left, const char* right);

// Compare two numbers
#define COMPARE_NO(l, r) ((l)<(r) ? -1 : ((l)>(r) ? 1 : 0))

// Compare two strings
#define COMPARE_STR(l, r) (strcmp((l), (r)))

// Compare two dates 
// #define COMPARE_DATE(l, r) (compare_date_aligned((l), (r)))
#define COMPARE_DATE(l, r) (strcmp((l), (r)))

struct qual_field_t {
   flid_t fno;
   char* high;
   char* low;

   qual_field_t()
   {
      fno=INV_FIELD;
      high = low = NULL;
   }
};

struct file_info_t {
    char        table_name[MAX_TABLE_NAME_SIZE];
    stid_t      fid;
    PageLayout page_layout;
    flid_t     no_fields;
    flid_t     no_ffields;
    Size2       fixed_rsize; // Size of fixed part of record
    FieldType   *field_type;
    Size2       *field_size; // -1 for varchar

    file_info_t(PageLayout pl=NO_LAYOUT, int nf=0, int nff=0) {
        fid = stid_t::null;
        page_layout = pl;
        if (nf==0) {
            no_fields = no_ffields = 0;
            field_type=NULL;
            field_size=NULL;
        }
        else {
            no_fields = nf;
            no_ffields = nff;
            field_type = new FieldType[no_fields];
            field_size = new Size2[no_fields];
        }
        fixed_rsize=0;
    }

    void to_prec(precord_t& prec)
    {
        flid_t i;
        for (i=0; i<no_fields; ++i)
            prec.field_size[i] = field_size[i];
    }

    void to_hpl_rec(hpl_record_t& hpl_rec)
    {
         flid_t i;
         for (i=0; i<no_fields; ++i)
        	 hpl_rec.fieldSize[i] = field_size[i];
     }

    ~file_info_t() {
        //if (field_type) delete field_type;
        //if (field_size) delete field_size;
    	delete[] field_type;
    	delete[] field_size;
    }
};

struct join_table_t
{
    flid_t join_col;  // "fake" join column (in the new record)
    flid_t *ifields;
    bool (*ipred)(const void*, const file_info_t&);
    file_info_t finfo;

    join_table_t(flid_t jcol,
				 flid_t nifi,
                 flid_t *ifi,
                 bool (*ip)(const void*, const file_info_t&),
                 const file_info_t& fin)
    {
	w_assert3(ifi != NULL);
	flid_t i;

	join_col = jcol;
	ifields = ifi;
	ipred=ip;

        finfo.no_fields = nifi;
	finfo.fid = stid_t::null;
	finfo.page_layout = fin.page_layout;
	finfo.field_size = new Size2[finfo.no_fields];
	finfo.field_type = new FieldType[finfo.no_fields];

	for (i=0; i<nifi; ++i)
	{
	    finfo.field_size[i] = fin.field_size[ifi[i]];
	    finfo.field_type[i] = fin.field_type[ifi[i]];
	    if (ifi[i]==jcol) join_col=i;
	    if (ifi[i] < fin.no_ffields)
	    {
	        finfo.fixed_rsize += finfo.field_size[i];
	        finfo.no_ffields++;
	    }
	    else
	    {
	        finfo.fixed_rsize += sizeof(Offset2);
	    }
	}

    }
};

typedef smlevel_0::smksize_t    smksize_t;

extern w_rc_t get_frec_from_line(const file_info_t& finfo, char* record, char* line, u_int& record_size);

extern w_rc_t get_prec_from_line(const file_info_t& finfo, precord_t& prec, char* line);

extern w_rc_t get_hpl_rec_from_line(const file_info_t& finfo, hpl_record_t& hpl_rec, char* line);

extern rc_t read_schema(const char* table_name, file_info_t& finfo);

extern void print_field(const file_info_t& finfo,
                        const flid_t fno,
                        const char* value,
                        FILE* output_to = stdout);

extern Offset2 find_offset(const file_info_t& finfo,
                       const char* frec, uint4_t col);

extern Offset2 find_offset101(const file_info_t& finfo,
                       const char* frec, uint4_t col);

#define FIELD_START(finfo, frec, col) ((frec) + find_offset(finfo,frec,col))

#define FIELD_START101(finfo, frec, col) ((frec) + find_offset101(finfo,frec,col))

extern void print_frec(const file_info_t& finfo, const char* frec, FILE* output_to = stdout);

extern void print_two_frecs(const file_info_t& finfo1, const char* frec1, const file_info_t& finfo2, const char* frec2, FILE* output_to = stdout);

extern void print_prec(const file_info_t& finfo, const precord_t& prec, FILE* output_to = stdout);

extern void print_two_precs(const file_info_t& finfo1, const precord_t& prec1, const file_info_t& finfo2, const precord_t& prec2, FILE* output_to = stdout);

extern void print_two_hpl_recs(const file_info_t& finfo1, const hpl_record_t& hpl_rec1, const file_info_t& finfo2, const hpl_record_t& hpl_rec2, FILE* output_to = stdout);

extern void print_hpl_rec(const file_info_t& finfo, const hpl_record_t& hpl_rec, FILE* output_to = stdout);


extern const char* toString(const file_info_t& finfo,
                      const precord_t& prec,
                      uint4_t col,
                      char* buf);

extern const char* toString(const file_info_t& finfo,
                      const hpl_record_t& hpl_rec,
                      uint4_t col,
                      char* buf);

extern const char* toString(const file_info_t& finfo,
                      const char* frec,
                      uint4_t col,
                      char* buf);

extern void frec_to_minirec(const file_info_t& finfo,
			    const char* frec,
			    const file_info_t& mini_finfo,
                            const flid_t* ifields,
		            char** mini_rec,
			    Size2& mini_rec_size);
extern time_t convertYYMMDDtoTimestamp(char* dateString);

extern bool compareVarField(hpl_record_t *hpl_rec, const flid_t fieldNumber, const char* constStr);

#endif
