#include "w_defines.h"
#include <sys/types.h>
#include <assert.h>
#include "sm_exec.h"
#include "sm_query.h"

// This include brings in all header files needed for writing a VAs 
#include "sm_vas.h"

#ifdef _WINDOWS
#include "getopt.h"
#endif

rc_t
NSM_load_file (const char* table_name, file_info_t& finfo)
{
    rc_t 	rc;

    stid_t root_iid;  // root index ID
    rid_t rid;

    // Set filename for table data
    char data_file[FILE_NAME_SIZE];
    sprintf(data_file, "%s.tbl", table_name);

    FILE* fp;

    u_int rec_size=0; // total record size
    int num_rec = 0;

    W_DO(ssm->begin_xct());
	    cerr << "Creating a new file." << endl;
	    W_DO(ssm->create_file(VolID, finfo.fid, smlevel_3::t_regular));
	    if ((fp=fopen(data_file, "r"))==NULL)
	    {
	            cerr << "Error: Could not load" << data_file << endl;
	            return RC_AUGMENT(rc);
	    }
	    cerr << "Starting load from " << data_file << " at ";
	    PRINT_DATE;
	    cerr << endl;
	    append_file_i af(finfo.fid);
	    // read the line
	    char line[LINE_SIZE+1];
	    char record[MAX_REC_SIZE];
	    PRINT_PAGE_STATS("Before start of record insertion");
            while (fgets(line,LINE_SIZE,fp)!=NULL)
	    {
	        num_rec++;
		W_DO(get_frec_from_line(finfo, record, line, rec_size));
		W_DO(af.create_rec(vec_t(), rec_size, 
				    vec_t(record, rec_size), rid));
		PRINT_PROGRESS_MESSAGE(num_rec);
	    }
	    fclose(fp);
	    PRINT_PAGE_STATS("After record insertion");

	    cerr << num_rec << " records inserted." << endl;

	    // record file info in the root index
	    W_DO(ss_m::vol_root_index(VolID, root_iid));
	    W_DO(ss_m::create_assoc(root_iid,
                    vec_t(table_name, strlen(table_name)),
                    vec_t(&finfo, sizeof(finfo))));

    W_DO(ssm->commit_xct());
    return RCOK;
}

static void 
_print_frec(const file_info_t& finfo, const char* frec)
{
    int rs=0, i;
    int next_vstart_offset=finfo.fixed_rsize;
    for (i=0; i<finfo.no_fields; i++) {
    	if (finfo.field_type[i]!=VARCHAR) {
    	    print_field(finfo, i, frec+rs);
	    rs += finfo.field_size[i];
    	}
    	else {
	    print_field(finfo, i, frec+next_vstart_offset);
	    next_vstart_offset = *(Offset2*)(frec+rs);
	    rs += sizeof(Offset2);
    	}
    }
    cout << endl;
}

w_rc_t select_star_q::run_nsm()
{
    cout << "Scanning... " << endl;
    PRINT_PAGE_STATS("Before scan");
    scan_file_i scan(finfo[0].fid, cc);
    pin_i*      handle;
    bool        eof = false;
    int         i = 0;

    while (1) {
        W_DO(scan.next(handle, 0, eof));
        if (!eof) _print_frec(finfo[0], handle->rec()->body());
        else break;    // reached the end of file
        i++;
    }
    cout << "scanned " << i << " records." << endl;
    PRINT_PAGE_STATS("After scan");
    return RCOK;
}

void NSM_select_star(const char* table_name, ss_m::concurrency_t cc)
{
    file_info_t finfo;
    W_COERCE(find_vol_assoc(table_name, finfo));
    W_COERCE(read_schema(table_name, finfo));

    cout << "Scanning... " << endl;
    PRINT_PAGE_STATS("Before scan");
    scan_file_i scan(finfo.fid, cc);
    pin_i*      handle;
    bool        eof = false;
    int         i = 0;

    while (1) {
        W_COERCE(scan.next(handle, 0, eof));
	if (!eof) _print_frec(finfo, handle->rec()->body());
	else break;    // reached the end of file
        i++;
    }
    cout << "scanned " << i << " records." << endl;
    PRINT_PAGE_STATS("After scan");
}


void NSM_select_field_with_no_proj (const char* table_name,
					ss_m::concurrency_t cc,
					int q_hi,
                                   	int q_lo,
				   	int no_proj)
{
   double tmp;
   int no_qual=0, i=0, s, f;
   double *sum;
   bool eof=false;
   char* buffer;

   file_info_t finfo;
    W_COERCE(find_vol_assoc(table_name, finfo));
   W_COERCE(read_schema(table_name, finfo));

   // this is only for double fields
   for (f=0; f<finfo.no_fields; ++f)
   	assert(finfo.field_type[f]==FLOAT);

   sum = new double[no_proj];
   for (s=0; s<no_proj; ++s) sum[s]=0;

   cout << "select ";
   for (s=1; s<=no_proj; ++s)
   {
        if (s>1) cout << ", ";
         cout << "avg(R.a" << s << ")";
   }

   cout << "\nfrom R\nwhere a0 > " << q_lo << " and a0 <= " << q_hi << endl;

   scan_file_i scan_q(finfo.fid, cc);
   pin_i*       handle_q;

   W_COERCE(scan_q.next(handle_q, 0, eof));
   while (!eof) {
        // read field a0, assuming all double
        ++i;
        buffer = (char*)handle_q->rec()->body();
#ifdef MEMCPY_FIELDS
        memcpy((char*)&tmp, buffer + 0*sizeof(double), sizeof(double));
#else
        tmp = *(double*)(buffer + 0*sizeof(double));
#endif
        if (tmp > q_lo && tmp <= q_hi)
        {
	   for (s=0; s<no_proj; ++s)
	   {
#ifdef MEMCPY_FIELDS
             memcpy((char*)&tmp, buffer + (s+1)*sizeof(double), sizeof(double));
             sum[s] += tmp;
#else
             sum[s] += *(double*)(buffer + (s+1)*sizeof(double));
#endif
	   }
           ++no_qual;
        }
        W_COERCE(scan_q.next(handle_q, 0, eof));
   }
   for (s=0; s<no_proj; ++s)
   {
      cout << "Average: " << ((no_qual>0) ? sum[s]/no_qual : 0)
           << ". " << no_qual << " records qualified ("
           << ((double)no_qual*100/i) << "% selectivity)." << endl;
   }
   cout << "Query complete.\n";

   // free willy
   delete [] sum;
}

void NSM_select_field_with_no_pred (const char* table_name,
                                   ss_m::concurrency_t cc,
                                   int q_hi,
                                   int q_lo,
                                   int no_pred)
{
   // pred goes from 1 to no_pred
   // predicate field range a1-a7

   double tmp;
   int no_qual=0, i=0, q, f;
   double sum=0;
   bool eof=false, condition = true;
   char* buffer;

   file_info_t finfo;
   W_COERCE(find_vol_assoc(table_name, finfo));
   W_COERCE(read_schema(table_name, finfo));

   // this is only for double fields
   for (f=0; f<finfo.no_fields; ++f)
   	assert(finfo.field_type[f]==FLOAT);

   cout << "select avg(R.a0)\nfrom R\nwhere " ;

   for (q=1; q<=no_pred; ++q)
   {
        if (q>1) cout << "  and ";
        cout << "a" << q << " > " << q_lo << " and a" <<
                                q << " <= " << q_hi << endl;
   }
   // define this to scan the predicates
   scan_file_i scan_q(finfo.fid, cc);
   pin_i*       handle_q;

   W_COERCE(scan_q.next(handle_q, 0, eof));
   while (!eof) {
        // read q_field, assuming all double
        ++i;
        buffer = (char*)handle_q->rec()->body();
	for (q=1; q<=no_pred; ++q) {
#ifdef MEMCPY_FIELDS
            memcpy((char*)&tmp, buffer + q*sizeof(double), sizeof(double));
#else
            tmp = *(double*)(buffer + q*sizeof(double));
#endif
            condition = (condition && (tmp > q_lo && tmp <= q_hi));
	}
	if (condition)
        {
#ifdef MEMCPY_FIELDS
             memcpy((char*)&tmp, buffer + 0*sizeof(double), sizeof(double));
             sum += tmp;
#else
             sum += *(double*)(buffer + 0*sizeof(double));
#endif
             ++no_qual;
        }
	else condition=true;
        W_COERCE(scan_q.next(handle_q, 0, eof));
   }
   cout << "Average: " << ((no_qual>0) ? sum/no_qual : 0)
        << ". " << no_qual << " records qualified ("
        << ((double)no_qual*100/i) << "% selectivity)." << endl;
   cout << "Query complete.\n";
}

void NSM_select_field_with_pred (const char* table_name,
                                ss_m::concurrency_t cc,
                             	int s_field,
                             	int q_field,
                             	int q_hi,
                             	int q_lo)
{
   // selects a field with predicate
   // to determine functionality
   double tmp;
   int no_qual=0, i=0, f;
   double sum=0;
   bool eof=false;
   char* buffer;

   file_info_t finfo;
   W_COERCE(find_vol_assoc(table_name, finfo));
   W_COERCE(read_schema(table_name, finfo));

   // this is only for double fields
   for (f=0; f<finfo.no_fields; ++f)
   	assert(finfo.field_type[f]==FLOAT);

   cout << "select avg(R.a" << s_field << ")\nfrom R\nwhere a" << q_field << " > "
        << q_lo << " and a" << q_field << " <= " << q_hi << endl;

   scan_file_i scan_q(finfo.fid, cc);
   pin_i*       handle_q;

   W_COERCE(scan_q.next(handle_q, 0, eof));
   while (!eof) {
        // read q_field, assuming all double
	++i;
	buffer = (char*)handle_q->rec()->body();
#ifdef MEMCPY_FIELDS
	memcpy((char*)&tmp, buffer + q_field*sizeof(double), sizeof(double));
#else
	tmp = *(double*)(buffer + q_field*sizeof(double));
#endif
	if (tmp > q_lo && tmp <= q_hi)
        {
#ifdef MEMCPY_FIELDS
	     memcpy((char*)&tmp, buffer + s_field*sizeof(double), sizeof(double));
             sum += tmp;
#else
	     sum += *(double*)(buffer + s_field*sizeof(double));
#endif
             ++no_qual;
        }
        W_COERCE(scan_q.next(handle_q, 0, eof));
   }
   cout << "Average: " << ((no_qual>0) ? sum/no_qual : 0)
   	<< ". " << no_qual << " records qualified ("
        << ((double)no_qual*100/i) << "% selectivity)." << endl;
   cout << "Query complete.\n";
}


