#include "w_defines.h"
#include <sys/types.h>
#include <assert.h>
#include "sm_exec.h"

// This include brings in all header files needed for writing a VAs 
#include "sm_vas.h"

#ifdef _WINDOWS
#include "getopt.h"
#endif

/*
 * This function either formats a new device and creates a
 * volume on it, or mounts an already existing device and
 * returns the ID of the volume on it.
 */
rc_t
PAX_load_file (const char* table_name, file_info_t& finfo)
{
    rc_t 	rc;

    stid_t root_iid;  // root index ID
    int i;

    // Set filename for table data
    char data_file[FILE_NAME_SIZE];
    sprintf(data_file, "%s.tbl", table_name);

    FILE* fp;

    rid_t rid;
    u_int rec_size=0; // total record size
    int num_rec = 0;

    precord_t prec(finfo.no_fields, finfo.no_ffields);
    for (i=0; i<finfo.no_fields; ++i)
    	prec.field_size[i] = finfo.field_size[i];

    W_DO(ssm->begin_xct());
	cerr << "Creating a new file." << endl;
	W_DO(ssm->create_pfile(VolID, finfo.fid,
                                        smlevel_3::t_regular, prec));
	if ((fp=fopen(data_file, "r"))==NULL)
	{
	    cerr << "Error: Could not load" << data_file << endl;
	    return RC_AUGMENT(rc);
	}
	cerr << "Starting load from " << data_file << " at ";
	PRINT_DATE;
	cerr << endl;
	PRINT_PAGE_STATS("Before start of record insertion");
	// read the line
	char line[LINE_SIZE+1];
        while (fgets(line,LINE_SIZE,fp)!=NULL)
	{
	    num_rec++;
	    W_DO(get_prec_from_line(finfo, prec, line, rec_size));
	    W_DO(ssm->create_prec(finfo.fid, prec, rid));

	    for (i=0; i<finfo.no_fields; ++i)  {
                delete prec.data[i];
                prec.data[i]=NULL;
            }
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

void PAX_select_star(const char* table_name, ss_m::concurrency_t cc)
{
    file_info_t finfo;
    W_COERCE(find_vol_assoc(table_name, finfo));
    W_COERCE(read_schema(table_name, finfo));

    cout << "Scanning... " << endl;
    scan_pfile_i scan(finfo.fid, cc);
    pin_prec_i*         handle;
    bool        eof = false;
    int         f, i = 0;

    while (1) {
        W_COERCE(scan.next(handle, eof));
	if (!eof)  {
	    for (f=0; f<finfo.no_fields; f++)
	    	print_field(finfo, f, handle->precord().data[f]);
	    cout << endl;
	}
	else break;
        i++;
    }
    cout << "scanned " << i << " records." << endl;
}

// scans only field fno
void PAX_scan_f(const char* table_name, const flid_t fno,
                ss_m::concurrency_t cc)
{
    file_info_t finfo;
    W_COERCE(find_vol_assoc(table_name, finfo));
    W_COERCE(read_schema(table_name, finfo));

    cout << "starting scan_f of field " << fno << "." << endl;
    scan_pfield_i scan(finfo.fid, fno, cc);
    pin_pfield_i*       handle;
    bool        eof = false;
    int         i = 0;

    while (1) {
        W_COERCE(scan.next(handle, eof));
	if (!eof) print_field(finfo, fno, handle->pfield().data);
        i++;
    }

        // make sure we got the right record
    cout << "scanned " << i << " records." << endl;
}


void PAX_select_field_with_no_proj (const char* table_name,
                                    ss_m::concurrency_t cc,
                                    int q_hi,
                                    int q_lo,
				    int no_proj)
{
   double tmp;
   int no_qual=0, i=0, s, f;
   double *sum;
   bool eof=false;

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

   // define this to get some info about the projected attrs
   scan_pfield_i **scan_s = new scan_pfield_i*[no_proj];
   pin_pfield_i  **handle_s = new pin_pfield_i*[no_proj];
   for (s=0; s<no_proj; ++s)
   {
       // scan field number q+1
       scan_s[s] = new scan_pfield_i(finfo.fid, s+1, cc);
       // start the scan
       W_COERCE(scan_s[s]->next(handle_s[s], eof));
   }

   scan_pfield_i scan_q(finfo.fid, 0, cc);
   pin_pfield_i*       handle_q;
   W_COERCE(scan_q.next(handle_q, eof));
   while (!eof) {
        // read field a0, assuming all double
        ++i;
#ifdef MEMCPY_FIELDS
        memcpy((char*)&tmp, handle_q->pfield().data, sizeof(double));
#else
        tmp = *(double*)(handle_q->pfield().data);
#endif
        if (tmp > q_lo && tmp <= q_hi)
        {
	  for (s=0; s<no_proj; ++s)
	  {
            // the record is pinned already, so...
            handle_q->another_pfield((pfield_t&)handle_s[s]->pfield());
#ifdef MEMCPY_FIELDS
             memcpy((char*)&tmp, handle_s[s]->pfield().data, sizeof(double));
             sum[s] += tmp;
#else
             sum[s] += *(double*)(handle_s[s]->pfield().data);
#endif
	  }
          ++no_qual;
        }
        W_COERCE(scan_q.next(handle_q, eof));
   }
   for (s=0; s<no_proj; ++s)
   {
      cout << "Average: " << ((no_qual>0) ? sum[s]/no_qual : 0)
           << ". " << no_qual << " records qualified ("
           << ((double)no_qual*100/i) << "% selectivity)." << endl;
   }
   cout << "Query complete.\n";
   // free memory
   for (s=0; s<no_proj; ++s)
	delete scan_s[s];
   delete [] scan_s;
   delete [] handle_s;
   delete [] sum;
}

void PAX_select_field_with_no_pred (const char* table_name,
				    ss_m::concurrency_t cc,
				    int q_hi,
				    int q_lo,
				    int no_pred)     // no_pred = a1...no_pred
{
   // pred goes from 1 to no_pred
   // predicate field range a1-a7

   double tmp;
   int i=0, q, no_qual=0, f;
   double sum=0;
   bool eof=false, condition = true;

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
   // define this to scan the projected attribute
   scan_pfield_i scan_s (finfo.fid, 0, cc);
   pin_pfield_i  *handle_s;
   // Scan once for field a0 to get the right values (size etc)
   W_COERCE(scan_s.next(handle_s, eof));

   // define this to scan the predicates
   scan_pfield_i **scan_q = new scan_pfield_i*[no_pred];
   pin_pfield_i  **handle_q = new pin_pfield_i*[no_pred];
   for (q=0; q<no_pred; ++q)
   {
       // scan field number q+1
       scan_q[q] = new scan_pfield_i(finfo.fid, q+1, cc);
       // start the scan
       W_COERCE(scan_q[q]->next(handle_q[q], eof));
   }


   while (!eof) {
        // read q_field, assuming all double
        ++i;
        for (q=0; q<no_pred; ++q) {
#ifdef MEMCPY_FIELDS
          memcpy((char*)&tmp, handle_q[q]->pfield().data, sizeof(double));
#else
          tmp = *(double*)(handle_q[q]->pfield().data);
#endif
          // long shot
          condition = (condition && (tmp > q_lo && tmp <= q_hi));
        }
        if (condition)
        {
            // the record is pinned already, so...
            handle_q[0]->another_pfield((pfield_t&)handle_s->pfield());
#ifdef MEMCPY_FIELDS
            memcpy((char*)&tmp, handle_s->pfield().data, sizeof(double));
             sum += tmp;
#else
             sum += *(double*)(handle_s->pfield().data);
#endif
            ++no_qual;
        }
	else condition=true;
	// scan field 1, get the rest with another_field
        W_COERCE(scan_q[0]->next(handle_q[0], eof));
	if (!eof)
	{
	  for (q=1; q<no_pred; ++q)
	  {
	    handle_q[0]->another_pfield((pfield_t&)handle_q[q]->pfield());
	  }
	}
   }
   cout << "Average: " << ((no_qual>0) ? sum/no_qual : 0)
        << ". " << no_qual << " records qualified ("
        << ((double)no_qual*100/i) << "% selectivity)." << endl;
   cout << "Query complete.\n";

   // free memory
   for (q=0; q<no_pred; ++q)
   	delete scan_q[q];
   delete [] scan_q;
   delete [] handle_q;

}

void PAX_select_field_with_pred (const char* table_name,
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

   file_info_t finfo;
    W_COERCE(find_vol_assoc(table_name, finfo));
   W_COERCE(read_schema(table_name, finfo));

   // this is only for double fields
   for (f=0; f<finfo.no_fields; ++f)
   	assert(finfo.field_type[f]==FLOAT);

   cout << "select avg(R.a" << s_field << ")\nfrom R\nwhere a" << q_field << " > "
        << q_lo << " and a" << q_field << " <= " << q_hi << endl;

   scan_pfield_i scan_q(finfo.fid, q_field, cc);
   pin_pfield_i*       handle_q;

   // define this to get some info
   scan_pfield_i scan_s(finfo.fid, s_field, cc);
   pin_pfield_i*       handle_s;

   // Scan once for s_field to get the right values (size etc)
   W_COERCE(scan_s.next(handle_s, eof));

   W_COERCE(scan_q.next(handle_q, eof));
   while (!eof) {
        // read q_field, assuming all double
        ++i;
#ifdef MEMCPY_FIELDS
        memcpy((char*)&tmp, handle_q->pfield().data, sizeof(double));
#else
        tmp = *(double*)(handle_q->pfield().data);
#endif
        if (tmp > q_lo && tmp <= q_hi)
        {
            // the record is pinned already, so...
            handle_q->another_pfield((pfield_t&)handle_s->pfield());
#ifdef MEMCPY_FIELDS
             memcpy((char*)&tmp, handle_s->pfield().data, sizeof(double));
             sum += tmp;
#else
             sum += *(double*)(handle_s->pfield().data);
#endif
            ++no_qual;
        }
        W_COERCE(scan_q.next(handle_q, eof));
   }
   cout << "Average: " << ((no_qual>0) ? sum/no_qual : 0)
        << ". " << no_qual << " records qualified ("
        << ((double)no_qual*100/i) << "% selectivity)." << endl;
   cout << "Query complete.\n";
}
