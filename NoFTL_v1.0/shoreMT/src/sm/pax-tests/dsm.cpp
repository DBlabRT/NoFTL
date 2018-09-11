
#include "w_defines.h"
#include <sys/types.h>
#include <assert.h>
#include "sm_exec.h"

// This include brings in all header files needed for writing a VAs 
#include "sm_vas.h"

#ifdef _WINDOWS
#include "getopt.h"
#endif

#define DSM_KEY_SIZE	100
#define STRIPE		"/tmp/stripe"

/*
 * finfo contains all I need to know about the file. 
 * I will 
 * 	- store it with the rest in order to be able to retrieve it
 * 	  later to get no_fields etc.
 * 	- create <no_fields>-many finfo's for each of the subfiles
 * 	  and store those too
 */

rc_t
DSM_load_file (const char* table_name, file_info_t& finfo)
{
    rc_t 	rc;

    int i;
    stid_t root_iid;  // root index ID
    char dsm_key[DSM_KEY_SIZE]; // used to make the key of dsm files
    rid_t rid;

    precord_t prec_i (1,1); // for the dummy 1-field records I create

    // create and fill file to scan
    FILE* fp, *cmdfp;

    file_info_t finfo_i;
    finfo_i.no_fields = finfo_i.no_ffields = 1;
    finfo_i.page_layout = DSM;

    u_int rec_size=0; // total record size
    int num_rec;

    // Set filename for table data
    char data_file[FILE_NAME_SIZE];
    sprintf(data_file, "%s.tbl", table_name);

    W_DO(ssm->begin_xct());

    for (i=0; i<finfo.no_fields; ++i)
    {
	PRINT_PAGE_STATS("Before start of record insertion");

	finfo_i.field_type = new FieldType[1];
	finfo_i.field_size = new Size2[1];
	// Create finfo_i, partition file information
	finfo_i.field_type[0] = finfo.field_type[i];
	finfo_i.field_size[0] = finfo.field_size[i];
	if (finfo.field_type[i] != VARCHAR)
		finfo_i.fixed_rsize = finfo.field_size[i];
	else finfo_i.fixed_rsize = 0;

	W_DO(ssm->create_file(VolID, finfo_i.fid, smlevel_3::t_regular));
	append_file_i app_file(finfo_i.fid);
	cerr << "Created file number " << i << ", fid "<< finfo_i.fid << endl;

	cmdfp = fopen("cmd", "w");
	fprintf(cmdfp, "cat %s | awk -F\\| '{print $%d\"|\"}' > %s", data_file, i+1, STRIPE);
	fclose(cmdfp);
	system("bash cmd");
	if ((fp=fopen(STRIPE, "r"))==NULL)
	{
	    cerr << "Error: Could not load " << STRIPE << endl;
	    return RC_AUGMENT(rc);
	}
	cerr << "Starting load field " << i << " from " << 
						data_file << " at ";
	PRINT_DATE;

	num_rec=0;

	// read the line
	char line[LINE_SIZE+1];
        while (fgets(line,LINE_SIZE,fp)!=NULL)
	{
	    num_rec++;
	    W_DO(get_prec_from_line(finfo_i, prec_i, line, rec_size));
	    W_DO(app_file.create_rec(vec_t(), prec_i.field_size[0], 
		 vec_t(prec_i.data[0], prec_i.field_size[0]), rid));
            // if (num_rec == 1) finfo_i.first_rid = rid;

	    delete prec_i.data[0]; 
	    prec_i.data[0]=NULL;

	    PRINT_PROGRESS_MESSAGE(num_rec);
	}
	cerr << num_rec << " records inserted." << endl;
	fclose(fp);
	memset(dsm_key, '\0', DSM_KEY_SIZE);
	sprintf(dsm_key, "%d%s", i, table_name);
	// I NEED to do this so that info is stored with NULL fields
	// and destructors work correctly
	delete [] finfo_i.field_type;
	finfo_i.field_type = NULL;
	delete [] finfo_i.field_size;
	finfo_i.field_size = NULL;
        W_DO(ss_m::vol_root_index(VolID, root_iid));
	W_DO(ss_m::create_assoc(root_iid,
		vec_t(dsm_key, strlen(dsm_key)),
		vec_t(&(finfo_i), sizeof(file_info_t))));
    }

    // record generic file info in the root index
    // This file DOES NOT EXIST, so don't ever try to read its fid.
    // It's only there to lead to the attributes of the sub-files.
    delete finfo.field_type;
    finfo.field_type = NULL;
    delete finfo.field_size;
    finfo.field_size = NULL;
    W_DO(ss_m::vol_root_index(VolID, root_iid));
    W_DO(ss_m::create_assoc(root_iid,
                            vec_t(table_name, strlen(table_name)),
                            vec_t(&finfo, sizeof(finfo))));

    W_DO(ssm->commit_xct());

    return RCOK;
}

static rc_t 
_find_field_vol_assoc(const char* file_key, u_int fno, file_info_t& finfo_i)
{
    char dsm_key[DSM_KEY_SIZE];
    memset(dsm_key, '\0', DSM_KEY_SIZE);
    sprintf(dsm_key, "%d%s", fno, file_key);
    W_DO(find_vol_assoc(dsm_key, finfo_i));
    return RCOK;
}

void DSM_select_star(const char* table_name, ss_m::concurrency_t cc)
{

    file_info_t finfo, finfo_i;
    W_COERCE(read_schema(table_name, finfo));

    cout << "Scanning... " << endl;
    scan_file_i **scan = new (scan_file_i*)[finfo.no_fields];
    pin_i **handle = new (pin_i*)[finfo.no_fields];
    int f;
    for (f=0; f<finfo.no_fields; ++f)
    {
	W_COERCE(_find_field_vol_assoc(table_name, f, finfo_i));
    	scan[f] = new scan_file_i(finfo_i.fid, cc);
    }
    bool  eof = false;

    int i = 0;
    while (1) {
    	for (f=0; f<finfo.no_fields; ++f)
	    W_COERCE(scan[f]->next(handle[f], 0, eof));
	if (!eof)
	{
	    for (f=0; f<finfo.no_fields; ++f)
	    	print_field(finfo, f, handle[f]->rec()->body());
	    cout << endl;
	}
	else   // reached the end of file
	   break;
	i++;
    } 
    for (f=0; f<finfo.no_fields; ++f)
    {
    	delete scan[f];
    	// delete handle[f];
    }
    delete [] scan;
    delete [] handle;
    cout << "scanned " << i << " records." << endl;
}


void DSM_select_field_with_no_pred (const char* table_name,
                                    ss_m::concurrency_t cc,
                                    int q_hi,
                                    int q_lo,
				    int no_pred)
{
/************************************************************************/
table_name = table_name;
cc=cc;
q_hi=q_hi;
q_lo=q_lo;
no_pred=no_pred;
/************************************************************************
   // pred goes from 1 to no_pred
   // predicate field range a1-a7
   double tmp;
   int no_qual=0, i=0, q;
   double sum=0,  condition = true;
   bool eof=false;

   cout << "select avg(R.a0)\nfrom R\nwhere " ;

   for (q=1; q<=no_pred; ++q)
   {
        if (q>1) cout << "  and ";
        cout << "a" << q << " > " << q_lo << " and a" <<
                                q << " <= " << q_hi << endl;
   }

   pin_i        handle_s;

   rid_t f_rid_q;
   rid_t f_rid_s;

   W_COERCE (ssm->serial_to_rid(lvid, dsm_finfo[1].first_rid, f_rid_q));
   W_COERCE (ssm->serial_to_rid(lvid, dsm_finfo[0].first_rid, f_rid_s));

   rid_t rid_s;

   // Scan the predicates with these
   scan_file_i **scan_q = new scan_file_i*[no_pred];
   pin_i**       handle_q = new pin_i*[no_pred];

   for (q=0; q<no_pred; ++q)
   {
       // scan field number q+1
       scan_q[q] = new scan_file_i(lvid, dsm_finfo[q+1].fid, cc);
       // start the scan
       W_COERCE(scan_q[q]->next(handle_q[q], 0, eof));
   }

   while (!eof) {
        ++i;
        // read q_field, assuming all double
	for (q=0; q<no_pred; ++q) {
#ifdef MEMCPY_FIELDS
           memcpy((char*)&tmp, handle_q[q]->rec()->body(), sizeof(double));
#else
           tmp = *(double*)(handle_q[q]->rec()->body());
#endif
           condition = (condition && (tmp > q_lo && tmp <= q_hi));
	}
	if (condition)
        {
             // adjust rid_s
             rid_s = f_rid_s;
             rid_s.pid.page += (handle_q[0]->rid().pid.page - f_rid_q.pid.page);
             rid_s.pid._stid.store +=
                (handle_q[0]->rid().pid._stid.store - f_rid_q.pid._stid.store);

             // needs to be pinned as it is a separate record
	     rid_s.slot = handle_q[0]->rid().slot;
             handle_s.pin(rid_s, 0);
#ifdef MEMCPY_FIELDS
             memcpy((char*)&tmp, handle_s.rec()->body(), sizeof(double));
             sum += tmp;
#else
             sum += *(double*)(handle_s.rec()->body());
#endif
             ++no_qual;
        }
	else condition=true;
	for (q=0; q<no_pred; ++q)
             W_COERCE(scan_q[q]->next(handle_q[q], 0, eof));
   }
   cout << "Average: " << ((no_qual>0) ? sum/no_qual : 0)
        << ". " << no_qual << " records qualified ("
        << ((double)no_qual*100/i) << "% selectivity)." << endl;
   cout << "Query complete.\n";
   
   // free Willy
      for (q=0; q<no_pred; ++q)
         delete scan_q[q];
      delete [] scan_q;
      delete [] handle_q;
************************************************************************/
}

void DSM_select_field_with_no_proj (const char* table_name,
                                    ss_m::concurrency_t cc,
                                    int q_hi,
                                    int q_lo,
				    int no_proj)
{
/************************************************************************/
table_name = table_name;
cc=cc;
q_hi=q_hi;
q_lo=q_lo;
no_proj=no_proj;
/************************************************************************
   double tmp;
   int no_qual=0, i=0, s;
   double *sum;
   bool eof=false;

   sum = new double[no_proj];
   for (s=0; s<no_proj; ++s) sum[s]=0;

   cout << "select ";
   for (s=1; s<=no_proj; ++s)
   {
        if (s>1) cout << ", ";
         cout << "avg(R.a" << s << ")";
   }

   cout << "\nfrom R\nwhere a0 > " << q_lo << " and a0 <= " << q_hi << endl;

   // open the sub-files we care about
   scan_file_i scan_q (lvid, dsm_finfo[0].fid, cc);
   pin_i*       handle_q;
   pin_i        handle_s;

   rid_t f_rid_q;
   rid_t *f_rid_s = new rid_t[no_proj];

   W_COERCE (ssm->serial_to_rid(lvid, dsm_finfo[0].first_rid, f_rid_q));
   for (s=0; s<no_proj; ++s)
     W_COERCE (ssm->serial_to_rid(lvid, dsm_finfo[s+1].first_rid, f_rid_s[s]));

   rid_t rid_s;

   W_COERCE(scan_q.next(handle_q, 0, eof));
   while (!eof) {
        ++i;
        // read q_field, assuming all double
#ifdef MEMCPY_FIELDS
        memcpy((char*)&tmp, handle_q->rec()->body(), sizeof(double));
#else
        tmp = *(double*)(handle_q->rec()->body());
#endif
        if (tmp > q_lo && tmp <= q_hi)
        {
	  for (s=0; s<no_proj; ++s)
          {
             // adjust rid_s
             rid_s = f_rid_s[s];
             rid_s.pid.page += (handle_q->rid().pid.page - f_rid_q.pid.page);
             rid_s.pid._stid.store +=
                (handle_q->rid().pid._stid.store - f_rid_q.pid._stid.store);

	     rid_s.slot = handle_q->rid().slot;
             // each attr needs to be pinned as it is a separate record
             handle_s.pin(rid_s, 0);
#ifdef MEMCPY_FIELDS
             memcpy((char*)&tmp, handle_s.rec()->body(), sizeof(double));
             sum[s] += tmp;
#else
             sum[s] += *(double*)(handle_s.rec()->body());
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

   // free Willy
   delete [] f_rid_s;
   delete [] sum;
************************************************************************/
}

void DSM_select_field_with_pred (const char* table_name,
                                 ss_m::concurrency_t cc, 
			         int s_field, 
			         int q_field, 
			         int q_hi, 
			         int q_lo)
{
/************************************************************************/
table_name = table_name;
cc=cc;
s_field=s_field;
q_field=q_field;
q_hi=q_hi;
q_lo=q_lo;
/************************************************************************
   // selects a field with predicate
   // to determine functionality
   cout << "select avg(R.a" << s_field << ")\nfrom R\nwhere a" << q_field << " > " 
        << q_lo << " and a" << q_field << " <= " << q_hi << endl;

   // open the two sub-files we care about
   scan_file_i scan_q (lvid, dsm_finfo[q_field].fid, cc);
   pin_i* 	handle_q;
   pin_i 	handle_s;
   double tmp;
   int no_qual=0, i=0;
   double sum=0;
   bool eof=false;

   rid_t f_rid_q;
   rid_t f_rid_s;

   W_COERCE (ssm->serial_to_rid(lvid, dsm_finfo[q_field].first_rid, f_rid_q));
   W_COERCE (ssm->serial_to_rid(lvid, dsm_finfo[s_field].first_rid, f_rid_s));

   rid_t rid_s;

   W_COERCE(scan_q.next(handle_q, 0, eof));
   while (!eof) {
	++i;
	// read q_field, assuming all double
#ifdef MEMCPY_FIELDS
        memcpy((char*)&tmp, handle_q->rec()->body(), sizeof(double));
#else
        tmp = *(double*)(handle_q->rec()->body());
#endif
        if (tmp > q_lo && tmp <= q_hi)
	{
	     // adjust rid_s
	     rid_s = f_rid_s;
	     rid_s.pid.page += (handle_q->rid().pid.page - f_rid_q.pid.page);
	     rid_s.pid._stid.store += 
		(handle_q->rid().pid._stid.store - f_rid_q.pid._stid.store);
	     // needs to be pinned as it is a separate record
	     rid_s.slot = handle_q->rid().slot;
	     handle_s.pin(rid_s, 0);
	     // assert(handle_q->serial_no()==handle_s.serial_no());
#ifdef MEMCPY_FIELDS
             memcpy((char*)&tmp, handle_s.rec()->body(), sizeof(double));
             sum += tmp;
#else
	     sum += *(double*)(handle_s.rec()->body());
#endif

	     ++no_qual;
	}
        W_COERCE(scan_q.next(handle_q, 0, eof));
   }
   cout << "Average: " << ((no_qual>0) ? sum/no_qual : 0)
   	<< ". " << no_qual << " records qualified ("
	<< ((double)no_qual*100/i) << "% selectivity)." << endl;
   cout << "Query complete.\n";
************************************************************************/
}


