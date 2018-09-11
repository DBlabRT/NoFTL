/*
 * member functions for query_c class
 */

#include "w_defines.h"
#include <sys/types.h>
#include "sm_query.h"
#include "sm_exec.h"

#define STRIPE		"/tmp/stripe"

query_c::query_c(int no_tables, char** tbname, ss_m::concurrency_t conc)
{
	int count;
	cc = conc;
	table_name = tbname;
	scanned = qualified = 0;
	output_file = stdout;

	for (count = 0; count < no_tables; ++count)
	{
		sprintf(finfo[count].table_name, "%s", table_name[count]);
		W_COERCE(find_vol_assoc(table_name[count], finfo[count]));
		W_COERCE(read_schema(table_name[count], finfo[count]));
	}
}

/*
 *  methods to load raw data in files
 *  for different page formats
 */

load_file_q::load_file_q(char* tbname, PageLayout pl)
{
    finfo[0].page_layout = pl;
    sprintf(finfo[0].table_name, "%s", tbname);
    W_COERCE(read_schema(tbname, finfo[0]));
}

w_rc_t load_file_q::run_nsm()
{
    rc_t 	rc;

    stid_t root_iid;  // root index ID
    rid_t rid;

    // Set filename for table data
    char data_file[FILE_NAME_SIZE];
    sprintf(data_file, "%s/%s.tbl", TABLE_DATA_DIR, finfo[0].table_name);

    FILE* fp;

    u_int rec_size=0; // total record size

    W_DO(ssm->begin_xct());
	    cerr << "Creating " << finfo[0].table_name << ".\n";
	    W_DO(ssm->create_file(VolID, finfo[0].fid, smlevel_3::t_regular));
	    if ((fp=fopen(data_file, "r"))==NULL)
	    {
	            cerr << "Error: Could not load" << data_file << endl;
	            return RC_AUGMENT(rc);
	    }
	    cerr << "Starting load from " << data_file << " at ";
	    PRINT_DATE;
	    cerr << endl;
	    append_file_i af(finfo[0].fid);
	    // read the line
	    char line[LINE_SIZE+1];
	    char record[MAX_REC_SIZE];
	    PRINT_PAGE_STATS("Before start of record insertion");
	    scanned=0;
            while (fgets(line,LINE_SIZE,fp)!=NULL)
            {
            	scanned++;
            	W_DO(get_frec_from_line(finfo[0], record, line, rec_size));
            	W_DO(af.create_rec(vec_t(), rec_size,
            			vec_t(record, rec_size), rid));
            	//cout << "#" << rid << " with size [" << rec_size << "]" <<  endl;
            	PRINT_PROGRESS_MESSAGE(scanned);
            }
	    fclose(fp);
	    PRINT_PAGE_STATS("After record insertion");

	    cerr << scanned << " records inserted." << endl;

	    // record file info in the root index
	    W_DO(ss_m::vol_root_index(VolID, root_iid));
	    W_DO(ss_m::create_assoc(root_iid,
                    vec_t(finfo[0].table_name, strlen(finfo[0].table_name)),
                    vec_t(&finfo[0], sizeof(finfo[0]))));

    W_DO(ssm->commit_xct());
    return RCOK;
}

/*
 * This function either formats a new device and creates a
 * volume on it, or mounts an already existing device and
 * returns the ID of the volume on it.
 */
w_rc_t load_file_q::run_pax()
{
    rc_t 	rc;

    stid_t root_iid;  // root index ID
    flid_t f;

    // Set filename for table data
    char data_file[FILE_NAME_SIZE];
    sprintf(data_file, "%s/%s.tbl", TABLE_DATA_DIR, finfo[0].table_name);

    FILE* fp;

    rid_t rid;

    precord_t prec(finfo[0].no_fields, finfo[0].no_ffields);
	int ignoredFields = 0;
	for (f = 0; f < finfo[0].no_fields+ignoredFields; ++f)
	{
		if (finfo[0].field_type[f]!=SM_IGNORE)
			prec.field_size[f-ignoredFields] = finfo[0].field_size[f];
		else
			ignoredFields++;
	}

    W_DO(ssm->begin_xct());
	cerr << "Creating " << finfo[0].table_name << ".\n";
	W_DO(ssm->create_file(VolID, finfo[0].fid,
                                        smlevel_3::t_regular, prec));
	if ((fp=fopen(data_file, "r"))==NULL)
	{
	    cerr << "Error: Could not load " << data_file << endl;
	    return RC_AUGMENT(rc);
	}
	cerr << "Starting load from " << data_file << " at ";
	PRINT_DATE;
	cerr << endl;
	append_pfile_i af(finfo[0].fid);
	PRINT_PAGE_STATS("Before start of record insertion");
	// read the line
	char line[LINE_SIZE+1];
	scanned=0;
        while (fgets(line,LINE_SIZE,fp)!=NULL)
	{
	    scanned++;
	    W_DO(get_prec_from_line(finfo[0], prec, line));
	    W_DO(af.create_rec(prec, rid));

	    for (f=0; f<finfo[0].no_fields; ++f)  {
                delete prec.data[f];
                prec.data[f]=NULL;
            }
	    PRINT_PROGRESS_MESSAGE(scanned);
	}
	fclose(fp);
	PRINT_PAGE_STATS("After record insertion");

	cerr << scanned << " records inserted." << endl;

	// record file info in the root index
	W_DO(ss_m::vol_root_index(VolID, root_iid));
	W_DO(ss_m::create_assoc(root_iid,
                    vec_t(finfo[0].table_name, strlen(finfo[0].table_name)),
                    vec_t(&finfo[0], sizeof(finfo[0]))));

    W_DO(ssm->commit_xct());
    return RCOK;
}

w_rc_t load_file_q::run_hpl()
{
    rc_t rc;

	stid_t root_iid; // root index ID
	flid_t f;

	// Set filename for table data
	char data_file[FILE_NAME_SIZE];
	sprintf(data_file, "%s/%s.tbl", TABLE_DATA_DIR, finfo[0].table_name);

	FILE* fp;

	rid_t rid;

	hpl_record_t hpl_rec(finfo[0].no_fields, finfo[0].no_ffields);
	int ignoredFields = 0;
	for (f = 0; f < finfo[0].no_fields+ignoredFields; ++f)
	{
		if (finfo[0].field_type[f]!=SM_IGNORE)
			hpl_rec.fieldSize[f-ignoredFields] = finfo[0].field_size[f];
		else
			ignoredFields++;
	}

	W_DO(ssm->begin_xct());
	cerr << "Creating " << finfo[0].table_name << ".\n";
	W_DO(ssm->create_file(VolID, finfo[0].fid, smlevel_3::t_regular, hpl_rec));
	if ((fp = fopen(data_file, "r")) == NULL) {
		cerr << "Error: Could not load " << data_file << endl;
		return RC_AUGMENT(rc);
	}
	cerr << "Starting load from " << data_file << " at ";
	PRINT_DATE;
	cerr << endl;
	append_hpl_p_i af(finfo[0].fid);
	PRINT_PAGE_STATS("Before start of record insertion");
	// read the line
	char line[LINE_SIZE + 1];
	scanned = 0;
	while (fgets(line, LINE_SIZE, fp) != NULL) {
		scanned++;
		W_DO(get_hpl_rec_from_line(finfo[0], hpl_rec, line));
		W_DO(af.create_rec(hpl_rec, rid));

		for (f = 0; f < finfo[0].no_fields; ++f) {
			delete hpl_rec.data[f];
			hpl_rec.data[f] = NULL;
		}
		PRINT_PROGRESS_MESSAGE(scanned);
	}
	fclose(fp);
	PRINT_PAGE_STATS("After record insertion");

	cerr << scanned << " records inserted." << endl;

	// record file info in the root index
	W_DO(ss_m::vol_root_index(VolID, root_iid));
	W_DO(ss_m::create_assoc(root_iid,
					vec_t(finfo[0].table_name, strlen(finfo[0].table_name)),
					vec_t(&finfo[0], sizeof(finfo[0]))));

	W_DO(ssm->commit_xct());
	return RCOK;
}

/*
 * finfo contains all I need to know about the file. 
 * I will 
 * 	- store it with the rest in order to be able to retrieve it
 * 	  later to get no_fields etc.
 * 	- create <no_fields>-many finfo's for each of the subfiles
 * 	  and store those too
 */

w_rc_t load_file_q::run_dsm()
{
    rc_t 	rc;

    flid_t f;
    stid_t root_iid;  // root index ID
    char dsm_key[DSM_KEY_SIZE]; // used to make the key of dsm files
    rid_t rid;

    precord_t prec_f (1,1); // for the dummy 1-field records I create

    // create and fill file to scan
    FILE* fp, *cmdfp;

    file_info_t finfo_f;
    finfo_f.no_fields = finfo_f.no_ffields = 1;
    finfo_f.page_layout = DSM;

    // Set filename for table data
    char data_file[FILE_NAME_SIZE];
    sprintf(data_file, "%s/%s.tbl", TABLE_DATA_DIR, finfo[0].table_name);

    W_DO(ssm->begin_xct());

    cerr << "Creating " << finfo[0].table_name << ".\n";
    for (f=0; f<finfo[0].no_fields; ++f)
    {
	PRINT_PAGE_STATS("Before start of record insertion");

	finfo_f.field_type = new FieldType[1];
	finfo_f.field_size = new Size2[1];
	// Create finfo_f, partition file information
	finfo_f.field_type[0] = finfo[0].field_type[f];
	finfo_f.field_size[0] = finfo[0].field_size[f];
	if (finfo[0].field_type[f] != SM_VARCHAR)
		finfo_f.fixed_rsize = finfo[0].field_size[f];
	else finfo_f.fixed_rsize = 0;

	W_DO(ssm->create_file(VolID, finfo_f.fid, smlevel_3::t_regular));
	append_file_i app_file(finfo_f.fid);
	cerr << "Created file number " << f << ", fid "<< finfo_f.fid << endl;

	cmdfp = fopen("cmd", "w");
	fprintf(cmdfp, "cat %s | awk -F\\| '{print $%u\"|\"}' > %s", data_file, (unsigned int)f+1, STRIPE);
	fclose(cmdfp);
	system("sh cmd");
	if ((fp=fopen(STRIPE, "r"))==NULL)
	{
	    cerr << "Error: Could not load " << STRIPE << endl;
	    return RC_AUGMENT(rc);
	}
	cerr << "Starting load field " << f << " from " << 
						data_file << " at ";
	PRINT_DATE;

	scanned=0;
	// read the line
	char line[LINE_SIZE+1];
        while (fgets(line,LINE_SIZE,fp)!=NULL)
	{
	    W_DO(get_prec_from_line(finfo_f, prec_f, line));
	    W_DO(app_file.create_rec(vec_t((char*)(&scanned), sizeof(int)),
	    	 prec_f.field_size[0], 
		 vec_t(prec_f.data[0], prec_f.field_size[0]), rid));
	    scanned++;

	    delete prec_f.data[0]; 
	    prec_f.data[0]=NULL;

	    PRINT_PROGRESS_MESSAGE(scanned);
	}
	cerr << scanned << " records inserted." << endl;
	fclose(fp);
	memset(dsm_key, '\0', DSM_KEY_SIZE);
	sprintf(dsm_key, "%u%s", (unsigned int)f, finfo[0].table_name);
	// I NEED to do this so that info is stored with NULL fields
	// and destructors work correctly
	delete [] finfo_f.field_type;
	finfo_f.field_type = NULL;
	delete [] finfo_f.field_size;
	finfo_f.field_size = NULL;
        W_DO(ss_m::vol_root_index(VolID, root_iid));
	W_DO(ss_m::create_assoc(root_iid,
		vec_t(dsm_key, strlen(dsm_key)),
		vec_t(&(finfo_f), sizeof(file_info_t))));
    }

    // record generic file info in the root index
    // This file DOES NOT EXIST, so don't ever try to read its fid.
    // It's only there to lead to the attributes of the sub-files.
    delete finfo[0].field_type;
    finfo[0].field_type = NULL;
    delete finfo[0].field_size;
    finfo[0].field_size = NULL;
    W_DO(ss_m::vol_root_index(VolID, root_iid));
    W_DO(ss_m::create_assoc(root_iid,
                            vec_t(finfo[0].table_name, strlen(finfo[0].table_name)),
                            vec_t(&finfo[0], sizeof(finfo[0]))));

    W_DO(ssm->commit_xct());

    return RCOK;
}


/*
 *  method to select star
 *  for different page formats
 */

w_rc_t select_star_q::run()
{
   W_DO(query_c::run());
   fprintf(output_file, "Scanned %d records, %d qualified.\n", scanned, qualified);
   return RCOK;
}

w_rc_t select_star_q::run_nsm()
{
    scan_file_i scan(finfo[0].fid, cc);
    pin_i*      handle;
    bool        eof = false;

    while (1) {
        W_DO(scan.next(handle, 0, eof));
        if (!eof) print_frec(finfo[0], handle->rec()->body(), output_file);
        else break;    // reached the end of file
        scanned++;
    }
    qualified = scanned;
    return RCOK;
}

w_rc_t select_star_q::run_pax()
{
    scan_pfile_i scan(finfo[0].fid, cc);
    pin_prec_i*         handle;
    bool        eof = false;

    while (1) {
        W_DO(scan.next(handle, eof));
	if (!eof)  {
	    print_prec(finfo[0], handle->precord(), output_file);
	}
	else break;
        scanned++;
    }
    qualified = scanned;
    return RCOK;
}

w_rc_t select_star_q::run_hpl()
{
    scan_hpl_p_i 	scan(finfo[0].fid, cc);
    pin_hpl_rec_i*   	handle;
    bool        	eof = false;

    while (1) {
        W_DO(scan.next(handle, eof));
	if (!eof)  {
		print_hpl_rec(finfo[0], handle->hpl_record(), output_file);
	}
	else break;
        scanned++;
    }
    qualified = scanned;
    return RCOK;
}

w_rc_t select_star_q::run_dsm()
{
    file_info_t finfo_f;

    scan_file_i **scan = new scan_file_i*[finfo[0].no_fields];
    pin_i **handle = new pin_i*[finfo[0].no_fields];
    flid_t f;
    for (f=0; f<finfo[0].no_fields; ++f)
    {
	W_DO(find_field_vol_assoc(finfo[0].table_name, f, finfo_f));
    	scan[f] = new scan_file_i(finfo_f.fid, cc);
    }
    bool  eof = false;

    while (1) {
    	for (f=0; f<finfo[0].no_fields; ++f)
	    W_DO(scan[f]->next(handle[f], 0, eof));
	if (!eof)
	{
	    for (f=0; f<finfo[0].no_fields; ++f)
	    	print_field(finfo[0], f, handle[f]->rec()->body(), output_file);
	    fprintf(output_file, "\n");
	}
	else   // reached the end of file
	   break;
	scanned++;
    } 
    qualified = scanned;
    for (f=0; f<finfo[0].no_fields; ++f)
    {
    	delete scan[f];
    }
    delete [] scan;
    delete [] handle;
    return RCOK;
}



/*
 *  method for range select
 *  for different page formats
 */

w_rc_t range_select_q::run()
{
   W_DO(query_c::run());
   fprintf(output_file, "Scanned %d records, %d qualified.\n", scanned, qualified);
   return RCOK;
}


w_rc_t range_select_q::run_nsm()
{
    scan_file_i scan(finfo[0].fid, cc);
    pin_i*      handle;
    bool        eof = false;
    flid_t f;
    bool condition=true;
    const char *frec;
    const char* fstart;

    while (1) {
        W_DO(scan.next(handle, 0, eof));
        if (eof==false)
	{
            scanned++;
	    frec=handle->body();
	    for (f=0; f<no_qfields; ++f)
	    {
	    	fstart = FIELD_START(finfo[0],frec,qfields[f].fno);
	        condition = condition && 
	          (compare(fstart,qfields[f].low,
		  	finfo[0].field_type[qfields[f].fno]) >=0) && 
	          (compare(fstart,qfields[f].high,
		  	finfo[0].field_type[qfields[f].fno]) <=0);
	    }
	    if (condition==true) 
	    {
	       qualified++;
               for (f=0; f<no_ifields; ++f)
	           print_field(finfo[0], ifields[f], 
		   	FIELD_START(finfo[0],frec,ifields[f]), output_file);
	       fprintf(output_file, "\n");
	    }
	    else condition=true;
	}
        else break;    // reached the end of file
    }
    return RCOK;
}


w_rc_t range_select_q::run_pax()
{
    flid_t f, i;
    flid_t* tmp_ifi;
    flid_t total_ifields=no_ifields;
    flid_t *q_idx=new flid_t[no_qfields]; // where it is in the scanned record

    // see if I need to add it to the ifields
    for (f=0; f<no_qfields; ++f)
    {
	tmp_ifi=ifields;
    	while (*tmp_ifi !=INV_FIELD && *tmp_ifi != qfields[f].fno)
		tmp_ifi++;
	if (*tmp_ifi==INV_FIELD) // wasn't in there
	{
	    total_ifields++;
	    *tmp_ifi=qfields[f].fno;
    	}
    }

    // now sort the list for the get_rec
    flid_t *sorted_ifields = new flid_t[total_ifields];
    flid_t *ifields_toprint = new flid_t[no_ifields];

    sorted_ifields[0]=INV_FIELD;
    for (i=0; i<total_ifields; ++i)
    {
        if (ifields[i]<sorted_ifields[0]) 
	    sorted_ifields[0]=ifields[i];
    }

    for (f=1; f<total_ifields; ++f)
    {
        sorted_ifields[f]=INV_FIELD; 
        for (i=0; i<total_ifields; ++i)
	{
            if (ifields[i]<sorted_ifields[f] && ifields[i]>sorted_ifields[f-1])
	   	sorted_ifields[f]=ifields[i];
        }
    }

    for (f=0; f<no_ifields; ++f)
        for (i=0; i<total_ifields; ++i)
	    if (sorted_ifields[i]==ifields[f])
	        ifields_toprint[f]=i;

    // find predicate indexes
    for (f=0; f<no_qfields; ++f)
    {   
	q_idx[f]=INV_FIELD;
	for (i=0; i<total_ifields; ++i)
	{
	   if (sorted_ifields[i] == qfields[f].fno)
	   	q_idx[f]=i;
	}
        w_assert1(q_idx[f]!=INV_FIELD);
    }

    scan_pfile_i scan(finfo[0].fid, cc, total_ifields, sorted_ifields);
    pin_prec_i*  handle;
    bool         eof = false;
    bool condition=true;
    const precord_t* prec;

    while (1) {
        W_DO(scan.next(handle, eof));
	if (eof==false)  {
	    scanned++;
	    prec = &(handle->precord());
	    for (f=0; f<no_qfields; ++f)
	    {
	        condition = condition &&
		  (compare(prec->data[q_idx[f]],qfields[f].low,
		  		finfo[0].field_type[qfields[f].fno]) >=0) &&
		  (compare(prec->data[q_idx[f]],qfields[f].high,
		  		finfo[0].field_type[qfields[f].fno]) <=0); 
	    }
	    if (condition==true)
	    {
	       qualified++;
               for (f=0; f<no_ifields; ++f)
	           print_field(finfo[0], ifields[f], prec->data[ifields_toprint[f]], output_file);
	       fprintf(output_file, "\n");
	    }
	    else condition=true;
	}
	else break;
    }
    
    // restore values in ifields just in case this is run again
    for (f=no_ifields; f<total_ifields; f++)
	ifields[f]=INV_FIELD;

    delete [] sorted_ifields;
    delete [] ifields_toprint;
    delete [] q_idx;
    return RCOK;
}

w_rc_t range_select_q::run_hpl()
{
    flid_t f, i;
    flid_t* tmp_ifi;
    flid_t total_ifields=no_ifields;
    flid_t *q_idx=new flid_t[no_qfields]; // where it is in the scanned record

    // see if I need to add it to the ifields
    for (f=0; f<no_qfields; ++f)
    {
	tmp_ifi=ifields;
    	while (*tmp_ifi !=INV_FIELD && *tmp_ifi != qfields[f].fno)
		tmp_ifi++;
	if (*tmp_ifi==INV_FIELD) // wasn't in there
	{
	    total_ifields++;
	    *tmp_ifi=qfields[f].fno;
    	}
    }

    // now sort the list for the get_rec
    flid_t *sorted_ifields = new flid_t[total_ifields];
    flid_t *ifields_toprint = new flid_t[no_ifields];

    sorted_ifields[0]=INV_FIELD;
    for (i=0; i<total_ifields; ++i)
    {
        if (ifields[i]<sorted_ifields[0])
	    sorted_ifields[0]=ifields[i];
    }

    for (f=1; f<total_ifields; ++f)
    {
        sorted_ifields[f]=INV_FIELD;
        for (i=0; i<total_ifields; ++i)
	{
            if (ifields[i]<sorted_ifields[f] && ifields[i]>sorted_ifields[f-1])
	   	sorted_ifields[f]=ifields[i];
        }
    }

    for (f=0; f<no_ifields; ++f)
        for (i=0; i<total_ifields; ++i)
	    if (sorted_ifields[i]==ifields[f])
	        ifields_toprint[f]=i;

    // find predicate indexes
    for (f=0; f<no_qfields; ++f)
    {
	q_idx[f]=INV_FIELD;
	for (i=0; i<total_ifields; ++i)
	{
	   if (sorted_ifields[i] == qfields[f].fno)
	   	q_idx[f]=i;
	}
        w_assert1(q_idx[f]!=INV_FIELD);
    }

    scan_hpl_p_i scan(finfo[0].fid, cc, total_ifields, sorted_ifields);
    pin_hpl_rec_i*  handle;
    bool         	eof = false;
    bool 			condition=true;
    const hpl_record_t* hpl_rec;

    while (1) {
        W_DO(scan.next(handle, eof));
	if (eof==false)  {
	    scanned++;
	    hpl_rec = &(handle->hpl_record());
	    for (f=0; f<no_qfields; ++f)
	    {
	        condition = condition &&
		  (compare(hpl_rec->data[q_idx[f]],qfields[f].low,
		  		finfo[0].field_type[qfields[f].fno]) >=0) &&
		  (compare(hpl_rec->data[q_idx[f]],qfields[f].high,
		  		finfo[0].field_type[qfields[f].fno]) <=0);
	    }
	    if (condition==true)
	    {
	       qualified++;
               for (f=0; f<no_ifields; ++f)
	           print_field(finfo[0], ifields[f], hpl_rec->data[ifields_toprint[f]], output_file);
	       fprintf(output_file, "\n");
	    }
	    else condition=true;
	}
	else break;
    }

    // restore values in ifields just in case this is run again
    for (f=no_ifields; f<total_ifields; f++)
	ifields[f]=INV_FIELD;

    delete [] sorted_ifields;
    delete [] ifields_toprint;
    delete [] q_idx;
    return RCOK;
}


w_rc_t range_select_q::run_dsm()
{

    file_info_t finfo_f;

    scan_file_i **scan = new scan_file_i*[finfo[0].no_fields];
    pin_i **handle = new pin_i*[finfo[0].no_fields];
    uint4_t f;
    bool  eof = false;
    bool condition=true;
    
    for (f=0; f<finfo[0].no_fields; ++f) { scan[f]=NULL; }

    for (f=0; f<no_ifields; ++f)
    {
	W_DO(find_field_vol_assoc(finfo[0].table_name, ifields[f], finfo_f));
    	scan[ifields[f]] = new scan_file_i(finfo_f.fid, cc);
    }

    for (f=0; f<no_qfields; ++f)
    {
	if (scan[qfields[f].fno]==NULL) // in case I did it in the previous loop
	{
	    W_DO(find_field_vol_assoc(finfo[0].table_name,
	    					qfields[f].fno, finfo_f));
    	    scan[qfields[f].fno] = new scan_file_i(finfo_f.fid, cc);
        }
    }

    while (1) {
    	for (f=0; f<finfo[0].no_fields; ++f)
	{
	  if (scan[f]!=NULL) W_DO(scan[f]->next(handle[f], 0, eof));
	}
	if (eof==false)
	{
	    scanned++;
	    for (f=0; f<no_qfields; ++f)
	    {
	        condition = condition &&
		  (compare(handle[qfields[f].fno]->body(),qfields[f].low,
		  		finfo[0].field_type[qfields[f].fno]) >=0) &&
		  (compare(handle[qfields[f].fno]->body(),qfields[f].high,
		  		finfo[0].field_type[qfields[f].fno]) <=0); 
	    }

	    if (condition==true)
	    {
	      qualified++;
	      for (f=0; f<no_ifields; ++f)
	    	print_field(finfo[0], ifields[f], handle[ifields[f]]->body(), output_file);
	      fprintf(output_file, "\n");
	    }
	    else condition=true;
	}
	else   // reached the end of file
	   break;
    } 

    for (f=0; f<finfo[0].no_fields; ++f)
    {
    	if (scan[f]!=NULL) delete scan[f];
    }
    delete [] scan;
    delete [] handle;
    return RCOK;
}


/*
 *  method for field select
 *  for different page formats
 */

w_rc_t select_field_q::run()
{
   W_DO(query_c::run());
   fprintf(output_file, "Scanned %d records, %d qualified.\n", scanned, qualified);
   return RCOK;
}


w_rc_t select_field_q::run_nsm()
{
    scan_file_i scan(finfo[0].fid, cc);
    pin_i*       handle;
    bool  eof = false;

    while (1) {
        W_DO(scan.next(handle, 0, eof));
        if (!eof)
            print_field(finfo[0], field_no,
                    FIELD_START(finfo[0], handle->body(), field_no), output_file);
        else break;    // reached the end of file
        ++scanned;
    }
    qualified=scanned;
    return RCOK;
}


w_rc_t select_field_q::run_pax()
{
    scan_pfield_i scan(finfo[0].fid, field_no, cc);
    pin_pfield_i*       handle;
    bool  eof = false;

    while (1) {
        W_DO(scan.next(handle, eof));
        if (!eof)
            print_field(finfo[0], field_no, handle->pfield().data, output_file);
        else break;    // reached the end of file
        scanned++;
    }
    qualified=scanned;
    return RCOK;
}

w_rc_t select_field_q::run_hpl()
{
    scan_hpl_field_i 	scan(finfo[0].fid, field_no, cc);
    pin_hpl_field_i*    handle;
    bool  				eof = false;

    while (1) {
        W_DO(scan.next(handle, eof));
        if (!eof)
            print_field(finfo[0], field_no, handle->hpl_field().data, output_file);
        else break;    // reached the end of file
        scanned++;
    }
    qualified=scanned;
    return RCOK;
}


w_rc_t select_field_q::run_dsm()
{
    file_info_t finfo_f;
    bool  eof = false;

    W_DO(find_field_vol_assoc(finfo[0].table_name, field_no, finfo_f));
    scan_file_i scan(finfo_f.fid, cc);
    pin_i *handle;

    while (1) {
        W_DO(scan.next(handle, 0, eof));
        if (!eof)
            print_field(finfo[0], field_no, handle->body(), output_file);
        else break;
        scanned++;
    }
    qualified=scanned;
    return RCOK;
}

