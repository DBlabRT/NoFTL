#include "w_defines.h"
#include <w_stream.h>
#include <sys/types.h>
#include <cassert>
#include "sm_vas.h"
#include <memory.h>
#include <assert.h>
#include "sm_exec.h"
#include "sm_query.h"
#include "ahhjoin.h"
#include "sm_tpch.h"
#include <TimeMeasure.h>

#ifdef _WINDOWS
#include "getopt.h"
#endif
ss_m* ssm = 0;

vid_t VolID=1;

#define RUN_QUERY(o)	{ if (measure==true)  W_COERCE((o).measure_run(measure_mode));  \
				 else W_COERCE((o).run()); }

#ifdef Linux
static __inline__ unsigned long long rdtsc(void)
{
        unsigned long long ret;
        __asm__ __volatile__("rdtsc"
                            : "=A" (ret)
                            : );
        return ret;
}
#endif

typedef   smlevel_0::smksize_t    smksize_t;

smsize_t    _rec_size=0;

/*
static void
print_config_info()
{
    sm_config_info_t sminfo;
    ssm->config_info(sminfo);
    cout << "Storage Manager Info: <" << sminfo << ">" << endl;
}
*/


rc_t 
find_field_vol_assoc(const char* file_key, u_int fno, file_info_t& finfo_i)
{
    char dsm_key[DSM_KEY_SIZE];
    memset(dsm_key, '\0', DSM_KEY_SIZE);
    sprintf(dsm_key, "%d%s", fno, file_key);
    W_DO(find_vol_assoc(dsm_key, finfo_i));
    return RCOK;
}


rc_t
find_vol_assoc(const char* file_key, file_info_t& finfo)
{
    rc_t rc;

    bool        found;
    stid_t root_iid;  // root index ID
    smsize_t  finfo_len = sizeof(finfo);

    // W_DO(ssm->begin_xct());

    W_DO(ss_m::vol_root_index(VolID, root_iid));
    W_DO(ss_m::find_assoc(root_iid,
                      vec_t(file_key, strlen(file_key)),
                      &finfo, finfo_len, found));

    if (!found) {
        cerr << "No file information found for " << file_key <<endl;
        return RC_AUGMENT(rc);
    }

    // W_DO(ssm->commit_xct());

    return RCOK;
}

// find ID of the volume on the device
static rc_t
find_volume_id (const char* device_name, vid_t& volid)
{
    lvid_t* lvid_list = 0;
    u_int   lvid_cnt;
    W_DO(ssm->list_volumes(device_name, lvid_list, lvid_cnt));
    if (lvid_cnt != 1) return RC(smlevel_0::eBADVOL);

    W_DO(ssm->lvid_to_vid(lvid_list[0], volid));

    delete [] lvid_list;
    return RCOK;
}

/*
 * This function either formats a new device and creates a
 * volume on it, or mounts an already existing device and
 * returns the ID of the volume on it.
 */
static rc_t 
init_device_and_volume(bool init_device, const char* device_name, 
			smksize_t quota)
{
    devid_t	devid;
    u_int	vol_cnt;
    rc_t 	rc;

    // print_config_info();

    if (init_device)
    {
    	cout << "Formatting and mounting device: " << device_name 
	     << " with " << quota << "KB quota ..." << endl;
    	W_DO(ssm->format_dev(device_name, quota, true));
    }

    // mount the new device
    W_DO(ssm->mount_dev(device_name, vol_cnt, devid));

    if (init_device)
    {
        // generate identification and create the new volume
        // on the device
        lvid_t lvid;
		cout << "Generating new lvid: " << endl;
        W_DO(ssm->generate_new_lvid(lvid));
		cout << "Generated lvid " <<lvid<<  endl;

        cout << "Creating a volume on the device with " << quota << "KB quota.VolID: " <<VolID<< endl;
		//W_DO(ssm->create_vol(device_name, lvid, quota, true, VolID));
        W_DO(ssm->create_vol(device_name, lvid, quota, false, VolID));
		cout << "    with local handle(phys volid) " << VolID << endl;
		// no more used in ShoreMT    
		// create the logical ID index on the volume, reserving no IDs
        //W_DO(ssm->add_logical_id_index( lvid , 0 , 0 ));
    }

    W_DO(find_volume_id(device_name, VolID));

    return RCOK;
}

void
usage(option_group_t& options)
{
    cerr << "Usage: server [-h] [-i] -s s|r|h -l r|f [options]" << endl;
    cerr << "       -i initialize device/volume" << endl;
    cerr << "       -a <filename> append to filename (goes wice qith -i)" << endl;
    cerr << "       -u <filename> use (read) previously created filename" << endl;
    cerr << "       -M <mode> measure hardware events (mode = U|S|A U=user S=system A=all" << endl;
    cerr << "       -s s (scan all fields), -s <fno> (scan field fno)"  << endl;
    cerr << "       -j <left_field>x<right_field> " << endl;
    cerr << "       -l lock granularity r(record) or f(ile)" << endl;
    cerr << "       -q <fno> -H <high_val> -L <low_val> (range query)" << endl;
    cerr << "       -T <query_no> run a TPC-H query (currently query_no = 1|6|12|14" << endl;
    cerr << "Valid options are: " << endl;
    options.print_usage(true, cerr);
}

/* create an smthread based class for all sm-related work */
class smthread_user_t : public smthread_t {
	int		argc;
	char	**argv;
public:
	int	retval;

	smthread_user_t(int ac, char **av) 
		: smthread_t(t_regular, "smthread_user_t"),
		argc(ac), argv(av), retval(0) { }
	~smthread_user_t()  {}
	void run();
};


void smthread_user_t::run()
{
    rc_t rc;




    // pointers to options we will create for the grid server program
    option_t* opt_device_name = 0;
    option_t* opt_device_quota = 0;
    option_t* opt_page_layout = 0;

    const int option_level_cnt = 3; 
    option_group_t options(option_level_cnt);

    W_COERCE(options.add_option("device_name", "device/file name",
			 NULL, "device containg volume holding file to scan",
			 true, option_t::set_value_charstr,
			 opt_device_name));

    W_COERCE(options.add_option("device_quota", "# > 1000",
			 "2000", "quota for device",
			 false, option_t::set_value_long,
			 opt_device_quota));

    W_COERCE(options.add_option("page_layout", "# > 0",
			 NULL, "page layout",
			 true, option_t::set_value_long,
			 opt_page_layout));

   //  have the SSM add its options to the group
     W_COERCE(ss_m::setup_options(&options));

	rc = init_config_options(options, "server", argc, argv);
    if (rc.is_error()) {
	usage(options);
	retval = 1;
	return;
    }



    // process command line: looking for the "-i" flag
    bool init_device = false;
    bool measure = false;
    MeasureMode measure_mode = NO_MEASURE;
    int option;
    char* scan_type = 0;
    char* tpch_type = 0;
    char* join_args = 0;
    char* load_table_name[MAX_LD_TABLES];
    char* use_table_name[MAX_USE_TABLES];
    flid_t scan_fields[MAX_FIELDS];
    qual_field_t qual_fields[MAX_FIELDS];
    int fields_to_qual = 0;

    flid_t q_l=INV_FIELD, q_r=INV_FIELD;
    const char* lock_gran = "f";  // lock granularity (file by default)

    int i;
    flid_t f;
    QueryType query_type=NO_QUERY;

    for (i=0; i<MAX_LD_TABLES; ++i)
    	load_table_name[i]=0;
    int tables_to_load=0;

    for (i=0; i<MAX_USE_TABLES; ++i)
    	use_table_name[i]=0;
    int tables_to_use=0;

    for (i=0; i<MAX_FIELDS; ++i)
	scan_fields[i]=INV_FIELD;
    int fields_to_scan=0;

    while ((option = getopt(argc, argv, "a:hij:s:l:q:u:H:L:M:T:")) != -1) {
	switch (option) {
	case 'h' :
	    	usage(options);
	    	break;

	case 'M':
		measure=true;
		switch (optarg[0]) {
		   case 'U':
		   case 'u':
			measure_mode=SM_USER;
			break;
		   case 'S':
		   case 's':
			measure_mode=SM_SYSTEM;
			break;
		   case 'A':
		   case 'a':
			measure_mode=SM_ALL;
			break;
		   default:
	    		usage(options);
	    		retval = 1;
	    		return;
	    		break;
		}
		break;

	case 'i' :	// Initialize device and volume
	{
	    if (init_device) {
		cerr << "ERROR: only one -i parameter allowed" << endl;
		usage(options);
		retval = 1;
		return;
	    }
	    init_device = true;
	    break;
	}
	case 'a' :	// Tables to load
	{
	    if (tables_to_load+1 > MAX_LD_TABLES) {
	        cerr << "ERROR: Only " << MAX_LD_TABLES 
	        	 << " tables to load at one time" << endl;
	        usage(options);
	        retval = 1;
	        return;
	    }
	    load_table_name[tables_to_load] = optarg;
	    cout << "Loading table " << optarg << ".\n";
	    tables_to_load++;
	    break;
	}
	case 'u' :	// Table(s) to use for this query (already loaded)
	{
	    if (tables_to_use+1 > MAX_USE_TABLES) {
	        cerr << "ERROR: Only " << MAX_USE_TABLES
	        	 << " tables to load at one time" << endl;
	       	usage(options);
	        retval = 1;
	        return;
	    }
	    use_table_name[tables_to_use] = optarg;
	    if (measure==false)
	        cout << "Using table " << optarg << ".\n";
	    tables_to_use++;
	    break;
	}
	case 's':
	{
	    scan_type = optarg;
	    if (query_type!=NO_QUERY && query_type!=RANGE_SCAN)
	    {
	       cerr << "ERROR: -s option goes with a RANGE_SCAN query\n";
	       retval = 1;
	       return;
	    }
	    query_type=RANGE_SCAN;
	    if (isdigit(scan_type[0]))  // Scan the specific field
	    {
	        scan_fields[fields_to_scan] = atoi(scan_type);
		if (measure==false)
	            cout << "Selecting field " << scan_fields[fields_to_scan] << ".\n";
	        fields_to_scan++;
	    }
	    else if (scan_type[0]!='s'){
	        cerr << "ERROR: Invalid scan type option.\n" ;
	        retval = 1;
	        return;
	    }
	    else
	       if (measure==false)
	           cout << "Selecting all fields.\n";
	    break;
	}
	case 'j':
	{
	    scan_type = optarg;
	    if (query_type!=NO_QUERY)
	    {
	       cerr << "ERROR: You can only specify one type of query\n";
	       retval = 1;
	       return;
	    }
	    query_type=AHHJOIN;
	    join_args = scan_type;
	    if (isdigit(join_args[0]))
	    {
	      q_l = atoi(join_args);
	      while (isdigit(*join_args)) join_args++;
	      if (join_args[0]=='x' && isdigit(join_args[1]))
	          q_r = atoi(join_args+1);
	      else {
		cerr << "ERROR: Join option should be <left_fno>x<right_fno>\n";
		 retval = 1;
		 return;
	      }
	      if (measure==false)
	          cout << "Joining fields " << q_l << " and " << q_r << ".\n";
	    }
	    else {
		cerr << "ERROR: Join option should be <left_fno>x<right_fno>\n";
		retval = 1;
		return;
	    }
	    break;
	}
	case 'q':
	{
	    if (query_type!=NO_QUERY && query_type!=RANGE_SCAN)
	    {
	       cerr << "ERROR: -q option goes with a RANGE_SCAN query\n";
	       retval = 1;
	       return;
	    }
	    query_type=RANGE_SCAN;
	    // pattern is -q <fno> -H <hi> -L <lo>
	    if (isdigit(optarg[0])) {
		qual_fields[fields_to_qual].fno = atoi(optarg);
		fields_to_qual++;
	    }
	    else {
	       cerr << "ERROR: -q needs a field number.\n" ;
	       retval = 1;
	       return;
	    }
	    break;
	}
	case 'T':
	{
	    if (query_type!=NO_QUERY)
	    {
	       cerr << "ERROR: You must specify ONE query type\n";
	       retval = 1;
	       return;
	    }
	    query_type=TPCH;
	    tpch_type = optarg;
	    if (!isdigit(tpch_type[0])) {
		cerr << "ERROR: Invalid query " << tpch_type  << endl;
		retval = 1;
		return;
	    }
	    if (measure==false)
	        cout << "Running TPC-H query " << tpch_type << ".\n";
	}
	break;
	case 'H':
	    if (query_type!=NO_QUERY && query_type!=RANGE_SCAN)
	    {
	       cerr << "ERROR: -H option goes with a RANGE_SCAN query\n";
	       retval = 1;
	       return;
	    }
	    // high for range
	    qual_fields[fields_to_qual-1].high = optarg;
	    if (measure==false)
	        cout << "Qual: Field " << qual_fields[fields_to_qual-1].fno << 
	    	" will be <= " << qual_fields[fields_to_qual-1].high << ".\n";
	    break;
	case 'L':
	    if (query_type!=NO_QUERY && query_type!=RANGE_SCAN)
	    {
	       cerr << "ERROR: -L option goes with a RANGE_SCAN query\n";
	       retval = 1;
	       return;
	    }
	    // low for range
	    qual_fields[fields_to_qual-1].low = optarg;
	    if (measure==false)
	        cout << "Qual: Field " << qual_fields[fields_to_qual-1].fno << 
	    	" will be >= " << qual_fields[fields_to_qual-1].low << ".\n";
	    break;
	case 'l':
	    lock_gran = optarg;
	    if (lock_gran[0] != 'r' &&
		lock_gran[0] != 'f' ) {
		cerr << "ERROR: lock granularity option (-l) must r or f.\n";
		retval = 1;
		return;
	    }
	    if (measure==false)
	        cout << "Chosen lock granularity " << lock_gran << ".\n";
	    break;
	default:
	    usage(options);
	    retval = 1;
	    return;
	    break;
	}
    }

    if (query_type==RANGE_SCAN && fields_to_qual>0)
       for (f=0; f<fields_to_qual; ++f)
          if (qual_fields[f].high==NULL || qual_fields[f].low==NULL)
	  {
	     cerr << "ERROR: Field " << qual_fields[f].fno << 
	     	" cannot be between null values.\n";
	     retval = 1;
	     return;
	  }
    if (scan_type!=NULL || (tpch_type!=NULL && !isdigit(tpch_type[0]))) {
	for (i=0; i<tables_to_load; ++i)
		use_table_name[tables_to_use++] = load_table_name[i];
    	if (tables_to_use == 0)
	{
	    cerr << "Please specify table(s) to use for the query." << endl;
	    retval = 1;
	    return;
	}
    }





    // Start SSM and perform recovery
    ssm = new ss_m();
    if (!ssm) {
	cerr << "Error: Out of memory for ss_m" << endl;
	retval = 1;
	return;
    }

	cout << "Getting SSM config info for record size ..." << endl;

	//sm_config_info_t config_info;
	//W_COERCE(ss_m::config_info(config_info));
	//_rec_size = config_info.max_small_rec; // minus a header


    PageLayout page_layout = (PageLayout) atoi(opt_page_layout->value());
    w_assert3(page_layout >= 0 && page_layout < NO_LAYOUT);

    /*
     * Off to work. First see if init is needed
     */


    string filename = "timemeasure.P";
    filename.append(opt_page_layout->value());
    if (tpch_type!=NULL && tpch_type!=0)
    {
    	filename.append(".T");
    	filename.append(tpch_type);
    }
    else if (tables_to_load!=0)
    {
    	filename.append(".L");
    	filename.append(load_table_name[(tables_to_load-1)]);
    }
    filename.append(".csv");
    //"timemeasure."<<page_layout<<"."<<query_type<<".csv"
	TimeMeasure timer(filename);
	timer.startTimer();

    W_COERCE(init_device_and_volume (init_device, opt_device_name->value(), 
				     strtol(opt_device_quota->value(), 0, 0)));

    /*
     * Then load what's there to load
     */

    for (i=0; i < tables_to_load; i++)
    {
    	load_file_q file_ldq(load_table_name[i], page_layout);
		rc = file_ldq.run();
        
		if (rc.is_error()) 
		{
				cerr << "Could not set up device/volume due to: " << endl;
				cerr << rc << endl;
				delete ssm;
				rc = RCOK;   // force deletion of w_error_t info hanging off rc
							 // otherwise a leak for w_error_t will be reported
				retval = 1;
			if(rc.is_error()) 
				W_COERCE(rc); // avoid error not checked.
			return;
		}
    }


    // Set locking option
    ss_m::concurrency_t cc = ss_m::t_cc_file;
    if (lock_gran[0] == 'r') {
    	cc = ss_m::t_cc_record;
    }

    W_COERCE(ssm->begin_xct());

    // Then ask questions
    switch(query_type)
    {
	case RANGE_SCAN:
	{
	    if (fields_to_scan==0 && fields_to_qual==0) // plain select*
	    {
	        select_star_q sstar(use_table_name[0]);
		RUN_QUERY(sstar);
	    }
	    else
	    {
	        range_select_q rsel(use_table_name[0], 
		  			 fields_to_scan, scan_fields, 
		   			fields_to_qual, qual_fields);
		RUN_QUERY(rsel);
	    }
	    break;
	}
	case AHHJOIN:
	{
	    flid_t fl[3];
	    fl[0] = 0; fl[1] = 1; fl[2]=2;
	    flid_t fr[4];
	    fr[0] = 0; fr[1] = 1; fr[2]=2; fr[3]=3;
	    ahhjoin_q ahhj(use_table_name, q_l, q_r, 3, 4, fl, fr, dummy_pred, dummy_pred);
	    RUN_QUERY(ahhj);
	    break;
	}
	case TPCH:
	{
	  switch (atoi(tpch_type)) {
	  case 1:
	  {
	    char *t[1];
	    char buf0[MAX_TABLE_NAME_SIZE];
	    sprintf(buf0, "lineitem");
	    t[0]=buf0;
	    tpch_query_1 tpch_q1(t);
	    RUN_QUERY(tpch_q1);
	    break;
	  }
	  case 101:
	  {
	  	char *t[1];
		char buf0[MAX_TABLE_NAME_SIZE];
		sprintf(buf0, "lineitem2");
		t[0] = buf0;
		tpch_query_101 tpch_q101(t);
		RUN_QUERY(tpch_q101);
		break;
	  }
	  case 6:
	  {
	    char *t[1];
	    char buf0[MAX_TABLE_NAME_SIZE];
	    sprintf(buf0, "lineitem");
	    t[0]=buf0;
	    tpch_query_6 tpch_q6(t);
	    RUN_QUERY(tpch_q6);
	    break;
	  }
	  case 12:
	  {
	    char *t[2];
	    char buf0[MAX_TABLE_NAME_SIZE];
	    sprintf(buf0, "lineitem");
	    t[0]=buf0;
	    char buf1[MAX_TABLE_NAME_SIZE];
	    sprintf(buf1, "orders");
	    t[1]=buf1;
	    tpch_query_12 tpch_q12(t);
	    RUN_QUERY(tpch_q12);
	    break;
	  }
	  case 14:
	  {
	    char *t[2];
	    char buf0[MAX_TABLE_NAME_SIZE];
	    sprintf(buf0, "part");
	    t[0]=buf0;
	    char buf1[MAX_TABLE_NAME_SIZE];
	    sprintf(buf1, "lineitem");
	    t[1]=buf1;
	    tpch_query_14 tpch_q14(t);
	    RUN_QUERY(tpch_q14);
	    break;
	  }
	  default:
	    cerr << "Unimplemented TPC-H Query" << tpch_type << ".\n";
	    retval=1;
	    return;
          }
        }
        default:
	  // just leave it be
	  break;
    }

	timer.stopTimer();
	timer.printData();

/*	sm_stats_info_t       stats;
	W_COERCE(ss_m::gather_stats(stats));
	cout << " SM Statistics : " << endl
		 << stats  << endl;*/

    W_COERCE(ssm->commit_xct());

    delete ssm;

    if (measure==false)
        cout << "Done." << endl;

    return;
}

int
main(int argc, char* argv[])
{
	smthread_user_t *smtu = new smthread_user_t(argc, argv);
	if (!smtu)
		W_FATAL(fcOUTOFMEMORY);

	w_rc_t e = smtu->fork();
	if(e.is_error()) {
	    cerr << "error forking thread: " << e <<endl;
	    return 1;
	}
	e = smtu->join();
	if(e.is_error()) {
	    cerr << "error forking thread: " << e <<endl;
	    return 1;
	}

	int	rv = smtu->retval;
	delete smtu;

	return rv;
}
