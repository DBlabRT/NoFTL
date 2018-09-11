/* 
 * Query class implementation
 */

#ifndef SM_QUERY_H
#define SM_QUERY_H

#include "w_defines.h"


#include <stdio.h>
#include "query_util.h"


#define MAX_OPEN_FILES 20

#define L 0
#define R 1


class query_c {
  protected:
    uint4_t scanned, qualified;
    char ** table_name;
    file_info_t finfo[MAX_OPEN_FILES];	// file decriptions for query
    ss_m::concurrency_t cc;
    // in case you want to do something with the results
    FILE* output_file;

  public:
    // constructor fills up the finfo structure(s)
    query_c(int no_tables,
    	    char** tbname,
	    ss_m::concurrency_t cc=ss_m::t_cc_file);

    query_c() {}
    virtual ~query_c() {}

    virtual w_rc_t run() {
	rc_t rc;

    	switch (finfo[0].page_layout) {
	    case NSM:
	    	W_DO(run_nsm());
		break;
	    case PAX:
	    	W_DO(run_pax());
		break;
	    case HPL:
	    	W_DO(run_hpl());
		break;
	    case DSM:
	    	W_DO(run_dsm());
		break;
	    default:
	    	cerr << "Invalid page layout." << endl;
		return RC_AUGMENT(rc);
		break;
	}
	return RCOK;
    }
    virtual w_rc_t run_nsm() {rc_t rc; return RC_AUGMENT(rc);}
    virtual w_rc_t run_pax() {rc_t rc; return RC_AUGMENT(rc);}
    virtual w_rc_t run_hpl() {rc_t rc; return RC_AUGMENT(rc);}
    virtual w_rc_t run_dsm() {rc_t rc; return RC_AUGMENT(rc);}

    virtual w_rc_t measure_run(MeasureMode measure_mode);
};

class load_file_q :public query_c {
  public:
    load_file_q(char* tbname, PageLayout pl);
    ~load_file_q() {}

    w_rc_t run_nsm();
    w_rc_t run_pax();
    w_rc_t run_hpl();
    w_rc_t run_dsm();
};

class select_star_q :public query_c {
  public:
    select_star_q(char* table_name)
    	:query_c(1, &table_name)
	{};

    ~select_star_q() {}

    w_rc_t run();

    w_rc_t run_nsm();
    w_rc_t run_pax();
    w_rc_t run_hpl();
    w_rc_t run_dsm();
};

class range_select_q :public query_c {
private:
	flid_t no_ifields;
	flid_t* ifields;
	flid_t no_qfields;
	qual_field_t* qfields;
public:
	range_select_q(char* table_name, flid_t nifi, flid_t* ifi, flid_t qifi,
			qual_field_t* qfi)
	:query_c(1, &table_name), no_ifields(nifi), no_qfields(qifi)
	{
		w_assert1(nifi>0 || qifi>0);
		if (nifi>0) ifields=ifi;
		else ifields=NULL;
		if (qifi>0)
		{
			qfields=qfi;
			flid_t f;
			for (f=0; f<no_qfields;++f) {
				switch(finfo[0].field_type[qfields[f].fno]) {
				case SM_INTEGER:
					qfields[f].low = (char*)new int(atoi(qfields[f].low));
					qfields[f].high = (char*)new int(atoi(qfields[f].high));
					break;
				case SM_FLOAT:
					qfields[f].low = (char*)new double(atof(qfields[f].low));
					qfields[f].high = (char*)new double(atof(qfields[f].high));
					break;
				default:
					break;
				}
			}
		}
		else qfields=NULL;
	}

	~range_select_q() {}

	w_rc_t run();

	w_rc_t run_nsm();
	w_rc_t run_pax();
	w_rc_t run_hpl();
	w_rc_t run_dsm();
};

class select_field_q :public query_c {
  private:
    flid_t	field_no;
  public:
    select_field_q(char* table_name, flid_t fno)
    	:query_c(1, &table_name), field_no(fno)
	{};

    ~select_field_q() {}

    w_rc_t run();

    w_rc_t run_nsm();
    w_rc_t run_pax();
    w_rc_t run_hpl();
    w_rc_t run_dsm();
};

#endif
