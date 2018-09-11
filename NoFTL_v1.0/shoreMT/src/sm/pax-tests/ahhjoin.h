/* 
 * Query class implementation
 */

#ifndef AHHJOIN_H
#define AHHJOIN_H

#include "sm_query.h"

#define NUM_BUCKET	192		// orig 3
#define MEM_QUOTA	4096	// orig 64 in MBytesb
#define HT_SIZE		2368	// orig 37

class hash_table_t;

extern bool dummy_pred(const void*, const file_info_t&);

class ahhjoin_q :public query_c {
  private:
    join_table_t *jt[2];

    uint4_t num_bucket;
    uint4_t mem_quota;
    uint4_t ht_size;

    double result_tuples;

    hash_table_t** hash_tables;

    query_c *caller;

    void (*ft_func)(void*, const void*, const void*);

    w_rc_t partition_left_nsm (stid_t* fids, uint4_t& b_valid);
    w_rc_t partition_right_nsm (stid_t* fids, const uint4_t b_valid);

    w_rc_t partition_left_pax (stid_t* fids, uint4_t& b_valid);
    w_rc_t partition_right_pax (stid_t* fids, const uint4_t b_valid);

    w_rc_t partition_left_hpl (stid_t* fids, uint4_t& b_valid);
    w_rc_t partition_right_hpl (stid_t* fids, const uint4_t b_valid);

    w_rc_t build_hash_table_nsm(stid_t& pfid, hash_table_t* htable);
    w_rc_t probe_hash_table_nsm(stid_t& pfid, hash_table_t* htable);

    w_rc_t build_hash_table_pax(stid_t& pfid, hash_table_t* htable);
    w_rc_t probe_hash_table_pax(stid_t& pfid, hash_table_t* htable);

    w_rc_t build_hash_table_hpl(stid_t& pfid, hash_table_t* htable);
    w_rc_t probe_hash_table_hpl(stid_t& pfid, hash_table_t* htable);

    uint4_t freeze_hash_table_nsm(hash_table_t*& htable, append_file_i& af);
    uint4_t freeze_hash_table_pax(hash_table_t*& htable, append_pfile_i& af);
    uint4_t freeze_hash_table_hpl(hash_table_t*& htable, append_hpl_p_i& af);



  public:
    ahhjoin_q(char** table_name,
    	      flid_t l_jcol, flid_t r_jcol,
              flid_t l_nif,  flid_t r_nif,
	      flid_t *l_if,  flid_t *r_if,
	      bool (*l_ipred)(const void*, const file_info_t&),
	      bool (*r_ipred)(const void*, const file_info_t&),
	      query_c* cl=NULL,
	      void (*ft)(void*, const void*, const void*)=NULL,
	      uint4_t nbucket=NUM_BUCKET,
	      uint4_t mquota=MEM_QUOTA,
	      uint4_t htsz=HT_SIZE);

    ~ahhjoin_q() {if (jt[L]!=NULL) delete jt[L]; if (jt[R]!=NULL) delete jt[R]; }

    w_rc_t run_nsm();
    w_rc_t run_pax();
    w_rc_t run_hpl();
    w_rc_t run_dsm();
};

#endif
