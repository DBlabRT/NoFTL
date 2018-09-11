#ifndef FILE_SCAN_H
#define FILE_SCAN_H

//#include "sm_vas.h"
#include "query_util.h"

extern ss_m* ssm;
extern vid_t VolID;

typedef enum {
	NO_QUERY, RANGE_SCAN, AHHJOIN, TPCH
	} QueryType;

#define PRINT_PROGRESS_MESSAGE(num_rec) 		\
        if (num_rec%100000==0) { 			\
                        cerr << num_rec << " ... ";     \
			PRINT_DATE;			\
			cerr << endl;			\
	}

#if defined(__GNUG__) && __GNUC_MINOR__ < 6 && defined(Sparc)
    extern "C" int getopt(int argc, char** argv, char* optstring);
#endif

#if defined(__GNUG__) && defined(Sparc)
    extern char *optarg;
    extern int optind, opterr;
#endif

// this is implemented in options.cpp
extern w_rc_t init_config_options(option_group_t& options,
                        const char* prog_type,
                        int& argc, char** argv);

extern rc_t find_vol_assoc(const char* file_key, file_info_t& finfo);
extern rc_t find_field_vol_assoc(const char* file_key, u_int fno, file_info_t& finfo_i);

/******************  NSM functions  *******************************/
extern rc_t NSM_load_file (const char* table_name, file_info_t& finfo);
extern void NSM_select_star(const char* table_name, ss_m::concurrency_t cc);
extern void NSM_select_field_with_pred (const char* table_name, 
                             		ss_m::concurrency_t cc,
                             		int s_field,
                             		int q_field,
                             		int q_hi,
                             		int q_lo);
extern void NSM_select_field_with_no_proj (const char* table_name,
                              		ss_m::concurrency_t cc,
                              		int q_hi,
                              		int q_lo,
                              		int no_proj);
extern void NSM_select_field_with_no_pred (const char* table_name,
					ss_m::concurrency_t cc,
                                   	int q_hi,
                                   	int q_lo,
                                   	int no_pred);

/******************  DSM functions  *******************************/
extern rc_t DSM_load_file (const char* table_name, file_info_t& finfo);
extern void DSM_select_star(const char* table_name, ss_m::concurrency_t cc);
extern void DSM_select_field_with_pred (const char* table_name, 
                                 	ss_m::concurrency_t cc,
                                 	int s_field,
                                 	int q_field,
                                 	int q_hi,
                                 	int q_lo);
extern void DSM_select_field_with_no_proj (const char* table_name,
                              		ss_m::concurrency_t cc,
                              		int q_hi,
                              		int q_lo,
                              		int no_proj);
extern void DSM_select_field_with_no_pred (const char* table_name,
                                   	ss_m::concurrency_t cc,
                                   	int q_hi,
                                   	int q_lo,
                                   	int no_pred);

/******************  PAX functions  *******************************/
extern rc_t PAX_load_file (const char* table_name, file_info_t& finfo);
extern void PAX_select_star(const char* table_name, ss_m::concurrency_t cc);
extern void PAX_scan_f(const char* table_name, 
			const flid_t fno, 
			ss_m::concurrency_t cc);
extern void PAX_select_field_with_pred (const char* table_name,
                             		ss_m::concurrency_t cc,
                             		int s_field,
                             		int q_field,
                             		int q_hi,
                             		int q_lo);
extern void PAX_select_field_with_no_proj (const char* table_name,
                              		ss_m::concurrency_t cc,
                              		int q_hi,
                              		int q_lo,
                              		int no_proj);
extern void PAX_select_field_with_no_pred (const char* table_name,
                                   	ss_m::concurrency_t cc,
                                   	int q_hi,
                                   	int q_lo,
                                   	int no_pred);

/******************  HPL functions  *******************************/
/*extern rc_t PAX_load_file (const char* table_name, file_info_t& finfo);
extern void PAX_select_star(const char* table_name, ss_m::concurrency_t cc);
extern void PAX_scan_f(const char* table_name,
			const flid_t fno,
			ss_m::concurrency_t cc);
extern void PAX_select_field_with_pred (const char* table_name,
                             		ss_m::concurrency_t cc,
                             		int s_field,
                             		int q_field,
                             		int q_hi,
                             		int q_lo);
extern void PAX_select_field_with_no_proj (const char* table_name,
                              		ss_m::concurrency_t cc,
                              		int q_hi,
                              		int q_lo,
                              		int no_proj);
extern void PAX_select_field_with_no_pred (const char* table_name,
                                   	ss_m::concurrency_t cc,
                                   	int q_hi,
                                   	int q_lo,
                                   	int no_pred);
*/

/********************* Macros to functions ***************************/

#define DISTANCE(cmd, table_name, cc, distance) \
    { \
	cmd (table_name, cc, distance, 0,10000,0); \
	cmd (table_name, cc, distance, 0,20000,10000); \
	cmd (table_name, cc, distance, 0,15000,5000); \
	cmd (table_name, cc, distance, 0,12000,2000); \
	cmd (table_name, cc, distance, 0,17000,7000); \
    }

#define PROJECTIVITY(cmd, table_name, cc, no_proj) \
    { \
	cmd (table_name, cc, 10000,0, no_proj); \
	cmd (table_name, cc, 20000,10000, no_proj); \
	cmd (table_name, cc, 12000,2000, no_proj); \
	cmd (table_name, cc, 18000,8000, no_proj); \
	cmd (table_name, cc, 14000,4000, no_proj); \
    }

#define PREDICATES(cmd, table_name, cc, no_pred) \
    { \
	cmd (table_name, cc, 10000,0, no_pred); \
	cmd (table_name, cc, 20000,10000, no_pred); \
	cmd (table_name, cc, 12000,2000, no_pred); \
	cmd (table_name, cc, 18000,8000, no_pred); \
	cmd (table_name, cc, 14000,4000, no_pred); \
    }

#define SELECTIVITY(cmd, table_name, cc, selectivity) \
{ \
      switch (selectivity) { \
       case 1: \
       { \
          cmd (table_name,cc,4,0,10200,10000); \
          cmd (table_name,cc,4,0,2200,2000); \
          cmd (table_name,cc,4,0,14200,14000); \
          cmd (table_name,cc,4,0,200,0); \
          cmd (table_name,cc,4,0,18200,18000); \
          break; \
       } \
       case 5: \
       { \
          cmd (table_name,cc,4,0,2000,1000); \
          cmd (table_name,cc,4,0,16000,15000); \
          cmd (table_name,cc,4,0,8000,7000); \
          cmd (table_name,cc,4,0,18000,17000); \
          cmd (table_name,cc,4,0,4000,3000); \
          break; \
       } \
       case 10: \
       { \
          cmd (table_name,cc,4,0,12000,10000); \
          cmd (table_name,cc,4,0,4000,2000); \
          cmd (table_name,cc,4,0,18000,16000); \
          cmd (table_name,cc,4,0,8000,6000); \
          cmd (table_name,cc,4,0,20000,18000); \
          break; \
       } \
       case 20: \
       { \
          cmd (table_name,cc,4,0,4000,0); \
          cmd (table_name,cc,4,0,20000,16000); \
          cmd (table_name,cc,4,0,16000,12000); \
          cmd (table_name,cc,4,0,8000,4000); \
          cmd (table_name,cc,4,0,17000,13000); \
          break; \
       } \
       case 50: \
       { \
          cmd (table_name,cc,4,0,10000,0); \
          cmd (table_name,cc,4,0,20000,10000); \
          cmd (table_name,cc,4,0,12000,2000); \
          cmd (table_name,cc,4,0,18000,8000); \
          cmd (table_name,cc,4,0,14000,4000); \
          break; \
       } \
       case 100: \
       { \
          cmd (table_name,cc,4,0,20000,0); \
          cmd (table_name,cc,4,0,20000,0); \
          cmd (table_name,cc,4,0,20000,0); \
          cmd (table_name,cc,4,0,20000,0); \
          cmd (table_name,cc,4,0,20000,0); \
          break; \
       } \
       default: \
          cerr << "Error! Invalid selectivity."; \
          return; \
      } \
}
#endif // FILE_SCAN_H
