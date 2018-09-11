#include "sm_query.h"

#define ASSERT(x)  w_assert1(x)
 

#if defined(LIB_PIC)

#include "libpic.h"

struct ss { char s0[20]; char s1[20]; };
static ss event_couples[]
= {{"Cycle_cnt", "Instr_cnt"},
   {"Dispatch0_IC_miss", "Dispatch0_mispred"},
   {"Dispatch0_storeBuf", "Dispatch0_FP_use"},
   {"IC_ref", "IC_hit"},
   {"DC_rd", "DC_rd_hit"},
   {"DC_wr", "DC_wr_hit"},
   {"Load_use", "Load_use_RAW"},
   {"EC_ref", "EC_hit"},
   {"EC_write_hit_RDO", "EC_wb"},
   {"EC_snoop_inv", "EC_snoop_cb"},
   {"EC_rd_hit", "EC_ic_hit"}};

static int no_event_couples = 11;
static double res[2];

#else
#if defined(PCL)

#ifdef Alpha
#define ALPHA_DEC
#endif // Alpha
#include "pcl.h"
#define PCL_NO_EVENTS   1
#define PCL_mode	PCL_MODE_USER
static int retval;
static int PCL_mode;

static int PCL_event_list[PCL_NO_EVENTS];
static PCL_CNT_TYPE PCL_i_res[PCL_NO_EVENTS];
static PCL_FP_CNT_TYPE PCL_fp_res[PCL_NO_EVENTS];
 
#define PCL_PRINT_EVENTS { \
        int i; \
        for (i=0; i<PCL_NO_EVENTS; ++i) \
          if (PCL_event_list[i]<PCL_MFLOPS) \
            fprintf(stdout, "%s\t%13.0f\n", \
                PCLeventname(PCL_event_list[i]), (double)(PCL_i_res[i])); \
          else \
            fprintf(stdout, "%s\t%13f\n", \
                PCLeventname(PCL_event_list[i]), PCL_fp_res[i]); \
          fflush(stdout); }
 
#define PCL_PRINT_EVENTS_PER_RECORD { \
        int i; \
        for (i=0; i<PCL_NO_EVENTS; ++i) \
            fprintf(stdout, "%s\t%.3f\n", \
                PCLeventname(PCL_event_list[i]), PCL_i_res[i]/no_records); \
            fflush(stdout); }
 
#define PCL_QUERY \
        (retval=PCLquery(PCL_event_list, PCL_NO_EVENTS, PCL_mode))
 
#define PCL_DO_QUERY  { \
        if (PCL_QUERY != PCL_SUCCESS) \
            fprintf(stderr, \
                "PCLquery returned %d: Requested events not possible\n", \
                                                        retval); }
 
#define PCL_START (retval=PCLstart(PCL_event_list, PCL_NO_EVENTS, PCL_mode))                                                                         
#define PCL_DO_START { \
        if (PCL_START != PCL_SUCCESS) \
            fprintf(stderr, "PCLstart returned %d: Something went wrong\n", \
                                                        retval); }
 
#define PCL_STOP (retval=PCLstop(&PCL_i_res[0], &PCL_fp_res[0], PCL_NO_EVENTS))
#define PCL_DO_STOP \
        if (PCL_STOP != PCL_SUCCESS) \
            fprintf(stderr, "PCLstop returned %d: Unable to stop counters\n", \
                                                        retval);
 
#define PCL_STOP_AND_PRINT { PCL_DO_STOP else  PCL_PRINT_EVENTS; }
 
#define PCL_READ (retval=PCLread(PCL_i_res, PCL_fp_res, PCL_NO_EVENTS))
 
#define PCL_DO_READ \
        if (PCL_READ != PCL_SUCCESS) \
            fprintf(stderr,  \
                "PCLread returned %d: Problem in reading counters\n", retval);
#define PCL_READ_AND_PRINT { PCL_DO_READ else  PCL_PRINT_EVENTS; }
 
#define DETERMINE_MODE  { \
	    switch(measure_mode) { \
			case SM_USER: PCL_mode=PCL_MODE_USER; break; \
			case SM_SYSTEM: PCL_mode=PCL_MODE_SYSTEM; break; \
			default: PCL_mode=PCL_MODE_USER_SYSTEM; break; } }
				
#define INIT_MEASURE    DETERMINE_MODE; ASSERT(PCLinit() == PCL_SUCCESS)
#define QUERY_EVENTS    ASSERT(PCL_QUERY == PCL_SUCCESS)
#define START_COUNTERS  ASSERT(PCL_START==PCL_SUCCESS)
#define STOP_COUNTERS   ASSERT(PCL_STOP==PCL_SUCCESS)
#define READ_COUNTERS   ASSERT(PCL_READ==PCL_SUCCESS)
#define PRINT_RESULT    PCL_PRINT_EVENTS;
#define END_MEASURE
 
#define MEASURE_ONE_TEST(function_call) { \
                } \
            } \
        }

#else
#if defined(PAPI)
#include "papi.h"

static int retval;
static int PAPI_mode;

static int ecouples[][2]
= {{PAPI_L1_DCM, PAPI_L1_ICM},
   {PAPI_L1_TCM, PAPI_L2_TCM},
   {PAPI_L1_LDM, PAPI_L1_STM},
   {PAPI_L1_DCA, PAPI_L1_ICA},
   {PAPI_L2_DCR, PAPI_L2_DCW},
   {PAPI_L2_ICA, PAPI_L1_ICR},
   {PAPI_L2_ICR, PAPI_L1_ICW},
   {PAPI_L2_TCA, PAPI_L2_TCW},
   {PAPI_BTAC_M, PAPI_BR_INS},
   {PAPI_BR_MSP, PAPI_BR_TKN},
   {PAPI_RES_STL, PAPI_RAT_STL},
   {PAPI_ILD_STL, PAPI_TLB_IM},
   {PAPI_BUS_IFE, PAPI_BUS_BRD},
   {PAPI_BUS_DRDY, PAPI_BUS_ROUT},
   {PAPI_BR_DEC, PAPI_DCU_MOUT},
   {PAPI_L2_IFE, PAPI_L2_IFM},
   {PAPI_L2_DRD, PAPI_L2_DRM},
   {PAPI_L2_DWR, PAPI_L2_WRM},
   {PAPI_IFU_STL, PAPI_UOP_RET},
   {PAPI_TOT_CYC, PAPI_TOT_INS},
   {PAPI_TOT_IIS, PAPI_LST_INS},
};

static int no_ecouples = 21;

static int PAPI_eset;
static const PAPI_preset_info_t *PAPI_einfo;
static long long int res[2];
static PAPI_option_t options;
static int no_events;


#define DETERMINE_MODE  { \
		switch(measure_mode) { \
			case SM_USER: PAPI_mode=PAPI_DOM_USER; break; \
			case SM_SYSTEM: PAPI_mode=PAPI_DOM_KERNEL; break; \
			default: PAPI_mode=PAPI_DOM_ALL; break; } }
				
#define PAPI_INIT (retval = PAPI_library_init(PAPI_VER_CURRENT))
#define PAPI_DEBUG (retval = PAPI_set_debug(PAPI_VERB_ECONT))
#define PAPI_DOMAIN (retval = PAPI_set_domain(PAPI_mode))
#define PAPI_CREATE_ESET (retval = PAPI_create_eventset(&PAPI_eset))
#define PAPI_QUERY_EVENT(x) (retval = PAPI_query_event(ecouples[i][x]))
#define PAPI_ADD_EVENTS \
                (retval = PAPI_add_events(&PAPI_eset, ecouples[i], no_events))
#define PAPI_CLEANUP_EVENTSET (retval = PAPI_cleanup_eventset(&PAPI_eset))
#define PAPI_START (retval = PAPI_start(PAPI_eset))
#define PAPI_STOP (retval = PAPI_stop(PAPI_eset, res))
#define PAPI_READ (retval = PAPI_read(PAPI_eset, res))
#define PAPI_DESTROY_EVENTSET (retval = PAPI_destroy_eventset(&PAPI_eset))

#define INIT_MEASURE \
         { DETERMINE_MODE; \
	   PAPI_einfo = NULL; \
           ASSERT((PAPI_einfo = PAPI_query_all_events_verbose()) !=NULL); \
           ASSERT(PAPI_INIT == PAPI_VER_CURRENT); \
           ASSERT(PAPI_DEBUG == PAPI_OK); }

#define QUERY_EVENTS \
         { no_events = 0; \
           if (ecouples[i][0]!=0) \
              { ASSERT(PAPI_QUERY_EVENT(0) == PAPI_OK); \
                no_events++; } \
           if (ecouples[i][1]!=0) \
              { ASSERT(PAPI_QUERY_EVENT(1) == PAPI_OK); \
                no_events++; } \
           ASSERT(PAPI_CREATE_ESET == PAPI_OK); \
           options.domain.eventset=PAPI_eset; \
           options.domain.domain=PAPI_DOM_USER; \
           ASSERT(PAPI_set_opt(PAPI_SET_DOMAIN, &options)==PAPI_OK); \
           ASSERT(PAPI_ADD_EVENTS==PAPI_OK) ; }

#define START_COUNTERS  ASSERT(PAPI_START == PAPI_OK)
#define STOP_COUNTERS \
         { ASSERT(PAPI_STOP == PAPI_OK) ; \
           ASSERT(PAPI_CLEANUP_EVENTSET == PAPI_OK); \
           ASSERT(PAPI_DESTROY_EVENTSET==PAPI_OK); }

#define READ_COUNTERS   ASSERT(PAPI_READ == PAPI_OK, "PAPI_read")

#define PRINT_RESULT \
           { fprintf(stderr, "%47s\t%15.0f\n", \
                (PAPI_einfo[ecouples[i][0]].event_descr ?  \
                 PAPI_einfo[ecouples[i][0]].event_descr : ""), \
                (double) res[0]); \
             fprintf(stderr, "%47s\t%15.0f\n", \
                (PAPI_einfo[ecouples[i][1]].event_descr ?  \
                 PAPI_einfo[ecouples[i][1]].event_descr : ""), \
                (double) res[1]); }

#define END_MEASURE

#else
#define EXTERNAL_CONTROL
#endif // PAPI
#endif // PCL
#endif // LIB_PIC


#ifdef EXTERNAL_CONTROL
w_rc_t query_c::measure_run(MeasureMode )
#else
w_rc_t query_c::measure_run(MeasureMode measure_mode)
#endif
{
    // send query results to never never land
    w_assert1((output_file=fopen("/dev/null", "w"))!=NULL);
    // warmup
    W_DO(run());

#if defined(LIB_PIC)
 {
    int i;
    init_pic();
    for (i=0; i<no_event_couples; ++i)
    {
	switch(measure_mode) {
	    case SM_USER: 
		start_pic(USER, event_couples[i].s0, event_couples[i].s1);
		break;
	    case SM_SYSTEM:
		start_pic(SYSTEM, event_couples[i].s0, event_couples[i].s1);
		break;
	    default:
		start_pic(ALL, event_couples[i].s0, event_couples[i].s1);
		break;
	}
        W_DO(run());
	stop_pic(&res[0], &res[1]);
	fprintf(stderr, "%s\t%15.0f\n", event_couples[i].s0, res[0]);
	fprintf(stderr, "%s\t%15.0f\n", event_couples[i].s1, res[1]);
    }
 }
#else
#if defined(PCL)
  {
    for (PCL_event_list[0]=0;
         PCL_event_list[0] < PCL_MAX_EVENT;
	 ++PCL_event_list[0])
    {
    	if (PCL_QUERY == PCL_SUCCESS)
    	{
	    START_COUNTERS;
	    W_DO(run());
	    STOP_COUNTERS;
	    PRINT_RESULT;
	}
    }
  }
#else 
#if defined(PAPI)
  {
    INIT_MEASURE;
    for (int i=0; i<no_ecouples; ++i)
    {
        QUERY_EVENTS;
        START_COUNTERS;
        W_DO(run());
        STOP_COUNTERS;
        PRINT_RESULT;
    }
    END_MEASURE;
  }
#else // external control
  {
// just run it more times for external control
	    W_DO(run());
	    W_DO(run());
  }

#endif // PAPI
#endif // PCL
#endif // LIB_PIC
    return RCOK;
}

