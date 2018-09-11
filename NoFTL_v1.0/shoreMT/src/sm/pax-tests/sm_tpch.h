/*
 * TPC-H Query class implementation
 */

#ifndef SM_TPCH_H
#define SM_TPCH_H

#include "sm_query.h"

#define MAX_QUAL_RIDS (1024*1024)

class tpch_query_1 :public query_c {
  private:
    double sum_qty[4],
    	   sum_base_price[4],
	   sum_disc_price[4],
	   sum_charge[4],
	   sum_disc[4],
	   count_order[4];
    enum rf_ls {AF=0, NF=1, NO=2, RF=3};

  public:
    tpch_query_1(char** table_name)
    	:query_c(1, table_name)
    {
        int i;
	for (i=0; i<4; ++i) {
	   sum_qty[i]=0;
	   sum_base_price[i]=0;
	   sum_disc_price[i]=0;
	   sum_charge[i]=0;
	   sum_disc[i]=0;
	   count_order[i]=0;
	}
    }
    ~tpch_query_1() {}

    w_rc_t run();

    w_rc_t run_nsm();
    w_rc_t run_pax();
    w_rc_t run_hpl();
    w_rc_t run_dsm();
};

class tpch_query_101 :public query_c {
  private:
    double sum_qty[4],
    	   sum_base_price[4],
	   sum_disc_price[4],
	   sum_charge[4],
	   sum_disc[4],
	   count_order[4];
    enum rf_ls {AF=0, NF=1, NO=2, RF=3};

  public:
    tpch_query_101(char** table_name)
    	:query_c(1, table_name)
    {
        int i;
	for (i=0; i<4; ++i) {
	   sum_qty[i]=0;
	   sum_base_price[i]=0;
	   sum_disc_price[i]=0;
	   sum_charge[i]=0;
	   sum_disc[i]=0;
	   count_order[i]=0;
	}
    }
    ~tpch_query_101() {}

    w_rc_t run();

    w_rc_t run_nsm();
    w_rc_t run_pax();
    w_rc_t run_hpl();
   // w_rc_t run_dsm();
};


class tpch_query_6 :public query_c {
  private:
    double sum;
  public:
    tpch_query_6(char** table_name)
    	:query_c(1, table_name)
	{ sum=0; }
    ~tpch_query_6() {}

    w_rc_t run();

    w_rc_t run_nsm();
    w_rc_t run_pax();
    w_rc_t run_hpl();
    w_rc_t run_dsm();
};


class tpch_query_12 :public query_c {
  public:
    uint4_t r_high, r_low, s_high, s_low;
    tpch_query_12(char** table_name)
    	:query_c(1, table_name)
	{r_high = r_low = s_high = s_low = 0;}
    ~tpch_query_12() {}

    w_rc_t run();
};

class tpch_query_14 :public query_c {
  public:
    double cond_sum, sum;
    tpch_query_14(char** table_name)
    	:query_c(1, table_name)
	{cond_sum = sum = 0;}
    ~tpch_query_14() {}

    w_rc_t run();
};

// tables field information

// nation
#define N_NATIONKEY		0
#define N_NAME			1
#define N_REGIONKEY		2
#define N_COMMENT		3

// region
#define R_REGIONKEY		0
#define R_NAME			1
#define R_COMMENT		2

// part - put all variable fields at the end
#define P_RETAILPRICE	0
#define P_PARTKEY		1
#define P_SIZE			2
#define P_MFGR			3
#define P_BRAND			4
#define P_CONTAINER		5
#define P_COMMENT		6
#define P_TYPE			7
#define P_NAME			8



// supplier
#define S_SUPPKEY		0
#define S_NAME			1
#define S_ADDRESS		2
#define S_NATIONKEY		3
#define S_PHONE			4
#define S_ACCTBAL		5
#define S_COMMENT		6

// partsupp
#define PS_PARTKEY		0
#define PS_SUPPKEY		1
#define PS_AVAILQTY		2
#define PS_SUPPLYCOST	3
#define PS_COMMENT		4

// customer
#define C_CUSTKEY		0
#define C_NAME			1
#define C_ADDRESS		2
#define C_NATIONKEY		3
#define C_PHONE			4
#define C_ACCTBAL		5
#define C_MKTSEGMENT	6
#define C_COMMENT		7

// orders
#define O_ORDERKEY		0
#define O_CUSTKEY		1
#define O_ORDERSTATUS	2
#define O_TOTALPRICE	3
#define O_ORDERDATE		4
#define O_SHIPPRIORITY	5
#define O_ORDERPRIORITY	6
#define O_CLERK			7
#define O_COMMENT		8

// change by hpl
// lineitem columns
#define L_ORDERKEY		0
#define L_PARTKEY		1
#define L_SUPPKEY		2
#define L_LINENUMBER	3
#define L_QUANTITY		4
#define L_EXTENDEDPRICE	5
#define L_DISCOUNT		6
#define L_TAX			7
#define L_RETURNFLAG	8
#define L_LINESTATUS	9
#define L_SHIPDATE		10
#define L_COMMITDATE	11
#define L_RECEIPTDATE	12
#define L_SHIPINSTRUCT	13
#define L_SHIPMODE		14
#define L_COMMENT		15


// lineitem2 columns
#define L2_QUANTITY		0
#define L2_EXTENDEDPRICE	1
#define L2_DISCOUNT		2
#define L2_TAX			3
#define L2_RETURNFLAG	4
#define L2_LINESTATUS	5
#define L2_SHIPDATE		6

#endif
