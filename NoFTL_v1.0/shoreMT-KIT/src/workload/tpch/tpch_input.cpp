/* -*- mode:C++; c-basic-offset:4 -*-
   Shore-kits -- Benchmark implementations for Shore-MT
   
   Copyright (c) 2007-2009
   Data Intensive Applications and Systems Labaratory (DIAS)
   Ecole Polytechnique Federale de Lausanne
   
   All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/** @file tpch_input.cpp
 *
 *  @brief Implementation of the (common) inputs for the TPC-H trxs
 *  @brief Generate inputs for the TPCH TRXs
 *
 *  @author Ippokratis Pandis (ipandis)
 */


#include "util.h"
#include "workload/tpch/tpch_random.h" 
#include "workload/tpch/tpch_input.h"

#include "workload/tpch/dbgen/dss.h"
#include "workload/tpch/dbgen/dsstypes.h"

using namespace dbgentpch; // for random MODES etc..

ENTER_NAMESPACE(tpch);


q2_input_t create_q2_input(const double /* sf */, const int /* specificWH */) { q2_input_t a; return a; }
q3_input_t create_q3_input(const double /* sf */, const int /* specificWH */) { q3_input_t a; return a; }
q5_input_t create_q5_input(const double /* sf */, const int /* specificWH */) { q5_input_t a; return a; }
q7_input_t create_q7_input(const double /* sf */, const int /* specificWH */) { q7_input_t a; return a; }
q8_input_t create_q8_input(const double /* sf */, const int /* specificWH */) { q8_input_t a; return a; }
q9_input_t create_q9_input(const double /* sf */, const int /* specificWH */) { q9_input_t a; return a; }
q10_input_t create_q10_input(const double /* sf */, const int /* specificWH */) { q10_input_t a; return a; }
q11_input_t create_q11_input(const double /* sf */, const int /* specificWH */) { q11_input_t a; return a; }
q15_input_t create_q15_input(const double /* sf */, const int /* specificWH */) { q15_input_t a; return a; }
q16_input_t create_q16_input(const double /* sf */, const int /* specificWH */) { q16_input_t a; return a; }
q17_input_t create_q17_input(const double /* sf */, const int /* specificWH */) { q17_input_t a; return a; }
q18_input_t create_q18_input(const double /* sf */, const int /* specificWH */) { q18_input_t a; return a; }
q19_input_t create_q19_input(const double /* sf */, const int /* specificWH */) { q19_input_t a; return a; }
q20_input_t create_q20_input(const double /* sf */, const int /* specificWH */) { q20_input_t a; return a; }
q21_input_t create_q21_input(const double /* sf */, const int /* specificWH */) { q21_input_t a; return a; }
q22_input_t create_q22_input(const double /* sf */, const int /* specificWH */) { q22_input_t a; return a; }




/******************************************************************** 
 *
 *  Q1
 *
 ********************************************************************/


q1_input_t& q1_input_t::operator=(const q1_input_t& rhs)
{
    l_shipdate = rhs.l_shipdate;
    return (*this);
};


q1_input_t  create_q1_input(const double /* sf */, 
                            const int /* specificWH */)
{
    q1_input_t q1_input;

    struct tm shipdate;

    shipdate.tm_year = 98;//since 1900
    shipdate.tm_mon = 11;//month starts from 0
    shipdate.tm_mday = 1-URand(60, 120);//day starts from 1
    shipdate.tm_hour = 0;
    shipdate.tm_min = 0;
    shipdate.tm_sec = 0;

    // The format: YYYY-MM-DD
    
    q1_input.l_shipdate = mktime(&shipdate);

    return (q1_input);
};



/******************************************************************** 
 *
 *  Q4
 *
 ********************************************************************/


q4_input_t& q4_input_t::operator=(const q4_input_t& rhs)
{
    o_orderdate = rhs.o_orderdate;
    return (*this);
};


q4_input_t  create_q4_input(const double /* sf */, 
                            const int /* specificWH */)
{
    q4_input_t q4_input;

    struct tm orderdate;

    int month=URand(0,57);
    int year=month/12;
    month=month%12;

    orderdate.tm_year = 93+year;
    orderdate.tm_mon = month;
    orderdate.tm_mday = 1;
    orderdate.tm_hour = 0;
    orderdate.tm_min = 0;
    orderdate.tm_sec = 0;

    // The format: YYYY-MM-DD
  
    q4_input.o_orderdate = mktime(&orderdate);

    return (q4_input);
};



/******************************************************************** 
 *
 *  Q6
 *
 ********************************************************************/


q6_input_t& q6_input_t::operator=(const q6_input_t& rhs)
{
    l_shipdate = rhs.l_shipdate;
    l_quantity = rhs.l_quantity;
    l_discount = rhs.l_discount;
    return (*this);
};


q6_input_t    create_q6_input(const double /* sf */, 
                              const int /* specificWH */)
{
    q6_input_t q6_input;

    struct tm shipdate;

    //random() % 5 + 1993;
    shipdate.tm_year = URand(93, 97);//year starts from 1900
    shipdate.tm_mon = 0;//month starts from 0
    shipdate.tm_mday = 1;
    shipdate.tm_hour = 0;
    shipdate.tm_min = 0;
    shipdate.tm_sec = 0;
    q6_input.l_shipdate = mktime(&shipdate);

    //random() % 2 + 24;
    q6_input.l_quantity = (double)URand(24, 25);

    //random() % 8 + 2
    q6_input.l_discount = ((double)(URand(2,9))) / (double)100.0;
	
    return (q6_input);
};



/******************************************************************** 
 *
 *  Q12
 *
 *  l_shipmode1:   Random within the list of MODES defined in 
 *                 Clause 5.2.2.13 (tpc-h-2.8.0 pp.91)
 *  l_shipmode2:   Random within the list of MODES defined in
 *                 Clause 5.2.2.13 (tpc-h-2.8.0 pp.91)
 *                 and different from l_shipmode1
 *  l_receiptdate: First of January of a random year within [1993 .. 1997]
 *
 ********************************************************************/


q12_input_t& q12_input_t::operator=(const q12_input_t& rhs)
{    
    l_shipmode1=rhs.l_shipmode1;
    l_shipmode2=rhs.l_shipmode2;
    l_receiptdate = rhs.l_receiptdate;
    return (*this);
};


q12_input_t    create_q12_input(const double /* sf */, 
                                const int /* specificWH */)
{
    q12_input_t q12_input;

    //pick_str(&l_smode_set, L_SMODE_SD, q12_input.l_shipmode1);
    //pick_str(&l_smode_set, L_SMODE_SD, q12_input.l_shipmode2);
    q12_input.l_shipmode1=URand(0,END_SHIPMODE-1);
    q12_input.l_shipmode2=(q12_input.l_shipmode1+URand(0,END_SHIPMODE-2)+1)%END_SHIPMODE;

    struct tm receiptdate;

    // Random year [1993 .. 1997]
    receiptdate.tm_year = URand(93, 97);

    // First of January
    receiptdate.tm_mon = 0;
    receiptdate.tm_mday = 1;
    receiptdate.tm_hour = 0;
    receiptdate.tm_min = 0;
    receiptdate.tm_sec = 0;

    q12_input.l_receiptdate = mktime(&receiptdate);
	
    return (q12_input);
};



/******************************************************************** 
 *
 *  Q13
 *
 *  o_comment: WORD1 randomly selected from: special, pending, unusual, express
 *             WORD2 randomly selected from: packages, requests, accounts, deposits
 *
 ********************************************************************/


q13_input_t& q13_input_t::operator=(const q13_input_t& rhs)
{
    strcpy(WORD1,rhs.WORD1);
    strcpy(WORD2,rhs.WORD2);
    return (*this);
};


q13_input_t    create_q13_input(const double /* sf */, 
                                const int /* specificWH */)
{
    q13_input_t q13_input;

    int num_first_entries, num_second_entries;
    //WORD1 randomly selected from: special, pending, unusual, express
    //WORD2 randomly selected from: packages, requests, accounts, deposits
    static char const* FIRST[] = {"special", "pending", "unusual", "express"};
    static char const* SECOND[] = {"packages", "requests", "accounts", "deposits"};

    num_first_entries  = sizeof(FIRST)/sizeof(FIRST[0]);
    num_second_entries = sizeof(SECOND)/sizeof(SECOND[0]);

    strcpy(q13_input.WORD1,FIRST[URand(1,num_first_entries)-1]);
    strcpy(q13_input.WORD2,SECOND[URand(1,num_second_entries)-1]);

    return (q13_input);
};


/******************************************************************** 
 *
 *  Q14
 *
 *  l_shipdate: The first day of a month randomly selected from a
 *              random year within [1993 .. 1997]
 *
 ********************************************************************/


q14_input_t& q14_input_t::operator=(const q14_input_t& rhs)
{
    l_shipdate = rhs.l_shipdate;
    return (*this);
};


q14_input_t    create_q14_input(const double /* sf */, 
                                const int /* specificWH */)
{
    q14_input_t q14_input;

    struct tm shipdate;

    // Random year within [1993 .. 1997]
    shipdate.tm_year = URand(93, 97);

    // Random month ([1 .. 12])
    shipdate.tm_mon = URand(0,11);

    // First day
    shipdate.tm_mday = 1;
    shipdate.tm_hour = 0;
    shipdate.tm_min = 0;
    shipdate.tm_sec = 0;

    q14_input.l_shipdate = mktime(&shipdate);
	
    return (q14_input);
};



EXIT_NAMESPACE(tpch);
