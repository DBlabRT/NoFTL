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
#include "workload/ssb/ssb_random.h" 
#include "workload/ssb/ssb_util.h" 
#include "workload/ssb/ssb_input.h"

#include "workload/ssb/dbgen/dss.h"
#include "workload/ssb/dbgen/dsstypes.h"

using namespace dbgenssb; // for random MODES etc..

ENTER_NAMESPACE(ssb);

qdate_input_t       create_qdate_input(const double /*sf*/, const int /*specificWH*/) 
{qdate_input_t a;return a;}
qpart_input_t       create_qpart_input(const double /*sf*/, const int /*specificWH*/) 
{qpart_input_t a;return a;}
qsupplier_input_t       create_qsupplier_input(const double /*sf*/, const int /*specificWH*/) 
{qsupplier_input_t a;return a;}
qcustomer_input_t       create_qcustomer_input(const double /*sf*/, const int /*specificWH*/) 
{qcustomer_input_t a;return a;}
qlineorder_input_t create_qlineorder_input(const double /*sf*/, const int /*specificWH*/) 
{qlineorder_input_t a;return a;}
q1_1_input_t create_q1_1_input(const double /*sf*/, const int /*specificWH*/) {q1_1_input_t a;return a;}
q1_2_input_t create_q1_2_input(const double /*sf*/, const int /*specificWH*/) {q1_2_input_t a;return a;}
q1_3_input_t create_q1_3_input(const double /*sf*/, const int /*specificWH*/) {q1_3_input_t a;return a;}
q2_1_input_t create_q2_1_input(const double /*sf*/, const int /*specificWH*/) {q2_1_input_t a;return a;}
q2_2_input_t create_q2_2_input(const double /*sf*/, const int /*specificWH*/) {q2_2_input_t a;return a;}
q2_3_input_t create_q2_3_input(const double /*sf*/, const int /*specificWH*/) {q2_3_input_t a;return a;}
q3_1_input_t create_q3_1_input(const double /*sf*/, const int /*specificWH*/) {q3_1_input_t a;return a;}
q3_3_input_t create_q3_3_input(const double /*sf*/, const int /*specificWH*/) {q3_3_input_t a;return a;}
q3_4_input_t create_q3_4_input(const double /*sf*/, const int /*specificWH*/) {q3_4_input_t a;return a;}
q4_1_input_t create_q4_1_input(const double /*sf*/, const int /*specificWH*/) {q4_1_input_t a;return a;}
q4_2_input_t create_q4_2_input(const double /*sf*/, const int /*specificWH*/) {q4_2_input_t a;return a;}
q4_3_input_t create_q4_3_input(const double /*sf*/, const int /*specificWH*/) {q4_3_input_t a;return a;}


/******************************************************************** 
 *
 *  Q3_2
 *
 ********************************************************************/


q3_2_input_t& q3_2_input_t::operator=(const q3_2_input_t& rhs)
{
    _year_lo = rhs._year_lo;
    _year_hi = rhs._year_hi;
    
    strcpy(_nation,rhs._nation);
    
    return (*this);
}


q3_2_input_t create_q3_2_input(const double /*sf*/, const int /*specificWH*/)
{
    q3_2_input_t q3_2_in;
    int _id_nation;
    
    q3_2_in._year_lo=URand(1992,1998);
    q3_2_in._year_hi=URand(q3_2_in._year_lo,1998);
    _id_nation=URand(0,24);
    get_nation(q3_2_in._nation,(ssb_nation)_id_nation);
    
    printf("q3_2_in: %d %d %s\n",q3_2_in._year_lo,q3_2_in._year_hi,q3_2_in._nation);
    //printf("ENUM: %d %d",ALGERIA,VIETNAM);
    //strcpy(q3_2_in._nation,"UNITED STATES");
    
    return q3_2_in;
}


EXIT_NAMESPACE(ssb);
