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
   portions thereof, and that both notices appear in custorting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/** @file:   qpipe_qcustomer.cpp
 *
 *  @brief:  Implementation of simple QPIPE SSB tablescans over Shore-MT
 *
 *  @author: Manos Athanassoulis
 *  @date:   July 2010
 */

#include "workload/ssb/shore_ssb_env.h"
#include "qpipe.h"

using namespace shore;
using namespace qpipe;



ENTER_NAMESPACE(ssb);


/******************************************************************** 
 *
 * QPIPE qcustomer - Structures needed by operators 
 *
 ********************************************************************/

// the tuples after tablescan projection
typedef struct ssb_customer_tuple projected_tuple;

struct nation_tuple
{
    int  C_NATION_PREFIX;
    char C_NATION[16];
};

struct count_tuple
{
    int COUNT;
};




class customer_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prcust;
    rep_row_t _rr;

    ssb_customer_tuple _customer;

public:

    customer_tscan_filter_t(ShoreSSBEnv* ssbdb, qcustomer_input_t &in) 
        : tuple_filter_t(ssbdb->customer_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a customer tupple from the tuple cache and allocate space
        _prcust = _ssbdb->customer_man()->get_tuple();
        _rr.set_ts(_ssbdb->customer_man()->ts(),
                   _ssbdb->customer_desc()->maxsize());
        _prcust->_rep = &_rr;

    }

    ~customer_tscan_filter_t()
    {
        // Give back the customer tuple 
        _ssbdb->customer_man()->give_tuple(_prcust);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next customer and read its shipdate
        if (!_ssbdb->customer_man()->load(_prcust, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        return (true);
    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        //        projected_tuple *dest;
        //dest = aligned_cast<projected_tuple>(d.data);
        nation_tuple *dest;
        dest = aligned_cast<nation_tuple>(d.data);

        _prcust->get_value(4, _customer.C_NATION_PREFIX);
        _prcust->get_value(5, _customer.C_NATION,15);


        TRACE( TRACE_RECORD_FLOW, "NATIONS: %s --d\n",
               _customer.C_NATION);


        dest->C_NATION_PREFIX = _customer.C_NATION_PREFIX;
        strcpy(dest->C_NATION,_customer.C_NATION);
    }

    customer_tscan_filter_t* clone() const {
        return new customer_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("customer_tscan_filter_t()");
    }
};


struct count_aggregate_t : public tuple_aggregate_t {
    default_key_extractor_t _extractor;
    
    count_aggregate_t()
        : tuple_aggregate_t(sizeof(projected_tuple))
    {
    }
    virtual key_extractor_t* key_extractor() { return &_extractor; }
    
    virtual void aggregate(char* agg_data, const tuple_t &) {
        count_tuple* agg = aligned_cast<count_tuple>(agg_data);
        agg->COUNT++;
    }

    virtual void finish(tuple_t &d, const char* agg_data) {
        memcpy(d.data, agg_data, tuple_size());
    }
    virtual count_aggregate_t* clone() const {
        return new count_aggregate_t(*this);
    }
    virtual c_str to_string() const {
        return "count_aggregate_t";
    }
};



class ssb_qcustomer_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** qcustomer ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** SUM_QTY\tSUM_BASE\tSUM_DISC...\n");
    }
    
    virtual void process(const tuple_t& output) {
        projected_tuple *tuple;
        tuple = aligned_cast<projected_tuple>(output.data);
        TRACE( TRACE_QUERY_RESULTS, "%d|%s --d\n",
               tuple->C_CUSTKEY,
               tuple->C_NAME);
    }
};



/******************************************************************** 
 *
 * QPIPE qcustomer - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qpipe_qcustomer(const int xct_id, 
                                  qcustomer_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** qcustomer *********\n");

   
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();
    

    // TSCAN PACKET
    tuple_fifo* tscan_out_buffer =
        new tuple_fifo(sizeof(projected_tuple));
        tscan_packet_t* customer_tscan_packet =
        new tscan_packet_t("TSCAN CUSTOMER",
                           tscan_out_buffer,
                           new customer_tscan_filter_t(this,in),
                           this->db(),
                           _pcustomer_desc.get(),
                           pxct
                           //, SH 
                           );


    /*    tuple_fifo* agg_output = new tuple_fifo(sizeof(count_tuple));
    aggregate_packet_t* agg_packet =
                new aggregate_packet_t(c_str("COUNT_AGGREGATE"),
                               agg_output,
                               new trivial_filter_t(agg_output->tuple_size()),
                               new count_aggregate_t(),
                               new int_key_extractor_t(),
                               date_tscan_packet);
    
        new partial_aggregate_packet_t(c_str("COUNT_AGGREGATE"),
                               agg_output,
                               new trivial_filter_t(agg_output->tuple_size()),
                               qcustomer_tscan_packet,
                               new count_aggregate_t(),
                               new default_key_extractor_t(),
                               new int_key_compare_t());
    */
    qpipe::query_state_t* qs = dp->query_state_create();
    customer_tscan_packet->assign_query_state(qs);
    //agg_packet->assign_query_state(qs);
        
    // Dispatch packet
    ssb_qcustomer_process_tuple_t pt;
    process_query(customer_tscan_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK); 
}


EXIT_NAMESPACE(ssb);

