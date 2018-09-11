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

/** @file:   qpipe_qsupplier.cpp
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
 * QPIPE qsupplier - Structures needed by operators 
 *
 ********************************************************************/

// the tuples after tablescan projection
typedef struct ssb_supplier_tuple projected_tuple;

struct count_tuple
{
    int COUNT;
};




class supplier_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prsupp;
    rep_row_t _rr;

    ssb_supplier_tuple _supplier;

public:

    supplier_tscan_filter_t(ShoreSSBEnv* ssbdb, qsupplier_input_t &in) 
        : tuple_filter_t(ssbdb->supplier_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a supplier tupple from the tuple cache and allocate space
        _prsupp = _ssbdb->supplier_man()->get_tuple();
        _rr.set_ts(_ssbdb->supplier_man()->ts(),
                   _ssbdb->supplier_desc()->maxsize());
        _prsupp->_rep = &_rr;

    }

    ~supplier_tscan_filter_t()
    {
        // Give back the supplier tuple 
        _ssbdb->supplier_man()->give_tuple(_prsupp);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next supplier and read its shipdate
        if (!_ssbdb->supplier_man()->load(_prsupp, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        return (true);
    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        projected_tuple *dest;
        dest = aligned_cast<projected_tuple>(d.data);

        _prsupp->get_value(0, _supplier.S_SUPPKEY);
        _prsupp->get_value(1, _supplier.S_NAME,25);


        TRACE( TRACE_RECORD_FLOW, "%d|%s --d\n",
               _supplier.S_SUPPKEY,
               _supplier.S_NAME);


        dest->S_SUPPKEY = _supplier.S_SUPPKEY;
        strcpy(dest->S_NAME,_supplier.S_NAME);
    }

    supplier_tscan_filter_t* clone() const {
        return new supplier_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("supplier_tscan_filter_t()");
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



class ssb_qsupplier_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** qsupplier ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** SUM_QTY\tSUM_BASE\tSUM_DISC...\n");
    }
    
    virtual void process(const tuple_t& output) {
        projected_tuple *tuple;
        tuple = aligned_cast<projected_tuple>(output.data);
        TRACE( TRACE_QUERY_RESULTS, "%d|%s --d\n",
               tuple->S_SUPPKEY,
               tuple->S_NAME);
    }
};



/******************************************************************** 
 *
 * QPIPE qsupplier - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qpipe_qsupplier(const int xct_id, 
                                  qsupplier_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** qsupplier *********\n");

   
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();
    

    // TSCAN PACKET
    tuple_fifo* tscan_out_buffer =
        new tuple_fifo(sizeof(projected_tuple));
        tscan_packet_t* supplier_tscan_packet =
        new tscan_packet_t("TSCAN SUPPLIER",
                           tscan_out_buffer,
                           new supplier_tscan_filter_t(this,in),
                           this->db(),
                           _psupplier_desc.get(),
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
                               qsupplier_tscan_packet,
                               new count_aggregate_t(),
                               new default_key_extractor_t(),
                               new int_key_compare_t());
    */
    qpipe::query_state_t* qs = dp->query_state_create();
    supplier_tscan_packet->assign_query_state(qs);
    //agg_packet->assign_query_state(qs);
        
    // Dispatch packet
    ssb_qsupplier_process_tuple_t pt;
    process_query(supplier_tscan_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK); 
}


EXIT_NAMESPACE(ssb);

