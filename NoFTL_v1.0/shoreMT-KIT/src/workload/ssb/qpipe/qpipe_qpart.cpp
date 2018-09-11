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

/** @file:   qpipe_qpart.cpp
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
 * QPIPE QPART - Structures needed by operators 
 *
 ********************************************************************/

// the tuples after tablescan projection
typedef struct ssb_part_tuple projected_tuple;

struct count_tuple
{
    long COUNT;
};


class part_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prpart;
    rep_row_t _rr;

    ssb_part_tuple _part;

public:

    part_tscan_filter_t(ShoreSSBEnv* ssbdb, qpart_input_t &in) 
        : tuple_filter_t(ssbdb->part_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a part tupple from the tuple cache and allocate space
        _prpart = _ssbdb->part_man()->get_tuple();
        _rr.set_ts(_ssbdb->part_man()->ts(),
                   _ssbdb->part_desc()->maxsize());
        _prpart->_rep = &_rr;

    }

    ~part_tscan_filter_t()
    {
        // Give back the part tuple 
        _ssbdb->part_man()->give_tuple(_prpart);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next part and read its shippart
        if (!_ssbdb->part_man()->load(_prpart, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        return (true);
    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &/*s*/) {        

        projected_tuple *dest;
        dest = aligned_cast<projected_tuple>(d.data);

        _prpart->get_value(0, _part.P_PARTKEY);
        _prpart->get_value(1, _part.P_NAME, 18);

        TRACE( TRACE_RECORD_FLOW, "%d|%s --d\n",
               _part.P_PARTKEY,
               _part.P_NAME);

        //TRACE(TRACE_ALWAYS,"Key:%s\n",dest->key_to_str());

        dest->P_PARTKEY = _part.P_PARTKEY;
        strcpy(dest->P_NAME,_part.P_NAME);
    }

    part_tscan_filter_t* clone() const {
        return new part_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("part_tscan_filter_t()");
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



class ssb_qpart_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** QPART ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** SUM_QTY\tSUM_BASE\tSUM_DISC...\n");
    }
    
    virtual void process(const tuple_t& output) {
        projected_tuple *tuple;
        tuple = aligned_cast<projected_tuple>(output.data);
        TRACE( TRACE_QUERY_RESULTS, "%d|%s --d\n",
               tuple->P_PARTKEY,
               tuple->P_NAME);

    }
};



/******************************************************************** 
 *
 * QPIPE QPART - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qpipe_qpart(const int xct_id, 
                                  qpart_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** QPART *********\n");

   
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();
    

    // TSCAN PACKET
    tuple_fifo* tscan_out_buffer =
        new tuple_fifo(sizeof(projected_tuple));
    tscan_packet_t* part_tscan_packet =
        new tscan_packet_t("TSCAN PART",
                           tscan_out_buffer,
                           new part_tscan_filter_t(this,in),
                           this->db(),
                           _ppart_desc.get(),
                           pxct
                           //, SH 
                           );


        tuple_fifo* agg_output = new tuple_fifo(sizeof(count_tuple));
          /*
    aggregate_packet_t* agg_packet =
                new aggregate_packet_t(c_str("COUNT_AGGREGATE"),
                               agg_output,
                               new trivial_filter_t(agg_output->tuple_size()),
                               new count_aggregate_t(),
                               new int_key_extractor_t(),
                               date_tscan_packet);
    */
        partial_aggregate_packet_t* agg_packet =
        new partial_aggregate_packet_t(c_str("COUNT_AGGREGATE"),
                               agg_output,
                               new trivial_filter_t(agg_output->tuple_size()),
                               part_tscan_packet,
                               new count_aggregate_t(),
                               new default_key_extractor_t(),
                               new int_key_compare_t());
    
    qpipe::query_state_t* qs = dp->query_state_create();
    part_tscan_packet->assign_query_state(qs);
    agg_packet->assign_query_state(qs);
        


    // Dispatch packet
    ssb_qpart_process_tuple_t pt;
    //process_query(part_tscan_packet, pt);
    process_query(agg_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK); 
}


EXIT_NAMESPACE(ssb);
