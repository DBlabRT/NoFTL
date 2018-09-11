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

/** @file:   qpipe_qdate.cpp
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
 * QPIPE qdate - Structures needed by operators 
 *
 ********************************************************************/

// the tuples after tablescan projection
typedef struct ssb_date_tuple projected_tuple;

struct count_tuple
{
    int COUNT;
};

typedef struct count_tuple process_tuple;
//typedef projected_tuple process_tuple;


class date_tscan_filter_t : public tuple_filter_t 
{
private:
    ShoreSSBEnv* _ssbdb;
    table_row_t* _prdate;
    rep_row_t _rr;

    ssb_date_tuple _date;

public:

    date_tscan_filter_t(ShoreSSBEnv* ssbdb, qdate_input_t &in) 
        : tuple_filter_t(ssbdb->date_desc()->maxsize()), _ssbdb(ssbdb)
    {

    	// Get a date tupple from the tuple cache and allocate space
        _prdate = _ssbdb->date_man()->get_tuple();
        _rr.set_ts(_ssbdb->date_man()->ts(),
                   _ssbdb->date_desc()->maxsize());
        _prdate->_rep = &_rr;

    }

    ~date_tscan_filter_t()
    {
        // Give back the date tuple 
        _ssbdb->date_man()->give_tuple(_prdate);
    }


    // Predication
    bool select(const tuple_t &input) {

        // Get next date and read its shipdate
        if (!_ssbdb->date_man()->load(_prdate, input.data)) {
            assert(false); // RC(se_WRONG_DISK_DATA)
        }

        _prdate->get_value(0, _date.D_DATEKEY);
        int date=_date.D_DATEKEY;
        return (date>19970000);

        //        return (true);
    }

    
    // Projection
    void project(tuple_t &d, const tuple_t &s) {        

        projected_tuple *dest;
        dest = aligned_cast<projected_tuple>(d.data);

        _prdate->get_value(0, _date.D_DATEKEY);
        _prdate->get_value(1, _date.D_DATE, 18);

        TRACE( TRACE_RECORD_FLOW, "%d|%s --\n",
               _date.D_DATEKEY,
               _date.D_DATE);

        //TRACE(TRACE_ALWAYS,"Key:%s\n",dest->key_to_str());

        dest->D_DATEKEY = _date.D_DATEKEY;
        strcpy(dest->D_DATE,_date.D_DATE);
    }

    date_tscan_filter_t* clone() const {
        return new date_tscan_filter_t(*this);
    }

    c_str to_string() const {
        return c_str("date_tscan_filter_t()");
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



class ssb_qdate_process_tuple_t : public process_tuple_t 
{    
public:
        
    void begin() {
        TRACE(TRACE_QUERY_RESULTS, "*** QDATE ANSWER ...\n");
        TRACE(TRACE_QUERY_RESULTS, "*** SUM_QTY\tSUM_BASE\tSUM_DISC...\n");
    }
    
    virtual void process(const tuple_t& output) {
        process_tuple *tuple;
        tuple = aligned_cast<process_tuple>(output.data);
        TRACE( TRACE_QUERY_RESULTS, "%d --\n",
          tuple->COUNT);
            /*TRACE( TRACE_QUERY_RESULTS, "%d|%s --\n",
               tuple->D_DATEKEY,
               tuple->D_DATE);*/
    }
};



/******************************************************************** 
 *
 * QPIPE qdate - Packet creation and submission
 *
 ********************************************************************/

w_rc_t ShoreSSBEnv::xct_qpipe_qdate(const int xct_id, 
                                  qdate_input_t& in)
{
    TRACE( TRACE_ALWAYS, "********** qdate *********\n");

   
    policy_t* dp = this->get_sched_policy();
    xct_t* pxct = smthread_t::me()->xct();
    

    // TSCAN PACKET
    tuple_fifo* tscan_out_buffer =
        new tuple_fifo(sizeof(projected_tuple));
    tscan_packet_t* date_tscan_packet =
        new tscan_packet_t("TSCAN DATE",
                           tscan_out_buffer,
                           new date_tscan_filter_t(this,in),
                           this->db(),
                           _pdate_desc.get(),
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
                               date_tscan_packet,
                               new count_aggregate_t(),
                               new default_key_extractor_t(),
                               new int_key_compare_t());
    
    qpipe::query_state_t* qs = dp->query_state_create();
    date_tscan_packet->assign_query_state(qs);
    agg_packet->assign_query_state(qs);
        
    // Dispatch packet
    ssb_qdate_process_tuple_t pt;
    process_query(agg_packet, pt);
    //process_query(date_tscan_packet, pt);
    dp->query_state_destroy(qs);

    return (RCOK); 
}


EXIT_NAMESPACE(ssb);
