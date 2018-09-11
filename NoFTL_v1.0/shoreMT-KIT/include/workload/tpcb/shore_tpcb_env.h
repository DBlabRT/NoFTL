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

/** @file:   shore_tpcb_env.h
 *
 *  @brief:  Definition of the Shore TPC-B environment
 *
 *  @author: Ryan Johnson      (ryanjohn)
 *  @author: Ippokratis Pandis (ipandis)
 *  @date:   July 2009
 */

#ifndef __SHORE_TPCB_ENV_H
#define __SHORE_TPCB_ENV_H


#include "sm_vas.h"
#include "util.h"

#include "workload/tpcb/tpcb_input.h"

#include "sm/shore/shore_env.h"
#include "sm/shore/shore_trx_worker.h"

#include "workload/tpcb/shore_tpcb_schema_man.h"

#include <map>

using namespace shore;


ENTER_NAMESPACE(tpcb);


using std::map;




/******************************************************************** 
 * 
 *  ShoreTPCBEnv Stats
 *  
 *  Shore TPC-B Database transaction statistics
 *
 ********************************************************************/

struct ShoreTPCBTrxCount
{
    uint acct_update;
    uint populate_db;

    ShoreTPCBTrxCount& operator+=(ShoreTPCBTrxCount const& rhs) {
        acct_update += rhs.acct_update;        
	return (*this);
    }

    ShoreTPCBTrxCount& operator-=(ShoreTPCBTrxCount const& rhs) {
        acct_update -= rhs.acct_update;
	return (*this);
    }

    uint total() const {
        return (acct_update);
    }
    
}; // EOF: ShoreTPCBTrxCount


struct ShoreTPCBTrxStats
{
    ShoreTPCBTrxCount attempted;
    ShoreTPCBTrxCount failed;
    ShoreTPCBTrxCount deadlocked;

    ShoreTPCBTrxStats& operator+=(ShoreTPCBTrxStats const& other) {
        attempted  += other.attempted;
        failed     += other.failed;
        deadlocked += other.deadlocked;
        return (*this);
    }

    ShoreTPCBTrxStats& operator-=(ShoreTPCBTrxStats const& other) {
        attempted  -= other.attempted;
        failed     -= other.failed;
        deadlocked -= other.deadlocked;
        return (*this);
    }

}; // EOF: ShoreTPCBTrxStats



/******************************************************************** 
 * 
 *  ShoreTPCBEnv
 *  
 *  Shore TPC-B Database.
 *
 ********************************************************************/

class ShoreTPCBEnv : public ShoreEnv
{
public:

    typedef std::map<pthread_t, ShoreTPCBTrxStats*> statmap_t;

    class table_builder_t;
    class table_creator_t;
    struct checkpointer_t;

private:

    w_rc_t _post_init_impl();

    w_rc_t _pad_BRANCHES();
    w_rc_t _pad_TELLERS();

public:    

    ShoreTPCBEnv();
    virtual ~ShoreTPCBEnv();


    // DB INTERFACE

    virtual int set(envVarMap* /* vars */) { return(0); /* do nothing */ };
    virtual int open() { return(0); /* do nothing */ };
    virtual int pause() { return(0); /* do nothing */ };
    virtual int resume() { return(0); /* do nothing */ };    
    virtual w_rc_t newrun() { return(RCOK); /* do nothing */ };

    virtual int post_init();
    virtual w_rc_t load_schema();

    virtual int conf();
    virtual int start();
    virtual int stop();
    virtual int info() const;
    virtual int statistics(); 

    int dump();

    virtual void print_throughput(const double iQueriedSF, 
                                  const int iSpread, 
                                  const int iNumOfThreads,
                                  const double delay,
                                  const ulong_t mioch,
                                  const double avgcpuusage);


    // Public methods //    

    // --- operations over tables --- //
    w_rc_t loaddata();  
    w_rc_t warmup();
    w_rc_t check_consistency();


    // TPCB Tables
    DECLARE_TABLE(branch_t,branch_man_impl,branch);
    DECLARE_TABLE(teller_t,teller_man_impl,teller);
    DECLARE_TABLE(account_t,account_man_impl,account);
    DECLARE_TABLE(history_t,history_man_impl,history);


    // --- kit baseline trxs --- //

    w_rc_t run_one_xct(Request* prequest);

    // Transactions
    DECLARE_TRX(acct_update);
  
  // Database population
    DECLARE_TRX(populate_db);

    // for thread-local stats
    virtual void env_thread_init();
    virtual void env_thread_fini();   

    // stat map
    statmap_t _statmap;

    // snapshot taken at the beginning of each experiment    
    ShoreTPCBTrxStats _last_stats;
    virtual void reset_stats();
    ShoreTPCBTrxStats _get_stats();

    // set load imbalance and time to apply it
    void set_skew(int area, int load, int start_imbalance);
    void start_load_imbalance();
    void reset_skew();


}; // EOF ShoreTPCBEnv
   


EXIT_NAMESPACE(tpcb);


#endif /* __SHORE_TPCB_ENV_H */

