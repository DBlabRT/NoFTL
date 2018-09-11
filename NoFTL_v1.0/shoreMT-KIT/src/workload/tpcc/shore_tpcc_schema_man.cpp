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

/** @file:   shore_tpcc_schema.h
 *
 *  @brief:  Implementation of the workload-specific access methods 
 *           on TPC-C tables
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#include "workload/tpcc/shore_tpcc_schema_man.h"

using namespace shore;


ENTER_NAMESPACE(tpcc);



/*********************************************************************
 *
 * Workload-specific access methods on tables
 *
 *********************************************************************/


/* ----------------- */
/* --- WAREHOUSE --- */
/* ----------------- */


w_rc_t 
warehouse_man_impl::wh_index_probe(ss_m* db,
                                   warehouse_tuple* ptuple,
                                   const int w_id)
{
    assert (ptuple);    
    ptuple->set_value(0, w_id);
    return (index_probe_by_name(db, "W_IDX", ptuple));
}


w_rc_t 
warehouse_man_impl::wh_index_probe_forupdate(ss_m* db,
                                             warehouse_tuple* ptuple,
                                             const int w_id)
{
    assert (ptuple);    
    ptuple->set_value(0, w_id);
    return (index_probe_forupdate_by_name(db, "W_IDX", ptuple));
}


w_rc_t 
warehouse_man_impl::wh_update_ytd(ss_m* db,
                                  warehouse_tuple* ptuple,
                                  const double amount,
                                  lock_mode_t lm)
{ 
    // 1. increases the ytd of the warehouse and updates the table

    assert (ptuple);
    assert (ptuple->is_rid_valid());

    double ytd;
    ptuple->get_value(8, ytd);
    ytd += amount;
    ptuple->set_value(8, ytd);
    W_DO(update_tuple(db, ptuple, lm));
    return (RCOK);
}

w_rc_t warehouse_man_impl::wh_load_fromFile(ss_m* _pssm, warehouse_tuple* prwh)
{
	w_rc_t e = RCOK;

	//==========================================================
	// read from file WAREHOUSE
	//==========================================================
	file_info_t finfo(9,5);
	finfo.field_type[0]=SM_INTEGER;	// w_id
	finfo.field_type[1]=SM_VARCHAR;	// name
	finfo.field_size[1]= 11;
	finfo.field_type[2]=SM_VARCHAR;	// street_1
	finfo.field_size[2]= 21;
	finfo.field_type[3]=SM_VARCHAR;	// street_2
	finfo.field_size[3]= 21;
	finfo.field_type[4]=SM_VARCHAR;	// city
	finfo.field_size[4]= 21;
	finfo.field_type[5]=SM_CHAR;	// state
	finfo.field_size[5]= 4;
	finfo.field_type[6]=SM_CHAR;	// zip
	finfo.field_size[6]= 8;
	finfo.field_type[7]=SM_FLOAT;	// tax
	finfo.field_type[8]=SM_FLOAT;	// yeld

	// Set filename for table data
	char data_file[101];
	sprintf(data_file, "Data/Warehouse.data");

	flid_t f;
	rc_t rc;

	FILE* fp;

	hpl_record_t hpl_rec(finfo.no_fields, finfo.no_ffields);
	int ignoredFields = 0;
	for (f = 0; f < finfo.no_fields; ++f)
		hpl_rec.fieldSize[f] = finfo.field_size[f];


	if ((fp = fopen(data_file, "r")) == NULL) {
		cout << "Error: Could not load " << data_file << endl;
		return RC_AUGMENT(rc);
	}
	// read the line
	char line[1000 + 1];
	int scanned = 0;
	while (fgets(line, 1000, fp) != NULL) {
		scanned++;
		W_DO(this->get_hpl_rec_from_line(finfo, hpl_rec, line));

		/*int w_id = *(int*)hpl_rec.data[0];
		char name[11] = *(char*)hpl_rec.data[1];
		char street_1[21]=hpl_rec.data[2];
		char street_2[21]=hpl_rec.data[3];
		char city[21]=hpl_rec.data[4];
		char state[3]=hpl_rec.data[5];
		char zip[10]=hpl_rec.data[6];
		double tax=hpl_rec.data[7];
		double yeld=hpl_rec.data[8];*/


		// warehouse
		prwh->set_value(0, *(int*)hpl_rec.data[0]);	 // w_id
		prwh->set_value(1, (char*)hpl_rec.data[1]); // name
		prwh->set_value(2, (char*)hpl_rec.data[2]); // street_1
		prwh->set_value(3, (char*)hpl_rec.data[3]); // street_2
		prwh->set_value(4, (char*)hpl_rec.data[4]);	// city
		prwh->set_value(5, (char*)hpl_rec.data[5]); // state
		prwh->set_value(6, (char*)hpl_rec.data[6]); // zip
		prwh->set_value(7, *(double*)hpl_rec.data[7]); // tax
		prwh->set_value(8, *(double*)hpl_rec.data[8]); // yeld
		//e = _pwarehouse_man->add_tuple(_pssm, prwh);
		e = this->add_tuple(_pssm, prwh);


		for (f = 0; f < finfo.no_fields; ++f) {
			delete hpl_rec.data[f];
			hpl_rec.data[f] = NULL;
		}
		//PRINT_PROGRESS_MESSAGE(scanned);
	}

	fclose(fp);

	return e;
}


/* ---------------- */
/* --- DISTRICT --- */
/* ---------------- */


w_rc_t district_man_impl::dist_index_probe(ss_m* db,
                                           district_tuple* ptuple,
                                           const int w_id,
                                           const int d_id)
{
    assert (ptuple);
    ptuple->set_value(0, d_id);
    ptuple->set_value(1, w_id);
    return (index_probe_by_name(db, "D_IDX", ptuple));
}

w_rc_t district_man_impl::dist_index_probe_forupdate(ss_m* db,
                                                     district_tuple* ptuple,
                                                     const int w_id,
                                                     const int d_id)
{
    assert (ptuple);
    ptuple->set_value(0, d_id);
    ptuple->set_value(1, w_id);
    return (index_probe_forupdate_by_name(db, "D_IDX", ptuple));
}

w_rc_t district_man_impl::dist_update_ytd(ss_m* db,
                                          district_tuple* ptuple,
                                          const double amount,
                                          lock_mode_t lm)
{
    // 1. increases the ytd of the district and updates the table

    assert (ptuple);
    assert (ptuple->is_rid_valid());

    double d_ytd;
    ptuple->get_value(9, d_ytd);
    d_ytd += amount;
    ptuple->set_value(9, d_ytd);
    W_DO(update_tuple(db, ptuple, lm));
    return (RCOK);
}

w_rc_t district_man_impl::dist_update_next_o_id(ss_m* db,
                                                district_tuple* ptuple,
                                                const int next_o_id,
                                                lock_mode_t lm)
{
    // 1. updates the next order id of the district

    assert (ptuple);
    assert (ptuple->is_rid_valid());

    ptuple->set_value(10, next_o_id);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t district_man_impl::dist_load_fromFile(ss_m* _pssm, district_tuple* prdist)
{
	w_rc_t e = RCOK;


	//==========================================================
	// read from file DISTRICT
	//==========================================================
	file_info_t finfo_d(11,7);
	finfo_d.field_type[0]=SM_INTEGER;	// d_id
	finfo_d.field_type[1]=SM_INTEGER;	// d_w_id
	finfo_d.field_type[2]=SM_VARCHAR;	// name
	finfo_d.field_size[2]= 11;
	finfo_d.field_type[3]=SM_VARCHAR;	// street_1
	finfo_d.field_size[3]= 21;
	finfo_d.field_type[4]=SM_VARCHAR;	// street_2
	finfo_d.field_size[4]= 21;
	finfo_d.field_type[5]=SM_VARCHAR;	// city
	finfo_d.field_size[5]= 21;
	finfo_d.field_type[6]=SM_CHAR;		// state
	finfo_d.field_size[6]= 4;
	finfo_d.field_type[7]=SM_CHAR;		// zip
	finfo_d.field_size[7]= 8;
	finfo_d.field_type[8]=SM_FLOAT;		// tax
	finfo_d.field_type[9]=SM_FLOAT;		// yeld
	finfo_d.field_type[10]=SM_INTEGER;	// next_o_id

	// Set filename for table data
	char data_file_d[101];
	sprintf(data_file_d, "Data/District.data");


	flid_t f;
	rc_t rc;

	FILE* fp;

	hpl_record_t hpl_rec_d(finfo_d.no_fields, finfo_d.no_ffields);
	for (f = 0; f < finfo_d.no_fields; ++f)
		hpl_rec_d.fieldSize[f] = finfo_d.field_size[f];


	if ((fp = fopen(data_file_d, "r")) == NULL) {
		cout << "Error: Could not load " << data_file_d << endl;
		return RC_AUGMENT(rc);
	}
	// read the line
	char line[1000 + 1];
	int scanned = 0;
	while (fgets(line, 1000, fp) != NULL) {
		scanned++;
		W_DO(this->get_hpl_rec_from_line(finfo_d, hpl_rec_d, line));


		// district
		prdist->set_value(0, *(int*)hpl_rec_d.data[0]);	 // d_id
		prdist->set_value(1, *(int*)hpl_rec_d.data[1]);	 // d_w_id
		prdist->set_value(2, (char*)hpl_rec_d.data[2]); // name
		prdist->set_value(3, (char*)hpl_rec_d.data[3]); // street_1
		prdist->set_value(4, (char*)hpl_rec_d.data[4]); // street_2
		prdist->set_value(5, (char*)hpl_rec_d.data[5]);	// city
		prdist->set_value(6, (char*)hpl_rec_d.data[6]); // state
		prdist->set_value(7, (char*)hpl_rec_d.data[7]); // zip
		prdist->set_value(8, *(double*)hpl_rec_d.data[8]); // tax
		prdist->set_value(9, *(double*)hpl_rec_d.data[9]); // yeld
		prdist->set_value(10, *(int*)hpl_rec_d.data[10]);	 // next_o_id
		e = this->add_tuple(_pssm, prdist);



		for (f = 0; f < finfo_d.no_fields; ++f) {
			delete hpl_rec_d.data[f];
			hpl_rec_d.data[f] = NULL;
		}
	}

	fclose(fp);


	return e;
}



/* ---------------- */
/* --- CUSTOMER --- */
/* ---------------- */


w_rc_t customer_man_impl::cust_get_iter_by_index(ss_m* db,
                                                 customer_index_iter* &iter,
                                                 customer_tuple* ptuple,
                                                 rep_row_t &replow,
                                                 rep_row_t &rephigh,
                                                 const int w_id,
                                                 const int d_id,
                                                 const char* c_last,
                                                 lock_mode_t alm,
                                                 bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("C_NAME_IDX");
    assert (pindex);

    // C_NAME_IDX: {2 - 1 - 5 - 3 - 0}

    // prepare the key to be probed
    ptuple->set_value(0, 0);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    ptuple->set_value(3, "");
    ptuple->set_value(5, c_last);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    char   temp[2];
    temp[0] = MAX('z', 'Z')+1;
    temp[1] = '\0';
    ptuple->set_value(3, temp);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);    

    /* index only access */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


w_rc_t customer_man_impl::cust_index_probe(ss_m* db,
                                           customer_tuple* ptuple,
                                           const int w_id,
                                           const int d_id,
                                           const int c_id)
{
    assert (ptuple);
    return (cust_index_probe_by_name(db, "C_IDX", ptuple, w_id, d_id, c_id));
}

w_rc_t customer_man_impl::cust_index_probe_by_name(ss_m* db,
                                                   const char* idx_name,
                                                   customer_tuple* ptuple,
                                                   const int w_id,
                                                   const int d_id,
                                                   const int c_id)
{
    assert (idx_name);
    ptuple->set_value(0, c_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    return (index_probe_by_name(db, idx_name, ptuple));
}

w_rc_t customer_man_impl::cust_index_probe_forupdate(ss_m * db,
                                                     customer_tuple* ptuple,
                                                     const int w_id,
                                                     const int d_id,
                                                     const int c_id)
{
    assert (ptuple);
    ptuple->set_value(0, c_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    return (index_probe_forupdate_by_name(db, "C_IDX", ptuple));
}

w_rc_t customer_man_impl::cust_update_tuple(ss_m* db,
                                            customer_tuple* ptuple,
                                            const tpcc_customer_tuple& acustomer,
                                            const char* adata1,
                                            const char* adata2,
                                            lock_mode_t lm)
{
    assert (ptuple);
    ptuple->set_value(16, acustomer.C_BALANCE);
    ptuple->set_value(17, acustomer.C_YTD_PAYMENT);
    ptuple->set_value(19, acustomer.C_PAYMENT_CNT);

    if (adata1)
	ptuple->set_value(20, adata1);

    if (adata2)
	ptuple->set_value(21, adata2);

    return (update_tuple(db, ptuple, lm));
}


w_rc_t customer_man_impl::cust_update_discount_balance(ss_m* db,
                                                       customer_tuple* ptuple,
                                                       const decimal discount,
                                                       const decimal balance,
                                                       lock_mode_t lm)
{
    assert (ptuple);
    assert (discount>=0);
    ptuple->set_value(15, discount);
    ptuple->set_value(16, balance);
    return (update_tuple(db, ptuple, lm));
}

w_rc_t customer_man_impl::cust_load_fromFile(ss_m* _pssm, customer_tuple* prcust)
{
	w_rc_t e = RCOK;


	//==========================================================
	// read from file CUSTOMER
	//==========================================================
	file_info_t finfo(22,16);
	finfo.field_type[0]=SM_INTEGER;	//c_id
	finfo.field_type[1]=SM_INTEGER;	//c_d_id
	finfo.field_type[2]=SM_INTEGER;	//c_w_id
	finfo.field_type[3]=SM_CHAR;	//c_first
	finfo.field_size[3]= 16;
	finfo.field_type[4]=SM_CHAR;	//c_middle
	finfo.field_size[4]= 4;
	finfo.field_type[5]=SM_CHAR;	//c_last
	finfo.field_size[5]= 16;
	finfo.field_type[6]=SM_VARCHAR;	//C_STREET1
	finfo.field_size[6]= 21;
	finfo.field_type[7]=SM_VARCHAR;	//C_STREET2
	finfo.field_size[7]= 21;
	finfo.field_type[8]=SM_VARCHAR;	//C_CITY
	finfo.field_size[8]= 21;
	finfo.field_type[9]=SM_CHAR;	//C_STATE
	finfo.field_size[9]= 4;
	finfo.field_type[10]=SM_CHAR;//C_ZIP
	finfo.field_size[10]= 8;
	finfo.field_type[11]=SM_VARCHAR;//C_PHONE
	finfo.field_size[11]= 17;
	finfo.field_type[12]=SM_FLOAT;	//C_SINCE
	finfo.field_type[13]=SM_CHAR;	//C_CREDIT
	finfo.field_size[13]= 4;
	finfo.field_type[14]=SM_FLOAT;	//C_CREDIT_LIM
	finfo.field_type[15]=SM_FLOAT;	//C_DISCOUNT
	finfo.field_type[16]=SM_FLOAT;	//C_BALANCE
	finfo.field_type[17]=SM_FLOAT;	//C_YTD_PAYMENT
	finfo.field_type[18]=SM_FLOAT;	//C_LAST_PAYMENT
	finfo.field_type[19]=SM_INTEGER;//C_PAYMENT_CNT
	finfo.field_type[20]=SM_VARCHAR;//C_DATA_1
	finfo.field_size[20]= 251;
	finfo.field_type[21]=SM_VARCHAR;//C_DATA_2
	finfo.field_size[21]= 251;

	// Set filename for table data
	char data_file[101];
	sprintf(data_file, "Data/Customer.data");


	flid_t f;
	rc_t rc;

	FILE* fp;

	hpl_record_t hpl_rec(finfo.no_fields, finfo.no_ffields);
	for (f = 0; f < finfo.no_fields; ++f)
		hpl_rec.fieldSize[f] = finfo.field_size[f];


	if ((fp = fopen(data_file, "r")) == NULL) {
		cout << "Error: Could not load " << data_file << endl;
		return RC_AUGMENT(rc);
	}
	// read the line
	char line[1000 + 1];
	int scanned = 0;
	while (fgets(line, 1000, fp) != NULL) {
		scanned++;
		W_DO(this->get_hpl_rec_from_line(finfo, hpl_rec, line));


		// customer
		prcust->set_value(0, *(int*)hpl_rec.data[0]);			// c_id
		prcust->set_value(1, *(int*)hpl_rec.data[1]);			// c_d_id
		prcust->set_value(2, *(int*)hpl_rec.data[2]);			// c_w_id
		prcust->set_value(3, (char*)hpl_rec.data[3]);			// c_first
		prcust->set_value(4, (char*)hpl_rec.data[4]);			// c_middle
		prcust->set_value(5, (char*)hpl_rec.data[5]);			// c_last
		prcust->set_value(6, (char*)hpl_rec.data[6]);			// C_STREET1
		prcust->set_value(7, (char*)hpl_rec.data[7]);			// C_STREET2
		prcust->set_value(8, (char*)hpl_rec.data[8]); 		// C_CITY
		prcust->set_value(9, (char*)hpl_rec.data[9]); 		// C_STATE
		prcust->set_value(10, (char*)hpl_rec.data[10]);		// C_ZIP
		prcust->set_value(11, (char*)hpl_rec.data[11]);		// C_PHONE
		prcust->set_value(12, *(double*)hpl_rec.data[12]);	// C_SINCE
		prcust->set_value(13, (char*)hpl_rec.data[13]);		// C_CREDIT
		prcust->set_value(14, *(double*)hpl_rec.data[14]); 	// C_CREDIT_LIM
		prcust->set_value(15, *(double*)hpl_rec.data[15]); 	// C_DISCOUNT
		prcust->set_value(16, *(double*)hpl_rec.data[16]); 	// C_BALANCE
		prcust->set_value(17, *(double*)hpl_rec.data[17]); 	// C_YTD_PAYMENT
		prcust->set_value(18, *(double*)hpl_rec.data[18]); 	// C_LAST_PAYMENT
		prcust->set_value(19, *(int*)hpl_rec.data[19]); 		// C_PAYMENT_CNT
		prcust->set_value(20, (char*)hpl_rec.data[20]); 		// C_DATA_1
		prcust->set_value(21, (char*)hpl_rec.data[21]); 		// C_DATA_2
		e = this->add_tuple(_pssm, prcust);



		for (f = 0; f < finfo.no_fields; ++f) {
			delete hpl_rec.data[f];
			hpl_rec.data[f] = NULL;
		}
	}

	fclose(fp);


	return e;
}

/* --------------- */
/* --- HISTORY --- */
/* --------------- */

w_rc_t history_man_impl::hist_load_fromFile(ss_m* _pssm, history_tuple* tuple)
{
	w_rc_t e = RCOK;


	//==========================================================
	// read from file HISTORY
	//==========================================================
	file_info_t finfo(8,7);
	finfo.field_type[0]=SM_INTEGER;	//H_C_ID
	finfo.field_type[1]=SM_INTEGER;	//H_C_D_ID
	finfo.field_type[2]=SM_INTEGER;	//H_C_W_ID
	finfo.field_type[3]=SM_INTEGER;	//H_D_ID
	finfo.field_type[4]=SM_INTEGER;	//H_W_ID
	finfo.field_type[5]=SM_FLOAT;	//H_DATE
	finfo.field_type[6]=SM_FLOAT;	//H_AMOUNT
	finfo.field_type[7]=SM_VARCHAR;	//H_DATA
	finfo.field_size[7]= 26;


	// Set filename for table data
	char data_file[101];
	sprintf(data_file, "Data/History.data");


	flid_t f;
	rc_t rc;

	FILE* fp;

	hpl_record_t hpl_rec(finfo.no_fields, finfo.no_ffields);
	for (f = 0; f < finfo.no_fields; ++f)
		hpl_rec.fieldSize[f] = finfo.field_size[f];


	if ((fp = fopen(data_file, "r")) == NULL) {
		cout << "Error: Could not load " << data_file << endl;
		return RC_AUGMENT(rc);
	}
	// read the line
	char line[1000 + 1];
	int scanned = 0;
	while (fgets(line, 1000, fp) != NULL) {
		scanned++;
		W_DO(this->get_hpl_rec_from_line(finfo, hpl_rec, line));


		// HISTORY
		tuple->set_value(0, *(int*)hpl_rec.data[0]);			// H_C_ID
		tuple->set_value(1, *(int*)hpl_rec.data[1]);			// H_C_D_ID
		tuple->set_value(2, *(int*)hpl_rec.data[2]);			// H_C_W_ID
		tuple->set_value(3, *(int*)hpl_rec.data[3]);			// H_D_ID
		tuple->set_value(4, *(int*)hpl_rec.data[4]);			// H_W_ID
		tuple->set_value(5, *(double*)hpl_rec.data[5]);		// H_DATE
		tuple->set_value(6, *(double*)hpl_rec.data[6]);		// H_AMOUNT
		tuple->set_value(7, (char*)hpl_rec.data[7]);		// H_DATA
		e = this->add_tuple(_pssm, tuple);


		for (f = 0; f < finfo.no_fields; ++f) {
			delete hpl_rec.data[f];
			hpl_rec.data[f] = NULL;
		}
	}

	fclose(fp);


	return e;
}

/* ----------------- */
/* --- NEW_ORDER --- */
/* ----------------- */

				    
w_rc_t new_order_man_impl::no_get_iter_by_index(ss_m* db,
                                                new_order_index_iter* &iter,
                                                new_order_tuple* ptuple,
                                                rep_row_t &replow,
                                                rep_row_t &rephigh,
                                                const int w_id,
                                                const int d_id,
                                                lock_mode_t alm,
                                                bool need_tuple)
{
    assert (ptuple);

    // find the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("NO_IDX");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(0, 0);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(1, d_id+1);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    /* get the tuple iterator (index only scan) */
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


w_rc_t new_order_man_impl::no_delete_by_index(ss_m* db,
                                              new_order_tuple* ptuple,
                                              const int w_id,
                                              const int d_id,
                                              const int o_id)
{
    assert (ptuple);

    // 1. idx probe new_order
    // 2. deletes the retrieved new_order

    ptuple->set_value(0, o_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
//     W_DO(index_probe_forupdate_by_name(db, "NO_IDX", ptuple));
    W_DO(delete_tuple(db, ptuple));

    return (RCOK);
}


w_rc_t new_order_man_impl::new_order_load_fromFile(ss_m* _pssm, new_order_tuple* tuple)
{
	w_rc_t e = RCOK;


	//==========================================================
	// read from file NEW_ORDER
	//==========================================================
	file_info_t finfo(3,3);
	finfo.field_type[0]=SM_INTEGER;	//NO_O_ID
	finfo.field_type[1]=SM_INTEGER;	//NO_D_ID
	finfo.field_type[2]=SM_INTEGER;	//NO_W_ID


	// Set filename for table data
	char data_file[101];
	sprintf(data_file, "Data/NewOrder.data");


	flid_t f;
	rc_t rc;

	FILE* fp;

	hpl_record_t hpl_rec(finfo.no_fields, finfo.no_ffields);
	for (f = 0; f < finfo.no_fields; ++f)
		hpl_rec.fieldSize[f] = finfo.field_size[f];


	if ((fp = fopen(data_file, "r")) == NULL) {
		cout << "Error: Could not load " << data_file << endl;
		return RC_AUGMENT(rc);
	}
	// read the line
	char line[1000 + 1];
	int scanned = 0;
	while (fgets(line, 1000, fp) != NULL) {
		scanned++;
		W_DO(this->get_hpl_rec_from_line(finfo, hpl_rec, line));


		// NEW_ORDER
		tuple->set_value(0, *(int*)hpl_rec.data[0]);			// c_id
		tuple->set_value(1, *(int*)hpl_rec.data[1]);			// c_d_id
		tuple->set_value(2, *(int*)hpl_rec.data[2]);			// c_w_id
		e = this->add_tuple(_pssm, tuple);



		for (f = 0; f < finfo.no_fields; ++f) {
			delete hpl_rec.data[f];
			hpl_rec.data[f] = NULL;
		}
	}

	fclose(fp);


	return e;
}



/* ------------- */
/* --- ORDER --- */
/* ------------- */


w_rc_t order_man_impl::ord_get_iter_by_index(ss_m* db,
                                             order_index_iter* &iter,
                                             order_tuple* ptuple,
                                             rep_row_t &replow,
                                             rep_row_t &rephigh,
                                             const int w_id,
                                             const int d_id,
                                             const int c_id,
                                             lock_mode_t alm,
                                             bool need_tuple)
{
    assert (ptuple);

    // find index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("O_CUST_IDX");
    assert (pindex);

    /* get the lowest key value */
    ptuple->set_value(0, 0);
    ptuple->set_value(1, c_id);
    ptuple->set_value(2, d_id);
    ptuple->set_value(3, w_id);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(1, c_id+1);

    int highsz  = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}


w_rc_t order_man_impl::ord_update_carrier_by_index(ss_m* db,
                                                   order_tuple* ptuple,
                                                   const int carrier_id)
{
    assert (ptuple);

    // 1. idx probe for update the order
    // 2. update carrier_id and update table

    W_DO(index_probe_forupdate_by_name(db, "O_IDX", ptuple));

    ptuple->set_value(5, carrier_id);
    W_DO(update_tuple(db, ptuple));

    return (RCOK);
}

w_rc_t order_man_impl::order_load_fromFile(ss_m* _pssm, order_tuple* tuple)
{
	w_rc_t e = RCOK;


	//==========================================================
	// read from file ORDER
	//==========================================================
	file_info_t finfo(8,8);
	finfo.field_type[0]=SM_INTEGER;	//O_ID
	finfo.field_type[1]=SM_INTEGER;	//O_C_ID
	finfo.field_type[2]=SM_INTEGER;	//O_D_ID
	finfo.field_type[3]=SM_INTEGER;	//O_W_ID
	finfo.field_type[4]=SM_FLOAT;	//O_ENTRY_D
	finfo.field_type[5]=SM_INTEGER;	//O_CARRIER_ID
	finfo.field_type[6]=SM_INTEGER;	//O_OL_CNT
	finfo.field_type[7]=SM_INTEGER;	//O_ALL_LOCAL


	// Set filename for table data
	char data_file[101];
	sprintf(data_file, "Data/Order.data");


	flid_t f;
	rc_t rc;

	FILE* fp;

	hpl_record_t hpl_rec(finfo.no_fields, finfo.no_ffields);
	for (f = 0; f < finfo.no_fields; ++f)
		hpl_rec.fieldSize[f] = finfo.field_size[f];


	if ((fp = fopen(data_file, "r")) == NULL) {
		cout << "Error: Could not load " << data_file << endl;
		return RC_AUGMENT(rc);
	}
	// read the line
	char line[1000 + 1];
	int scanned = 0;
	while (fgets(line, 1000, fp) != NULL) {
		scanned++;
		W_DO(this->get_hpl_rec_from_line(finfo, hpl_rec, line));


		// ORDER
		tuple->set_value(0, *(int*)hpl_rec.data[0]);			// O_ID
		tuple->set_value(1, *(int*)hpl_rec.data[1]);			// O_C_ID
		tuple->set_value(2, *(int*)hpl_rec.data[2]);			// O_D_ID
		tuple->set_value(3, *(int*)hpl_rec.data[3]);			// O_W_ID
		tuple->set_value(4, *(double*)hpl_rec.data[4]);			// O_ENTRY_D
		tuple->set_value(5, *(int*)hpl_rec.data[5]);			// O_CARRIER_ID
		tuple->set_value(6, *(int*)hpl_rec.data[6]);			// O_OL_CNT
		tuple->set_value(7, *(int*)hpl_rec.data[7]);			// O_ALL_LOCAL
		e = this->add_tuple(_pssm, tuple);



		for (f = 0; f < finfo.no_fields; ++f) {
			delete hpl_rec.data[f];
			hpl_rec.data[f] = NULL;
		}
	}

	fclose(fp);


	return e;
}



/* ----------------- */
/* --- ORDERLINE --- */
/* ----------------- */


w_rc_t order_line_man_impl::ol_get_range_iter_by_index(ss_m* db,
                                                       order_line_index_iter* &iter,
                                                       order_line_tuple* ptuple,
                                                       rep_row_t &replow,
                                                       rep_row_t &rephigh,
                                                       const int w_id,
                                                       const int d_id,
                                                       const int low_o_id,
                                                       const int high_o_id,
                                                       lock_mode_t alm,
                                                       bool need_tuple)
{
    assert (ptuple);

    // OL_IDX - { 2, 1, 0, 3 } = { OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER }

    // pointer to the index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("OL_IDX");
    assert (pindex);

    // get the lowest key value
    ptuple->set_value(0, low_o_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    ptuple->set_value(3, (int)0);  /* assuming that ol_number starts from 1 */

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    // get the highest key value
    ptuple->set_value(0, high_o_id+1);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);
    
    // get the tuple iterator (not index only scan)
    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}



w_rc_t order_line_man_impl::ol_get_probe_iter_by_index(ss_m* db,
                                                       order_line_index_iter* &iter,
                                                       order_line_tuple* ptuple,
                                                       rep_row_t &replow,
                                                       rep_row_t &rephigh,
                                                       const int w_id,
                                                       const int d_id,
                                                       const int o_id,
                                                       lock_mode_t alm,
                                                       bool need_tuple)
{
    assert (ptuple);

    // OL_IDX - { 2, 1, 0, 3 } = { OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER }

    // find index
    assert (_ptable);
    index_desc_t* pindex = _ptable->find_index("OL_IDX");
    assert (pindex);

    ptuple->set_value(0, o_id);
    ptuple->set_value(1, d_id);
    ptuple->set_value(2, w_id);
    ptuple->set_value(3, (int)0);

    int lowsz = format_key(pindex, ptuple, replow);
    assert (replow._dest);

    /* get the highest key value */
    ptuple->set_value(0, o_id+1);

    int highsz = format_key(pindex, ptuple, rephigh);
    assert (rephigh._dest);

    W_DO(get_iter_for_index_scan(db, pindex, iter,
                                 alm, need_tuple,
				 scan_index_i::ge, vec_t(replow._dest, lowsz),
				 scan_index_i::lt, vec_t(rephigh._dest, highsz)));
    return (RCOK);
}

w_rc_t order_line_man_impl::order_line_load_fromFile(ss_m* _pssm, order_line_tuple* tuple)
{
	w_rc_t e = RCOK;


	//==========================================================
	// read from file ORDERLINE
	//==========================================================
	file_info_t finfo(10,9);
	finfo.field_type[0]=SM_INTEGER;	//OL_O_ID
	finfo.field_type[1]=SM_INTEGER;	//OL_D_ID
	finfo.field_type[2]=SM_INTEGER;	//OL_W_ID
	finfo.field_type[3]=SM_INTEGER;	//OL_NUMBER
	finfo.field_type[4]=SM_INTEGER;	//OL_I_ID
	finfo.field_type[5]=SM_INTEGER;	//OL_SUPPLY_W_ID
	finfo.field_type[6]=SM_FLOAT;	//OL_DELIVERY_D
	finfo.field_type[7]=SM_INTEGER;	//OL_QUANTITY
	finfo.field_type[8]=SM_INTEGER;	//OL_AMOUNT
	finfo.field_type[9]=SM_VARCHAR;//OL_DIST_INFO
	finfo.field_size[9]= 26;


	// Set filename for table data
	char data_file[101];
	sprintf(data_file, "Data/OrderLine.data");


	flid_t f;
	rc_t rc;

	FILE* fp;

	hpl_record_t hpl_rec(finfo.no_fields, finfo.no_ffields);
	for (f = 0; f < finfo.no_fields; ++f)
		hpl_rec.fieldSize[f] = finfo.field_size[f];


	if ((fp = fopen(data_file, "r")) == NULL) {
		cout << "Error: Could not load " << data_file << endl;
		return RC_AUGMENT(rc);
	}
	// read the line
	char line[1000 + 1];
	int scanned = 0;
	while (fgets(line, 1000, fp) != NULL) {
		scanned++;
		W_DO(this->get_hpl_rec_from_line(finfo, hpl_rec, line));


		// ORDERLINE
		tuple->set_value(0, *(int*)hpl_rec.data[0]);			// OL_O_ID
		tuple->set_value(1, *(int*)hpl_rec.data[1]);			// OL_D_ID
		tuple->set_value(2, *(int*)hpl_rec.data[2]);			// OL_W_ID
		tuple->set_value(3, *(int*)hpl_rec.data[3]);			// OL_NUMBER
		tuple->set_value(4, *(int*)hpl_rec.data[4]);			// OL_I_ID
		tuple->set_value(5, *(int*)hpl_rec.data[5]);			// OL_SUPPLY_W_ID
		tuple->set_value(6, *(double*)hpl_rec.data[6]);			// OL_DELIVERY_D
		tuple->set_value(7, *(int*)hpl_rec.data[7]);			// OL_QUANTITY
		tuple->set_value(8, *(int*)hpl_rec.data[8]);			// OL_AMOUNT
		tuple->set_value(9, (char*)hpl_rec.data[9]);			// OL_DIST_INFO
		e = this->add_tuple(_pssm, tuple);



		for (f = 0; f < finfo.no_fields; ++f) {
			delete hpl_rec.data[f];
			hpl_rec.data[f] = NULL;
		}
	}

	fclose(fp);


	return e;
}


/* ------------ */
/* --- ITEM --- */
/* ------------ */


w_rc_t item_man_impl::it_index_probe(ss_m* db, 
                                     item_tuple* ptuple,
                                     const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    return (index_probe_by_name(db, "I_IDX", ptuple));
}

w_rc_t item_man_impl::it_index_probe_forupdate(ss_m* db, 
                                               item_tuple* ptuple,
                                               const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    return (index_probe_forupdate_by_name(db, "I_IDX", ptuple));
}

w_rc_t item_man_impl::item_load_fromFile(ss_m* _pssm, item_tuple* tuple)
{
	w_rc_t e = RCOK;


	//==========================================================
	// read from file ITEM
	//==========================================================
	file_info_t finfo(5,3);
	finfo.field_type[0]=SM_INTEGER;	//I_ID
	finfo.field_type[1]=SM_INTEGER;	//I_IM_ID
	finfo.field_type[2]=SM_VARCHAR;	//I_NAME
	finfo.field_size[2]=26;
	finfo.field_type[3]=SM_INTEGER;	//I_PRICE
	finfo.field_type[4]=SM_VARCHAR;	//I_DATA
	finfo.field_size[4]=51;


	// Set filename for table data
	char data_file[101];
	sprintf(data_file, "Data/Item.data");


	flid_t f;
	rc_t rc;

	FILE* fp;

	hpl_record_t hpl_rec(finfo.no_fields, finfo.no_ffields);
	for (f = 0; f < finfo.no_fields; ++f)
		hpl_rec.fieldSize[f] = finfo.field_size[f];


	if ((fp = fopen(data_file, "r")) == NULL) {
		cout << "Error: Could not load " << data_file << endl;
		return RC_AUGMENT(rc);
	}
	// read the line
	char line[1000 + 1];
	int scanned = 0;
	while (fgets(line, 1000, fp) != NULL) {
		scanned++;
		W_DO(this->get_hpl_rec_from_line(finfo, hpl_rec, line));


		// ITEM
		tuple->set_value(0, *(int*)hpl_rec.data[0]);			// I_ID
		tuple->set_value(1, *(int*)hpl_rec.data[1]);			// I_IM_ID
		tuple->set_value(2, (char*)hpl_rec.data[2]);			// I_NAME
		tuple->set_value(3, *(int*)hpl_rec.data[3]);			// I_PRICE
		tuple->set_value(4, (char*)hpl_rec.data[4]);			// I_DATA
		e = this->add_tuple(_pssm, tuple);



		for (f = 0; f < finfo.no_fields; ++f) {
			delete hpl_rec.data[f];
			hpl_rec.data[f] = NULL;
		}
	}

	fclose(fp);


	return e;
}

/* ------------- */
/* --- STOCK --- */
/* ------------- */


w_rc_t stock_man_impl::st_index_probe(ss_m* db,
                                      stock_tuple* ptuple,
                                      const int w_id,
                                      const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    ptuple->set_value(1, w_id);
    return (index_probe_by_name(db, "S_IDX", ptuple));
}

w_rc_t stock_man_impl::st_index_probe_forupdate(ss_m* db,
                                                stock_tuple* ptuple,
                                                const int w_id,
                                                const int i_id)
{
    assert (ptuple);
    ptuple->set_value(0, i_id);
    ptuple->set_value(1, w_id);
    return (index_probe_forupdate_by_name(db, "S_IDX", ptuple));
}

w_rc_t  stock_man_impl::st_update_tuple(ss_m* db,
                                        stock_tuple* ptuple,
                                        const tpcc_stock_tuple* pstock,
                                        lock_mode_t lm)
{
    // 1. updates the specified tuple

    assert (ptuple);
    assert (ptuple->is_rid_valid());

    ptuple->set_value(2, pstock->S_REMOTE_CNT);
    ptuple->set_value(3, pstock->S_QUANTITY);
    ptuple->set_value(4, pstock->S_ORDER_CNT);
    ptuple->set_value(5, pstock->S_YTD);
    //    return (table_man_impl<stock_t>::update_tuple(db, ptuple));
    return (update_tuple(db, ptuple, lm));
}

w_rc_t stock_man_impl::st_load_fromFile(ss_m* _pssm, stock_tuple* tuple)
{
	w_rc_t e = RCOK;


	//==========================================================
	// read from file STOCK
	//==========================================================
	file_info_t finfo(17,6);
	finfo.field_type[0]=SM_INTEGER;	//S_I_ID
	finfo.field_type[1]=SM_INTEGER;	//S_W_ID
	finfo.field_type[2]=SM_INTEGER;	//S_REMOTE_CNT
	finfo.field_type[3]=SM_INTEGER;	//S_QUANTITY
	finfo.field_type[4]=SM_INTEGER;	//S_ORDER_CNT
	finfo.field_type[5]=SM_INTEGER;	//S_YTD
	finfo.field_type[6]=SM_VARCHAR;	//S_DIST0
	finfo.field_size[6]= 25;
	finfo.field_type[7]=SM_VARCHAR;	//S_DIST1
	finfo.field_size[7]= 25;
	finfo.field_type[8]=SM_VARCHAR;	//S_DIST2
	finfo.field_size[8]= 25;
	finfo.field_type[9]=SM_VARCHAR;	//S_DIST3
	finfo.field_size[9]= 25;
	finfo.field_type[10]=SM_VARCHAR;	//S_DIST4
	finfo.field_size[10]= 25;
	finfo.field_type[11]=SM_VARCHAR;	//S_DIST5
	finfo.field_size[11]= 25;
	finfo.field_type[12]=SM_VARCHAR;	//S_DIST6
	finfo.field_size[12]= 25;
	finfo.field_type[13]=SM_VARCHAR;	//S_DIST7
	finfo.field_size[13]= 25;
	finfo.field_type[14]=SM_VARCHAR;	//S_DIST8
	finfo.field_size[14]= 25;
	finfo.field_type[15]=SM_VARCHAR;	//S_DIST9
	finfo.field_size[15]= 25;
	finfo.field_type[16]=SM_VARCHAR;	//S_DATA
	finfo.field_size[16]= 51;


	// Set filename for table data
	char data_file[101];
	sprintf(data_file, "Data/Stock.data");


	flid_t f;
	rc_t rc;

	FILE* fp;

	hpl_record_t hpl_rec(finfo.no_fields, finfo.no_ffields);
	for (f = 0; f < finfo.no_fields; ++f)
		hpl_rec.fieldSize[f] = finfo.field_size[f];


	if ((fp = fopen(data_file, "r")) == NULL) {
		cout << "Error: Could not load " << data_file << endl;
		return RC_AUGMENT(rc);
	}
	// read the line
	char line[1000 + 1];
	int scanned = 0;
	while (fgets(line, 1000, fp) != NULL) {
		scanned++;
		W_DO(this->get_hpl_rec_from_line(finfo, hpl_rec, line));


		// STOCK
		tuple->set_value(0, *(int*)hpl_rec.data[0]);			// S_I_ID
		tuple->set_value(1, *(int*)hpl_rec.data[1]);			// S_W_ID
		tuple->set_value(2, *(int*)hpl_rec.data[2]);			// S_REMOTE_CNT
		tuple->set_value(3, *(int*)hpl_rec.data[3]);			// S_QUANTITY
		tuple->set_value(4, *(int*)hpl_rec.data[4]);			// S_ORDER_CNT
		tuple->set_value(5, *(int*)hpl_rec.data[5]);			// S_YTD
		tuple->set_value(6, (char*)hpl_rec.data[6]);		// S_DIST0
		tuple->set_value(7, (char*)hpl_rec.data[7]);		// S_DIST1
		tuple->set_value(8, (char*)hpl_rec.data[8]);		// S_DIST2
		tuple->set_value(9, (char*)hpl_rec.data[9]);		// S_DIST3
		tuple->set_value(10, (char*)hpl_rec.data[10]);		// S_DIST4
		tuple->set_value(11, (char*)hpl_rec.data[11]);		// S_DIST5
		tuple->set_value(12, (char*)hpl_rec.data[12]);		// S_DIST6
		tuple->set_value(13, (char*)hpl_rec.data[13]);		// S_DIST7
		tuple->set_value(14, (char*)hpl_rec.data[14]);		// S_DIST8
		tuple->set_value(15, (char*)hpl_rec.data[15]);		// S_DIST9
		tuple->set_value(16, (char*)hpl_rec.data[16]);		// S_DATA
		e = this->add_tuple(_pssm, tuple);



		for (f = 0; f < finfo.no_fields; ++f) {
			delete hpl_rec.data[f];
			hpl_rec.data[f] = NULL;
		}
	}

	fclose(fp);


	return e;
}

EXIT_NAMESPACE(tpcc);
