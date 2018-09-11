/*
 * TPC-H Query class implementation
 */

#include "sm_tpch.h"
#include "sm_exec.h"
#include "ahhjoin.h"
#include "hpl_p.h"
#include <TimeMeasure.h>

/* 
 * Query 1
 * 
 * select
 *         l_returnflag,
 *         l_linestatus,
 *         sum(l_quantity) as sum_qty,
 *         sum(l_extendedprice) as sum_base_price,
 *         sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
 *         sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) 
 *                                                     as sum_charge,
 *         avg(l_quantity) as avg_qty,
 *         avg(l_extendedprice) as avg_price,
 *         avg(l_discount) as avg_disc,
 *         count(*) as count_order
 * from
 *         tpcd.lineitem
 * where
 *         l_shipdate <= "1998-08-07"
 * group by
 *         l_returnflag,
 *         l_linestatus
 * order by
 *         l_returnflag,
 *         l_linestatus;
 *
 *
 */

w_rc_t tpch_query_1::run_nsm()
{
	scan_file_i scan(finfo[0].fid, cc);
	pin_i* handle;
	bool eof = false;
	const char* frec;
	int idx;
	rc_t rc;
	double tax, quantity, xprice, discount;

	time_t timestamp;
	char* consTime = "1998-08-07";
	timestamp = convertYYMMDDtoTimestamp(consTime);

	while (1)
	{
		W_DO(scan.next(handle, 0, eof));
		if (eof == false)
		{
			scanned++;
			frec = handle->body();
			// apply condition
			//if (COMPARE_DATE(FIELD_START(finfo[0],frec,L_SHIPDATE),"1998-08-07") <= 0)
			if (COMPARE_NO(*(time_t*)FIELD_START(finfo[0],frec,L_SHIPDATE), timestamp) <= 0)
			{
				qualified++;
				switch (FIELD_START(finfo[0],frec,L_RETURNFLAG)[0])
				{
				case 'A':
					w_assert1(FIELD_START(finfo[0],frec,L_LINESTATUS)[0]=='F');
					idx = AF;
					break;
				case 'N':
					if (FIELD_START(finfo[0],frec,L_LINESTATUS)[0] == 'F')
						idx = NF;
					else
					{
						w_assert1(FIELD_START(finfo[0],frec,L_LINESTATUS)[0]=='O');
						idx = NO;
					}
					break;
				case 'R':
					w_assert1(FIELD_START(finfo[0],frec,L_LINESTATUS)[0]=='F');
					idx = RF;
					break;
				default:
					return RC_AUGMENT(rc);
				}
				memcpy((char*) &quantity,
						FIELD_START(finfo[0],frec,L_QUANTITY), sizeof(double));
				memcpy((char*) &xprice,
						FIELD_START(finfo[0],frec,L_EXTENDEDPRICE),
						sizeof(double));
				memcpy((char*) &discount,
						FIELD_START(finfo[0],frec,L_DISCOUNT), sizeof(double));
				memcpy((char*) &tax, FIELD_START(finfo[0],frec,L_TAX),
						sizeof(double));
				sum_qty[idx] += quantity;
				sum_base_price[idx] += xprice;
				sum_disc_price[idx] += xprice * (1 - discount);
				sum_charge[idx] += xprice * (1 - discount) * (1 + tax);
				sum_disc[idx] += discount;
				count_order[idx] += 1;
			}
		}
		else
			break;
	}
	return RCOK;
}

w_rc_t tpch_query_1::run_pax()
{
	flid_t ifi_lineitem[7];
	ifi_lineitem[0] = L_QUANTITY;
	ifi_lineitem[1] = L_EXTENDEDPRICE;
	ifi_lineitem[2] = L_DISCOUNT;
	ifi_lineitem[3] = L_TAX;
	ifi_lineitem[4] = L_RETURNFLAG;
	ifi_lineitem[5] = L_LINESTATUS;
	ifi_lineitem[6] = L_SHIPDATE;

	const flid_t lq = 0, le = 1, ld = 2, lt = 3, lr = 4, ll = 5, ls = 6;

	const precord_t* prec;

	scan_pfile_i scan(finfo[0].fid, cc, 7, ifi_lineitem);
	pin_prec_i *handle;

	bool eof = false;

	int idx;
	rc_t rc;

	time_t timestamp;
	char* consTime = "1998-08-07";
	timestamp = convertYYMMDDtoTimestamp(consTime);

	while (1)
	{
		//TimeMeasure time1("timemeasure.P1.T1.scan.csv");
		//time1.startTimer();
		W_DO(scan.next(handle, eof));
		//time1.stopTimer();
		//time1.printData();

		if (eof == false)
		{
			scanned++;
			prec = &(handle->precord());
			//if (COMPARE_DATE(prec->data[ls], "1998-08-07") <= 0)
			if (COMPARE_NO(*(time_t*)prec->data[ls], timestamp) <= 0)
			{
				qualified++;
				switch (prec->data[lr][0])
				{
				case 'A':
					w_assert1(prec->data[ll][0]=='F');
					idx = AF;
					break;
				case 'N':
					if (prec->data[ll][0] == 'F')
						idx = NF;
					else
					{
						w_assert1(prec->data[ll][0]=='O');
						idx = NO;
					}
					break;
				case 'R':
					w_assert1(prec->data[ll][0]=='F');
					idx = RF;
					break;
				default:
					return RC_AUGMENT(rc);
				}
				sum_qty[idx] += *(double*) (prec->data[lq]);
				sum_base_price[idx] += *(double*) (prec->data[le]);
				sum_disc_price[idx] += (*(double*) (prec->data[le]) * (1
						- *(double*) (prec->data[ld])));
				sum_charge[idx] += (*(double*) (prec->data[le]) * (1
						- *(double*) (prec->data[ld])) * (1
						+ *(double*) (prec->data[lt])));
				sum_disc[idx] += *(double*) (prec->data[ld]);
				count_order[idx] += 1;
			}
		}
		else
			break;
	}
	return RCOK;
}

w_rc_t tpch_query_1::run_hpl()
{
	flid_t ifi_lineitem[7];
	ifi_lineitem[0] = L_QUANTITY;
	ifi_lineitem[1] = L_EXTENDEDPRICE;
	ifi_lineitem[2] = L_DISCOUNT;
	ifi_lineitem[3] = L_TAX;
	ifi_lineitem[4] = L_RETURNFLAG;
	ifi_lineitem[5] = L_LINESTATUS;
	ifi_lineitem[6] = L_SHIPDATE;

	const flid_t lq = 0, le = 1, ld = 2, lt = 3, lr = 4, ll = 5, ls = 6;

	const hpl_record_t* hpl_rec;

	scan_hpl_p_i scan(finfo[0].fid, cc, 7, ifi_lineitem);
	pin_hpl_rec_i *handle;

	bool eof = false;

	int idx;
	rc_t rc;

	time_t timestamp;
	char* consTime = "1998-08-07";
	timestamp = convertYYMMDDtoTimestamp(consTime);

	while (1)
	{
		//TimeMeasure time1("timemeasure.P2.T1.scan.csv");
		//time1.startTimer();
		W_DO(scan.next(handle, eof));
		//time1.stopTimer();
		//time1.printData();


		if (eof == false)
		{
			scanned++;
			hpl_rec = &(handle->hpl_record());
			//if (COMPARE_DATE(hpl_rec->data[ls], "1998-08-07") <= 0)
			if (COMPARE_NO(*(time_t*)hpl_rec->data[ls], timestamp) <= 0)
			{
				if(handle->isValidRec())
				{
					qualified++;
					switch (hpl_rec->data[lr][0])
					{
					case 'A':
						w_assert1(hpl_rec->data[ll][0]=='F');
						idx = AF;
						break;
					case 'N':
						if (hpl_rec->data[ll][0] == 'F')
							idx = NF;
						else
						{
							w_assert1(hpl_rec->data[ll][0]=='O');
							idx = NO;
						}
						break;
					case 'R':
						w_assert1(hpl_rec->data[ll][0]=='F');
						idx = RF;
						break;
					default:
						return RC_AUGMENT(rc);
					}
					sum_qty[idx] += *(double*) (hpl_rec->data[lq]);
					sum_base_price[idx] += *(double*) (hpl_rec->data[le]);
					sum_disc_price[idx] += (*(double*) (hpl_rec->data[le]) * (1
							- *(double*) (hpl_rec->data[ld])));
					sum_charge[idx] += (*(double*) (hpl_rec->data[le]) * (1
							- *(double*) (hpl_rec->data[ld])) * (1
							+ *(double*) (hpl_rec->data[lt])));
					sum_disc[idx] += *(double*) (hpl_rec->data[ld]);
					count_order[idx] += 1;
				}
			}
		}
		else
			break;
	}
	return RCOK;
}

w_rc_t tpch_query_1::run_dsm()
{
	file_info_t finfo_i;

	// scan all subrelations in parallel
	W_DO(find_field_vol_assoc(finfo[0].table_name, L_QUANTITY, finfo_i));
	scan_file_i scan_lq(finfo_i.fid, cc);
	pin_i* handle_lq;
	W_DO(find_field_vol_assoc(finfo[0].table_name, L_EXTENDEDPRICE, finfo_i));
	scan_file_i scan_le(finfo_i.fid, cc);
	pin_i* handle_le;
	W_DO(find_field_vol_assoc(finfo[0].table_name, L_DISCOUNT, finfo_i));
	scan_file_i scan_ld(finfo_i.fid, cc);
	pin_i* handle_ld;
	W_DO(find_field_vol_assoc(finfo[0].table_name, L_TAX, finfo_i));
	scan_file_i scan_lt(finfo_i.fid, cc);
	pin_i* handle_lt;
	W_DO(find_field_vol_assoc(finfo[0].table_name, L_RETURNFLAG, finfo_i));
	scan_file_i scan_lr(finfo_i.fid, cc);
	pin_i* handle_lr;
	W_DO(find_field_vol_assoc(finfo[0].table_name, L_LINESTATUS, finfo_i));
	scan_file_i scan_ll(finfo_i.fid, cc);
	pin_i* handle_ll;
	W_DO(find_field_vol_assoc(finfo[0].table_name, L_SHIPDATE, finfo_i));
	scan_file_i scan_ls(finfo_i.fid, cc);
	pin_i* handle_ls;

	uint4_t *qual_rids = new uint4_t[MAX_QUAL_RIDS];

	uint4_t qr, idx;
	bool eof = false;

	rc_t rc;

	// collect qualifying oids

	while (1)
	{
		W_DO(scan_ls.next(handle_ls, 0, eof));
		if (eof == false)
		{
			if (COMPARE_DATE(handle_ls->body(), "1998-08-07") <= 0)
			{
				w_assert3(qualified < MAX_QUAL_RIDS);
				qual_rids[qualified] = *(uint4_t*) (handle_ls->hdr());
				qualified++;
			}
		}
		else
			break;
		scanned++;
	}

	qr = 0;
	// Now sweep through the selected attribute files
	while (qr < qualified)
	{
		W_DO(scan_lq.next(handle_lq, 0, eof)); w_assert1 (eof==false); // loop stops under another condition
		W_DO(scan_le.next(handle_le, 0, eof)); w_assert1 (eof==false); // loop stops under another condition
		W_DO(scan_ld.next(handle_ld, 0, eof)); w_assert1 (eof==false); // loop stops under another condition
		W_DO(scan_lt.next(handle_lt, 0, eof)); w_assert1 (eof==false); // loop stops under another condition
		W_DO(scan_lr.next(handle_lr, 0, eof)); w_assert1 (eof==false); // loop stops under another condition
		W_DO(scan_ll.next(handle_ll, 0, eof)); w_assert1 (eof==false); // loop stops under another condition

		switch (handle_lr->body()[0])
		{
		case 'A':
			w_assert1(handle_ll->body()[0]=='F');
			idx = AF;
			break;
		case 'N':
			if (handle_ll->body()[0] == 'F')
				idx = NF;
			else
			{
				w_assert1(handle_ll->body()[0]=='O');
				idx = NO;
			}
			break;
		case 'R':
			w_assert1(handle_ll->body()[0]=='F');
			idx = RF;
			break;
		default:
			return RC_AUGMENT(rc);
		}
		sum_qty[idx] += *(double*) (handle_lq->body());
		sum_base_price[idx] += *(double*) (handle_le->body());
		sum_disc_price[idx] += (*(double*) (handle_le->body()) * (1
				- *(double*) (handle_ld->body())));
		sum_charge[idx] += (*(double*) (handle_le->body()) * (1
				- *(double*) (handle_ld->body())) * (1
				+ *(double*) (handle_lt->body())));
		sum_disc[idx] += *(double*) (handle_ld->body());
		count_order[idx] += 1;

		qr++;
	}

	delete[] qual_rids;

	return RCOK;
}

w_rc_t tpch_query_1::run()
{
	W_DO(query_c::run());

	fprintf(output_file, "Scanned %d records, %d qualified.\n", scanned,
			qualified);

	fprintf(output_file, "QUERY RESULT: \n");
	fprintf(
			output_file,
			"\tL_RETURNFLAG\tL_LINESTATUS\tSUM_QTY\tSUM_BASE_PRICE\tSUM_DISC_PRICE\tSUM_CHARGE\tAVG_QTY\tAVG_PRICE\tAVG_DISC\tCOUNT_ORDER\n\n");
	fprintf(output_file,
			"\tA\t\tF\t\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\n",
			sum_qty[AF], sum_base_price[AF], sum_disc_price[AF],
			sum_charge[AF], sum_qty[AF] / count_order[AF], sum_disc[AF]
					/ count_order[AF], count_order[AF]);
	fprintf(output_file,
			"\tN\t\tF\t\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\n",
			sum_qty[NF], sum_base_price[NF], sum_disc_price[NF],
			sum_charge[NF], sum_qty[NF] / count_order[NF], sum_disc[NF]
					/ count_order[NF], count_order[NF]);
	fprintf(output_file,
			"\tN\t\tO\t\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\n",
			sum_qty[NO], sum_base_price[NO], sum_disc_price[NO],
			sum_charge[NO], sum_qty[NO] / count_order[NO], sum_disc[NO]
					/ count_order[NO], count_order[NO]);
	fprintf(output_file,
			"\tR\t\tF\t\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\n",
			sum_qty[RF], sum_base_price[RF], sum_disc_price[RF],
			sum_charge[RF], sum_qty[RF] / count_order[RF], sum_disc[RF]
					/ count_order[RF], count_order[RF]);
	return RCOK;
}


w_rc_t tpch_query_101::run_nsm()
{
	scan_file_i scan(finfo[0].fid, cc);
	pin_i* handle;
	bool eof = false;
	const char* frec;
	int idx;
	rc_t rc;
	double tax, quantity, xprice, discount;


	time_t timestamp;
	char* consTime = "1998-08-07";
	timestamp = convertYYMMDDtoTimestamp(consTime);

	while (1)
	{
		W_DO(scan.next(handle, 0, eof));
		if (eof == false)
		{
			scanned++;
			frec = handle->body();
			// apply condition
			//if (COMPARE_DATE(FIELD_START101(finfo[0],frec,L_SHIPDATE), "1998-08-07") <= 0)
			if (COMPARE_NO(*(time_t*)FIELD_START101(finfo[0],frec,L_SHIPDATE), timestamp) <= 0)
			{
				qualified++;
				switch (FIELD_START101(finfo[0],frec,L_RETURNFLAG)[0])
				{
				case 'A':
					w_assert1(FIELD_START101(finfo[0],frec,L_LINESTATUS)[0]=='F');
					idx = AF;
					break;
				case 'N':
					if (FIELD_START101(finfo[0],frec,L_LINESTATUS)[0] == 'F')
						idx = NF;
					else
					{
						w_assert1(FIELD_START101(finfo[0],frec,L_LINESTATUS)[0]=='O');
						idx = NO;
					}
					break;
				case 'R':
					w_assert1(FIELD_START101(finfo[0],frec,L_LINESTATUS)[0]=='F');
					idx = RF;
					break;
				default:
					return RC_AUGMENT(rc);
				}
				memcpy((char*) &quantity,
						FIELD_START101(finfo[0],frec,L_QUANTITY), sizeof(double));
				memcpy((char*) &xprice,
						FIELD_START101(finfo[0],frec,L_EXTENDEDPRICE),
						sizeof(double));
				memcpy((char*) &discount,
						FIELD_START101(finfo[0],frec,L_DISCOUNT), sizeof(double));
				memcpy((char*) &tax, FIELD_START101(finfo[0],frec,L_TAX),
						sizeof(double));
				sum_qty[idx] += quantity;
				sum_base_price[idx] += xprice;
				sum_disc_price[idx] += xprice * (1 - discount);
				sum_charge[idx] += xprice * (1 - discount) * (1 + tax);
				sum_disc[idx] += discount;
				count_order[idx] += 1;
			}
		}
		else
			break;
	}
	return RCOK;
}

w_rc_t tpch_query_101::run_pax()
{
	flid_t ifi_lineitem[7];
	ifi_lineitem[0] = L2_QUANTITY;
	ifi_lineitem[1] = L2_EXTENDEDPRICE;
	ifi_lineitem[2] = L2_DISCOUNT;
	ifi_lineitem[3] = L2_TAX;
	ifi_lineitem[4] = L2_RETURNFLAG;
	ifi_lineitem[5] = L2_LINESTATUS;
	ifi_lineitem[6] = L2_SHIPDATE;

	const flid_t lq = 0, le = 1, ld = 2, lt = 3, lr = 4, ll = 5, ls = 6;

	const precord_t* prec;

	scan_pfile_i scan(finfo[0].fid, cc, 7, ifi_lineitem);
	pin_prec_i *handle;

	bool eof = false;

	int idx;
	rc_t rc;

	time_t timestamp;
	char* consTime = "1998-08-07";
	timestamp = convertYYMMDDtoTimestamp(consTime);

	while (1)
	{
		W_DO(scan.next(handle, eof));
		if (eof == false)
		{
			scanned++;
			prec = &(handle->precord());

			//if (COMPARE_DATE(prec->data[ls], "1998-08-07") <= 0)
			if (COMPARE_NO(*(time_t*)prec->data[ls], timestamp) <= 0)
			{
				qualified++;
				switch (prec->data[lr][0])
				{
				case 'A':
					w_assert1(prec->data[ll][0]=='F');
					idx = AF;
					break;
				case 'N':
					if (prec->data[ll][0] == 'F')
						idx = NF;
					else
					{
						w_assert1(prec->data[ll][0]=='O');
						idx = NO;
					}
					break;
				case 'R':
					w_assert1(prec->data[ll][0]=='F');
					idx = RF;
					break;
				default:
					return RC_AUGMENT(rc);
				}
				sum_qty[idx] += *(double*) (prec->data[lq]);
				sum_base_price[idx] += *(double*) (prec->data[le]);
				sum_disc_price[idx] += (*(double*) (prec->data[le]) * (1
						- *(double*) (prec->data[ld])));
				sum_charge[idx] += (*(double*) (prec->data[le]) * (1
						- *(double*) (prec->data[ld])) * (1
						+ *(double*) (prec->data[lt])));
				sum_disc[idx] += *(double*) (prec->data[ld]);
				count_order[idx] += 1;
			}
		}
		else
			break;
	}
	return RCOK;
}

w_rc_t tpch_query_101::run_hpl()
{
	flid_t ifi_lineitem[7];
	ifi_lineitem[0] = L2_QUANTITY;
	ifi_lineitem[1] = L2_EXTENDEDPRICE;
	ifi_lineitem[2] = L2_DISCOUNT;
	ifi_lineitem[3] = L2_TAX;
	ifi_lineitem[4] = L2_RETURNFLAG;
	ifi_lineitem[5] = L2_LINESTATUS;
	ifi_lineitem[6] = L2_SHIPDATE;

	const flid_t lq = 0, le = 1, ld = 2, lt = 3, lr = 4, ll = 5, ls = 6;

	const hpl_record_t* hpl_rec;
	scan_hpl_p_i scan(finfo[0].fid, cc, 7, ifi_lineitem);
	pin_hpl_rec_i *handle;

	bool eof = false;

	int idx;
	rc_t rc;

	time_t timestamp;
	char* consTime = "1998-08-07";
	timestamp = convertYYMMDDtoTimestamp(consTime);

	while (1)
	{
		W_DO(scan.next(handle, eof));
		if (eof == false)
		{
			scanned++;
			hpl_rec = &(handle->hpl_record());

			//if (COMPARE_DATE(hpl_rec->data[ls], "1998-08-07") <= 0)
			if (COMPARE_NO(*(time_t*)hpl_rec->data[ls], timestamp) <= 0)
			{
				qualified++;
				switch (hpl_rec->data[lr][0])
				{
				case 'A':
					w_assert1(hpl_rec->data[ll][0]=='F');
					idx = AF;
					break;
				case 'N':
					if (hpl_rec->data[ll][0] == 'F')
						idx = NF;
					else
					{
						w_assert1(hpl_rec->data[ll][0]=='O');
						idx = NO;
					}
					break;
				case 'R':
					w_assert1(hpl_rec->data[ll][0]=='F');
					idx = RF;
					break;
				default:
					return RC_AUGMENT(rc);
				}
				sum_qty[idx] += *(double*) (hpl_rec->data[lq]);
				sum_base_price[idx] += *(double*) (hpl_rec->data[le]);
				sum_disc_price[idx] += (*(double*) (hpl_rec->data[le]) * (1
						- *(double*) (hpl_rec->data[ld])));
				sum_charge[idx] += (*(double*) (hpl_rec->data[le]) * (1
						- *(double*) (hpl_rec->data[ld])) * (1
						+ *(double*) (hpl_rec->data[lt])));
				sum_disc[idx] += *(double*) (hpl_rec->data[ld]);
				count_order[idx] += 1;
			}
		}
		else
			break;
	}
	return RCOK;
}

w_rc_t tpch_query_101::run()
{
	W_DO(query_c::run());

	fprintf(output_file, "Scanned %d records, %d qualified.\n", scanned,
			qualified);

	fprintf(output_file, "QUERY RESULT: \n");
	fprintf(
			output_file,
			"\tL_RETURNFLAG\tL_LINESTATUS\tSUM_QTY\tSUM_BASE_PRICE\tSUM_DISC_PRICE\tSUM_CHARGE\tAVG_QTY\tAVG_PRICE\tAVG_DISC\tCOUNT_ORDER\n\n");
	fprintf(output_file,
			"\tA\t\tF\t\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\n",
			sum_qty[AF], sum_base_price[AF], sum_disc_price[AF],
			sum_charge[AF], sum_qty[AF] / count_order[AF], sum_disc[AF]
					/ count_order[AF], count_order[AF]);
	fprintf(output_file,
			"\tN\t\tF\t\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\n",
			sum_qty[NF], sum_base_price[NF], sum_disc_price[NF],
			sum_charge[NF], sum_qty[NF] / count_order[NF], sum_disc[NF]
					/ count_order[NF], count_order[NF]);
	fprintf(output_file,
			"\tN\t\tO\t\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\n",
			sum_qty[NO], sum_base_price[NO], sum_disc_price[NO],
			sum_charge[NO], sum_qty[NO] / count_order[NO], sum_disc[NO]
					/ count_order[NO], count_order[NO]);
	fprintf(output_file,
			"\tR\t\tF\t\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\t%6.3f\n",
			sum_qty[RF], sum_base_price[RF], sum_disc_price[RF],
			sum_charge[RF], sum_qty[RF] / count_order[RF], sum_disc[RF]
					/ count_order[RF], count_order[RF]);
	return RCOK;
}

/*
 * Query 6
 *
 * select sum(l_extendedprice * l_discount) as revenue
 * from tpcd.lineitem
 * where l_shipdate >= date ('1997-01-01') and 
 *       l_shipdate < date ('1997-01-01') + 1 year and 
 *       l_discount between 0.05 - 0.01 and 0.05 + 0.01 and 
 *       l_quantity < 24
 *
 * select sum(f5 * f6)
 * from lineitem
 * where f10 >= "1997-01-01" and 
 *       f10 < "1998-01-01" and 
 *       f6 >= 0.04 and f6 <= 0.06 and
 *       f4 < 24
 */

w_rc_t tpch_query_6::run_nsm()
{
	scan_file_i scan(finfo[0].fid, cc);
	pin_i* handle;
	bool eof = false;
	const char* frec;
	double discount;
	double quantity;
	double xprice;


	time_t timestamp1,timestamp2;
	char* consTime1 = "1997-01-01";
	char* consTime2 = "1998-01-01";
	timestamp1 = convertYYMMDDtoTimestamp(consTime1);
	timestamp2 = convertYYMMDDtoTimestamp(consTime2);

	while (1)
	{
		W_DO(scan.next(handle, 0, eof));
		if (eof == false)
		{
			scanned++;
			frec = handle->body();
			// apply condition
			//if (COMPARE_DATE(FIELD_START(finfo[0],frec,L_SHIPDATE), "1997-01-01") >= 0 && COMPARE_DATE(FIELD_START(finfo[0],frec,L_SHIPDATE), "1998-01-01") < 0)
			if ((COMPARE_NO(*(time_t*)FIELD_START(finfo[0],frec,L_SHIPDATE), timestamp1) >= 0 && COMPARE_NO(*(time_t*)FIELD_START(finfo[0],frec,L_SHIPDATE),timestamp2) < 0))
			{
				memcpy((char*) &discount,
						FIELD_START(finfo[0],frec,L_DISCOUNT), sizeof(double));
				if (discount >= 0.04 && discount <= 0.06)
				{
					memcpy((char*) &quantity,
							FIELD_START(finfo[0],frec,L_QUANTITY),
							sizeof(double));
					if (quantity < 24)
					{
						qualified++;
						memcpy((char*) &xprice,
								FIELD_START(finfo[0],frec,L_EXTENDEDPRICE),
								sizeof(double));
						sum += xprice * discount;
					}
				}
			}
		}
		else
			break;
	}

	return RCOK;
}

w_rc_t tpch_query_6::run_pax()
{
	scan_pfield_i scan_ls(finfo[0].fid, L_SHIPDATE, cc);
	scan_pfield_i scan_ld(finfo[0].fid, L_DISCOUNT, cc);
	scan_pfield_i scan_le(finfo[0].fid, L_EXTENDEDPRICE, cc);
	scan_pfield_i scan_lq(finfo[0].fid, L_QUANTITY, cc);
	pin_pfield_i *handle_ls;
	pin_pfield_i *handle_ld;
	pin_pfield_i *handle_le;
	pin_pfield_i *handle_lq;
	bool eof = false;

	double discount;
	double quantity;
	double xprice;

	// Do everything once to get the right pfield values in the handles
	W_DO(scan_ls.next(handle_ls, eof));
	W_DO(scan_ld.next(handle_ld, eof));
	W_DO(scan_le.next(handle_le, eof));
	W_DO(scan_lq.next(handle_lq, eof));

	time_t timestamp1,timestamp2;
	char* consTime1 = "1997-01-01";
	char* consTime2 = "1998-01-01";
	timestamp1 = convertYYMMDDtoTimestamp(consTime1);
	timestamp2 = convertYYMMDDtoTimestamp(consTime2);

	while (eof == false)
	{
		scanned++;
		//if (COMPARE_DATE(handle_ls->pfield().data, "1997-01-01") >= 0 && COMPARE_DATE(handle_ls->pfield().data, "1998-01-01") < 0)
		if ((COMPARE_NO(*(time_t*)handle_ls->pfield().data, timestamp1) >= 0 && COMPARE_NO(*(time_t*)handle_ls->pfield().data,timestamp2) < 0))
		{
			handle_ls->another_pfield((pfield_t&) handle_ld->pfield());
			memcpy((char*) &discount, handle_ld->pfield().data, sizeof(double));
			if (discount >= 0.04 && discount <= 0.06)
			{
				handle_ls->another_pfield((pfield_t&) handle_lq->pfield());
				memcpy((char*) &quantity, handle_lq->pfield().data,
						sizeof(double));
				if (quantity < 24)
				{
					handle_ls->another_pfield((pfield_t&) handle_le->pfield());
					memcpy((char*) &xprice, handle_le->pfield().data,
							sizeof(double));
					sum += xprice * discount;
					qualified++;
				}
			}
		}
		//TimeMeasure time1("timemeasure.P1.T6.field_scan.csv");
		//time1.startTimer();
		W_DO(scan_ls.next(handle_ls, eof));
		//time1.stopTimer();
		//time1.printData();
	}

	return RCOK;
}

w_rc_t tpch_query_6::run_hpl()
{
	scan_hpl_field_i scan_ls(finfo[0].fid, L_SHIPDATE, cc);
	scan_hpl_field_i scan_ld(finfo[0].fid, L_DISCOUNT, cc);
	scan_hpl_field_i scan_le(finfo[0].fid, L_EXTENDEDPRICE, cc);
	scan_hpl_field_i scan_lq(finfo[0].fid, L_QUANTITY, cc);
	pin_hpl_field_i *handle_ls;
	pin_hpl_field_i *handle_ld;
	pin_hpl_field_i *handle_le;
	pin_hpl_field_i *handle_lq;
	bool eof = false;

	double discount;
	double quantity;
	double xprice;

	// Do everything once to get the right pfield values in the handles
	W_DO(scan_ls.next(handle_ls, eof));
	W_DO(scan_ld.next(handle_ld, eof));
	W_DO(scan_le.next(handle_le, eof));
	W_DO(scan_lq.next(handle_lq, eof));


	time_t timestamp1,timestamp2;
	char* consTime1 = "1997-01-01";
	char* consTime2 = "1998-01-01";
	timestamp1 = convertYYMMDDtoTimestamp(consTime1);
	timestamp2 = convertYYMMDDtoTimestamp(consTime2);

	while (eof == false)
	{
		scanned++;
		//if (COMPARE_DATE(handle_ls->hpl_field().data, "1997-01-01") >= 0 && COMPARE_DATE(handle_ls->hpl_field().data, "1998-01-01") < 0)
		if ((COMPARE_NO(*(time_t*)handle_ls->hpl_field().data, timestamp1) >= 0 &&
				COMPARE_NO(*(time_t*)handle_ls->hpl_field().data,timestamp2) < 0)  &&
				handle_ls->isValidField())
		{
			handle_ls->another_hpl_field((hpl_field_t&) handle_ld->hpl_field());
			memcpy((char*) &discount, handle_ld->hpl_field().data, sizeof(double));
			if (discount >= 0.04 && discount <= 0.06)
			{
				handle_ls->another_hpl_field((hpl_field_t&) handle_lq->hpl_field());
				memcpy((char*) &quantity, handle_lq->hpl_field().data,
						sizeof(double));
				if (quantity < 24 && handle_ls->isValidRec())
				{
					handle_ls->another_hpl_field((hpl_field_t&) handle_le->hpl_field());
					memcpy((char*) &xprice, handle_le->hpl_field().data,
							sizeof(double));
					sum += xprice * discount;
					qualified++;
				}
			}
		}
		//TimeMeasure time1("timemeasure.P2.T6.field_scan.csv");
		//time1.startTimer();
		W_DO(scan_ls.next(handle_ls, eof));
		//time1.stopTimer();
		//time1.printData();
	}



	return RCOK;
}

w_rc_t tpch_query_6::run_dsm()
{
	file_info_t finfo_i;

	// scan all subrelations in parallel
	W_DO(find_field_vol_assoc(finfo[0].table_name, L_SHIPDATE, finfo_i));
	scan_file_i scan_ls(finfo_i.fid, cc);

	W_DO(find_field_vol_assoc(finfo[0].table_name, L_DISCOUNT, finfo_i));
	scan_file_i scan_ld(finfo_i.fid, cc);

	W_DO(find_field_vol_assoc(finfo[0].table_name, L_EXTENDEDPRICE, finfo_i));
	scan_file_i scan_le(finfo_i.fid, cc);

	W_DO(find_field_vol_assoc(finfo[0].table_name, L_QUANTITY, finfo_i));
	scan_file_i scan_lq(finfo_i.fid, cc);

	pin_i* handle_ls;
	pin_i* handle_ld;
	pin_i* handle_le;
	pin_i* handle_lq;

	uint4_t *qual_rids = new uint4_t[MAX_QUAL_RIDS];

	uint4_t qr = 0, sqr;
	bool eof = false;

	// collect semi-qualifying oids

	while (1)
	{
		W_DO(scan_ls.next(handle_ls, 0, eof));
		if (eof == false)
		{
			// get the rest
			W_DO(scan_lq.next(handle_lq, 0, eof));
			if (COMPARE_DATE(handle_ls->body(), "1997-01-01") >= 0
					&& COMPARE_DATE(handle_ls->body(), "1998-01-01") < 0
					&& *(double*) (handle_lq->body()) < 24)
			{
				w_assert3(qr < MAX_QUAL_RIDS);
				qual_rids[qr] = *(uint4_t*) (handle_ls->hdr());
				qr++;
			}
		}
		else
			break;
		scanned++;
	}

	sqr = qr;
	qr = 0;
	// Now sweep through the selected attribute files
	while (qr < sqr)
	{
		W_DO(scan_le.next(handle_le, 0, eof)); w_assert1 (eof==false); // this loop stops under another condition
		W_DO(scan_ld.next(handle_ld, 0, eof));
		if (*(uint4_t*) (handle_ld->hdr()) == qual_rids[qr])
		{
			if (*(double*) (handle_ld->body()) >= 0.04
					&& *(double*) (handle_ld->body()) <= 0.06)
			{
				sum += (*(double*) (handle_le->body())
						* *(double*) (handle_ld->body()));
				qualified++;
			}
			qr++;
		}
	}

	delete[] qual_rids;

	return RCOK;
}

w_rc_t tpch_query_6::run()
{
	W_DO(query_c::run());

	fprintf(output_file, "QUERY RESULT: Sum is %13.0f\n", sum);
	fprintf(output_file, "Scanned %d records, %d qualified.\n", scanned,
			qualified);
	return RCOK;
}

/* 
 * Query 12
 * 
 * select l_shipmode, sum(case
 * 	when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH'
 * 	then 1
 * 	else 0 end) as high_line_count, 
 *    sum(case
 *      when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH'
 *      then 1
 *      else 0 end) as low_line_count
 * from tpcd.orders, tpcd.lineitem
 * where o_orderkey = l_orderkey and
 *       (l_shipmode = "SHIP" or l_shipmode = "RAIL") and
 *       l_commitdate < l_receiptdate and
 *       l_shipdate < l_commitdate and
 *       l_receiptdate >= "1994-01-01" and
 *       l_receiptdate < "1995-01-01"
 * group by l_shipmode
 * order by l_shipmode
 *
 */

bool q12_l_pred_nsm(const void* tuple, const file_info_t& finfo)
{
	time_t timestamp1,timestamp2;
	char* consTime1 = "1994-01-01";
	char* consTime2 = "1995-01-01";
	timestamp1 = convertYYMMDDtoTimestamp(consTime1);
	timestamp2 = convertYYMMDDtoTimestamp(consTime2);


	char* frec = (char*) tuple;
	char* shipmode = FIELD_START(finfo, frec, L_SHIPMODE);
	char* commitdate = FIELD_START(finfo, frec, L_COMMITDATE);
	char* receiptdate = FIELD_START(finfo, frec, L_RECEIPTDATE);
	char* shipdate = FIELD_START(finfo, frec, L_SHIPDATE);


	if ((strcmp(shipmode, "SHIP") == 0 || strcmp(shipmode, "RAIL") == 0)
			//&& COMPARE_DATE(commitdate, receiptdate) < 0
			&& COMPARE_NO(*(time_t*)commitdate, *(time_t*)receiptdate) < 0
			//&& COMPARE_DATE(shipdate, commitdate) < 0
			&& COMPARE_NO(*(time_t*)shipdate, *(time_t*)commitdate) < 0
			//&& COMPARE_DATE(receiptdate, "1994-01-01") >= 0
			&& COMPARE_NO(*(time_t*)receiptdate,timestamp1) >= 0
			//&& COMPARE_DATE(receiptdate, "1995-01-01") < 0)
			&& COMPARE_NO(*(time_t*)receiptdate,timestamp2) < 0)
		return true;
	else
		return false;
}

bool q12_l_pred_pax(const void* tuple, const file_info_t&)
{

	time_t timestamp1,timestamp2;
	char* consTime1 = "1994-01-01";
	char* consTime2 = "1995-01-01";
	timestamp1 = convertYYMMDDtoTimestamp(consTime1);
	timestamp2 = convertYYMMDDtoTimestamp(consTime2);

	precord_t *prec = (precord_t*) tuple;
	if ((strcmp(prec->data[4], "SHIP") == 0 || strcmp(prec->data[4], "RAIL")== 0)
			//&& COMPARE_DATE(prec->data[2], prec->data[3]) < 0
			&& COMPARE_NO(*(time_t*)prec->data[2], *(time_t*)prec->data[3]) < 0
			//&& COMPARE_DATE(prec->data[1], prec->data[2]) < 0
			&& COMPARE_NO(*(time_t*)prec->data[1], *(time_t*)prec->data[2]) < 0
			//&& COMPARE_DATE(prec->data[3], "1994-01-01") >= 0
			&& COMPARE_NO(*(time_t*)prec->data[3],timestamp1) >= 0
			//&& COMPARE_DATE(prec->data[3], "1995-01-01") < 0)
			&& COMPARE_NO(*(time_t*)prec->data[3],timestamp2) < 0)
		return true;
	else
		return false;
}

bool q12_l_pred_hpl(const void* tuple, const file_info_t&)
{
	time_t timestamp1,timestamp2;
	char* consTime1 = "1994-01-01";
	char* consTime2 = "1995-01-01";
	timestamp1 = convertYYMMDDtoTimestamp(consTime1);
	timestamp2 = convertYYMMDDtoTimestamp(consTime2);

	hpl_record_t *hpl_rec = (hpl_record_t*) tuple;
	//if ((strcmp(hpl_rec->data[4], "SHIP") == 0 || strcmp(hpl_rec->data[4], "RAIL") == 0)
	if ((compareVarField(hpl_rec,4,"SHIP") || compareVarField(hpl_rec,4,"RAIL"))
			//&& COMPARE_DATE(hpl_rec->data[2], hpl_rec->data[3]) < 0
			&& COMPARE_NO(*(time_t*)hpl_rec->data[2], *(time_t*)hpl_rec->data[3]) < 0
			//&& COMPARE_DATE(hpl_rec->data[1], hpl_rec->data[2]) < 0
			&& COMPARE_NO(*(time_t*)hpl_rec->data[1], *(time_t*)hpl_rec->data[2]) < 0
			//&& COMPARE_DATE(hpl_rec->data[3], "1994-01-01") >= 0
			&& COMPARE_NO(*(time_t*)hpl_rec->data[3],timestamp1) >= 0
			//&& COMPARE_DATE(hpl_rec->data[3], "1995-01-01") < 0)
			&& COMPARE_NO(*(time_t*)hpl_rec->data[3],timestamp2) < 0)
		return true;
	else
		return false;
}

void q12_final_touch_nsm(void *query, const void* tuple1, const void* tuple2)
{
	tpch_query_12 *q = (tpch_query_12 *) query;

	char* left_frec = (char*) tuple1;
	char* right_frec = (char*) tuple2;


	//char* shipmode = FIELD_START(finfo, left_frec, L_SHIPMODE);
	//char* orderpriority = FIELD_START(finfo, right_frec, O_ORDERPRIORITY);

	w_assert3(strcmp(left_frec+30, "RAIL")==0 || strcmp(left_frec+30,"SHIP")==0);

	if (strcmp(left_frec + 30, "RAIL") == 0)
		if (strcmp(right_frec + 6, "1-URGENT") == 0 || strcmp(right_frec + 6, "2-HIGH")
				== 0)
			q->r_high++;
		else
		{
			w_assert3(strcmp(right_frec+4, "1-URGENT")!=0 &&
					strcmp(right_frec + 6, "2-HIGH")!=0);
			q->r_low++;
		}
	else if (strcmp(left_frec + 30, "SHIP") == 0)
	{
		w_assert3(strcmp(left_frec+30, "SHIP")==0);
		if (strcmp(right_frec + 6, "1-URGENT") == 0 || strcmp(right_frec + 6, "2-HIGH")
				== 0)
			q->s_high++;
		else
		{
			w_assert3(strcmp(right_frec + 6, "1-URGENT")!=0 &&
					strcmp(right_frec + 6, "2-HIGH")!=0);
			q->s_low++;
		}
	}
	else
	{
		cout << "Expected SHIP or RAIL but got: " << (left_frec + 30) << endl;
	}
}

void q12_final_touch_pax(void *query, const void* tuple1, const void* tuple2)
{
	tpch_query_12 *q = (tpch_query_12 *) query;

	precord_t *left_prec = (precord_t*) tuple1;
	precord_t *right_prec = (precord_t*) tuple2;
	w_assert3(strcmp(left_prec->data[4], "RAIL")==0 ||
			strcmp(left_prec->data[4],"SHIP")==0);

	if (strcmp(left_prec->data[4], "RAIL") == 0)
		if (strcmp(right_prec->data[1], "1-URGENT") == 0 || strcmp(right_prec->data[1], "2-HIGH") == 0)
		{
#if HPL_DEBUG>0
			cout << "Rail Hi: \"" << right_prec->data[1] << "\"" << endl;
#endif
			q->r_high++;
		}
		else
		{
#if HPL_DEBUG>0
			cout << "Rail Low: \"" << right_prec->data[1] << "\"" << endl;
#endif
			w_assert3(strcmp(right_prec->data[1], "1-URGENT")!=0 &&
					strcmp(right_prec->data[1], "2-HIGH")!=0);
			q->r_low++;
		}
	else
	{
		w_assert3(strcmp(left_prec->data[4], "SHIP")==0);
		if (strcmp(right_prec->data[1], "1-URGENT") == 0 || strcmp(
				right_prec->data[1], "2-HIGH") == 0)
			q->s_high++;
		else
		{
			w_assert3(strcmp(right_prec->data[1], "1-URGENT")!=0 &&
					strcmp(right_prec->data[1], "2-HIGH")!=0);
			q->s_low++;
		}
	}
}

void q12_final_touch_hpl(void *query, const void* tuple1, const void* tuple2)
{
	tpch_query_12 *q = (tpch_query_12 *) query;

	hpl_record_t *left_hpl_rec = (hpl_record_t*) tuple1;
	hpl_record_t *right_hpl_rec = (hpl_record_t*) tuple2;

	/* w_assert3(strcmp(left_hpl_rec->data[4], "RAIL")==0 ||
			strcmp(left_hpl_rec->data[4],"SHIP")==0); */

	//if (strcmp(left_hpl_rec->data[4], "RAIL") == 0)
	if(compareVarField(left_hpl_rec,4,"RAIL"))
		//if (strcmp(right_hpl_rec->data[0], "1-URGENT") == 0 || strcmp(right_hpl_rec->data[0], "2-HIGH") == 0)
		if(compareVarField(right_hpl_rec,1,"1-URGENT") || compareVarField(right_hpl_rec,1,"2-HIGH"))
		{
			q->r_high++;
		}
		else
		{
			//w_assert3(strcmp(right_hpl_rec->data[0], "1-URGENT")!=0 && strcmp(right_hpl_rec->data[0], "2-HIGH")!=0);
			q->r_low++;
		}
	else
	{
		//w_assert3(strcmp(left_hpl_rec->data[4], "SHIP")==0);
		//if (strcmp(right_hpl_rec->data[0], "1-URGENT") == 0 || strcmp(right_hpl_rec->data[0], "2-HIGH") == 0)
		if(compareVarField(right_hpl_rec,1,"1-URGENT") || compareVarField(right_hpl_rec,1,"2-HIGH"))
		{
			q->s_high++;
		}
		else
		{
			//w_assert3(strcmp(right_hpl_rec->data[0], "1-URGENT")!=0 && strcmp(right_hpl_rec->data[0], "2-HIGH")!=0);
			q->s_low++;
		}
	}
}

w_rc_t tpch_query_12::run()
{
	rc_t rc;

	flid_t ifi_lineitem[5];
	ifi_lineitem[0] = L_ORDERKEY;
	ifi_lineitem[1] = L_SHIPDATE;
	ifi_lineitem[2] = L_COMMITDATE;
	ifi_lineitem[3] = L_RECEIPTDATE;
	ifi_lineitem[4] = L_SHIPMODE;

	flid_t ifi_order[2];
	ifi_order[0] = O_ORDERKEY;
	ifi_order[1] = O_ORDERPRIORITY;

	switch (finfo[0].page_layout)
	{
	case NSM:
	{
		ahhjoin_q q12(table_name, L_ORDERKEY, O_ORDERKEY, 5, 2, ifi_lineitem,
				ifi_order, q12_l_pred_nsm, NULL, (query_c*) this,
				q12_final_touch_nsm);
		W_DO(q12.run());
		break;
	}
	case PAX:
	{
		ahhjoin_q q12(table_name, L_ORDERKEY, O_ORDERKEY, 5, 2, ifi_lineitem,
				ifi_order, q12_l_pred_pax, NULL, (query_c*) this,
				q12_final_touch_pax);
		W_DO(q12.run());
		break;
	}
	case HPL:
	{
		ahhjoin_q q12(table_name, L_ORDERKEY, O_ORDERKEY, 5, 2, ifi_lineitem,
				ifi_order, q12_l_pred_hpl, NULL, (query_c*) this,
				q12_final_touch_hpl);
		W_DO(q12.run());
		break;
	}
	default:
	{
		cerr << "Invalid page layout " << (int) finfo[0].page_layout
				<< " for this query." << endl;
		return RC_AUGMENT(rc);
		break;
	}
	}
	fprintf(
			output_file,
			"QUERY RESULT: \n\tSHIPMODE\tHIGH\tLOW\n\tRAIL\t\t%6.0f\t%6.0f\n\tSHIP\t\t%6.0f\t%6.0f\n",
			(double) r_high, (double) r_low, (double) s_high, (double) s_low);
	return RCOK;
}

/* 
 * Query 14
 * 
 * select 100.00 * sum(case
 *                     when p_type like 'PROMO%'
 *                     then l_extendedprice * (1 - l_discount)
 *                     else 0 end) 
 *                / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
 * from tpcd.lineitem, tpcd.part
 * where l_partkey = p_partkey and
 *       l_shipdate >= date ('1997-09-01') and
 *       l_shipdate < date ('1997-09-01') + 1 month
 * 
 * 
 * select 100 * sum (case
 *                   when p_type like 'PROMO%'
 *                   then l_extendedprice * (1-l_discount)
 *                   else 0 end)
 *		/ sum(l_extendedprice * (1 - l_discount))
 * from lineitem, part
 * where l_partkey = p_partkey and 
 *       l_shipdate >= "1997-09-01" and
 *       l_shipdate < "1997-10-01"
 *
 */

bool q14_l_pred_nsm(const void* tuple, const file_info_t& finfo)
{
	char* frec = (char*) tuple;

	time_t timestamp1,timestamp2;
	char* consTime1 = "1997-09-01";
	char* consTime2 = "1997-10-01";
	timestamp1 = convertYYMMDDtoTimestamp(consTime1);
	timestamp2 = convertYYMMDDtoTimestamp(consTime2);

	if (//COMPARE_DATE(FIELD_START(finfo,frec,L_SHIPDATE), "1997-09-01") >= 0
			COMPARE_NO(*(time_t*)FIELD_START(finfo,frec,L_SHIPDATE), timestamp1)  >= 0
			//&& COMPARE_DATE(FIELD_START(finfo,frec,L_SHIPDATE), "1997-10-01") < 0)
			&& COMPARE_NO(*(time_t*)FIELD_START(finfo,frec,L_SHIPDATE), timestamp2)< 0)
		return true;
	else
		return false;
}

bool q14_l_pred_pax(const void* tuple, const file_info_t&)
{
	precord_t *prec = (precord_t*) tuple;
	time_t timestamp1,timestamp2;
	char* consTime1 = "1997-09-01";
	char* consTime2 = "1997-10-01";
	timestamp1 = convertYYMMDDtoTimestamp(consTime1);
	timestamp2 = convertYYMMDDtoTimestamp(consTime2);

	if (//COMPARE_DATE(prec->data[3], "1997-09-01") >= 0
			COMPARE_NO(*(time_t*)prec->data[3],timestamp1) >= 0
			//&& COMPARE_DATE(prec->data[3], "1997-10-01") < 0)
			&& COMPARE_NO(*(time_t*)prec->data[3], timestamp2) < 0)
		return true;
	else
		return false;
}

bool q14_l_pred_hpl(const void* tuple, const file_info_t&)
{
	hpl_record_t* hpl_rec = (hpl_record_t*) tuple;
	time_t timestamp1,timestamp2;
	char* consTime1 = "1997-09-01";
	char* consTime2 = "1997-10-01";
	timestamp1 = convertYYMMDDtoTimestamp(consTime1);
	timestamp2 = convertYYMMDDtoTimestamp(consTime2);

	//if (COMPARE_DATE(hpl_rec->data[3], "1997-09-01") >= 0 && COMPARE_DATE(hpl_rec->data[3], "1997-10-01") < 0)
	if (COMPARE_NO(*(time_t*)hpl_rec->data[3],timestamp1)>= 0 && COMPARE_NO(*(time_t*)hpl_rec->data[3],timestamp2) < 0)
		return true;
	else
		return false;
}

void q14_final_touch_nsm(void *query, const void* tuple1, const void* tuple2)
{
	tpch_query_14 *q = (tpch_query_14 *) query;

	char* left_frec = (char*) tuple1;
	char* right_frec = (char*) tuple2;
	double d;
	q->sum += (d = (*(double*) (right_frec) * (1 - *(double*) (right_frec + 8))));
	if (strncmp(left_frec + 4 + 2, "PROMO", 5) == 0)
		q->cond_sum += d;
}

void q14_final_touch_pax(void *query, const void* tuple1, const void* tuple2)
{
	tpch_query_14 *q = (tpch_query_14 *) query;

	precord_t *left_prec = (precord_t*) tuple1;
	precord_t *right_prec = (precord_t*) tuple2;
	double d;
	q->sum += (d = (*(double*) (right_prec->data[0]) * (1 - (*(double*) (right_prec->data[1])))));
	if (strncmp(left_prec->data[1], "PROMO", 5) == 0)
		q->cond_sum += d;
}

void q14_final_touch_hpl(void *query, const void* tuple1, const void* tuple2)
{
	tpch_query_14 *q = (tpch_query_14 *) query;

	hpl_record_t *left_hpl_rec = (hpl_record_t*) tuple1;
	hpl_record_t *right_hpl_rec = (hpl_record_t*) tuple2;
	double d;
	q->sum += (d = (*(double*) (right_hpl_rec->data[0]) * (1 - (*(double*) (right_hpl_rec->data[1])))));
	//if (strncmp(left_hpl_rec->data[1], "PROMO", 5) == 0)
	if(compareVarField(left_hpl_rec,1,"PROMO"))
		q->cond_sum += d;
}

w_rc_t tpch_query_14::run()
{
	rc_t rc;

	flid_t ifi_part[2];
	ifi_part[0] = P_PARTKEY;
	ifi_part[1] = P_TYPE;

	flid_t ifi_lineitem[4];
	ifi_lineitem[0] = L_EXTENDEDPRICE;
	ifi_lineitem[1] = L_DISCOUNT;
	ifi_lineitem[2] = L_PARTKEY;
	ifi_lineitem[3] = L_SHIPDATE;

	switch (finfo[0].page_layout)
	{
	case NSM:
	{
		ahhjoin_q q14(table_name, P_PARTKEY, L_PARTKEY, 2, 4, ifi_part,
				ifi_lineitem, dummy_pred, q14_l_pred_nsm, (query_c*) this,
				q14_final_touch_nsm);
		W_DO(q14.run());
		break;
	}
	case PAX:
	{
		ahhjoin_q q14(table_name, P_PARTKEY, L_PARTKEY, 2, 4, ifi_part,
				ifi_lineitem, NULL, q14_l_pred_pax, (query_c*) this,
				q14_final_touch_pax);
		W_DO(q14.run());
		break;
	}
	case HPL:
	{
		ahhjoin_q q14(table_name, P_PARTKEY, L_PARTKEY, 2, 4, ifi_part,
				ifi_lineitem, NULL, q14_l_pred_hpl, (query_c*) this,
				q14_final_touch_hpl);
		W_DO(q14.run());
		break;
	}
	default:
	{
		cerr << "Invalid page layout " << (int) finfo[0].page_layout
				<< " for this query." << endl;
		return RC_AUGMENT(rc);
		break;
	}
	}
	fprintf(output_file, "QUERY RESULT: cond_sum = %13.3f, sum = %13.3f\nQUERY RESULT: %13.3f.\n", cond_sum, sum, 100 * cond_sum / sum);

	return RCOK;
}

