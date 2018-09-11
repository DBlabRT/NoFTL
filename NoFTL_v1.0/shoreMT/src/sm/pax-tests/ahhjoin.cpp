/*
 * member functions for ahhjoin_q class
 */

#include "ahhjoin.h"
#include "sm_hash.h"
#include "sm_exec.h"

bool dummy_pred(const void*, const file_info_t&) {
	return true;
}

struct hash_elem_t {
	char* tuple;
	Size2 length;

	hash_elem_t(const char* t = NULL, Size2 l = 0) {
		length = l;
		if (t != NULL) {
			w_assert3(l>0);
			tuple = new char[l];
			memcpy(tuple, t, l);
		}
	}

	~hash_elem_t() {
		if (tuple != NULL)
			delete tuple;
	}
};

ahhjoin_q::ahhjoin_q(char** table_name,
					flid_t l_jcol, flid_t r_jcol,
					flid_t l_nif, flid_t r_nif,
					flid_t *l_if, flid_t *r_if,
					bool(*l_ipred)(const void*, const file_info_t&),
					bool(*r_ipred)(const void*,	const file_info_t&),
					query_c* cl,
					void(*ft)(void*, const void*,const void*),
					uint4_t nbucket, uint4_t mquota, uint4_t htsz) :
						query_c(2, table_name),
						num_bucket(nbucket),
						mem_quota(mquota),
						ht_size(htsz),
						caller(cl),
						ft_func(ft)
{
	jt[L] = new join_table_t(l_jcol, l_nif, l_if, l_ipred, finfo[L]);
	jt[R] = new join_table_t(r_jcol, r_nif, r_if, r_ipred, finfo[R]);
	hash_tables = NULL;
	result_tuples = 0;
}

/*
 *	NSM join methods
 *
 */
w_rc_t ahhjoin_q::run_nsm() {
	uint4_t i, ht_valid, bukIndex;

	hash_tables = new hash_table_t*[num_bucket];
	w_assert3(hash_tables != NULL);

	// Creating one hash table per bucket.
	for (i = 0; i < num_bucket; ++i)
		hash_tables[i] = new hash_table_t(ht_size);
	ht_valid = num_bucket; // number of valid hash tables

	// Create an array of store ids
	stid_t* leftfids = new stid_t[num_bucket];

	for (i = 0; i < num_bucket; i++)
		W_DO(ssm->create_file(VolID, leftfids[i], ss_m::t_temporary));

	// partition left and build hash table.
	W_DO(partition_left_nsm(leftfids, ht_valid));

	stid_t* rightfids = new stid_t[num_bucket];
	for (i = 0; i < num_bucket; i++)
		W_DO(ssm->create_file(VolID, rightfids[i], ss_m::t_temporary));

	// partition the right and probe
	W_DO(partition_right_nsm(rightfids, ht_valid));

	// remove the in memory hash tables leave only the first one.
	for (i = 1; i < ht_valid; i++) {
		if (hash_tables[i] != NULL) {
			hash_tables[i]->reset();
			delete hash_tables[i];
			hash_tables[i] = NULL;
		}
	}
	// create the alias for the first hash table,
	// reset it  for further use
	if (hash_tables[0] == NULL) {
		hash_tables[0] = new hash_table_t(ht_size);
	}
	hash_table_t* htable = hash_tables[0];

	// build and probe for the remaining partitions.
	for (bukIndex = ht_valid; bukIndex < num_bucket; bukIndex++) {
		htable->reset();
		W_DO(build_hash_table_nsm(leftfids[bukIndex], htable));
		W_DO(probe_hash_table_nsm(rightfids[bukIndex], htable));
	}

	// fprintf(stderr, "Hash join produced %13.0f tuples.\n", result_tuples);

	// clean up
	htable->reset();
	delete hash_tables[0];
	delete[] hash_tables;
	hash_tables = NULL;

	for (i = 0; i < num_bucket; i++) {
		W_DO(ssm->destroy_file(rightfids[i]));
		W_DO(ssm->destroy_file(leftfids[i]));
	}

	delete[] rightfids;
	delete[] leftfids;

	return RCOK;
}

w_rc_t ahhjoin_q::partition_left_nsm(stid_t* fids, uint4_t& b_valid) {
	uint4_t i;
	//int memQuota = mem_quota * 1024 * 1024;
	int memQuota = mem_quota * 1024;

	// This is where I'll write the buckets
	append_file_i **af = new append_file_i*[num_bucket];
	hash_func_c hashf(num_bucket);

	for (i = 0; i < num_bucket; ++i)
		af[i] = new append_file_i(fids[i]);

	uint4_t hashkey = 0;

	scan_file_i scan(finfo[L].fid, cc);
	pin_i* handle;
	bool eof = false;
	char buf[1024];
	hash_elem_t* hash_elem;

	rid_t rid;

	while (1) {
		// Scan for tuple
		W_DO(scan.next(handle, 0, eof));
		if (eof == false) {
			// clean up the riff-raff
			if (jt[L]->ipred != NULL)
				if (jt[L]->ipred(handle->body(), finfo[L]) == false)
					continue;

			// pack it into a small thing

			hash_elem = new hash_elem_t;
			frec_to_minirec(finfo[L], handle->body(), jt[L]->finfo,
					jt[L]->ifields, &(hash_elem->tuple), hash_elem->length);

			hashkey = hashf(jt[L]->finfo, hash_elem->tuple, jt[L]->join_col);
			//cout << "Lhashkey=" << hashkey << "; b_valid=" << b_valid << endl;
			if (hashkey < b_valid) {
				hash_tables[hashkey]->add(toString(jt[L]->finfo,
						hash_elem->tuple, jt[L]->join_col, buf), hash_elem);
				memQuota -= (hash_elem->length + sizeof(uint4_t));

				// test if the quota is used up.
				while (memQuota < 0) {
					b_valid--;
					memQuota += freeze_hash_table_nsm(hash_tables[b_valid],
							*af[b_valid]);
					hash_tables[b_valid] = NULL;
				}
			} else {
				// the hashtable is frozen; write to file.
				vec_t rec(hash_elem->tuple, hash_elem->length);
				W_DO(af[hashkey]->create_rec(vec_t(), hash_elem->length, rec, rid));
				delete hash_elem;
				hash_elem = 0;
			}
		} else
			break;
	} // while

	for (i = 0; i < num_bucket; ++i)
		delete af[i];
	delete[] af;

	return RCOK;
}

// build hash table for the R partitions on the disk.
w_rc_t ahhjoin_q::partition_right_nsm(stid_t* fids, const uint4_t b_valid) {
	uint4_t i;

	append_file_i **af = new append_file_i*[num_bucket];
	hash_func_c hashf(num_bucket);

	for (i = 0; i < num_bucket; ++i)
		af[i] = new append_file_i(fids[i]);

	scan_file_i scan(finfo[R].fid, cc);
	pin_i* handle;
	bool eof = false;
	char buf[1024];
	hash_table_t::val_array_t * vPtr;
	uint4_t hashkey = 0;

	hash_elem_t *hash_elem;

	rid_t rid;

	char *mini_rec = NULL;
	Size2 mini_rec_size;

	while (1) {
		W_DO(scan.next(handle, 0, eof));
		if (eof == false) {
			// clean up the riff-raff
			if (jt[R]->ipred != NULL)
				if (jt[R]->ipred(handle->body(), finfo[R]) == false)
					continue;

			// pack it into a small thing
			frec_to_minirec(finfo[R], handle->body(), jt[R]->finfo,
					jt[R]->ifields, &mini_rec, mini_rec_size);

			hashkey = hashf(jt[R]->finfo, mini_rec, jt[R]->join_col);
			//cout << "Rhashkey=" << hashkey << "; b_valid=" << b_valid << endl;
			if (hashkey < b_valid) {
				// the hash table is in memory
				// probe it and output if match
				vPtr = hash_tables[hashkey]->find_match(toString(jt[R]->finfo,
						mini_rec, jt[R]->join_col, buf));

				if (vPtr != NULL) {
					for (i = 0; i < vPtr->getsize(); i++) {
						hash_elem = (hash_elem_t*) (*vPtr)[i];

						if (compare(
								FIELD_START(jt[L]->finfo, hash_elem->tuple, jt[L]->join_col),
								FIELD_START(jt[R]->finfo, mini_rec, jt[R]->join_col),
								jt[L]->finfo.field_type[jt[L]->join_col]) == 0) {
							if (ft_func != NULL)
								ft_func(caller, hash_elem->tuple, mini_rec);
							else
								print_two_frecs(jt[L]->finfo, hash_elem->tuple,
										jt[R]->finfo, mini_rec);
							result_tuples++;
						}
					}
				}
			} else {
				// write to disk
				vec_t rec(mini_rec, mini_rec_size);
				W_DO(af[hashkey]->create_rec(vec_t(), mini_rec_size, rec, rid));
			}
		} else
			break;
	} // while (1)

	for (i = 0; i < num_bucket; ++i)
		delete af[i];
	delete[] af;

	return RCOK;
}

// build hash table for the L partitions on the disk.
// the same as in Grace Hash Join
w_rc_t ahhjoin_q::build_hash_table_nsm(stid_t& pfid, hash_table_t* htable) {
	scan_file_i scan(pfid);
	pin_i* handle;
	bool eof = false;
	char buf[1024];
	hash_elem_t* hash_elem = NULL;

	while (1) {
		W_DO(scan.next(handle, 0, eof));
		if (eof == false) {
			hash_elem = new hash_elem_t(handle->body(), handle->body_size());
			htable->add(toString(jt[L]->finfo, handle->body(), jt[L]->join_col,
					buf), hash_elem);
		} else
			break;
	}
	return RCOK;
}

// probe the hash table using the on-disk partition
w_rc_t ahhjoin_q::probe_hash_table_nsm(stid_t& pfid, hash_table_t* htable) {
	scan_file_i scan(pfid);
	pin_i* handle;
	bool eof = false;
	char buf[1024];
	hash_table_t::val_array_t * vPtr;
	hash_elem_t* hash_elem;
	uint4_t i;

	while (1) {
		W_DO(scan.next(handle, 0, eof));
		if (eof == false) {
			vPtr = htable->find_match(toString(jt[R]->finfo,
					handle->body(),
					jt[R]->join_col, buf));

			if (vPtr != NULL) {
				for (i = 0; i < vPtr->getsize(); i++) {
					hash_elem = (hash_elem_t*) (*vPtr)[i];

					if (compare(
							FIELD_START(jt[L]->finfo, hash_elem->tuple, jt[L]->join_col),
							FIELD_START(jt[R]->finfo, handle->body(), jt[R]->join_col),
							jt[L]->finfo.field_type[jt[L]->join_col]) == 0) {
						if (ft_func != NULL)
							ft_func(caller, hash_elem->tuple, handle->body());
						else
							print_two_frecs(jt[L]->finfo, hash_elem->tuple,
									jt[R]->finfo, handle->body());
						result_tuples++;
					}
				}
			}
		} else
			break;
	}
	return RCOK;
}

// freeze a partition.
// write the tuples to the file, free the memory of the hash table.
// delete the hashtable, set it to NULL.
// return number of bytes reclaimed by free the elements .
uint4_t ahhjoin_q::freeze_hash_table_nsm(hash_table_t* &htable,
		append_file_i& af) {
	if (htable == NULL)
		return 0;

	uint4_t nbytes = 0;
	uint4_t bukIndex;
	uint4_t i;
	hash_table_t::table_entry_t* entry;

	rid_t rid;

	// it should be wrapped by hashtable. UGLY Code.
	for (bukIndex = 0; bukIndex < htable->m_size; bukIndex++) {
		for (entry = htable->m_table[bukIndex].next; entry
				!= &(htable->m_table[bukIndex]); entry = entry->next) {
			w_assert3(entry);
			for (i = 0; i < entry->val_array.getsize(); i++) {
				hash_elem_t* hash_elem = (hash_elem_t*) (entry->val_array[i]);
				vec_t rec(hash_elem->tuple, hash_elem->length);
				W_COERCE(af.create_rec(vec_t(), hash_elem->length, rec, rid));
				nbytes += (hash_elem->length + sizeof(uint4_t));
			}
		}
	}

	htable->reset();
	delete htable;
	htable = NULL;

	return nbytes;
}

/*
 *	PAX join methods
 *
 */
w_rc_t ahhjoin_q::run_pax() {
	uint4_t i, ht_valid, bukIndex;

	hash_tables = new hash_table_t*[num_bucket];
	w_assert3(hash_tables != NULL);

	// Creating one hash table per bucket.
	for (i = 0; i < num_bucket; ++i)
		hash_tables[i] = new hash_table_t(ht_size);
	ht_valid = num_bucket; // number of valid hash tables

	// Create an array of store ids for the file on the left
	stid_t* leftfids = new stid_t[num_bucket];

	precord_t left_prec(jt[L]->finfo.no_fields, jt[L]->finfo.no_ffields);
	jt[L]->finfo.to_prec(left_prec);
	for (i = 0; i < num_bucket; i++)
		W_DO(ssm->create_file(VolID, leftfids[i], ss_m::t_temporary, left_prec));

	// partition left and build hash table.
	W_DO(partition_left_pax(leftfids, ht_valid));

	// Create an array of store ids for the file on the right
	stid_t* rightfids = new stid_t[num_bucket];

	precord_t right_prec(jt[R]->finfo.no_fields, jt[R]->finfo.no_ffields);
	jt[R]->finfo.to_prec(right_prec);
	for (i = 0; i < num_bucket; i++)
		W_DO(ssm->create_file(VolID, rightfids[i], ss_m::t_temporary, right_prec));

	// partition the right and probe
	W_DO(partition_right_pax(rightfids, ht_valid));

	// remove the in memory hash tables leave only the first one.
	for (i = 1; i < ht_valid; i++) {
		if (hash_tables[i] != NULL) {
			hash_tables[i]->reset();
			delete hash_tables[i];
			hash_tables[i] = NULL;
		}
	}
	// create the alias for the first hash table,
	// reset it  for further use
	if (hash_tables[0] == NULL) {
		hash_tables[0] = new hash_table_t(ht_size);
	}
	hash_table_t* htable = hash_tables[0];

	// build and probe for the remaining partitions.
	for (bukIndex = ht_valid; bukIndex < num_bucket; bukIndex++) {
		htable->reset();
		W_DO(build_hash_table_pax(leftfids[bukIndex], htable));
		W_DO(probe_hash_table_pax(rightfids[bukIndex], htable));
	}

	// fprintf(stderr, "Hash join produced %13.0f tuples.\n", result_tuples);

	// clean up
	htable->reset();
	delete hash_tables[0];
	delete[] hash_tables;
	hash_tables = NULL;

	for (i = 0; i < num_bucket; i++) {
		W_DO(ssm->destroy_file(rightfids[i]));
		W_DO(ssm->destroy_file(leftfids[i]));
	}

	delete[] rightfids;
	delete[] leftfids;

	return RCOK;
}

w_rc_t ahhjoin_q::partition_left_pax(stid_t* fids, uint4_t& b_valid) {
	uint4_t i;
	int memQuota = mem_quota * 1024;

	// This is where I'll write the buckets
	append_pfile_i **af = new append_pfile_i*[num_bucket];
	hash_func_c hashf(num_bucket);

	for (i = 0; i < num_bucket; ++i)
		af[i] = new append_pfile_i(fids[i]);

	uint4_t hashkey = 0;

	scan_pfile_i scan(finfo[L].fid, cc, jt[L]->finfo.no_fields, jt[L]->ifields);
	pin_prec_i* handle;
	bool eof = false;
	char buf[1024];
	rid_t rid;
	precord_t *new_prec;

	while (1) {
		W_DO(scan.next(handle, eof));
		if (eof == false) {
			// clean up the riff-raff
			if (jt[L]->ipred != NULL)
				if (jt[L]->ipred(&(handle->precord()), jt[L]->finfo) == false)
					continue;

			hashkey = hashf(jt[L]->finfo, handle->precord(), jt[L]->join_col);
			//cout << "hashkey=" << hashkey << "; b_valid=" << b_valid << endl;
			if (hashkey < b_valid) {
				new_prec = new precord_t(handle->precord());
				hash_tables[hashkey]->add(toString(jt[L]->finfo, *new_prec,
						jt[L]->join_col, buf), new_prec);
				memQuota -= new_prec->size();

				// test if the quota is used up.
				while (memQuota < 0) {
					b_valid--;
					memQuota += freeze_hash_table_pax(hash_tables[b_valid],
							*af[b_valid]);
					hash_tables[b_valid] = NULL;
				}
			} else {
				// the hashtable is frozen; write to file.
				W_DO(af[hashkey]->create_rec(handle->precord(), rid));
			}
		} else
			break;
	} // while

	for (i = 0; i < num_bucket; ++i)
		delete af[i];
	delete[] af;

	return RCOK;
}

// build hash table for the R partitions on the disk.
w_rc_t ahhjoin_q::partition_right_pax(stid_t* fids, const uint4_t b_valid) {
	uint4_t i;
	append_pfile_i **af = new append_pfile_i*[num_bucket];
	hash_func_c hashf(num_bucket);

	for (i = 0; i < num_bucket; ++i)
		af[i] = new append_pfile_i(fids[i]);

	scan_pfile_i scan(finfo[R].fid, cc, jt[R]->finfo.no_fields, jt[R]->ifields);
	pin_prec_i* handle;
	bool eof = false;
	char buf[1024];
	hash_table_t::val_array_t * vPtr;
	uint4_t hashkey = 0;
	precord_t *left_prec;

	rid_t rid;

	while (1) {
		W_DO(scan.next(handle, eof));
		if (eof == false) {
			// clean up the riff-raff
			if (jt[R]->ipred != NULL)
				if (jt[R]->ipred(&(handle->precord()), jt[R]->finfo) == false)
					continue;
			hashkey = hashf(jt[R]->finfo, handle->precord(), jt[R]->join_col);
			if (hashkey < b_valid) {
				// the hash table is in memory
				// probe it and output if match
				vPtr = hash_tables[hashkey]->find_match(toString(jt[R]->finfo,
						handle->precord(), jt[R]->join_col, buf));

				if (vPtr != NULL) {
					for (i = 0; i < vPtr->getsize(); i++) {
						left_prec = (precord_t*) (*vPtr)[i];

						if (compare(left_prec->data[jt[L]->join_col],
								handle->precord().data[jt[R]->join_col],
								jt[L]->finfo.field_type[jt[L]->join_col]) == 0) {
							if (ft_func != NULL)
								ft_func(caller, left_prec, &(handle->precord()));
							else
								print_two_precs(jt[L]->finfo, *left_prec,
										jt[R]->finfo, handle->precord());
							result_tuples++;
						}
					}
				}
			} else {
				// write to disk
				W_DO(af[hashkey]->create_rec(handle->precord(), rid));
			}
		} else
			break;
	} // while (1)

	for (i = 0; i < num_bucket; ++i)
		delete af[i];
	delete[] af;

	return RCOK;
}

// build hash table for the L partitions on the disk.
// the same as in Grace Hash Join
w_rc_t ahhjoin_q::build_hash_table_pax(stid_t& pfid, hash_table_t* htable) {
	scan_pfile_i scan(pfid);
	pin_prec_i* handle;
	bool eof = false;
	char buf[1024];
	precord_t *new_prec;

	while (1) {
		W_DO(scan.next(handle, eof));
		if (eof == false) {
			new_prec = new precord_t(handle->precord());
			htable->add(
					toString(jt[L]->finfo, *new_prec, jt[L]->join_col, buf),
					new_prec);
		} else
			break;
	}
	return RCOK;
}

// probe the hash table using the on-disk partition
w_rc_t ahhjoin_q::probe_hash_table_pax(stid_t& pfid, hash_table_t* htable) {
	scan_pfile_i scan(pfid);
	pin_prec_i* handle;
	bool eof = false;
	char buf[1024];
	hash_table_t::val_array_t * vPtr;
	uint4_t i;

	precord_t* left_prec;

	while (1) {
		W_DO(scan.next(handle, eof));
		if (eof == false) {
			vPtr = htable->find_match(toString(jt[R]->finfo, handle->precord(),
					jt[R]->join_col, buf));

			if (vPtr != NULL) {
				for (i = 0; i < vPtr->getsize(); i++) {
					left_prec = (precord_t*) (*vPtr)[i];

					if (compare(left_prec->data[jt[L]->join_col],
							handle->precord().data[jt[R]->join_col],
							jt[L]->finfo.field_type[jt[L]->join_col]) == 0) {
						if (ft_func != NULL)
							ft_func(caller, left_prec, &(handle->precord()));
						else
							print_two_precs(jt[L]->finfo, *left_prec,
									jt[R]->finfo, handle->precord());
						result_tuples++;
					}
				}
			}
		} else
			break;
	}
	return RCOK;
}

// freeze a partition.
// write the tuples to the file, free the memory of the hash table.
// delete the hashtable, set it to NULL.
// return number of bytes reclaimed by free the elements .
uint4_t ahhjoin_q::freeze_hash_table_pax(hash_table_t* &htable,
		append_pfile_i& af) {
	if (htable == NULL)
		return 0;

	uint4_t nbytes = 0;
	uint4_t bukIndex;
	uint4_t i;
	hash_table_t::table_entry_t* entry;

	precord_t *prec;
	rid_t rid;

	// it should be wrapped by hashtable. UGLY Code.
	for (bukIndex = 0; bukIndex < htable->m_size; bukIndex++) {
		for (entry = htable->m_table[bukIndex].next; entry
				!= &(htable->m_table[bukIndex]); entry = entry->next) {
			w_assert3(entry);
			for (i = 0; i < entry->val_array.getsize(); i++) {
				prec = (precord_t*) (entry->val_array[i]);
				W_COERCE(af.create_rec(*prec, rid));
				nbytes += prec->size();
			}
		}
	}

	htable->reset();
	delete htable;
	htable = NULL;

	return nbytes;
}

/*
 *	HPL join methods
 *
 */
w_rc_t ahhjoin_q::run_hpl() {
	uint4_t i, ht_valid, bukIndex;

	hash_tables = new hash_table_t*[num_bucket];
	w_assert3(hash_tables != NULL);

	// Creating one hash table per bucket.
	for (i = 0; i < num_bucket; ++i)
		hash_tables[i] = new hash_table_t(ht_size);
	ht_valid = num_bucket; // number of valid hash tables

	// Create an array of store ids for the file on the left
	stid_t* leftfids = new stid_t[num_bucket];

	hpl_record_t left_hpl_rec(jt[L]->finfo.no_fields, jt[L]->finfo.no_ffields);
	jt[L]->finfo.to_hpl_rec(left_hpl_rec);
	for (i = 0; i < num_bucket; i++)
		W_DO(ssm->create_file(VolID, leftfids[i], ss_m::t_temporary, left_hpl_rec));

	// partition left and build hash table.
	W_DO(partition_left_hpl(leftfids, ht_valid));

	// Create an array of store ids for the file on the right
	stid_t* rightfids = new stid_t[num_bucket];

	hpl_record_t right_hpl_rec(jt[R]->finfo.no_fields, jt[R]->finfo.no_ffields);
	jt[R]->finfo.to_hpl_rec(right_hpl_rec);
	for (i = 0; i < num_bucket; i++)
		W_DO(ssm->create_file(VolID, rightfids[i], ss_m::t_temporary, right_hpl_rec));

	// partition the right and probe
	W_DO(partition_right_hpl(rightfids, ht_valid));

	// remove the in memory hash tables leave only the first one.
	for (i = 1; i < ht_valid; i++) {
		if (hash_tables[i] != NULL) {
			hash_tables[i]->reset();
			delete hash_tables[i];
			hash_tables[i] = NULL;
		}
	}
	// create the alias for the first hash table,
	// reset it  for further use
	if (hash_tables[0] == NULL) {
		hash_tables[0] = new hash_table_t(ht_size);
	}
	hash_table_t* htable = hash_tables[0];

	// build and probe for the remaining partitions.
	for (bukIndex = ht_valid; bukIndex < num_bucket; bukIndex++) {
		htable->reset();
		W_DO(build_hash_table_hpl(leftfids[bukIndex], htable));
		W_DO(probe_hash_table_hpl(rightfids[bukIndex], htable));
	}

	// fprintf(stderr, "Hash join produced %13.0f tuples.\n", result_tuples);

	// clean up
	htable->reset();
	delete hash_tables[0];
	delete[] hash_tables;
	hash_tables = NULL;

	for (i = 0; i < num_bucket; i++) {
		W_DO(ssm->destroy_file(rightfids[i]));
		W_DO(ssm->destroy_file(leftfids[i]));
	}

	delete[] rightfids;
	delete[] leftfids;

	return RCOK;
}

w_rc_t ahhjoin_q::partition_left_hpl(stid_t* fids, uint4_t& b_valid) {
	uint4_t i;
	int memQuota = mem_quota * 1024;

	// This is where I'll write the buckets
	append_hpl_p_i **af = new append_hpl_p_i*[num_bucket];
	hash_func_c hashf(num_bucket);

	for (i = 0; i < num_bucket; ++i)
		af[i] = new append_hpl_p_i(fids[i]);

	uint4_t hashkey = 0;

	scan_hpl_p_i scan(finfo[L].fid, cc, jt[L]->finfo.no_fields, jt[L]->ifields);
	pin_hpl_rec_i* handle;
	bool eof = false;
	char buf[1024];
	rid_t rid;
	hpl_record_t *new_hpl_rec;


	while (1)
	{

		W_DO(scan.next(handle, eof));
		if (eof == false)
		{
			// clean up the riff-raff
			if (jt[L]->ipred != NULL)
				if (jt[L]->ipred(&(handle->hpl_record()), jt[L]->finfo) == false)
					continue;


			hashkey = hashf(jt[L]->finfo, handle->hpl_record(), jt[L]->join_col);
			//cout << "hashkey=" << hashkey << "; b_valid=" << b_valid << endl;
			if (hashkey < b_valid)
			{

				new_hpl_rec = new hpl_record_t(handle->hpl_record());
				hash_tables[hashkey]->add(
						toString(jt[L]->finfo, *new_hpl_rec, jt[L]->join_col, buf), new_hpl_rec);
				memQuota -= new_hpl_rec->size();

				// test if the quota is used up.
				while (memQuota < 0)
				{
					b_valid--;
					memQuota += freeze_hash_table_hpl(hash_tables[b_valid], *af[b_valid]);
					hash_tables[b_valid] = NULL;
				}
			}
			else
			{

				// the hashtable is frozen; write to file.
				W_DO(af[hashkey]->create_rec(handle->hpl_record(), rid));
			}
		}
		else
			break;
	} // while



	for (i = 0; i < num_bucket; ++i)
		delete af[i];
	delete[] af;

	return RCOK;
}

// build hash table for the R partitions on the disk.
w_rc_t ahhjoin_q::partition_right_hpl(stid_t* fids, const uint4_t b_valid) {
	uint4_t i;
	append_hpl_p_i **af = new append_hpl_p_i*[num_bucket];
	hash_func_c hashf(num_bucket);

	for (i = 0; i < num_bucket; ++i)
		af[i] = new append_hpl_p_i(fids[i]);

	scan_hpl_p_i scan(finfo[R].fid, cc, jt[R]->finfo.no_fields, jt[R]->ifields);
	pin_hpl_rec_i* handle;
	bool eof = false;
	char buf[1024];
	hash_table_t::val_array_t * vPtr;
	uint4_t hashkey = 0;
	hpl_record_t *left_hpl_rec;

	rid_t rid;


	while (1)
	{
		W_DO(scan.next(handle, eof));
		if (eof == false)
		{
			// clean up the riff-raff
			if (jt[R]->ipred != NULL)
				if (jt[R]->ipred(&(handle->hpl_record()), jt[R]->finfo) == false)
					continue;

			hashkey = hashf(jt[R]->finfo, handle->hpl_record(), jt[R]->join_col);

			if (hashkey < b_valid)
			{

				// the hash table is in memory
				// probe it and output if match
				vPtr = hash_tables[hashkey]->find_match(toString(jt[R]->finfo,
						handle->hpl_record(), jt[R]->join_col, buf));

				if (vPtr != NULL)
				{
					for (i = 0; i < vPtr->getsize(); i++)
					{
						left_hpl_rec = (hpl_record_t*) (*vPtr)[i];

						if (compare(left_hpl_rec->data[jt[L]->join_col],
								handle->hpl_record().data[jt[R]->join_col],
								jt[L]->finfo.field_type[jt[L]->join_col]) == 0)
						{
							if (ft_func != NULL)
							{
								ft_func(caller, left_hpl_rec, &(handle->hpl_record()));
							}
							else
								print_two_hpl_recs(jt[L]->finfo, *left_hpl_rec, jt[R]->finfo,
										handle->hpl_record());
							result_tuples++;
						}
					}
				}
			}
			else
			{
				// write to disk
				W_DO(af[hashkey]->create_rec(handle->hpl_record(), rid));
			}
		}
		else
			break;
	} // while (1)

	for (i = 0; i < num_bucket; ++i)
		delete af[i];
	delete[] af;

	return RCOK;
}

// build hash table for the L partitions on the disk.
// the same as in Grace Hash Join
w_rc_t ahhjoin_q::build_hash_table_hpl(stid_t& pfid, hash_table_t* htable) {
	scan_hpl_p_i scan(pfid);
	pin_hpl_rec_i* handle;
	bool eof = false;
	char buf[1024];
	hpl_record_t *new_hpl_rec;

	while (1) {
		W_DO(scan.next(handle, eof));
		if (eof == false) {
			new_hpl_rec = new hpl_record_t(handle->hpl_record());
			htable->add(toString(jt[L]->finfo, *new_hpl_rec, jt[L]->join_col, buf), new_hpl_rec);
		} else
			break;
	}
	return RCOK;
}

// probe the hash table using the on-disk partition
w_rc_t ahhjoin_q::probe_hash_table_hpl(stid_t& pfid, hash_table_t* htable) {
	scan_hpl_p_i scan(pfid);
	pin_hpl_rec_i* handle;
	bool eof = false;
	char buf[1024];
	hash_table_t::val_array_t * vPtr;
	uint4_t i;

	hpl_record_t* left_hpl_rec;

	while (1)
	{
		W_DO(scan.next(handle, eof));
		if (eof == false)
		{
			vPtr = htable->find_match(toString(jt[R]->finfo, handle->hpl_record(), jt[R]->join_col,
					buf));

			if (vPtr != NULL)
			{
				for (i = 0; i < vPtr->getsize(); i++)
				{
					left_hpl_rec = (hpl_record_t*) (*vPtr)[i];

					if (compare(left_hpl_rec->data[jt[L]->join_col],
							handle->hpl_record().data[jt[R]->join_col],
							jt[L]->finfo.field_type[jt[L]->join_col]) == 0)
					{
						if (ft_func != NULL)
						{
							ft_func(caller, left_hpl_rec, &(handle->hpl_record()));
						}
						else
						{
							print_two_hpl_recs(jt[L]->finfo, *left_hpl_rec, jt[R]->finfo,handle->hpl_record());
						}
						result_tuples++;
					}
				}
			}
		}
		else
			break;
	}
	return RCOK;
}

// freeze a partition.
// write the tuples to the file, free the memory of the hash table.
// delete the hashtable, set it to NULL.
// return number of bytes reclaimed by free the elements .
uint4_t ahhjoin_q::freeze_hash_table_hpl(hash_table_t* &htable, append_hpl_p_i& af) {
	if (htable == NULL)
		return 0;

	uint4_t nbytes = 0;
	uint4_t bukIndex;
	uint4_t i;
	hash_table_t::table_entry_t* entry;

	hpl_record_t *hpl_rec;
	rid_t rid;

	// it should be wrapped by hashtable. UGLY Code.
	for (bukIndex = 0; bukIndex < htable->m_size; bukIndex++) {
		for (entry = htable->m_table[bukIndex].next; entry
				!= &(htable->m_table[bukIndex]); entry = entry->next) {
			w_assert3(entry);
			for (i = 0; i < entry->val_array.getsize(); i++) {
				hpl_rec = (hpl_record_t*) (entry->val_array[i]);
				W_COERCE(af.create_rec(*hpl_rec, rid));
				nbytes += hpl_rec->size();
			}
		}
	}

	htable->reset();
	delete htable;
	htable = NULL;

	return nbytes;
}

/*
 *	DSM join methods
 *
 */
w_rc_t ahhjoin_q::run_dsm() {
	return RCOK;
}

