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

/** @file shore_table.cpp
 *
 *  @brief Implementation of shore_table class
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#include "sm/shore/shore_table.h"

using namespace shore;

/* ---------------------------------------------------------------
 *
 * @brief: Macros for correct offset calculation
 *
 * --------------------------------------------------------------- */

//#define VAR_SLOT(start, offset)   ((offset_t*)((start)+(offset)))
#define VAR_SLOT(start, offset)   ((start)+(offset))
#define SET_NULL_FLAG(start, offset)                            \
    (*(char*)((start)+((offset)>>3))) &= (1<<((offset)>>3))
#define IS_NULL_FLAG(start, offset)                     \
    (*(char*)((start)+((offset)>>3)))&(1<<((offset)>>3))

/****************************************************************** 
 *
 *  class table_desc_t methods 
 *
 ******************************************************************/

table_desc_t::table_desc_t(const char* name, int fieldcnt, uint4_t pd) :
		file_desc_t(name, fieldcnt, pd), _db(NULL), _indexes(NULL), _primary_idx(
				NULL), _maxsize(0)
{
	// Create placeholders for the field descriptors
	_desc = new field_desc_t[fieldcnt];
}

table_desc_t::~table_desc_t()
{
	if (_desc)
	{
		delete[] _desc;
		_desc = NULL;
	}

	if (_indexes)
	{
		delete _indexes;
		_indexes = NULL;
	}

}

/* ----------------------------------------- */
/* --- create physical table and indexes --- */
/* ----------------------------------------- */

/********************************************************************* 
 *
 *  @fn:    create_physical_table
 *
 *  @brief: Creates the physical table and calls for the (physical) creation of 
 *          all the corresponding indexes
 *
 *********************************************************************/

w_rc_t table_desc_t::create_physical_table(ss_m* db)
{
	assert(db);
	_db = db;

	if (!is_vid_valid() || !is_root_valid())
	{
		W_DO(find_root_iid(db));
	}

	// Create the table
	index_desc_t* index = _indexes;

	W_DO(db->create_file(vid(), _fid, smlevel_3::t_regular));

	TRACE(TRACE_STATISTICS, "%s %d\n", name(), fid().store);

	// Add table entry to the metadata tree
	file_info_t file;
	file.set_ftype(FT_HEAP);
	file.set_fid(_fid);
	W_DO(
			ss_m::create_assoc(root_iid(), vec_t(name(), strlen(name())),
					vec_t(&file, sizeof(file_info_t))));

	// Create all the indexes of the table
	while (index)
	{

		// Create physically this index
		W_DO(create_physical_index(db, index));

		// Move to the next index of the table
		index = index->next();
	}

	return (RCOK);
}

/********************************************************************* 
 *
 *  @fn:    create_physical_index
 *
 *  @brief: Creates the physical index 
 *
 *********************************************************************/

w_rc_t table_desc_t::create_physical_index(ss_m* db, index_desc_t* index)
{
	// Store info
	file_info_t file;

	// Create all the indexes of the table
	stid_t iid = stid_t::null;
	ss_m::ndx_t smidx_type = ss_m::t_uni_btree;
	ss_m::concurrency_t smidx_cc = ss_m::t_cc_im;

	// Update the type of index to create

	// Regular BTree
	smidx_type = index->is_unique() ? ss_m::t_uni_btree : ss_m::t_btree;

	// what kind of CC will be used
	smidx_cc = index->is_relaxed() ? ss_m::t_cc_none : ss_m::t_cc_im;

	// if it is the primary, update file flag
	if (index->is_primary())
	{
		file.set_ftype(FT_PRIMARY_IDX);
	}
	else
	{
		file.set_ftype(FT_IDX);
	}

	if (index->is_rmapholder())
	{
		file.set_ftype(FT_NONE);
	}

	// create one index or multiple, if the index is partitioned
	if (index->is_partitioned())
	{
		for (int i = 0; i < index->get_partition_count(); i++)
		{

			W_DO(
					db->create_index(_vid, smidx_type, ss_m::t_regular,
							index_keydesc(index), smidx_cc, iid));

			index->set_fid(i, iid);

			// add index entry to the metadata tree
			file.set_fid(iid);
			char tmp[100];
			sprintf(tmp, "%s_%d", index->name(), i);
			W_DO(
					db->create_assoc(root_iid(), vec_t(tmp, strlen(tmp)),
							vec_t(&file, sizeof(file_info_t))));
		}
	}
	else
	{

		W_DO(
				db->create_index(_vid, smidx_type, ss_m::t_regular,
						index_keydesc(index), smidx_cc, iid));

		index->set_fid(0, iid);

		// Add index entry to the metadata tree
		file.set_fid(iid);
		W_DO(
				db->create_assoc(root_iid(),
						vec_t(index->name(), strlen(index->name())),
						vec_t(&file, sizeof(file_info_t))));
	}

	// Print info
	TRACE(TRACE_STATISTICS, "%s %d (%s) (%s) (%s) (%s) (%s)\n", index->name(),
			iid.store, (index->is_mr() ? "mrbt" : "normal"),
			(index->is_latchless() ? "no latch" : "latch"),
			(index->is_relaxed() ? "relaxed" : "no relaxed"),
			(index->is_partitioned() ? "part" : "no part"),
			(index->is_unique() ? "unique" : "no unique"));

	return (RCOK);
}

/****************************************************************** 
 *  
 *  @fn:    create_index_desc
 *
 *  @brief: Create the description of a regular or primary index on the table
 *
 *  @note:  This only creates the index decription for the index in memory. 
 *
 ******************************************************************/

#warning Cannot update fields included at indexes - delete and insert again

#warning Only the last field of an index can be of variable length

bool table_desc_t::create_index_desc(const char* name, int partitions,
		const uint* fields, const uint num, const bool unique,
		const bool primary, const uint4_t& pd)
{
	index_desc_t* p_index = new index_desc_t(name, num, partitions, fields,
			unique, primary, pd);

	// check the validity of the index
	for (uint_t i = 0; i < num; i++)
	{
		assert(fields[i] < _field_count);

		// only the last field in the index can be variable lengthed
#warning IP: I am not sure if still only the last field in the index can be variable lengthed

		if (_desc[fields[i]].is_variable_length() && i != num - 1)
		{
			assert(false);
		}
	}

	// link it to the list
	if (_indexes == NULL)
		_indexes = p_index;
	else
		_indexes->insert(p_index);

	// add as primary
	if (p_index->is_unique() && p_index->is_primary())
		_primary_idx = p_index;

	return true;
}

bool table_desc_t::create_primary_idx_desc(const char* name, int partitions,
		const uint* fields, const uint num, const uint4_t& pd)
{
	index_desc_t* p_index = new index_desc_t(name, num, partitions, fields,
			true, true, pd);

	// check the validity of the index
	for (uint_t i = 0; i < num; i++)
	{
		assert(fields[i] < _field_count);

		// only the last field in the index can be variable lengthed
		if (_desc[fields[i]].is_variable_length() && i != num - 1)
		{
			assert(false);
		}
	}

	// link it to the list of indexes
	if (_indexes == NULL)
		_indexes = p_index;
	else
		_indexes->insert(p_index);

	// make it the primary index
	_primary_idx = p_index;

	return (true);
}

// Returns the stid of the primary index. If no primary index exists it
// returns the stid of the table
stid_t table_desc_t::get_primary_stid()
{
	stid_t stid = (_primary_idx ? _primary_idx->fid(0) : fid());
	return (stid);
}

/* ----------------- */
/* --- debugging --- */
/* ----------------- */

// For debug use only: print the description for all the field
void table_desc_t::print_desc(ostream& os)
{
	os << "Schema for table " << _name << endl;
	os << "Numer of fields: " << _field_count << endl;
	for (uint_t i = 0; i < _field_count; i++)
	{
		_desc[i].print_desc(os);
	}
}

/****************************************************************** 
 *
 *  class table_man_t methods 
 *
 ******************************************************************/

/* ---------------------------- */
/* --- formating operations --- */
/* ---------------------------- */

/********************************************************************* 
 *
 *  @fn:      format
 *
 *  @brief:   Return a string of the tuple (array of pvalues[]) formatted 
 *            to the appropriate disk format so it can be pushed down to 
 *            data pages in Shore. The size of the data buffer is in 
 *            parameter (bufsz).
 *
 *  @warning: This function should be the inverse of the load() 
 *            function changes to one of the two functions should be
 *            mirrored to the other.
 *
 *  @note:    convert: memory -> disk format
 *
 *********************************************************************/

int table_man_t::format(table_tuple* ptuple, rep_row_t &arep)
{
	// Format the data field by field

	// 1. Get the pre-calculated offsets

	// current offset for fixed length field values
	offset_t fixed_offset = ptuple->get_fixed_offset();

	// current offset for variable length field slots
	offset_t var_slot_offset = ptuple->get_var_slot_offset();

	// current offset for variable length field values
	offset_t var_offset = ptuple->get_var_offset();

	// 2. calculate the total space of the tuple
	//   (tupsize)    : total space of the tuple

	int tupsize = 0;

	int null_count = ptuple->get_null_count();
	int fixed_size = ptuple->get_var_slot_offset() - ptuple->get_fixed_offset();

	// loop over all the variable-sized fields and add their real size (set at ::set())
	for (uint_t i = 0; i < _ptable->field_count(); i++)
	{

		if (ptuple->_pvalues[i].is_variable_length())
		{
			// If it is of VARIABLE length, then if the value is null
			// do nothing, else add to the total tuple length the (real)
			// size of the value plus the size of an offset.

			if (ptuple->_pvalues[i].is_null())
				continue;
			tupsize += ptuple->_pvalues[i].realsize();
			tupsize += sizeof(offset_t);
		}

		// If it is of FIXED length, then increase the total tuple
		// length, as well as, the size of the fixed length part of
		// the tuple by the fixed size of this type of field.

		// IP: The length of the fixed-sized fields is added after the loop
	}

	// Add up the length of the fixed-sized fields
	tupsize += fixed_size;

	// In the total tuple length add the size of the bitmap that
	// shows which fields can be NULL
	if (null_count)
		tupsize += (null_count >> 3) + 1;
	assert(tupsize);

	// 3. allocate space for the formatted data
	arep.set(tupsize);

	// 4. Copy the fields to the array, field by field

	int null_index = -1;
	// iterate over all fields
	for (uint_t i = 0; i < _ptable->field_count(); i++)
	{

		// Check if the field can be NULL.
		// If it can be NULL, increase the null_index, and
		// if it is indeed NULL set the corresponding bit
		if (ptuple->_pvalues[i].field_desc()->allow_null())
		{
			null_index++;
			if (ptuple->_pvalues[i].is_null())
			{
				SET_NULL_FLAG(arep._dest, null_index);
			}
		}

		// Check if the field is of VARIABLE length.
		// If it is, copy the field value to the variable length part of the
		// buffer, to position  (buffer + var_offset)
		// and increase the var_offset.
		if (ptuple->_pvalues[i].is_variable_length())
		{
			ptuple->_pvalues[i].copy_value(arep._dest + var_offset);
			int offset = ptuple->_pvalues[i].realsize();
			var_offset += offset;

			// set the offset
			offset_t len = offset;
			memcpy(VAR_SLOT(arep._dest, var_slot_offset), &len,
					sizeof(offset_t));
			var_slot_offset += sizeof(offset_t);
		}
		else
		{
			// If it is of FIXED length, then copy the field value to the
			// fixed length part of the buffer, to position
			// (buffer + fixed_offset)
			// and increase the fixed_offset
			ptuple->_pvalues[i].copy_value(arep._dest + fixed_offset);
			fixed_offset += ptuple->_pvalues[i].maxsize();
		}
	}
	return (tupsize);
}

/*********************************************************************
 *
 *  @fn:      load
 *
 *  @brief:   Given a tuple in disk format, read it back into memory
 *            (_pvalues[] array).
 *
 *  @warning: This function should be the inverse of the format() function
 *            changes to one of the two functions should be mirrored to
 *            the other.
 *
 *  @note:    convert: disk -> memory format
 *
 *********************************************************************/

bool table_man_t::load(table_tuple* ptuple, const char* data)
{
	// Read the data field by field
	assert(ptuple);
	assert(data);

	// 1. Get the pre-calculated offsets

	// current offset for fixed length field values
	offset_t fixed_offset = ptuple->get_fixed_offset();

	// current offset for variable length field slots
	offset_t var_slot_offset = ptuple->get_var_slot_offset();

	// current offset for variable length field values
	offset_t var_offset = ptuple->get_var_offset();

	// 2. Read the data field by field

	int null_index = -1;
	for (uint_t i = 0; i < _ptable->field_count(); i++)
	{

		// Check if the field can be NULL.
		// If it can be NULL, increase the null_index,
		// and check if the bit in the null_flags bitmap is set.
		// If it is set, set the corresponding value in the tuple
		// as null, and go to the next field, ignoring the rest
		if (ptuple->_pvalues[i].field_desc()->allow_null())
		{
			null_index++;
			if (IS_NULL_FLAG(data, null_index))
			{
				ptuple->_pvalues[i].set_null();
				continue;
			}
		}

		// Check if the field is of VARIABLE length.
		// If it is, copy the offset of the value from the offset part of the
		// buffer (pointed by var_slot_offset). Then, copy that many chars from
		// the variable length part of the buffer (pointed by var_offset).
		// Then increase by one offset index, and offset of the pointer of the
		// next variable value
		if (ptuple->_pvalues[i].is_variable_length())
		{
			offset_t var_len;
			memcpy(&var_len, VAR_SLOT(data, var_slot_offset), sizeof(offset_t));
			ptuple->_pvalues[i].set_value(data + var_offset, var_len);
			var_offset += var_len;
			var_slot_offset += sizeof(offset_t);
		}
		else
		{
			// If it is of FIXED length, copy the data from the fixed length
			// part of the buffer (pointed by fixed_offset), and the increase
			// the fixed offset by the (fixed) size of the field
			ptuple->_pvalues[i].set_value(data + fixed_offset,
					ptuple->_pvalues[i].maxsize());
			fixed_offset += ptuple->_pvalues[i].maxsize();
		}
	}
	return (true);
}

/****************************************************************** 
 *
 *  @fn:      format_key
 *
 *  @brief:   Gets an index and for a selected row it copies to the
 *            passed buffer only the fields that are contained in the 
 *            index and returns the size of the newly allocated buffer, 
 *            which is the key_size for the index. The size of the 
 *            data buffer is in parameter (bufsz).
 *
 *  @warning: This function should be the inverse of the load_key() 
 *            function changes to one of the two functions should be
 *            mirrored to the other.
 *
 *  @note:    !!! Uses the maxsize() of each field, so even the
 *            variable legnth fields will be treated as of fixed size
 *
 ******************************************************************/

int table_man_t::format_key(index_desc_t* pindex, table_tuple* ptuple,
		rep_row_t &arep)
{
	assert(_ptable);
	assert(pindex);
	assert(ptuple);

	// 1. calculate the key size
	int isz = key_size(pindex, ptuple);
	assert(isz);

	// 2. allocate buffer space, if necessary
	arep.set(isz);

	// 3. write the buffer
	offset_t offset = 0;
	for (uint_t i = 0; i < pindex->field_count(); i++)
	{
		int ix = pindex->key_index(i);
		field_value_t* pfv = &ptuple->_pvalues[ix];

		// copy value
		if (!pfv->copy_value(arep._dest + offset))
		{
			assert(false); // problem in copying value
			return (0);
		}

		// IP: previously it was making distinction whether
		// the field was of fixed or variable length
		offset += pfv->maxsize();
	}
	return (isz);
}

/*********************************************************************
 *
 *  @fn:      load_key
 *
 *  @brief:   Given a buffer with the representation of the tuple in 
 *            disk format, read back into memory (to _pvalues[] array),
 *            but it reads only the fields that are contained to the
 *            specified index.
 *
 *  @warning: This function should be the inverse of the format_key() 
 *            function changes to one of the two functions should be
 *            mirrored to the other.
 *
 *  @note:    convert: disk -> memory format (for the key)
 *
 *********************************************************************/

bool table_man_t::load_key(const char* string, index_desc_t* pindex,
		table_tuple* ptuple)
{
	assert(_ptable);
	assert(pindex);
	assert(string);

	int offset = 0;
	for (uint_t i = 0; i < pindex->field_count(); i++)
	{
		uint_t field_index = pindex->key_index(i);
		uint_t size = ptuple->_pvalues[field_index].maxsize();
		ptuple->_pvalues[field_index].set_value(string + offset, size);
		offset += size;
	}

	return (true);
}

/****************************************************************** 
 *
 *  @fn:    min_key/max_key
 *
 *  @brief: Gets an index and for a selected row it sets all the 
 *          fields that are contained in the index to their 
 *          minimum or maximum value
 *
 ******************************************************************/

int table_man_t::min_key(index_desc_t* pindex, table_tuple* ptuple,
		rep_row_t &arep)
{
	assert(_ptable);
	for (uint_t i = 0; i < pindex->field_count(); i++)
	{
		uint_t field_index = pindex->key_index(i);
		ptuple->_pvalues[field_index].set_min_value();
	}
	return (format_key(pindex, ptuple, arep));
}

int table_man_t::max_key(index_desc_t* pindex, table_tuple* ptuple,
		rep_row_t &arep)
{
	assert(_ptable);
	for (uint_t i = 0; i < pindex->field_count(); i++)
	{
		uint_t field_index = pindex->key_index(i);
		ptuple->_pvalues[field_index].set_max_value();
	}
	return (format_key(pindex, ptuple, arep));
}

/****************************************************************** 
 *
 *  @fn:    key_size
 *
 *  @brief: For an index and a selected row it returns the 
 *          real or maximum size of the index key
 *
 *  @note: !!! Uses the maxsize() of each field, so even the
 *         variable legnth fields will be treated as of fixed size
 *
 *  @note: Since all fields of an index are of fixed length
 *         key_size() == maxkeysize()
 *
 ******************************************************************/

int table_man_t::key_size(index_desc_t* pindex, const table_tuple*) const
{
	assert(_ptable);
	return (_ptable->index_maxkeysize(pindex));
}

/* ---------------------------- */
/* --- access through index --- */
/* ---------------------------- */

int table_man_t::get_pnum(index_desc_t* pindex, table_tuple const* ptuple) const
{
	assert(ptuple);
	assert(pindex);
	if (!pindex->is_partitioned())
		return 0;

	int first_key;
	ptuple->get_value(pindex->key_index(0), first_key);
	return (first_key % pindex->get_partition_count());
}

mcs_lock table_man_t::register_table_lock;
std::map<stid_t, table_man_t*> table_man_t::stid_to_tableman;

void table_man_t::register_table_man()
{
	CRITICAL_SECTION(regtablecs, register_table_lock);
	stid_to_tableman[this->table()->fid()] = this;
}

/********************************************************************* 
 *
 *  @fn:    index_probe
 *  
 *  @brief: Finds the rid of the specified key using a certain index
 *
 *  @note:  The key is parsed from the tuple that it is passed as parameter
 *
 *********************************************************************/

w_rc_t table_man_t::index_probe(ss_m* db, index_desc_t* pindex,
		table_tuple* ptuple, lock_mode_t lock_mode, const lpid_t& root)
{
	assert(_ptable);
	assert(pindex);
	assert(ptuple);
	assert(ptuple->_rep);

	uint4_t system_mode = pindex->get_pd();

	bool found = false;
	smsize_t len = sizeof(rid_t);

	// ensure valid index
	W_DO(pindex->check_fid(db));

	// find the tuple in the index
	int key_sz = format_key(pindex, ptuple, *ptuple->_rep);
	assert(ptuple->_rep->_dest); // if NULL invalid key

	int pnum = get_pnum(pindex, ptuple);

	W_DO(
			ss_m::find_assoc(pindex->fid(pnum),
					vec_t(ptuple->_rep->_dest, key_sz), &(ptuple->_rid), len,
					found));

	if (!found)
		return RC(se_TUPLE_NOT_FOUND);

	// read the tuple
	pin_i pin;
	W_DO(pin.pin(ptuple->rid(), 0, lock_mode, LATCH_SH));

	if (!load(ptuple, pin.body()))
	{
		pin.unpin();
		return RC(se_WRONG_DISK_DATA);
	}
	pin.unpin();
	return (RCOK);
}

/* -------------------------- */
/* --- tuple manipulation --- */
/* -------------------------- */

/********************************************************************* 
 *
 *  @fn:    add_tuple
 *
 *  @brief: Inserts a tuple to a table and all the indexes of the table
 *
 *  @note:  This function should be called in the context of a trx.
 *          The passed tuple should be formed. If everything goes as
 *          expected the _rid of the tuple will be set. 
 *
 *********************************************************************/

w_rc_t table_man_t::add_tuple(ss_m* db, table_tuple* ptuple,
		const lock_mode_t lock_mode, const lpid_t& primary_root)
{
	assert(_ptable);
	assert(ptuple);
	assert(ptuple->_rep);
	uint4_t system_mode = _ptable->get_pd();

	// find the file
	W_DO(_ptable->check_fid(db));

	// append the tuple
	int tsz = format(ptuple, *ptuple->_rep);
	assert(ptuple->_rep->_dest); // if NULL invalid

	W_DO(
			db->create_rec(_ptable->fid(), vec_t(), tsz,
					vec_t(ptuple->_rep->_dest, tsz), ptuple->_rid));

	// update the indexes
	index_desc_t* index = _ptable->indexes();
	int ksz = 0;

	while (index)
	{
		ksz = format_key(index, ptuple, *ptuple->_rep);
		assert(ptuple->_rep->_dest); // if dest == NULL there is invalid key

		int pnum = get_pnum(index, ptuple);
		W_DO(index->find_fid(db, pnum));

		W_DO(
				db->create_assoc(index->fid(pnum),
						vec_t(ptuple->_rep->_dest, ksz),
						vec_t(&(ptuple->_rid), sizeof(rid_t))));
		// move to next index
		index = index->next();
	}
	return (RCOK);
}

/********************************************************************* 
 *
 *  @fn:    delete_tuple
 *
 *  @brief: Deletes a tuple from a table and the corresponding entries
 *          on all the indexes of the table
 *
 *  @note:  This function should be called in the context of a trx
 *          The passed tuple should be valid.
 *
 *********************************************************************/

w_rc_t table_man_t::delete_tuple(ss_m* db, table_tuple* ptuple,
		const lock_mode_t lock_mode, const lpid_t& primary_root)
{
	assert(_ptable);
	assert(ptuple);
	assert(ptuple->_rep);

	if (!ptuple->is_rid_valid())
		return RC(se_NO_CURRENT_TUPLE);

	uint4_t system_mode = _ptable->get_pd();
	rid_t todelete = ptuple->rid();

	// delete all the corresponding index entries
	index_desc_t* pindex = _ptable->indexes();
	int key_sz = 0;

	while (pindex)
	{
		key_sz = format_key(pindex, ptuple, *ptuple->_rep);
		assert(ptuple->_rep->_dest); // if NULL invalid key

		int pnum = get_pnum(pindex, ptuple);
		W_DO(pindex->find_fid(db, pnum));

		W_DO(
				db->destroy_assoc(pindex->fid(pnum),
						vec_t(ptuple->_rep->_dest, key_sz),
						vec_t(&(todelete), sizeof(rid_t))));

		// move to next index
		pindex = pindex->next();
	}

	W_DO(db->destroy_rec(todelete));

	// invalidate tuple
	ptuple->set_rid(rid_t::null);
	return (RCOK);
}

/********************************************************************* 
 *
 *  @fn:    update_tuple
 *
 *  @brief: Updates a tuple from a table, using direct access through
 *          its RID
 *
 *  @note:  This function should be called in the context of a trx.
 *          The passed tuple rid() should be valid. 
 *          There is no need of updating the indexes. That's why there
 *          is not parameter to primary_root.
 *
 *  !!! In order to update a field included by an index !!!
 *  !!! the tuple should be deleted and inserted again  !!!
 *
 *********************************************************************/

w_rc_t table_man_t::update_tuple(ss_m* /* db */, table_tuple* ptuple,
		const lock_mode_t lock_mode) // physical_design_t
{
	assert(_ptable);
	assert(ptuple);
	assert(ptuple->_rep);

	if (!ptuple->is_rid_valid())
		return RC(se_NO_CURRENT_TUPLE);

	uint4_t system_mode = _ptable->get_pd();

	// pin record
	pin_i pin;
	W_DO(pin.pin(ptuple->rid(), 0, lock_mode, LATCH_EX));
	int current_size = pin.body_size();

	// update record
	int tsz = format(ptuple, *ptuple->_rep);
	assert(ptuple->_rep->_dest); // if NULL invalid

	// a. if updated record cannot fit in the previous spot
	w_rc_t rc;
	if (current_size < tsz)
	{
#if HPL_DEBUG>0
		//cout << "appending..." << endl;
#endif
		zvec_t azv(tsz - current_size);

		rc = pin.append_rec(azv);

		// on error unpin
		if (rc.is_error())
		{
			TRACE(TRACE_DEBUG, "Error updating (by append) record\n");
			pin.unpin();
		}
		W_DO(rc);
	}

#if HPL_DEBUG>0
		//cout << "...." << endl;
#endif
	// b. else, simply update
	rc = pin.update_rec(0, vec_t(ptuple->_rep->_dest, tsz), 0);

	if (rc.is_error())
		TRACE(TRACE_DEBUG, "Error updating record\n");

	// 3. unpin
	pin.unpin();
	return (rc);
}

/********************************************************************* 
 *
 *  @fn:    read_tuple
 *
 *  @brief: Access a tuple directly through its RID
 *
 *  @note:  This function should be called in the context of a trx
 *          The passed RID should be valid.
 *          No index probe its involved that's why it does not get
 *          any hint about the starting root pid.
 *
 *********************************************************************/

w_rc_t table_man_t::read_tuple(table_tuple* ptuple, const lock_mode_t lock_mode)
{
	assert(_ptable);
	assert(ptuple);

	if (!ptuple->is_rid_valid())
		return RC(se_NO_CURRENT_TUPLE);

	uint4_t system_mode = _ptable->get_pd();

	pin_i pin;
	W_DO(pin.pin(ptuple->rid(), 0, lock_mode, LATCH_EX));
	if (!load(ptuple, pin.body()))
	{
		pin.unpin();
		return RC(se_WRONG_DISK_DATA);
	}
	pin.unpin();

	return (RCOK);
}

#include <sstream>
char const* db_pretty_print(table_desc_t const* ptdesc, int /* i=0 */,
		char const* /* s=0 */)
{
	static char data[1024];
	std::stringstream inout(data, stringstream::in | stringstream::out);
	//std::strstream inout(data, sizeof(data));
	((table_desc_t*) ptdesc)->print_desc(inout);
	inout << std::ends;
	return data;
}

