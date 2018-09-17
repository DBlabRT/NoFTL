#define _GNU_SOURCE

#include "dftl_external.h"
#include "list.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>

static int get_next_free_sector(struct dftl_s *dftl, unit_type_e unit_type, int is_gc);
static int get_map_sector_lsn (struct dftl_s *dftl, int data_sector_lsn);
static int single_mapping_update_in_map_sector(struct dftl_s *dftl, int data_sector_lsn, int data_sector_new_psn, int map_sector_new_psn, int is_gc);
static int batch_update_in_map_sector(struct dftl_s *dftl, struct list_head *batch_update_store, int is_gc);
static void update_gtd_entry (struct dftl_s *dftl, int lsn, int new_psn);
static void increment_io_statistic(struct dftl_s *dftl, enum io_type_e io_type, unit_type_e unit_type, int is_gc);

/******************************OOB / DATA IO***************************************/

static void * get_and_fix_sector_buffer(struct dftl_s *dftl)
{
	if (!dftl->sector_buffer.fixed) {
		dftl->sector_buffer.fixed = 1;
		return dftl->sector_buffer.buffer;
	}
	else {
		print_error("Attempt to use several sector buffers at a time. Not supported yet!");
		return NULL;
	}
}

static void unfix_sector_buffer(struct dftl_s *dftl)
{
	dftl->sector_buffer.fixed = 0;
}

static void clear_sector_buffer(struct dftl_s *dftl)
{
	if (dftl->sector_buffer.buffer)
		memset(dftl->sector_buffer.buffer, 0, dftl->params.dev_params->sector_size);
}

static int write_sector_oob_field(struct dftl_s *dftl, int psn, int field_idx, oob_field field_value, unit_type_e unit_type, int is_gc, int simulate_delay)
{
	int retval = 0;
	int block_idx;

	if (psn < 0)
		return -EINVAL;

	retval = access_sector_oob(dftl, psn, &field_value, (field_idx*sizeof(oob_field)), sizeof(oob_field), WRITE, simulate_delay);
	if (retval) {
		printf_error("Failed to write oob field psn=%d!", psn);
		return retval;
	}

	if (field_idx == OOB_STATUS_INDEX && field_value == INVALID_SECTOR) {
		printf_debug("Invalidating sector psn=%d", psn);
		block_idx = psn / dftl->params.dev_params->sectors_per_block;
		dftl->block_stat[block_idx].invalid_sector_count++;
	}

	increment_io_statistic(dftl, simulate_delay ? WRITE_SECTOR_OOB : WRITE_SECTOR_OOB_WITHOUT_DELAY, unit_type, is_gc);

	return 0;
}

static int read_sector_oob(struct dftl_s *dftl, int psn, oob_field *sector_oob, unit_type_e unit_type, int is_gc, int simulate_delay)
{
	int retval = 0;

	if (psn < 0)
		return -EINVAL;

	retval = access_sector_oob(dftl, psn, sector_oob, 0, sizeof(oob), READ, simulate_delay);
	if (retval) {
		printf_error("Failed to read oob psn=%d!", psn);
		return retval;
	}

	increment_io_statistic(dftl, simulate_delay ? READ_SECTOR_OOB : READ_SECTOR_OOB_WITHOUT_DELAY, unit_type, is_gc);

	return 0;
}

static int write_sector_oob(struct dftl_s *dftl, int psn, oob_field *sector_oob, unit_type_e unit_type, int is_gc, int simulate_delay)
{
	int retval = 0;
	int block_idx;

	if (psn < 0)
		return -EINVAL;

	retval = access_sector_oob(dftl, psn, sector_oob, 0, sizeof(oob), WRITE, simulate_delay);
	if (retval) {
		printf_error("Failed to write sector oob psn=%d!", psn);
		return retval;
	}

	if (sector_oob[OOB_STATUS_INDEX] == INVALID_SECTOR) {
		printf_debug("Invalidating sector psn=%d", psn);
		block_idx = psn / dftl->params.dev_params->sectors_per_block;
		dftl->block_stat[block_idx].invalid_sector_count++;
	}

	increment_io_statistic(dftl, simulate_delay ? WRITE_SECTOR_OOB : WRITE_SECTOR_OOB_WITHOUT_DELAY, unit_type, is_gc);

	return 0;
}

static int set_sector_status(struct dftl_s *dftl, int psn, oob_field sector_status, unit_type_e unit_type, int is_gc, int simulate_delay)
{
	return write_sector_oob_field(dftl, psn, OOB_STATUS_INDEX, sector_status, unit_type, is_gc, simulate_delay);
}

static int read_sector(struct dftl_s *dftl, int psn, void * buffer, unit_type_e unit_type, int is_gc)
{
	loff_t offset;

	printf_debug("Reading sector psn=%d, unit_type=%d", psn, unit_type);

	if (psn < 0)
		return -EINVAL;

	int read_bytes = pread(dftl->device_fd, buffer, dftl->params.dev_params->sector_size, ((long)psn*(long)dftl->params.dev_params->sector_size));
	if (read_bytes != dftl->params.dev_params->sector_size) {
		printf_error("pread command failed: %s\n", strerror(errno));
		return -EIO;
	}

	increment_io_statistic(dftl, READ_SECTOR, unit_type, is_gc);

	return 0;
}

static void end_sector_write(struct dftl_s *dftl, int psn, unit_type_e unit_type, int is_gc)
{
	int block_idx = psn / dftl->params.dev_params->sectors_per_block;
	if (dftl->block_stat[block_idx].block_status == FREE_BLOCK) {
		dftl->block_stat[block_idx].block_status = VALID_BLOCK;
		dftl->free_block_num--;
		dftl->block_stat[block_idx].block_type = unit_type;
	}
	dftl->block_stat[block_idx].free_sector_count--;
	dftl->last_written_sector[unit_type] = psn;

	increment_io_statistic(dftl, WRITE_SECTOR, unit_type, is_gc);
}

static int write_sector_with_oob (struct dftl_s *dftl, int psn, int lsn, const void *src, unit_type_e unit_type, int is_gc)
{
	int retval = 0;
	oob sector_oob;
	loff_t offset;

	printf_debug("Writing sector psn=%d, unit_type=%d", psn, unit_type);

	if (psn < 0)
		return -EINVAL;

	int written_bytes = pwrite(dftl->device_fd, src, dftl->params.dev_params->sector_size, ((long)psn*(long)dftl->params.dev_params->sector_size));
	if (written_bytes != dftl->params.dev_params->sector_size) {
		printf_error("ERROR: pwrite command failed: %s\n", strerror(errno));
		return -EIO;
	}

	//write oob
	sector_oob[OOB_LSN_INDEX] = lsn;
	sector_oob[OOB_STATUS_INDEX] = VALID_SECTOR;
	sector_oob[OOB_TYPE_INDEX] = unit_type;
	retval = write_sector_oob(dftl, psn, sector_oob, unit_type, is_gc, 0);
	if (retval)
		return retval;

	//statistics
	end_sector_write(dftl, psn, unit_type, is_gc);
	return 0;
}

static int erase_block(struct dftl_s *dftl, int block_idx)
{
	int retval = 0;

	printf_debug("Erasing block_idx=%d", block_idx);

	retval = ioctl(dftl->device_fd, ERASE_BLOCK, &block_idx);
	if (retval) {
		printf_error("Failed to erase block %d", block_idx);
		return -EIO;
	}

	increment_io_statistic(dftl, ERASE, dftl->block_stat[block_idx].block_type, GC);

	dftl->block_stat[block_idx].block_status = FREE_BLOCK;
	dftl->free_block_num++;
	dftl->block_stat[block_idx].erase_count++;
	dftl->block_stat[block_idx].free_sector_count = dftl->params.dev_params->sectors_per_block;
	dftl->block_stat[block_idx].invalid_sector_count = 0;

	return 0;
}

/****************************GARBAGE COLLECTOR*************************************/

struct mapping_update {
	struct list_head list;
	int data_sector_lsn;
	int data_sector_psn;
};

struct map_sector_update {
	int map_sector_lsn;
	struct list_head list;

	struct list_head mapping_updates;
};

static int find_victim_block(struct dftl_s *dftl)
{
	int i;
	int temp_max_invalid_sectors_cnt = 0;
	int victim_block_idx = -1;
	int current_data_block;
	int current_map_block;

	current_data_block = dftl->last_written_sector[DATA_UNIT] / dftl->params.dev_params->sectors_per_block;
	current_map_block = dftl->last_written_sector[MAP_UNIT] / dftl->params.dev_params->sectors_per_block;

	for (i = 0; i < dftl->params.dev_params->num_blocks; i++) {
		//Current data and map blocks with free sectors (not full) should be omitted
		if ((i == current_data_block || i == current_map_block) && dftl->block_stat[i].free_sector_count != 0)
			continue;

		if (dftl->block_stat[i].invalid_sector_count > temp_max_invalid_sectors_cnt) {
			victim_block_idx = i;
			temp_max_invalid_sectors_cnt = dftl->block_stat[i].invalid_sector_count;
			if (dftl->block_stat[i].invalid_sector_count == dftl->params.dev_params->sectors_per_block)
				break;
		}
	}


	if (victim_block_idx == -1) {
		print_error("Failed to find victim block. Disk is full!");
		return -ENOSPC;
	}

	return victim_block_idx;
}

static void free_batch_map_update(struct list_head * batch_update_store)
{
	struct map_sector_update * map_sec_update_cursor;
	struct map_sector_update * map_sec_update_tmp;
	struct mapping_update  * mapping_update_cursor;
	struct mapping_update  * mapping_update_tmp;

	if (batch_update_store && !list_empty(batch_update_store)) {
		list_for_each_entry_safe(map_sec_update_cursor, map_sec_update_tmp, batch_update_store, list) {
			list_for_each_entry_safe(mapping_update_cursor, mapping_update_tmp, &map_sec_update_cursor->mapping_updates, list) {
				 list_del(&mapping_update_cursor->list);
				 kfree(mapping_update_cursor);
			}
			 list_del(&map_sec_update_cursor->list);
			 kfree(map_sec_update_cursor);
		}
	}
}

static int add_sector_to_batch_map_update(struct dftl_s *dftl, struct list_head *batch_update_store, int lsn, int new_psn)
{
	struct map_sector_update * map_sec_update_cursor;
	struct map_sector_update * map_sec_update = NULL;
	struct mapping_update  * mapping_update;
	int map_sector_lsn;

	map_sector_lsn = get_map_sector_lsn(dftl, lsn);
	list_for_each_entry(map_sec_update_cursor, batch_update_store, list) {
		if (map_sec_update_cursor->map_sector_lsn == map_sector_lsn) {
			map_sec_update = map_sec_update_cursor;
			dftl->stat.save_map_updates_via_batch_update++;
			break;
		}
	}
	if (!map_sec_update) {
		//kmalloc is much faster than vmalloc and is OK for allocating less than 4 Mb
		map_sec_update = kmalloc(sizeof(struct map_sector_update));
		if (!map_sec_update) {
			print_error("Memory allocation failed!");
			return -ENOMEM;
		}
		map_sec_update->map_sector_lsn = map_sector_lsn;
		INIT_LIST_HEAD(&map_sec_update->mapping_updates);
		list_add_tail(&map_sec_update->list, batch_update_store);
	}
	mapping_update = kmalloc(sizeof(struct mapping_update));
	if (!mapping_update) {
		print_error("Memory allocation failed!");
		return -ENOMEM;
	}
	mapping_update->data_sector_lsn = lsn;
	mapping_update->data_sector_psn = new_psn;

	list_add(&mapping_update->list, &map_sec_update->mapping_updates);

	return 0;
}

static int gc_rewrite_sector(struct dftl_s *dftl, void *sector_buffer, int psn, int lsn, unit_type_e unit_type)
{
	int retval = 0;
	int new_psn = -1;

	new_psn = get_next_free_sector(dftl, unit_type, GC);
	if (new_psn == -1)
		return new_psn; //error_code

	clear_sector_buffer(dftl);
	retval = read_sector(dftl, psn, sector_buffer, unit_type, GC);
	if (retval)
		return retval;
	retval = write_sector_with_oob(dftl, new_psn, lsn, sector_buffer, unit_type, GC);
	if (retval)
		return retval;

	return new_psn;
}

static int gc_rewrite_data_block(struct dftl_s *dftl, int victim_block_idx)
{
	int retval = 0;
	int i, psn, new_psn;
	void * sector_buffer;
	oob sector_oob;
	struct list_head batch_update_store;

	printf_debug("Rewriting data block %d", victim_block_idx);

	if (dftl->block_stat[victim_block_idx].block_type != DATA_UNIT || dftl->block_stat[victim_block_idx].block_status == FREE_BLOCK) {
		print_error("Prerequisites are not satisfied!");
		return -EINVAL;
	}

	INIT_LIST_HEAD(&batch_update_store);
	sector_buffer = get_and_fix_sector_buffer(dftl);
	if (!sector_buffer)
		return -ENOMEM;

	for (i = 0; i < dftl->params.dev_params->sectors_per_block; i++) {

		psn = (victim_block_idx * dftl->params.dev_params->sectors_per_block) + i;
		retval = read_sector_oob(dftl, psn, sector_oob, DATA_UNIT, GC, 1);
		if (retval)
			break;
		//TODO REMOVE consistency check
		if (sector_oob[OOB_TYPE_INDEX] != DATA_UNIT) {
			retval = -EIO;
			printf_error("Consistency check failed! psn=%d, lsn=%d, unit_type=%d", psn, sector_oob[OOB_LSN_INDEX], sector_oob[OOB_TYPE_INDEX]);
			break;
		}

		if (sector_oob[OOB_STATUS_INDEX] == VALID_SECTOR) {

			new_psn = gc_rewrite_sector(dftl, sector_buffer, psn, sector_oob[OOB_LSN_INDEX], DATA_UNIT);
			if (new_psn < 0) {
				retval = new_psn; //error_code
				break;
			}

			if (dftl->sector_map[sector_oob[OOB_LSN_INDEX]].cache_status == CMT_REAL || dftl->sector_map[sector_oob[OOB_LSN_INDEX]].cache_status == CMT_GHOST) {
				printf_debug("mapping of sector with lsn=% is in cache", sector_oob[OOB_LSN_INDEX]);
				dftl->sector_map[sector_oob[OOB_LSN_INDEX]].psn = new_psn;
				dftl->sector_map[sector_oob[OOB_LSN_INDEX]].updated = 1;
				dftl->stat.delay_flash_update++;
			}
			else {
				retval = add_sector_to_batch_map_update(dftl, &batch_update_store, sector_oob[OOB_LSN_INDEX], new_psn);
				//retval = single_mapping_update_in_map_sector(dftl, sector_oob[OOB_LSN_INDEX], new_psn, GC);
				if (retval)
					break;
			}
		}
	}

	unfix_sector_buffer(dftl);

	if (!retval && !list_empty(&batch_update_store))
		retval = batch_update_in_map_sector(dftl, &batch_update_store, GC);

	free_batch_map_update(&batch_update_store);
	return retval;
}

static int gc_rewrite_map_block(struct dftl_s *dftl, int victim_block_idx)
{
	int retval = 0;
	int i, psn, new_psn;
	void * sector_buffer;
	oob sector_oob;

	printf_debug("rewriting map block %d", victim_block_idx);

	if (dftl->block_stat[victim_block_idx].block_type != MAP_UNIT || dftl->block_stat[victim_block_idx].block_status == FREE_BLOCK) {
		print_error("Prerequisites are not satisfied!");
		return -EINVAL;
	}

	sector_buffer = get_and_fix_sector_buffer(dftl);
	if (!sector_buffer)
		return -ENOMEM;

	for (i = 0; i < dftl->params.dev_params->sectors_per_block; i++) {
		psn = (victim_block_idx * dftl->params.dev_params->sectors_per_block) + i;
		retval = read_sector_oob(dftl, psn, sector_oob, MAP_UNIT, GC, 1);
		if (retval)
			return retval;
		//TODO REMOVE consistency check
		if (sector_oob[OOB_TYPE_INDEX] != MAP_UNIT) {
			printf_error("Consistency check failed! psn=%d, lsn=%d, unit_type=%d", psn, sector_oob[OOB_LSN_INDEX], sector_oob[OOB_TYPE_INDEX]);
			return -EIO;
		}

		if (sector_oob[OOB_STATUS_INDEX] == VALID_SECTOR) {
			printf_debug("valid sector psn=%d, lsn=%d rewriting", psn,sector_oob[OOB_LSN_INDEX]);
			new_psn = gc_rewrite_sector(dftl, sector_buffer, psn, sector_oob[OOB_LSN_INDEX], MAP_UNIT);
			if (new_psn < 0)
				return new_psn; //error_code
			update_gtd_entry(dftl, sector_oob[OOB_LSN_INDEX], new_psn);
		}
	}

	unfix_sector_buffer(dftl);
	return retval;
}

static int gc_run(struct dftl_s *dftl)
{
	int retval = 0;
	int victim_block_idx;

	print_debug("(begin)");

	victim_block_idx = find_victim_block(dftl);
	if (victim_block_idx < 0)
		return victim_block_idx; //error_code

	if (dftl->block_stat[victim_block_idx].block_type == DATA_UNIT)
		retval = gc_rewrite_data_block(dftl, victim_block_idx);
	else
		retval = gc_rewrite_map_block(dftl, victim_block_idx);

	if (retval) {
		print_error("Rewrite of block failed!");
		return retval;
	}

	retval = erase_block(dftl, victim_block_idx);
	if (retval)
		print_error("Erase block failed!");

	print_debug("(end)");

	return retval;
}

static int get_free_block(struct dftl_s *dftl, enum wl_type wl_type)
{
	int i;
	int temp_min_erase_cnt = INT_MAX;
	int block_idx = -1;

	if (wl_type == NO_WL) {
		for (i = 0; i < dftl->params.dev_params->num_blocks; i++) {
			if (dftl->block_stat[i].block_status == FREE_BLOCK) {
					block_idx = i;
					break;
			}
		}
	}
	else if (wl_type == MIN_EC_WL) {
		for (i = 0; i < dftl->params.dev_params->num_blocks; i++) {
			if (dftl->block_stat[i].block_status == FREE_BLOCK) {
				if (dftl->block_stat[i].erase_count < temp_min_erase_cnt) {
					temp_min_erase_cnt = dftl->block_stat[i].erase_count;
					block_idx = i;
					if (dftl->block_stat[i].erase_count == 0)
						break;
				}
			}
		}
	}

	if (block_idx == -1) {
		print_error("No free block could be found!");
		return -ENOSPC;
	}

	return block_idx;
}

static int get_next_free_sector(struct dftl_s *dftl, unit_type_e unit_type, int is_gc)
{
	int retval = 0;
	int block_idx = -1;

	//Garbage collector
	if (((dftl->last_written_sector[unit_type] % dftl->params.dev_params->sectors_per_block) == (dftl->params.dev_params->sectors_per_block - 1)) &&
			(is_gc == NOT_GC) && (dftl->free_block_num < GC_THRESHOLD))
	{
		while (dftl->free_block_num < GC_THRESHOLD + GC_RUN_STOCK_OVER_THRESHOLD) {
			retval = gc_run(dftl);
			if (retval) {
				print_error("Garbage collection failed!");
				return retval;
			}
		}
	}

	if ((dftl->last_written_sector[unit_type] == -1) ||
			(dftl->last_written_sector[unit_type] % dftl->params.dev_params->sectors_per_block) == (dftl->params.dev_params->sectors_per_block - 1))
	{
		block_idx = get_free_block(dftl, MIN_EC_WL);
		if (block_idx < 0)
			return block_idx; //error_code
		printf_debug("(end) next_free_sector=%d", (block_idx * dftl->params.dev_params->sectors_per_block));
		fprintf(dftl->debug_file, "     %d %d\n", unit_type, (block_idx * dftl->params.dev_params->sectors_per_block));
		return (block_idx * dftl->params.dev_params->sectors_per_block);
	}
	else {
		printf_debug("(end) next_free_sector=%d", (dftl->last_written_sector[unit_type] + 1));
		fprintf(dftl->debug_file, "     %d %d\n", unit_type, (dftl->last_written_sector[unit_type] + 1));
		return dftl->last_written_sector[unit_type] + 1;
	}
}

/******************************GTD & GMT******************************************/

static void update_gtd_entry (struct dftl_s *dftl, int lsn, int new_psn)
{
	dftl->gtd[lsn] = new_psn;
}

static int get_map_sector_lsn (struct dftl_s *dftl, int data_sector_lsn)
{
	return data_sector_lsn / dftl->params.gmt_entries_per_page;
}

static int get_map_sector_offset(struct dftl_s *dftl, int data_sector_lsn)
{
	return ((data_sector_lsn % dftl->params.gmt_entries_per_page) * sizeof(gmt_entry));
}

static int write_map_sector(struct dftl_s *dftl, const void * map_sector_data, int map_sector_lsn, int map_sector_new_psn, int is_gc)
{
	int retval = 0;

	printf_debug("Writing map sector lsn=%d, psn=%d", map_sector_lsn, map_sector_new_psn);

	retval = write_sector_with_oob(dftl, map_sector_new_psn, map_sector_lsn, map_sector_data, MAP_UNIT, is_gc);
	if (retval)
		return retval;

	//take the last (probably modified by GC) link to old map page
	if (dftl->gtd[map_sector_lsn] != -1) {
		retval = set_sector_status(dftl, dftl->gtd[map_sector_lsn], INVALID_SECTOR, MAP_UNIT, is_gc, 1);
		if (retval)
			return retval;
	}

	update_gtd_entry (dftl, map_sector_lsn, map_sector_new_psn);

	return 0;
}

static void single_mapping_update_in_map_sector_buffer(struct dftl_s *dftl, void *map_sector_buffer, int data_sector_lsn, int data_sector_new_psn)
{
	int offset = get_map_sector_offset(dftl, data_sector_lsn);
	memcpy(map_sector_buffer + offset, &data_sector_new_psn, sizeof(gmt_entry));
}

static int single_mapping_update_in_map_sector(struct dftl_s *dftl, int data_sector_lsn, int data_sector_new_psn, int map_sector_new_psn, int is_gc)
{
	int retval = 0;
	int map_sector_lsn;
	int map_sector_current_psn;
	void * map_sector_buffer;

	printf_debug("Updating map sector data_sector_lsn=%d, data_sector_new_psn=%d", data_sector_lsn, data_sector_new_psn);

	map_sector_lsn = get_map_sector_lsn(dftl, data_sector_lsn);
	map_sector_current_psn = dftl->gtd[map_sector_lsn];
	map_sector_buffer = get_and_fix_sector_buffer(dftl);
	if (!map_sector_buffer)
		return -ENOMEM;

	retval = read_sector(dftl, map_sector_current_psn, map_sector_buffer, MAP_UNIT, is_gc);
	if (retval)
		return retval;
	single_mapping_update_in_map_sector_buffer(dftl, map_sector_buffer, data_sector_lsn, data_sector_new_psn);
	retval = write_map_sector(dftl, map_sector_buffer, map_sector_lsn, map_sector_new_psn, is_gc);

	unfix_sector_buffer(dftl);
	return retval;
}

static int batch_update_in_map_sector(struct dftl_s *dftl, struct list_head *batch_update_store, int is_gc)
{
	int retval = 0;
	int map_sector_new_psn;
	int map_sector_current_psn;
	void * map_sector_buffer;
	struct map_sector_update * map_sector_cursor;
	struct mapping_update * mapping_cursor;

	print_debug("(begin)");

	map_sector_buffer = get_and_fix_sector_buffer(dftl);
	if (!map_sector_buffer)
		return -ENOMEM;

	list_for_each_entry(map_sector_cursor, batch_update_store, list){
		map_sector_new_psn = get_next_free_sector(dftl, MAP_UNIT, is_gc);
		if (map_sector_new_psn < 0)
			return map_sector_new_psn; //error_code
		map_sector_current_psn = dftl->gtd[map_sector_cursor->map_sector_lsn];
		retval = read_sector(dftl, map_sector_current_psn, map_sector_buffer, MAP_UNIT, is_gc);
		if (retval)
			break;
		list_for_each_entry(mapping_cursor, &map_sector_cursor->mapping_updates, list) {
			single_mapping_update_in_map_sector_buffer(dftl, map_sector_buffer, mapping_cursor->data_sector_lsn, mapping_cursor->data_sector_psn);
		}
		retval = write_map_sector(dftl, map_sector_buffer, map_sector_cursor->map_sector_lsn, map_sector_new_psn, is_gc);
		if(retval)
			return retval;
		clear_sector_buffer(dftl);
	}

	unfix_sector_buffer(dftl);
	print_debug("(end)");
	return retval;
}

static int load_psn_from_flash(struct dftl_s *dftl, int data_sector_lsn, int is_gc)
{
	int retval = 0;
	int data_sector_psn;
	int map_sector_lsn;
	int map_sector_psn;
	int map_sector_offset;
	void * map_sector_buffer;

	map_sector_lsn = get_map_sector_lsn(dftl, data_sector_lsn);
	map_sector_psn = dftl->gtd[map_sector_lsn];
	if (map_sector_psn == -1)
		return -1; //not error -> there is yet not mapping for lsn

	map_sector_buffer = get_and_fix_sector_buffer(dftl);
	if (!map_sector_buffer)
		return -ENOMEM;
	map_sector_offset = get_map_sector_offset(dftl, data_sector_lsn);
	retval = read_sector(dftl, map_sector_psn, map_sector_buffer, MAP_UNIT, is_gc);
	memcpy(&data_sector_psn, map_sector_buffer + map_sector_offset, sizeof(gmt_entry));
	unfix_sector_buffer(dftl);

	if (retval)
		return retval; //error_code
	else
		return data_sector_psn;
}

/********************************CACHE*********************************************/

static int find_lsn_with_min_age(struct dftl_s *dftl, int *array, int size, int *out_array_index)
{
	int temp_min_age = INT_MAX;
	int i;
	int lsn = -1;

	*out_array_index = -1;
	for (i = 0; i < size; i++) {
		if (dftl->sector_map[array[i]].age <= temp_min_age) {
			temp_min_age = dftl->sector_map[array[i]].age;
			lsn = array[i];
			*out_array_index = i;
		}
	}

	if (*out_array_index == -1 || lsn == -1)
		print_error("No entry found!");

	return lsn;
}

static int search_table(int *array, int size, int value)
{
    int i;
    for(i = 0 ; i < size; i++)
        if(array[i] == value)
            return i;

    print_error("No entry found!");
    return -1;
}

static int cache_hit_update(struct dftl_s *dftl, int lsn)
{
	map_entry * mapping;
	int real_lsn_with_min_age;
	int real_arr_index;
	int ghost_arr_index;

	mapping = &dftl->sector_map[lsn];
	if (mapping->cache_status != CMT_REAL && mapping->cache_status != CMT_GHOST) {
		print_error("Prerequisites are not satisfied!");
		return -EINVAL;
	}

	dftl->stat.cache_hit++;
	mapping->age++;

	if (dftl->cmt_entry_max_age < mapping->age)
		dftl->cmt_entry_max_age = mapping->age;

	printf_debug("lsn_age = %d, cache_max_age = %d", mapping->age, dftl->cmt_entry_max_age);

	if (mapping->cache_status == CMT_GHOST) {
		real_lsn_with_min_age = find_lsn_with_min_age(dftl, dftl->cmt_real_arr, dftl->params.cmt_real_max_entries, &real_arr_index);
		if (dftl->sector_map[real_lsn_with_min_age].age <= mapping->age) {
			//Exchange MIN_REAL->GHOST & MAX_GHOST->REAL
			mapping->cache_status = CMT_REAL;
			dftl->sector_map[real_lsn_with_min_age].cache_status = CMT_GHOST;

			ghost_arr_index = search_table(dftl->cmt_ghost_arr, dftl->params.cmt_ghost_max_entries, lsn);
			dftl->cmt_ghost_arr[ghost_arr_index] = real_lsn_with_min_age;
			dftl->cmt_real_arr[real_arr_index] = lsn;
			printf_debug("exchanging GHOST (lsn=%d)->REAL(lsn=%d)", lsn, real_lsn_with_min_age);
		}
	}
	return 0;
}

static int provide_free_pos_in_cmt_ghost(struct dftl_s *dftl)
{
	int retval = 0;
	int ghost_arr_idx;
	int ghost_lsn_with_min_age;
	int map_sector_new_psn;

	if ((dftl->params.cmt_ghost_max_entries - dftl->cmt_ghost_num_entries) == 0) { //GHOST is full
		//Find the victim MIN_GHOST
		ghost_lsn_with_min_age = find_lsn_with_min_age(dftl, dftl->cmt_ghost_arr, dftl->params.cmt_ghost_max_entries, &ghost_arr_idx);
		//Store updates if needed
		if (dftl->sector_map[ghost_lsn_with_min_age].updated == 1) {
			//Getting new sector should be done always first, before any reading (also getting from cache) and modifying data
			map_sector_new_psn = get_next_free_sector(dftl, MAP_UNIT, NOT_GC);
			if (map_sector_new_psn < 0)
				return map_sector_new_psn; ///error_code
			retval = single_mapping_update_in_map_sector(dftl, ghost_lsn_with_min_age, dftl->sector_map[ghost_lsn_with_min_age].psn, map_sector_new_psn, NOT_GC);
			if (retval)
				return retval;
		}
		//Evict MIN_GHOST from GHOST
		dftl->sector_map[ghost_lsn_with_min_age].age = 0;
		dftl->sector_map[ghost_lsn_with_min_age].psn = -1;
		dftl->sector_map[ghost_lsn_with_min_age].updated = 0;
		dftl->sector_map[ghost_lsn_with_min_age].cache_status = CMT_INVALID;
		dftl->cmt_ghost_arr[ghost_arr_idx] = -1;
		dftl->cmt_ghost_num_entries--;
		dftl->stat.cache_evict++;
		printf_debug("evicted entry with ghost_lsn_with_min_age=%d", ghost_lsn_with_min_age);
	}
	else {//find first free pos in GHOST
		ghost_arr_idx = search_table(dftl->cmt_ghost_arr, dftl->params.cmt_ghost_max_entries, -1);
		printf_debug("found free pos at ghost_arr_idx=%d", ghost_arr_idx);
	}

	return ghost_arr_idx;
}

static int provide_free_pos_in_cmt_real(struct dftl_s *dftl)
{
	int ghost_arr_idx;
	int real_arr_idx;
	int real_lsn_with_min_age;

	if ((dftl->params.cmt_real_max_entries - dftl->cmt_real_num_entries) == 0) { //REAL is full
		ghost_arr_idx = provide_free_pos_in_cmt_ghost(dftl);
		if (ghost_arr_idx < 0)
			return ghost_arr_idx; //error_code

		//evict MIN_REAL to GHOST
		real_lsn_with_min_age = find_lsn_with_min_age(dftl, dftl->cmt_real_arr, dftl->params.cmt_real_max_entries, &real_arr_idx);
		dftl->sector_map[real_lsn_with_min_age].cache_status = CMT_GHOST;
		dftl->cmt_ghost_arr[ghost_arr_idx] = real_lsn_with_min_age;
		dftl->cmt_real_arr[real_arr_idx] = -1;
		dftl->cmt_real_num_entries--;
		dftl->cmt_ghost_num_entries++;

		printf_debug("real with lsn=%d moved to ghost", real_lsn_with_min_age);
	}
	else {//find first free pos in REAL
		real_arr_idx = search_table(dftl->cmt_real_arr, dftl->params.cmt_real_max_entries, -1);
		printf_debug("found free pos at real_arr_idx = %d", real_arr_idx);
	}

	return real_arr_idx;
}

static int cache_maintenance(struct dftl_s *dftl, int lsn)
{
	int real_arr_idx = -1;
	map_entry * mapping = &dftl->sector_map[lsn];

	if (mapping->cache_status == CMT_REAL || mapping->cache_status == CMT_GHOST)
		return cache_hit_update(dftl, lsn);
	else { //mapping is not in cache
		real_arr_idx = provide_free_pos_in_cmt_real(dftl);
		if (real_arr_idx < 0) {
			printf_error("Failed to find free position in REAL CACHE for lsn=%d", lsn);
			return real_arr_idx; //error_code
		}
		//load and store new mapping in cache
		dftl->stat.flash_hit++;
		dftl->sector_map[lsn].psn = load_psn_from_flash(dftl, lsn, NOT_GC);
		if (dftl->sector_map[lsn].psn < 0 && dftl->sector_map[lsn].psn != -1) //error
			return dftl->sector_map[lsn].psn; //error_code
		dftl->sector_map[lsn].cache_status = CMT_REAL;
		dftl->sector_map[lsn].age = ++dftl->cmt_entry_max_age;
		dftl->sector_map[lsn].updated = 0;
		dftl->cmt_real_arr[real_arr_idx] = lsn;
		dftl->cmt_real_num_entries++;
		return 0;
	}
}

/*********************************INIT*********************************************/

static void calculate_dftl_parameters(struct dftl_s *dftl)
{
	int num_utilized_sectors;

	dftl->params.gmt_entries_per_page = dftl->params.dev_params->sector_size / sizeof (gmt_entry);
	num_utilized_sectors = (dftl->params.dev_params->num_blocks - dftl->params.num_extra_blocks) * dftl->params.dev_params->sectors_per_block;
	/*
	 * Here is the better approach of calculating the number of needed map sectors
	 * we need to map only data sectors. To find the number of map_sectors we need to solve following equations:
	 * 1) total_num_sectors = num_data_sectors + num_map_sectors;
	 * 2) num_map_sectors = num_data_sectors / map_entries_per_sector;
	 * => num_data_sectors = (total_num_sectors * map_entries_per_sector) / (map_entries_per_sector + 1)
	*/
	//dftl->params.num_data_sectors = (num_utilized_sectors * dftl->params.gmt_entries_per_page) / (dftl->params.gmt_entries_per_page + 1);
	//dftl->params.num_valid_map_sectors = num_utilized_sectors - dftl->params.num_data_sectors;

	//Original DFTL approach of calculation the number of needed map sectors.
	dftl->params.num_valid_map_sectors = num_utilized_sectors / dftl->params.gmt_entries_per_page;
	if (num_utilized_sectors % dftl->params.gmt_entries_per_page)
		dftl->params.num_valid_map_sectors++;

	dftl->params.gtd_size = dftl->params.num_valid_map_sectors * sizeof (gtd_entry);
	dftl->params.min_num_map_blocks = dftl->params.num_valid_map_sectors / dftl->params.dev_params->sectors_per_block;
	if (dftl->params.num_valid_map_sectors % dftl->params.dev_params->sectors_per_block)
		dftl->params.min_num_map_blocks++;
	//Bcz block could store either data or map sectors (!not mixed!)
	//We need to correct now the number of data sectors. data_blocks = all_blocks - min_map_blocks - extra_blocks
	dftl->params.num_data_sectors = (dftl->params.dev_params->num_blocks - dftl->params.min_num_map_blocks - dftl->params.num_extra_blocks) * dftl->params.dev_params->sectors_per_block;
}

static void increment_io_statistic(struct dftl_s *dftl, enum io_type_e io_type, unit_type_e unit_type, int is_gc)
{
	switch (io_type) {
	case READ_SECTOR:
		dftl->stat.read[unit_type][is_gc]++;
		dftl->req_stat.sector_read_num++;
		break;
	case WRITE_SECTOR:
		dftl->stat.write[unit_type][is_gc]++;
		dftl->req_stat.sector_write_num++;
		break;
	case READ_SECTOR_OOB:
		dftl->stat.oob_read[unit_type][is_gc]++;
		dftl->req_stat.sector_oob_read_num++;
		break;
	case WRITE_SECTOR_OOB:
		dftl->stat.oob_write[unit_type][is_gc]++;
		dftl->req_stat.sector_oob_write_num++;
		break;
	case READ_SECTOR_OOB_WITHOUT_DELAY:
		dftl->stat.oob_read_without_delay[unit_type][is_gc]++;
		break;
	case WRITE_SECTOR_OOB_WITHOUT_DELAY:
		dftl->stat.oob_write_without_delay[unit_type][is_gc]++;
		break;
	case ERASE:
		dftl->stat.erase[unit_type]++;
		dftl->req_stat.block_erase_num++;
		break;
	default:
		break;
	}
}

static void reset_request_statistics(struct dftl_s *dftl)
{
	dftl->req_stat.sector_read_num = 0;
	dftl->req_stat.sector_write_num = 0;
	dftl->req_stat.sector_oob_read_num = 0;
	dftl->req_stat.sector_oob_write_num = 0;
	dftl->req_stat.block_erase_num = 0;
	dftl->req_stat.psn = 0;
	dftl->req_stat.ftl_duration = 0;
}

void reset_dftl_statistics(struct dftl_s *dftl)
{
	dftl->stat.cache_hit = 0;
	dftl->stat.cache_evict = 0;
	dftl->stat.flash_hit = 0;

	dftl->stat.delay_flash_update = 0;
	dftl->stat.save_map_updates_via_batch_update = 0;
	dftl->stat.blind_read = 0;

	//[unit_type][is_gc]
	dftl->stat.read[DATA_UNIT][NOT_GC] = dftl->stat.write[DATA_UNIT][NOT_GC] = dftl->stat.oob_read[DATA_UNIT][NOT_GC] = dftl->stat.oob_write[DATA_UNIT][NOT_GC] =
			dftl->stat.oob_read_without_delay[DATA_UNIT][NOT_GC] = dftl->stat.oob_write_without_delay[DATA_UNIT][NOT_GC] = 0;

	dftl->stat.read[DATA_UNIT][GC] = dftl->stat.write[DATA_UNIT][GC] = dftl->stat.oob_read[DATA_UNIT][GC] = dftl->stat.oob_write[DATA_UNIT][GC] =
			dftl->stat.oob_read_without_delay[DATA_UNIT][GC] = dftl->stat.oob_write_without_delay[DATA_UNIT][GC] = 0;

	dftl->stat.read[MAP_UNIT][NOT_GC] = dftl->stat.write[MAP_UNIT][NOT_GC] = dftl->stat.oob_read[MAP_UNIT][NOT_GC] = dftl->stat.oob_write[MAP_UNIT][NOT_GC] =
			dftl->stat.oob_read_without_delay[MAP_UNIT][NOT_GC] = dftl->stat.oob_write_without_delay[MAP_UNIT][NOT_GC] = 0;

	dftl->stat.read[MAP_UNIT][GC] = dftl->stat.write[MAP_UNIT][GC] = dftl->stat.oob_read[MAP_UNIT][GC] = dftl->stat.oob_write[MAP_UNIT][GC] =
			dftl->stat.oob_read_without_delay[MAP_UNIT][GC] = dftl->stat.oob_write_without_delay[MAP_UNIT][GC] = 0;

	//[unit_type]
	dftl->stat.erase[DATA_UNIT] = dftl->stat.erase[MAP_UNIT] = 0;

	reset_request_statistics(dftl);
}

static void initialize_dftl_structures(struct dftl_s *dftl)
{
	int i;

	//sector_map
	for (i = 0; i < dftl->params.num_data_sectors; i++){
		dftl->sector_map[i].cache_status = CMT_INVALID;
		dftl->sector_map[i].age = 0;
		dftl->sector_map[i].psn = -1;
		dftl->sector_map[i].updated = 0;
	}
	//block_stat
	for (i = 0; i < dftl->params.dev_params->num_blocks; i++){
		dftl->block_stat[i].block_status = FREE_BLOCK;
		//dftl.block_stat[i].block_type = "undefined"
		dftl->block_stat[i].erase_count = 0;
		dftl->block_stat[i].free_sector_count = dftl->params.dev_params->sectors_per_block;
		dftl->block_stat[i].invalid_sector_count = 0;
	}
	//gtd
	for (i = 0; i < dftl->params.num_valid_map_sectors; i++)
		dftl->gtd[i] = -1; //the lsn of map sector is not yet assigned = free
	//cmt_real_arr
	for (i = 0; i < dftl->params.cmt_real_max_entries; i++)
		dftl->cmt_real_arr[i] = -1;
	//cmt_ghost_arr
	for (i = 0; i < dftl->params.cmt_ghost_max_entries; i++)
		dftl->cmt_ghost_arr[i] = -1;

	dftl->cmt_real_num_entries = 0;
	dftl->cmt_ghost_num_entries = 0;
	dftl->free_block_num = dftl->params.dev_params->num_blocks;
	dftl->cmt_entry_max_age = 0;

	dftl->last_written_sector[DATA_UNIT] = -1;
	dftl->last_written_sector[MAP_UNIT] = -1;
}

static int write_init_map_sectors(struct dftl_s *dftl)
{
	int retval = 0;
	int i, j;
	void *map_sector_buffer;
	int data_sector_lsn = 0;
	int map_sector_new_psn;

	map_sector_buffer = get_and_fix_sector_buffer(dftl);
	if (!map_sector_buffer)
		return -ENOMEM;

	for (i = 0; i < dftl->params.num_valid_map_sectors; i++) {
		clear_sector_buffer(dftl);
		for (j = 0; j < dftl->params.gmt_entries_per_page; j++)
			single_mapping_update_in_map_sector_buffer(dftl, map_sector_buffer, data_sector_lsn++, -1);
		map_sector_new_psn = get_next_free_sector(dftl, MAP_UNIT, NOT_GC);
		if (map_sector_new_psn < 0)
			return -EIO;
		retval = write_map_sector(dftl, map_sector_buffer, i, map_sector_new_psn, NOT_GC);
		if (retval)
			return retval;
	}

	unfix_sector_buffer(dftl);
	return retval;
}

struct dftl_s * init_dftl (int device_type, char *device_path, char *debug_file_path, int cache_level_1, int cache_level_2, int num_extra_blocks, int sector_size, int sectors_per_block, int num_blocks)
{
	struct dftl_s *dftl = NULL;

	dftl = vzalloc(sizeof(struct dftl_s));
	if (!dftl) {
		print_error("Not enough memory for allocating dftl structures!");
		return NULL;
	}

	dftl->device_fd = open(device_path, O_RDWR|O_SYNC|O_DIRECT, 0600);
	if (dftl->device_fd == -1) {
		printf_error("ERROR during openning a device file! %s", strerror(errno));
		return NULL;
	}

	dftl->debug_file = fopen(debug_file_path, "w");
	if (dftl->debug_file == NULL) {
		printf_error("ERROR during openning a debug file! %s", strerror(errno));
		return NULL;
	}

	dftl->params.dev_params = vzalloc(sizeof(struct device_params));
	dftl->params.device_type = device_type;
	dftl->params.dev_params->sector_size = sector_size;
	dftl->params.dev_params->sectors_per_block = sectors_per_block;
	dftl->params.dev_params->num_blocks = num_blocks;
	dftl->params.dev_params->num_sectors = dftl->params.dev_params->num_blocks * dftl->params.dev_params->sectors_per_block;
	dftl->params.cmt_real_max_entries = cache_level_1;
	dftl->params.cmt_ghost_max_entries = cache_level_2;
	dftl->params.num_extra_blocks = num_extra_blocks;



	calculate_dftl_parameters(dftl);

	//Memory allocation for dftl structures
	dftl->gtd = vzalloc(dftl->params.gtd_size);
	dftl->sector_map = vzalloc(dftl->params.num_data_sectors * sizeof (map_entry));
	dftl->block_stat = vzalloc(dftl->params.dev_params->num_blocks * sizeof (block_info));
	dftl->cmt_real_arr = vzalloc(dftl->params.cmt_real_max_entries * sizeof(int));
	dftl->cmt_ghost_arr = vzalloc(dftl->params.cmt_ghost_max_entries * sizeof(int));
	posix_memalign(&dftl->sector_buffer.buffer, dftl->params.dev_params->sector_size, dftl->params.dev_params->sector_size);
	dftl->sector_buffer.fixed = 0;

	if (!dftl->gtd || !dftl->sector_map || !dftl->block_stat || !dftl->cmt_real_arr || !dftl->cmt_ghost_arr || !dftl->sector_buffer.buffer) {
		print_error("init_dftl: Not enough memory for allocating dftl structures!");
		free_dftl(dftl);
		return NULL;
	}

	initialize_dftl_structures(dftl);
	reset_dftl_statistics(dftl);
	if (write_init_map_sectors(dftl)) {
		free_dftl(dftl);
		return NULL;
	}

	//clean stat after warm-up
	reset_dftl_statistics(dftl);

	printf_debug("num_valid_map_sectors = %d, min_num_map_blocks = %d, num_data_sectors = %d",
			dftl->params.num_valid_map_sectors, dftl->params.min_num_map_blocks, dftl->params.num_data_sectors);

	return dftl;
}

void free_dftl(struct dftl_s *dftl)
{
	if (dftl) {
		if (dftl->gtd) vfree(dftl->gtd);
		if (dftl->sector_map) vfree(dftl->sector_map);
		if (dftl->block_stat) vfree(dftl->block_stat);
		if (dftl->cmt_real_arr) vfree(dftl->cmt_real_arr);
		if (dftl->cmt_ghost_arr) vfree(dftl->cmt_ghost_arr);
		if (dftl->sector_buffer.buffer) vfree(dftl->sector_buffer.buffer);
		vfree(dftl);
	}
}

/**********************************************************************************/

int lsn_to_psn(struct dftl_s *dftl, int data_lsn, int rw)
{
	//t_time request_arrival;
	map_entry * mapping;
	oob sector_oob;
	oob new_sector_oob;
	int new_sector_psn;
	int retval = 0;

	printf_debug("(begin): data_lsn = %d, rw = %d", data_lsn, rw);

	//set_current_time(&request_arrival);
	reset_request_statistics(dftl);

	retval = cache_maintenance(dftl, data_lsn);
	if (retval) {
		printf_error("Cache maintenance failed on request: lsn=%d, rw=%d", data_lsn, rw);
		return retval;
	}
	mapping = &dftl->sector_map[data_lsn];

	if (mapping->psn != -1) {
		retval = read_sector_oob(dftl, mapping->psn, sector_oob, DATA_UNIT, NOT_GC, 0);
		if (retval)
			return retval;
		if (sector_oob[OOB_STATUS_INDEX] != VALID_SECTOR){
			printf_error("Internal error! Attempt to access invalid sector: psn=%d, status=%d, type=%d!",
					mapping->psn, sector_oob[OOB_STATUS_INDEX], sector_oob[OOB_TYPE_INDEX]);
			return -EIO;
		}
		if (sector_oob[OOB_LSN_INDEX] != data_lsn) {
			printf_error("Internal error! Consistency check failed: LSN is not the same: lsn=%d, psn=%d, status=%d, type=%d, lsn_in_oob=%d",
					data_lsn, mapping->psn, sector_oob[OOB_STATUS_INDEX], sector_oob[OOB_TYPE_INDEX], sector_oob[OOB_LSN_INDEX]);
			return -EIO;
		}
	}

	if (rw == READ || rw == READA) {
		increment_io_statistic(dftl, READ_SECTOR, DATA_UNIT, NOT_GC);
		if (mapping->psn == -1) {
			printf_debug("DFTL ALERT: Reading empty (unassigned) sector lsn=%d", data_lsn);
			dftl->stat.blind_read++;
			dftl->req_stat.psn = data_lsn;
			//dftl->req_stat.ftl_duration = get_duration_in_us_till_now(&request_arrival);
			return data_lsn;
		}
	}
	else if (rw == WRITE) {
		new_sector_psn = get_next_free_sector(dftl, DATA_UNIT, NOT_GC);
		if (new_sector_psn < 0) {
			printf_error ("Failed to find free sector for writing lsn=%d", data_lsn);
			return new_sector_psn; //error_code
		}

		new_sector_oob[OOB_LSN_INDEX] = data_lsn;
		new_sector_oob[OOB_STATUS_INDEX] = VALID_SECTOR;
		new_sector_oob[OOB_TYPE_INDEX] = DATA_UNIT;
		write_sector_oob(dftl, new_sector_psn, new_sector_oob, DATA_UNIT, NOT_GC, 0);
		end_sector_write(dftl, new_sector_psn, DATA_UNIT, NOT_GC);

		//invalidate old sector.
		//Here is safe to invalidate mapping->psn after get_next_free_sector, because GC if necessary will update cache
		if (mapping->psn != -1 && sector_oob[OOB_STATUS_INDEX] == VALID_SECTOR) {
			retval = set_sector_status(dftl, mapping->psn, INVALID_SECTOR, DATA_UNIT, NOT_GC, 1);
			if (retval)
				return retval;
		}

		mapping->psn = new_sector_psn;
		mapping->updated = 1;
	}
	else {
		print_error("Invalid command type!");
		return -EINVAL;
	}

	//Success
	dftl->req_stat.psn = mapping->psn;
	//dftl->req_stat.ftl_duration = get_duration_in_us_till_now(&request_arrival);
	printf_debug("(end): data_lsn = %d, rw = %d, psn = %d, duration_us = %d", data_lsn, rw, mapping->psn, dftl->req_stat.ftl_duration);
	return mapping->psn;
}

int access_sector_oob(struct dftl_s *dftl, int psn, void * buffer, int offset, int length, int rw, int simulate_delay)
{
	int retval = 0;
	struct access_oob params;

	params.psn = psn;
	params.offset = offset;
	params.length = length;
	params.rw = rw;
	params.simulate_delay = simulate_delay;
	params.buffer = buffer;

	retval = ioctl(dftl->device_fd, ACCESS_OOB, &params);
	if (retval == -1)
		return -1;

	return 0;
}

void print_statistics(struct dftl_s *dftl)
{
	print_msg("**************************STATISTICS***************************");
	printf("cache_hit = %d\n", dftl->stat.cache_hit);
	printf("cache_evict = %d\n", dftl->stat.cache_evict);
	printf("flash_hit = %d\n", dftl->stat.flash_hit);

	printf("delay_flash_update = %d\n", dftl->stat.delay_flash_update);
	printf("save_map_updates_via_batch_update = %d\n", dftl->stat.save_map_updates_via_batch_update);

	printf("read[DATA_UNIT][NOT_GC] = %d\n", dftl->stat.read[DATA_UNIT][NOT_GC]);
	printf("read[DATA_UNIT][GC] = %d\n", dftl->stat.read[DATA_UNIT][GC]);	
	printf("read[DATA_UNIT] = %d\n", dftl->stat.read[DATA_UNIT][NOT_GC] + dftl->stat.read[DATA_UNIT][GC]);	
	printf("read[MAP_UNIT][NOT_GC] = %d\n", dftl->stat.read[MAP_UNIT][NOT_GC]);
	printf("read[MAP_UNIT][GC] = %d\n", dftl->stat.read[MAP_UNIT][GC]);
	printf("read[MAP_UNIT] = %d\n", dftl->stat.read[MAP_UNIT][NOT_GC] + dftl->stat.read[MAP_UNIT][GC]);
	printf("read[GC] = %d\n", dftl->stat.read[DATA_UNIT][GC] + dftl->stat.read[MAP_UNIT][GC]);	
	printf("read[NOT_GC] = %d\n", dftl->stat.read[DATA_UNIT][NOT_GC] + dftl->stat.read[MAP_UNIT][NOT_GC]);
	printf("read = %d\n", dftl->stat.read[DATA_UNIT][NOT_GC] + dftl->stat.read[MAP_UNIT][NOT_GC] + dftl->stat.read[DATA_UNIT][GC] + dftl->stat.read[MAP_UNIT][GC]);

	printf("write[DATA_UNIT][NOT_GC] = %d\n", dftl->stat.write[DATA_UNIT][NOT_GC]);
	printf("write[DATA_UNIT][GC] = %d\n", dftl->stat.write[DATA_UNIT][GC]);	
	printf("write[DATA_UNIT] = %d\n", dftl->stat.write[DATA_UNIT][NOT_GC] + dftl->stat.write[DATA_UNIT][GC]);	
	printf("write[MAP_UNIT][NOT_GC] = %d\n", dftl->stat.write[MAP_UNIT][NOT_GC]);
	printf("write[MAP_UNIT][GC] = %d\n", dftl->stat.write[MAP_UNIT][GC]);
	printf("write[MAP_UNIT] = %d\n", dftl->stat.write[MAP_UNIT][NOT_GC] + dftl->stat.write[MAP_UNIT][GC]);
	printf("write[GC] = %d\n", dftl->stat.write[DATA_UNIT][GC] + dftl->stat.write[MAP_UNIT][GC]);	
	printf("write[NOT_GC] = %d\n", dftl->stat.write[DATA_UNIT][NOT_GC] + dftl->stat.write[MAP_UNIT][NOT_GC]);
	printf("write = %d\n", dftl->stat.write[DATA_UNIT][NOT_GC] + dftl->stat.write[MAP_UNIT][NOT_GC] + dftl->stat.write[DATA_UNIT][GC] + dftl->stat.write[MAP_UNIT][GC]);

	printf("oob_read[DATA_UNIT][NOT_GC] = %d\n", dftl->stat.oob_read[DATA_UNIT][NOT_GC]);
	printf("oob_read[DATA_UNIT][GC] = %d\n", dftl->stat.oob_read[DATA_UNIT][GC]);	
	printf("oob_read[DATA_UNIT] = %d\n", dftl->stat.oob_read[DATA_UNIT][NOT_GC] + dftl->stat.oob_read[DATA_UNIT][GC]);	
	printf("oob_read[MAP_UNIT][NOT_GC] = %d\n", dftl->stat.oob_read[MAP_UNIT][NOT_GC]);
	printf("oob_read[MAP_UNIT][GC] = %d\n", dftl->stat.oob_read[MAP_UNIT][GC]);
	printf("oob_read[MAP_UNIT] = %d\n", dftl->stat.oob_read[MAP_UNIT][NOT_GC] + dftl->stat.oob_read[MAP_UNIT][GC]);
	printf("oob_read[GC] = %d\n", dftl->stat.oob_read[DATA_UNIT][GC] + dftl->stat.oob_read[MAP_UNIT][GC]);	
	printf("oob_read[NOT_GC] = %d\n", dftl->stat.oob_read[DATA_UNIT][NOT_GC] + dftl->stat.oob_read[MAP_UNIT][NOT_GC]);
	printf("oob_read = %d\n", dftl->stat.oob_read[DATA_UNIT][NOT_GC] + dftl->stat.oob_read[MAP_UNIT][NOT_GC] + dftl->stat.oob_read[DATA_UNIT][GC] + dftl->stat.oob_read[MAP_UNIT][GC]);
	
	printf("oob_write[DATA_UNIT][NOT_GC] = %d\n", dftl->stat.oob_write[DATA_UNIT][NOT_GC]);
	printf("oob_write[DATA_UNIT][GC] = %d\n", dftl->stat.oob_write[DATA_UNIT][GC]);	
	printf("oob_write[DATA_UNIT] = %d\n", dftl->stat.oob_write[DATA_UNIT][NOT_GC] + dftl->stat.oob_write[DATA_UNIT][GC]);	
	printf("oob_write[MAP_UNIT][NOT_GC] = %d\n", dftl->stat.oob_write[MAP_UNIT][NOT_GC]);
	printf("oob_write[MAP_UNIT][GC] = %d\n", dftl->stat.oob_write[MAP_UNIT][GC]);
	printf("oob_write[MAP_UNIT] = %d\n", dftl->stat.oob_write[MAP_UNIT][NOT_GC] + dftl->stat.oob_write[MAP_UNIT][GC]);
	printf("oob_write[GC] = %d\n", dftl->stat.oob_write[DATA_UNIT][GC] + dftl->stat.oob_write[MAP_UNIT][GC]);	
	printf("oob_write[NOT_GC] = %d\n", dftl->stat.oob_write[DATA_UNIT][NOT_GC] + dftl->stat.oob_write[MAP_UNIT][NOT_GC]);
	printf("oob_write = %d\n", dftl->stat.oob_write[DATA_UNIT][NOT_GC] + dftl->stat.oob_write[MAP_UNIT][NOT_GC] + dftl->stat.oob_write[DATA_UNIT][GC] + dftl->stat.oob_write[MAP_UNIT][GC]);
	
	printf("oob_read_without_delay[DATA_UNIT][NOT_GC] = %d\n", dftl->stat.oob_read_without_delay[DATA_UNIT][NOT_GC]);
	printf("oob_read_without_delay[DATA_UNIT][GC] = %d\n", dftl->stat.oob_read_without_delay[DATA_UNIT][GC]);	
	printf("oob_read_without_delay[DATA_UNIT] = %d\n", dftl->stat.oob_read_without_delay[DATA_UNIT][NOT_GC] + dftl->stat.oob_read_without_delay[DATA_UNIT][GC]);	
	printf("oob_read_without_delay[MAP_UNIT][NOT_GC] = %d\n", dftl->stat.oob_read_without_delay[MAP_UNIT][NOT_GC]);
	printf("oob_read_without_delay[MAP_UNIT][GC] = %d\n", dftl->stat.oob_read_without_delay[MAP_UNIT][GC]);
	printf("oob_read_without_delay[MAP_UNIT] = %d\n", dftl->stat.oob_read_without_delay[MAP_UNIT][NOT_GC] + dftl->stat.oob_read_without_delay[MAP_UNIT][GC]);
	printf("oob_read_without_delay[GC] = %d\n", dftl->stat.oob_read_without_delay[DATA_UNIT][GC] + dftl->stat.oob_read_without_delay[MAP_UNIT][GC]);	
	printf("oob_read_without_delay[NOT_GC] = %d\n", dftl->stat.oob_read_without_delay[DATA_UNIT][NOT_GC] + dftl->stat.oob_read_without_delay[MAP_UNIT][NOT_GC]);
	printf("oob_read_without_delay = %d\n", dftl->stat.oob_read_without_delay[DATA_UNIT][NOT_GC] + dftl->stat.oob_read_without_delay[MAP_UNIT][NOT_GC] + dftl->stat.oob_read_without_delay[DATA_UNIT][GC] + dftl->stat.oob_read_without_delay[MAP_UNIT][GC]);

	printf("oob_write_without_delay[DATA_UNIT][NOT_GC] = %d\n", dftl->stat.oob_write_without_delay[DATA_UNIT][NOT_GC]);
	printf("oob_write_without_delay[DATA_UNIT][GC] = %d\n", dftl->stat.oob_write_without_delay[DATA_UNIT][GC]);	
	printf("oob_write_without_delay[DATA_UNIT] = %d\n", dftl->stat.oob_write_without_delay[DATA_UNIT][NOT_GC] + dftl->stat.oob_write_without_delay[DATA_UNIT][GC]);	
	printf("oob_write_without_delay[MAP_UNIT][NOT_GC] = %d\n", dftl->stat.oob_write_without_delay[MAP_UNIT][NOT_GC]);
	printf("oob_write_without_delay[MAP_UNIT][GC] = %d\n", dftl->stat.oob_write_without_delay[MAP_UNIT][GC]);
	printf("oob_write_without_delay[MAP_UNIT] = %d\n", dftl->stat.oob_write_without_delay[MAP_UNIT][NOT_GC] + dftl->stat.oob_write_without_delay[MAP_UNIT][GC]);
	printf("oob_write_without_delay[GC] = %d\n", dftl->stat.oob_write_without_delay[DATA_UNIT][GC] + dftl->stat.oob_write_without_delay[MAP_UNIT][GC]);	
	printf("oob_write_without_delay[NOT_GC] = %d\n", dftl->stat.oob_write_without_delay[DATA_UNIT][NOT_GC] + dftl->stat.oob_write_without_delay[MAP_UNIT][NOT_GC]);
	printf("oob_write_without_delay = %d\n", dftl->stat.oob_write_without_delay[DATA_UNIT][NOT_GC] + dftl->stat.oob_write_without_delay[MAP_UNIT][NOT_GC] + dftl->stat.oob_write_without_delay[DATA_UNIT][GC] + dftl->stat.oob_write_without_delay[MAP_UNIT][GC]);	

	printf("erase[DATA_UNIT] = %d\n", dftl->stat.erase[DATA_UNIT]);
	printf("erase[MAP_UNIT] = %d\n", dftl->stat.erase[MAP_UNIT]);
	printf("erase = %d\n", dftl->stat.erase[DATA_UNIT] + dftl->stat.erase[MAP_UNIT]);
	print_msg("**************************STATISTICS***************************");
}

int play_trace_file(FILE *trace_file, struct dftl_s *dftl)
{
	int rw;
	int size;
	int lsn;
	int psn;
	int i = 0;
	unsigned long seq_number;
	char line [128];
	char *line_cursor;
	static int request_number = 0;

	while (fgets (line, sizeof(line), trace_file) != NULL) {
		fputs (line, stdout);

		line_cursor = line;
        sscanf(line_cursor, "%lu", &seq_number);
        if (seq_number != i++) {
        	print_error("Trace inconsistency!");
    		free_dftl(dftl);
    		fclose (trace_file);
        	return -1;
        }

        while ((*line_cursor != ' ') && (*line_cursor != '\0')) line_cursor++;
        while (*line_cursor == ' ') line_cursor++;
        sscanf(line_cursor++, "%d", &lsn);

        while ((*line_cursor != ' ') && (*line_cursor != '\0')) line_cursor++;
        while (*line_cursor == ' ') line_cursor++;
        sscanf(line_cursor++, "%d", &rw);

        while ((*line_cursor != ' ') && (*line_cursor != '\0')) line_cursor++;
        while (*line_cursor == ' ') line_cursor++;
        sscanf(line_cursor++, "%d", &size);

        fprintf(dftl->debug_file, "%d %d %d %d\n", request_number++, lsn, rw, size);

        /*if (seq_number == 262571) {
    		printf ("catch");
    	}*/

		//lsn = kernel_sector / kernel_sectors_per_flashsim_sector;
        //Only for disksim traces, where 0 = WRITE and 1 = READ
        if (rw == 0)
        	rw = WRITE;
        else
        	rw = READ;

        do {
			psn = lsn_to_psn(dftl, lsn, rw);
			if (psn == -1) {
				if (rw == READ || rw == READA)
					psn = lsn;
				else {
					print_error("FTL Internal error.");
					fclose (trace_file);
					return -EIO;
				}
			}
			size--;
			lsn++;
        } while (size > 0);
	}
	return 0;
}

main(int argc, char * argv[])
{
	struct dftl_s *dftl;
	//char trace1_filename[] = "/home/timbuktu/simulator/flashsim/dftl_external/traces/warm_up_trace_1024_entries_per_map_page_0_lsn_offset";
	char trace1_filename[] = "/home/timbuktu/simulator/flashsim/dftl_external/1G_10_rand_write";
	char trace2_filename[] = "/home/timbuktu/simulator/flashsim/dftl_external/traces/simulation_trace_1024_entries_per_map_page_0_lsn_offset";
	FILE *trace1_file;
	FILE *trace2_file;

	dftl = init_dftl(0, "/dev/flashsim_block", "/home/timbuktu/Desktop/debug_file", 6552, 1640, 256, 4096, 64, 4096);
	if (!dftl) {
		print_error("Failed to initialize dftl");
		return -1;
	}

	/*********************************WARM_UP**********************************/
	trace1_file = fopen (trace1_filename, "r");
	if (trace1_file == NULL) {
			print_error("Failed to open trace file #1!");
			free_dftl(dftl);
			return -ENOENT;
		}
	if (play_trace_file(trace1_file, dftl)) {
		print_error("Failed on replaying the warm-up trace!");
		free_dftl(dftl);
		return;
	}
	fclose (trace1_file);

	print_statistics(dftl);
	reset_dftl_statistics(dftl);
	getchar();

	/*******************************SIMULATION********************************/
	trace2_file = fopen(trace2_filename, "r");
	if (trace2_file == NULL) {
			print_error("Failed to open trace file #2!");
			free_dftl(dftl);
			return -ENOENT;
		}
	if (play_trace_file(trace2_file, dftl)) {
		print_error("Failed on replaying the simulation trace!");
		free_dftl(dftl);
		return;
	}
	fclose (trace2_file);
    fclose(dftl->debug_file);

	print_statistics(dftl);
	free_dftl(dftl);
	return 0;
}
