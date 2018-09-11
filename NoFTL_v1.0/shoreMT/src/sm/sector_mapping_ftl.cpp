/*
 * TODO read device params from /proc or from stat
 * TODO singleton design: *instance -> manually destroy or instance -> auto destruction.
 * Auto is ok when we do not need to save/load FTL state from/to file
 * And very bad if we need
 */
#include "w_defines.h"

#define SM_SOURCE
#define SECTOR_MAPPING_FTL

#ifdef __GNUG__
#   pragma implementation "sector_mapping_ftl.h"
#endif

#include <sm_int_0.h>
#include <sector_mapping_ftl.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <sstream>
#include <string.h>

pthread_mutex_t sector_mapping_ftl::_ftl_mutex = PTHREAD_MUTEX_INITIALIZER;

sector_mapping_ftl & sector_mapping_ftl::getInstance(int device_fd)
{
	static sector_mapping_ftl ftl_instance(device_fd, FTL_NUM_BLOCKS, FTL_SECTORS_PER_BLOCK, FTL_SECTOR_SIZE);
	if (device_fd != -1)
		ftl_instance.device_fd = device_fd;

	return ftl_instance;
}

sector_mapping_ftl::sector_mapping_ftl(int device_fd, int num_blocks, int sectors_per_block, int sector_size)
{
	this->device_fd = device_fd;
	this->num_blocks = num_blocks;
	this->sectors_per_block = sectors_per_block;
	this->sector_size = sector_size;
	int extra_blocks = (num_blocks / 100) * FTL_EXTRA_BLOCKS_FRACTION;
	this->num_util_sectors = (num_blocks - extra_blocks) * sectors_per_block;
	this->num_sectors = num_blocks * sectors_per_block;

	w_assert0(sector_size == SM_PAGESIZE && extra_blocks >= FTL_GC_THRESHOLD + 1);

	sector_map.resize(num_util_sectors, -1);

	read_ftl_state_mode_from_file();
	reset_statistics();

	if (!state_mode || ((state_mode & ftl_state::LOAD_FROM_FILE) && load_ftl_state_from_file().is_error())) {
		num_free_blocks = num_blocks;
		for (int i = 0; i < num_blocks; i++) {
			block_info init_block_state;
			init_block_state.free = true;
			init_block_state.erase_count = 0;
			init_block_state.invalid_sector_count = 0;
			init_block_state.invalid_sector_bitmap.resize(sectors_per_block, false);

			block_stat.push_back(init_block_state);
		}

		last_written_sector = -1;
	}
	if (state_mode & ftl_state::RUN_TRACE_ONCE) {
		if (run_warm_up_trace(FTL_WARM_UP_TRACE_PATH).is_error())
			w_assert0(0);
		state_mode &= (~ftl_state::RUN_TRACE_ONCE);
		write_ftl_state_mode_to_file(state_mode);
		print_statistics_core();
		reset_statistics();
	}
}

rc_t sector_mapping_ftl::write_ftl_state_mode_to_file(int new_state_mode)
{
	ofstream ftl_state_mode_file(FTL_STATE_MODE_FILE_NAME);

	if (!ftl_state_mode_file)
		return RC(fcOS);

	//save new state mode
	printf("Writing new ftl_sate_mode %d to a file!\n", new_state_mode);
	ftl_state_mode_file << new_state_mode << endl;
	ftl_state_mode_file.close();

	return RCOK;
}

rc_t sector_mapping_ftl::read_ftl_state_mode_from_file()
{
	ifstream ftl_state_mode_file(FTL_STATE_MODE_FILE_NAME);

	if (!ftl_state_mode_file) {
		printf("Failed to read the FTL state mode from file! The default \"NONE\" value would be used!\n");
		state_mode = ftl_state::NONE;
		return RCOK;
	}

	ftl_state_mode_file >> state_mode;
	if (state_mode < 0 || state_mode > 7) {
		printf("Invalid value for FTL state mode has been read from the file! The default value would be used!\n");
		state_mode = ftl_state::NONE;
		return RCOK;
	}

	printf("FTL state mode has been read from the file. It is ");
	if (!state_mode)
		printf("NONE");
	if (state_mode & ftl_state::LOAD_FROM_FILE)
		printf("LOAD_FROM_FILE ");
	if (state_mode & ftl_state::RUN_TRACE_ONCE)
		printf("RUN_TRACE_ONCE ");
	if (state_mode & ftl_state::SAVE_TO_FILE)
		printf("SAVE_TO_FILE ");
	printf("\n");

	return RCOK;
}

void sector_mapping_ftl::print_statistics()
{
	CRITICAL_SECTION(ftl_save_cs, _ftl_mutex);

	sector_mapping_ftl &ftl = getInstance(-1);
	ftl.print_statistics_core();
}

void sector_mapping_ftl::print_statistics_core()
{
	printf("\n");
	printf("****************   FTL statistics   ****************\n");
	printf("\n");
	printf("read[NOT_GC]=%d\n", statistics.read[0]);
	printf("read[GC]=%d\n", statistics.read[1]);
	printf("read=%d\n", statistics.read[0] + statistics.read[1]);
	printf("\n");
	printf("write[NOT_GC]=%d\n", statistics.write[0]);
	printf("write[GC]=%d\n", statistics.write[1]);
	printf("write=%d\n", statistics.write[0] + statistics.write[1]);
	printf("\n");
	printf("oob_read[NOT_GC]=%d\n", statistics.oob_read[0]);
	printf("oob_read[GC]=%d\n", statistics.oob_read[1]);
	printf("oob_read=%d\n", statistics.oob_read[0] + statistics.oob_read[1]);
	printf("\n");
	printf("oob_write[NOT_GC]=%d\n", statistics.oob_write[0]);
	printf("oob_write[GC]=%d\n", statistics.oob_write[1]);
	printf("oob_write=%d\n", statistics.oob_write[0] + statistics.oob_write[1]);
	printf("\n");
	printf("erase=%d\n", statistics.erase);
	printf("\n");
	printf("oob_read_without_delay[NOT_GC]=%d\n", statistics.oob_read_without_delay[0]);
	printf("oob_read_without_delay[GC]=%d\n", statistics.oob_read_without_delay[1]);
	printf("oob_read_without_delay=%d\n", statistics.oob_read_without_delay[0] + statistics.oob_read_without_delay[1]);
	printf("\n");
	printf("oob_write_without_delay[NOT_GC]=%d\n", statistics.oob_write_without_delay[0]);
	printf("oob_write_without_delay[GC]=%d\n", statistics.oob_write_without_delay[1]);
	printf("oob_write_without_delay=%d\n", statistics.oob_write_without_delay[0] + statistics.oob_write_without_delay[1]);
	printf("\n");
	printf("**************   END FTL statistics   **************\n");
	printf("\n");
}

void sector_mapping_ftl::reset_statistics()
{
	statistics.read[0] = statistics.read[1] = 0;
	statistics.write[0] = statistics.write[1] = 0;
	statistics.oob_read[0] = statistics.oob_read[1] = 0;
	statistics.oob_write[0] = statistics.oob_write[1] = 0;
	statistics.oob_read_without_delay[0] = statistics.oob_read_without_delay[1] = 0;
	statistics.oob_write_without_delay[0] = statistics.oob_write_without_delay[1] = 0;
	statistics.erase = 0;
}

rc_t sector_mapping_ftl::close_ftl()
{
	CRITICAL_SECTION(ftl_save_cs, _ftl_mutex);

	sector_mapping_ftl &ftl = getInstance(-1);

	if (ftl.state_mode & ftl_state::SAVE_TO_FILE) {
		ofstream sector_map_file(SECTOR_MAP_FILE_NAME);
		ofstream block_stat_file(BLOCK_STAT_FILE_NAME);
		ofstream last_written_sector_file(LAST_WRITTEN_SECTOR_FILE_NAME);

		if (!sector_map_file || !block_stat_file || !last_written_sector_file)
			return RC(fcOS);

		//save sector map
		for (unsigned int i = 0; i < ftl.sector_map.size(); i++)
			sector_map_file << ftl.sector_map[i] << endl;

		//save block statistics
		for (int i = 0; i < ftl.num_blocks; i++) {
			block_info &block = ftl.block_stat[i];
			block_stat_file << block.free << ", " << block.erase_count << ", " << block.invalid_sector_count << ", ";
			for (int j = 0; j < ftl.sectors_per_block; j++) {
				block_stat_file << block.invalid_sector_bitmap[j];
				if (j != ftl.sectors_per_block - 1)
					block_stat_file << ", ";
			}
			block_stat_file << endl;
		}

		//save last written sector
		last_written_sector_file << ftl.last_written_sector << endl;
		printf("FTL state successfully saved to files!\n");
	}

	return RCOK;
}

//Should be called within critical section
rc_t sector_mapping_ftl::run_warm_up_trace(const char *trace_file_path)
{
	int lsn;
	int rw;
	int size;
	int psn;
	unsigned long trace_io_counter = 0;
	unsigned long req_seq_number;
	std::string line;

	ifstream trace_file(trace_file_path);
	if (!trace_file)
		W_DO_MSG(RC(fcOS), << "Failed to open the trace file!");

	printf("FTL start warm-up trace!\n");

	while (std::getline(trace_file, line)) {
		//fputs (line, stdout);

        sscanf(line.c_str(), "%lu %d %d %d", &req_seq_number, &lsn, &rw, &size);
        if (req_seq_number != trace_io_counter++)
        	W_DO_MSG(RC(fcINTERNAL), << "Trace file is not consistent!");

        do {
        	//in trace WRITE = 0, in shoreMT WRITE = 1
        	W_DO(lsn_to_psn(lsn, psn, (rw ? 0:1)));
        	//The real io is not needed, we care only about OOB and FTL state
			size--;
			lsn++;
        } while (size > 0);
	}

	printf("FTL start warm-up successfully finished!\n");

	return RCOK;
}

rc_t sector_mapping_ftl::load_ftl_state_from_file()
{
	ifstream sector_map_file(SECTOR_MAP_FILE_NAME);
	ifstream block_stat_file(BLOCK_STAT_FILE_NAME);
	ifstream last_written_sector_file(LAST_WRITTEN_SECTOR_FILE_NAME);

	if (!sector_map_file || !block_stat_file || !last_written_sector_file)
		return RC(fcOS);

	//load sector map
	for (unsigned int i = 0; i < sector_map.size(); i++)
		sector_map_file >> sector_map[i];

	//load block statistics
	num_free_blocks = 0;
	std::string line;
	while (std::getline(block_stat_file, line)) {
		block_info block;
		istringstream tokenizer(line);
		std::string token;

		std::getline(tokenizer, token, ',');
		block.free = atoi(token.c_str());
		if (block.free)
			num_free_blocks++;

		std::getline(tokenizer, token, ',');
		block.erase_count = atoi(token.c_str());

		std::getline(tokenizer, token, ',');
		block.invalid_sector_count = atoi(token.c_str());

		int tmp_invalid_sector_count = 0;
		while(std::getline(tokenizer, token, ',')) {
			bool is_sector_invalid = atoi(token.c_str());
			block.invalid_sector_bitmap.push_back(is_sector_invalid);
			if (is_sector_invalid)
				tmp_invalid_sector_count++;
		}

		if ((int)block.invalid_sector_bitmap.size() != sectors_per_block || tmp_invalid_sector_count != block.invalid_sector_count)
			return RC(fcASSERT);
		block_stat.push_back(block);
	}
	if ((int)block_stat.size() != num_blocks)
		return RC(fcASSERT);

	//load last written sector
	last_written_sector_file >> last_written_sector;
	printf("FTL state successfully loaded from files!\n");

	return RCOK;
}

rc_t sector_mapping_ftl::write_oob(int psn, bool valid, int lsn, bool with_delay, bool is_gc)
{
	struct sector_oob oob;
	oob.valid = valid;
	oob.lsn = lsn;

	if (with_delay)
		statistics.oob_write[is_gc]++;
	else
		statistics.oob_write_without_delay[is_gc]++;

	return me()->write_oob(device_fd, psn, 0, sizeof(oob), &oob, with_delay);
}

rc_t sector_mapping_ftl::read_oob(int psn, bool &valid, int &lsn, bool with_delay, bool is_gc)
{
	struct sector_oob oob;

	W_DO(me()->read_oob(device_fd, psn, 0, sizeof(oob), &oob, with_delay));

	valid = oob.valid;
	lsn = oob.lsn;

	if (with_delay)
		statistics.oob_read[is_gc]++;
	else
		statistics.oob_read_without_delay[is_gc]++;

	return RCOK;
}

rc_t sector_mapping_ftl::check_oob_consistency(int lsn, bool with_delay, bool is_gc)
{
	int oob_lsn = -1;
	bool oob_valid = false;

	W_DO(read_oob(sector_map[lsn], oob_valid, oob_lsn, with_delay, is_gc));

	if(!oob_valid || lsn != oob_lsn)
		return RC(fcINTERNAL);

	return RCOK;
}

rc_t sector_mapping_ftl::invalidate_sector(int psn, bool is_gc)
{
	return write_oob(psn, false, -1, true, is_gc);
}

rc_t sector_mapping_ftl::translate_file_pos(sthread_base_t::fileoff_t &pos, int rw, int device_fd)
{
	CRITICAL_SECTION(ftl_translate_cs, _ftl_mutex);

	sector_mapping_ftl &ftl = getInstance(device_fd);

	int lsn = pos / ftl.sector_size;
	if (pos < 0 || lsn >= ftl.num_util_sectors || (pos % ftl.sector_size))
		return RC(fcASSERT);

	int psn = -1;
	W_DO(ftl.lsn_to_psn(lsn, psn, rw));
	pos = (sthread_base_t::fileoff_t)psn * (sthread_base_t::fileoff_t)ftl.sector_size;

	return RCOK;
}

//In case of write request, the function returns free_psn to write to and
//immediately marks this sector as written (before actual write)
//so it is safe to use it in a concurrent environment (each thread after getting its psn could write real data when he wants)
rc_t sector_mapping_ftl::lsn_to_psn(int lsn, int &out_psn, int rw)
{
	if (sector_map[lsn] != -1 && FTL_MAINTAIN_OOB && FTL_CHECK_CONSISTENCY_WITH_OOB)
		W_DO_MSG(check_oob_consistency(lsn, false, false), << "FTL consistency check failed!");

	//READ
	if (rw == 0) {
		out_psn = sector_map[lsn];
		statistics.read[0]++;
	}
	//WRITE
	else {
		int new_psn = -1;
		W_DO(pop_next_free_sector(false, new_psn));
		//Update dirty_blocks/sectors if needed
		if (sector_map[lsn] != -1) {
			int prev_block_idx = sector_map[lsn] / sectors_per_block;
			int prev_sector_idx = sector_map[lsn] % sectors_per_block;
			block_stat[prev_block_idx].invalid_sector_count++;
			block_stat[prev_block_idx].invalid_sector_bitmap[prev_sector_idx] = true;

			if (FTL_MAINTAIN_OOB)
				W_DO_MSG(invalidate_sector(sector_map[lsn], false), << "FTL failed to invalidate a sector!");
		}
		if (FTL_MAINTAIN_OOB)
			W_DO_MSG(write_oob(new_psn, true, lsn, false, false), << "FTL failed to write sector metadata!");

		sector_map[lsn] = new_psn;
		out_psn = new_psn;
		statistics.write[0]++;
	}

	return RCOK;
}

rc_t sector_mapping_ftl::pop_next_free_sector(bool is_gc, int &out_free_sector)
{
	//Garbage collector
	if (!is_gc && ((last_written_sector % sectors_per_block) == (sectors_per_block - 1)) && (num_free_blocks < FTL_GC_THRESHOLD)) {
		while (num_free_blocks < FTL_GC_THRESHOLD + FTL_GC_RUN_STOCK_OVER_THRESHOLD)
			W_DO_MSG(gc_run(), << "Garbage collection failed!");
	}

	if ((last_written_sector == -1) || (last_written_sector % sectors_per_block) == (sectors_per_block - 1)) {
		if (!num_free_blocks)
			W_DO_MSG(RC(fcFULL), << "No free blocks available!");
		int free_block_idx = pop_free_block();
		last_written_sector = (free_block_idx * sectors_per_block);
		out_free_sector = last_written_sector;
	}
	else
		out_free_sector = ++last_written_sector;

	return RCOK;
}

rc_t sector_mapping_ftl::gc_run()
{
    int victim_block_idx = find_victim_block();

	for (int i = 0; i < sectors_per_block; i++) {
		int psn = (victim_block_idx * sectors_per_block) + i;
		if (!block_stat[victim_block_idx].invalid_sector_bitmap[i])
			W_DO_MSG(gc_rewrite_page(psn), << "GC failed to rewrite a sector!");
	}

	W_DO_MSG(erase_block(victim_block_idx), << "GC failed to erase a block!");
	return RCOK;
}

rc_t sector_mapping_ftl::erase_block(int blk_num)
{
	W_DO(me()->erase_block(device_fd, blk_num));
	block_stat[blk_num].free = true;
	block_stat[blk_num].erase_count++;
	std::fill(block_stat[blk_num].invalid_sector_bitmap.begin(), block_stat[blk_num].invalid_sector_bitmap.end(), false);
	block_stat[blk_num].invalid_sector_count = 0;
	num_free_blocks++;

	statistics.erase++;

	return RCOK;
}

rc_t sector_mapping_ftl::gc_rewrite_page(int psn)
{
	if (psn >= num_sectors)
		return RC(fcASSERT);

	int lsn = -1;
	if (FTL_MAINTAIN_OOB) {
		bool valid = false;
		W_DO(read_oob(psn, valid, lsn, true, true));
		if (!valid)
			W_DO_MSG(RC(fcINTERNAL), << "FTL consistency check fail!");
	}
	else {
		std::vector<int>::iterator it = std::find(sector_map.begin(), sector_map.end(), psn);
		if (it == sector_map.end())
			return RC(fcINTERNAL);
		lsn = *it;
	}

	//XXX we assume here we are in a single threaded context (critical section)
	// otherwise we need to use a dynamic memory, or add another critical section here, or...
	//to avoid wasting of time for allocating memory for each page we use a preallocated array.
	//using dynamic memory is bad because we do not have a destructor to free it
	static char page[SM_PAGESIZE];

    sthread_base_t::fileoff_t offset = (sthread_base_t::fileoff_t)psn * (sthread_base_t::fileoff_t)sector_size;

	W_DO(me()->pread(device_fd, page, sector_size, offset));
	statistics.read[1]++;

	int new_psn = -1;
	W_DO(pop_next_free_sector(true, new_psn));
	offset = (sthread_base_t::fileoff_t)new_psn * (sthread_base_t::fileoff_t)sector_size;

	W_DO(me()->pwrite(device_fd, page, sector_size, offset));
	statistics.write[1]++;
	if (FTL_MAINTAIN_OOB)
		W_DO_MSG(write_oob(new_psn, true, lsn, false, true), << "FTL failed to write sector metadata!");

	sector_map[lsn] = new_psn;

	return RCOK;
}

int sector_mapping_ftl::find_victim_block()
{
	int victim_block_idx = -1;
	int victim_block_num_invalid_sectors = 0;

	int current_block_idx = last_written_sector / sectors_per_block;
	if ((last_written_sector % sectors_per_block) == (sectors_per_block - 1))
		current_block_idx = -1;

	for(int i = 0; i < num_blocks; i++) {
		if (i == current_block_idx)
			continue;
		if (victim_block_idx == -1 || victim_block_num_invalid_sectors < block_stat[i].invalid_sector_count) {
			victim_block_idx = i;
			victim_block_num_invalid_sectors = block_stat[i].invalid_sector_count;
			if (victim_block_num_invalid_sectors == sectors_per_block)
				break;
		}
	}
	w_assert0(victim_block_num_invalid_sectors != 0);

	return victim_block_idx;
}

int sector_mapping_ftl::pop_free_block()
{
	int temp_min_erase_cnt = INT_MAX;
	int block_idx = -1;

	if (!FTL_WL_TYPE) {
		for (int i = 0; i < num_blocks; i++) {
			if (block_stat[i].free) {
					block_idx = i;
					break;
			}
		}
	}
	else {
		for (int i = 0; i < num_blocks; i++) {
			if (block_stat[i].free) {
				if (block_stat[i].erase_count < temp_min_erase_cnt) {
					temp_min_erase_cnt = block_stat[i].erase_count;
					block_idx = i;
					if (block_stat[i].erase_count == 0)
						break;
				}
			}
		}
	}

	w_assert0(block_idx != -1);
	block_stat[block_idx].free = false;
	num_free_blocks--;

	return block_idx;
}
