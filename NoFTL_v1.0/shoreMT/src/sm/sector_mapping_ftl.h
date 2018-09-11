#ifndef SECTOR_MAPPING_FTL_H
#define SECTOR_MAPPING_FTL_H

#include "w_defines.h"

#ifdef __GNUG__
#pragma interface
#endif

#include <vector>
#include <sthread.h>

#define SECTOR_MAP_FILE_NAME 			"sector_map_file"
#define BLOCK_STAT_FILE_NAME 			"block_stat_file"
#define LAST_WRITTEN_SECTOR_FILE_NAME	"current_pos_file"
#define FTL_STATE_MODE_FILE_NAME		"ftl_state_mode_file"

class sector_mapping_ftl {
public:
	struct ftl_state {
		typedef enum {
			NONE = 0,
			LOAD_FROM_FILE = 1,
			RUN_TRACE_ONCE = 2,
			SAVE_TO_FILE = 4
		} mode;
	};
private:
	struct block_info {
		bool free;
		int erase_count;
		int invalid_sector_count;
		std::vector<bool> invalid_sector_bitmap;
	};

	struct sector_oob {
		int valid : 1;
		int lsn;
	};

	sector_mapping_ftl(int device_fd, int num_blocks, int sectors_per_block, int sector_size);

	int state_mode;
	int sector_size;
	int num_blocks;
	int sectors_per_block;
	int num_sectors;
	int num_util_sectors;
	int device_fd;

	std::vector<int> sector_map;
	std::vector<block_info> block_stat;
	int num_free_blocks;
	int last_written_sector;

	struct ftl_statistics {
		int read[2];
		int write[2];
		int oob_read[2];
		int oob_write[2];
		int oob_read_without_delay[2];
		int oob_write_without_delay[2];
		int erase;
	} statistics;

	static pthread_mutex_t _ftl_mutex;

	rc_t gc_run();
	rc_t gc_rewrite_page(int psn);
	rc_t erase_block(int blk_num);
	rc_t invalidate_sector(int psn, bool is_gc);
	rc_t write_oob(int psn, bool valid, int lsn, bool with_delay, bool is_gc);
	rc_t read_oob(int psn, bool &valid, int &lsn, bool with_delay, bool is_gc);
	rc_t check_oob_consistency(int lsn, bool with_delay, bool is_gc);
	rc_t lsn_to_psn(int lsn, int &out_psn, int rw);
	rc_t pop_next_free_sector(bool is_gc, int &out_free_sector);

	int find_victim_block();
	int pop_free_block();

	void reset_statistics();
	void print_statistics_core();

	static sector_mapping_ftl & getInstance(int device_fd);
	rc_t load_ftl_state_from_file();
	rc_t run_warm_up_trace(const char *trace_file_name);
	rc_t read_ftl_state_mode_from_file();
	rc_t write_ftl_state_mode_to_file(int new_state_mode);

public:
	//we consider now single threaded FTL. Public API should be "mutexed"
	static void print_statistics();
	static rc_t close_ftl();
	static rc_t translate_file_pos(sthread_base_t::fileoff_t &pos, int rw, int device_fd);
};

#endif
