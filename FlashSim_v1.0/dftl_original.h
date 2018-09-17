struct flashsim_module;
struct device_params;

//Garbage collector
#define GC								1
#define NOT_GC							0
/*
 * Garbage collection will start if number of free blocks is LESS than this value.
 * Garbage collector works until the number of free block is LESS than (GC_THRESHOLD + GC_RUN_STOCK_OVER_THRESHOLD)
 * Example 1: GC_THRESHOLD == 3 && GC_RUN_STOCK_OVER_THRESHOLD == 0:  GC starts when number of free blocks == 2 and finishes at 3
 * Example 2: GC_THRESHOLD == 3 && GC_RUN_STOCK_OVER_THRESHOLD == 1:  GC starts when number of free blocks == 2 and finishes at 4
 * (GC_RUN_STOCK_OVER_THRESHOLD defines the "reserve/stock" of free blocks.)
 */
#define GC_THRESHOLD					5
#define GC_RUN_STOCK_OVER_THRESHOLD 	0

//OOB
#define OOB_LSN_INDEX					0
#define OOB_STATUS_INDEX				1
#define OOB_TYPE_INDEX					2
#define OOB_NUM_FIELDS					3

enum io_type_e {
	READ_SECTOR = 0,
	WRITE_SECTOR,
	READ_SECTOR_OOB,
	WRITE_SECTOR_OOB,
	READ_SECTOR_OOB_WITHOUT_DELAY,
	WRITE_SECTOR_OOB_WITHOUT_DELAY,
	ERASE
};

enum sector_status {
	VALID_SECTOR = 0,
	INVALID_SECTOR
};

enum block_status {
	FREE_BLOCK = 0,
	VALID_BLOCK
};

enum unit_type {
	DATA_UNIT = 0,
	MAP_UNIT = 1
};

enum wl_type {
	NO_WL = 0,
	MIN_EC_WL
};

enum cache_status {
	CMT_REAL = 0,
	CMT_GHOST,
	CMT_INVALID
};

struct request_ftl_statistics {
	unsigned short sector_read_num;
	unsigned short sector_write_num;
	unsigned short sector_oob_read_num;
	unsigned short sector_oob_write_num;
	unsigned short block_erase_num;
	int psn;
	int ftl_duration;
};

struct block_info {
	enum block_status block_status;
	enum unit_type block_type;
	int erase_count;
	int free_sector_count;
	int invalid_sector_count;
};

struct map_entry {
	enum cache_status cache_status;
	int age;
	int psn;
	int updated;
};

struct dftl_params {
	//Input params
	int device_type;
	struct device_params * dev_params;
	int cmt_real_max_entries;
	int cmt_ghost_max_entries;
	int num_extra_blocks;

	//Calculated parameters
	int gmt_entries_per_page;
	int num_valid_map_sectors;
	int min_num_map_blocks;
	int num_data_sectors;
	int gtd_size;
};

struct dftl_stat {
	int cache_hit;
	int cache_evict;
	int flash_hit;

	int delay_flash_update;
	int save_map_updates_via_batch_update;
	int blind_read;

	//[unit_type][is_gc]
	int read[2][2];
	int write[2][2];
	int oob_read[2][2];
	int oob_write[2][2];
	int oob_read_without_delay[2][2];
	int oob_write_without_delay[2][2];

	//[unit_type]
	int erase[2];
};

typedef enum unit_type unit_type_e;
typedef int oob_field;
typedef oob_field oob[OOB_NUM_FIELDS];
typedef struct map_entry map_entry;
typedef struct block_info block_info;
typedef int gtd_entry;
typedef int gmt_entry;

struct sector_buffer {
	void *buffer;
	int fixed;
};

struct dftl_s {
	struct flashsim_block_device * block_device;
	struct flashsim_char_device * char_device;
	struct dftl_params params;
	struct dftl_stat stat;

	map_entry * sector_map;
	block_info * block_stat;
	gtd_entry * gtd;

	int * cmt_real_arr;
	int * cmt_ghost_arr;

	int cmt_real_num_entries;
	int cmt_ghost_num_entries;
	int free_block_num;
	int cmt_entry_max_age;
	//[unit_type]
	int last_written_sector[2];

	struct sector_buffer sector_buffer;
	struct request_ftl_statistics req_stat; //statistics for CURRENT request. Reset after each lsn_to_psn call
};
