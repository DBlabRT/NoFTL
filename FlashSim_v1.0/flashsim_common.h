#include <linux/module.h>
#include <linux/blkdev.h>
#include <linux/timer.h>
#include <linux/delay.h>
#include <linux/bio.h>
#include <linux/ioctl.h>
#include <linux/cdev.h>

#define DEBUG								0

#define BLOCK_DEVICE						0
#define CHAR_DEVICE							1
#define SECTOR_DATA_AREA					0
#define SECTOR_OOB_AREA						1

#define MODULE_NAME 						"flashsim"
#define CHAR_DEVICE_NAME 					MODULE_NAME "_char"
#define BLOCK_DEVICE_NAME 					MODULE_NAME "_block"
#define MSG_PREFIX 							KERN_ALERT MODULE_NAME ": "
#define BLOCK_DEVICE_MINORS					16

#define print_msg(msg)						printk(MSG_PREFIX msg "\n")
#define PRINT_MSG(msg)						print_msg(msg)
#define printf_msg(format, args...) 		printk(MSG_PREFIX format "\n", ##args)
#define print_error(msg)					printk(MSG_PREFIX "ERROR in file=%s; function=%s; line=%d: " msg "\n", __FILE__, __func__, __LINE__)
#define printf_error(format, args...)		printk(MSG_PREFIX "ERROR in file=%s; function=%s; line=%d: " format "\n", __FILE__, __func__, __LINE__, ##args)
#define print_debug(msg) 					do { if (DEBUG) printk(MSG_PREFIX "DEBUG %s " msg "\n", __func__); } while (0)
#define printf_debug(format, args...)		do { if (DEBUG) printk(MSG_PREFIX "DEBUG %s " format "\n", __func__, ##args); } while (0)

/********************ioctl commands***********************/
/* The numbers for ioctl commands are defined here (for simplicity in user space) according
 * to the old style. Its, however, not critical for the flashsim simulator.
 * One can think to change this numbers to new encoding style (e.g., _IOW, _IOR, _IOWR)
 */
#define ERASE_BLOCK 					0x6b01
#define ACCESS_OOB						0x6b02
#define RESET_STAT						0x6b03
#define SET_DELAY_MODE					0x6b04

/**********************************************************/
typedef struct timespec t_time;

struct request_statistics_store {
	int req_stat_entry_size;
	int max_req_stat_entries;
	int num_req_stat_pages;
	int req_stat_entries_per_page;
	//Flag to indicate full request statistics store.
	int req_stat_overflow : 1;
	struct page **req_stat_pages;
};

struct device_statistics {
	struct cdev statistics_char_device;
	int statistics_char_device_major;
	int read_cursor;

	//Counter for external IOs
	unsigned long external_io_couter;
	unsigned long external_read_couter;
	unsigned long external_write_couter;

	//Number of requests, the execution time of which took more than need to be simulated (delay_mode == 1).
	//For SECTOR_DATA_AREA & SECTOR_OOB_AREA (if oob_pages are used).
	int num_time_overflowed_requests[2];

	struct request_statistics_store req_stat_store;
};

struct device_params {
	uint device_type;
	uint major_number;
	uint sector_size;
	uint num_blocks;
	uint sectors_per_block;
	uint read_sector_latency_us;
	uint program_sector_latency_us;
	uint erase_block_latency_us;
	uint oob_size;
	uint read_oob_latency_us;
	uint program_oob_latency_us;

	int ftl_mode;
	int delay_mode;
	int request_statistics_mode;

	//Calculated, read-only parameters
	uint num_sectors;
	u64 size; //in bytes
	uint num_pages;
	uint oobs_per_page;
	uint num_oob_pages;
};

struct dftl_s;

struct flashsim_device {
	struct device_params params;
	struct page **data_pages;
	struct page **oob_pages;
	struct device_statistics dev_stat;
	struct dftl_s *dftl;
};

struct flashsim_block_device {
	struct flashsim_device dev_core;

	spinlock_t lock;
	struct request_queue *queue;
	struct gendisk *disk;
	int kernel_sectors_per_flashsim_sector;
};

struct flashsim_char_device {
	struct flashsim_device dev_core;

	struct semaphore sem;
	struct cdev disk;
};

struct flashsim_module {
	struct flashsim_block_device block_dev;
	struct flashsim_char_device char_dev;
};

struct request_info {
	unsigned long request_sequence_number;
	int start_lsn;
	int rw;
	int num_sectors;
	int duration;
};

struct request_alignment {
	int start_page_idx;
	int end_page_idx;
	int start_page_offset;
	int end_page_offset; //last USED byte
	int sector_count;
};

struct page_info {
	struct page *page;
	void * page_address;
	int page_count;
	int page_offset;
};

struct access_oob {
	int psn;
	int offset;
	int length;
	int rw;
	int simulate_delay;
	void * buffer;
};

/*************************FTL*****************************/
#define NO_FTL			0
#define DFTL_ORIGINAL	1

int lsn_to_psn(struct dftl_s *, int, int);
struct dftl_s * __init init_dftl (void *, int, int *);
void free_dftl(struct dftl_s *);
void reset_dftl_statistics(struct dftl_s *dftl);
/*********************************************************/

int set_delay_mode_ioctl(struct flashsim_device *, unsigned long);
int erase_block_ioctl(struct flashsim_device *, unsigned long);
int erase_block_internal(struct flashsim_device *, int);

struct page * lookup_page(struct page **, uint);
int __init flashsim_alloc_pages (struct page **, uint);
void flashsim_free_pages(struct page **, uint);

int __init init_block_device (struct flashsim_block_device *);
int __init init_char_device (struct flashsim_char_device *);
void free_block_device (struct flashsim_block_device *);
void free_char_device (struct flashsim_char_device *);

void copy_to_flashsim_internal(struct flashsim_block_device *, const void *, uint);
void copy_from_flashsim_internal (struct flashsim_block_device *, void *, uint);
ssize_t char_device_read_internal (struct flashsim_char_device *, void *, size_t, loff_t *);
ssize_t char_device_write_internal (struct flashsim_char_device *, const void *, size_t, loff_t *);
int access_sector_oob(int, int, void *, int, size_t, int, int);
int access_oob_ioctl(int, unsigned long);
void calculate_oob_alignment(struct device_params *, struct request_alignment *, int, int);
void calculate_request_alignment(struct device_params *, struct request_alignment *, size_t, loff_t *);
int calculate_page_info(struct page **, struct request_alignment *, struct page_info *, uint);

void set_current_time(t_time *);
int get_duration_in_us(t_time *, t_time *);
int get_duration_in_us_till_now(t_time *);
void simulate_request_latency(struct flashsim_device *, int, int, int, t_time *);

void write_request_statistics(struct flashsim_device *, struct request_info *, t_time *);
int __init init_device_statistics (struct flashsim_device *);
void free_device_statistics(struct flashsim_device *);
/*****************************************************/
