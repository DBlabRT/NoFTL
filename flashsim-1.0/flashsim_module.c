#include <linux/init.h>
#include <linux/moduleparam.h>
#include <linux/major.h>
#include <linux/highmem.h>
#include <linux/slab.h>

#include <asm/uaccess.h>

#include "flashsim_common.h"

MODULE_LICENSE("Dual BSD/GPL");

/***********************************Parameters' loading*****************************/

/***************************************BLOCK DEVICE********************************/
static uint block_device_major_number = 0;
module_param(block_device_major_number, uint, 0);
MODULE_PARM_DESC(block_device_major_number, "Block device: The major number.");

static uint block_device_sector_size = PAGE_SIZE;
module_param(block_device_sector_size, uint, 0);
MODULE_PARM_DESC(block_device_sector_size, "Block device: The size of the sector (NAND page = read/write unit) (in bytes).");

static uint block_device_num_blocks = 0;
module_param(block_device_num_blocks, uint, 0);
MODULE_PARM_DESC(block_device_num_blocks, "Block device: Size of device in NAND blocks (erase units), i.e., total number of blocks in device.");

static uint block_device_sectors_per_block = 64;
module_param(block_device_sectors_per_block, uint, 0);
MODULE_PARM_DESC(block_device_sectors_per_block, "Block device: Sectors (NAND pages) per NAND block.");

static int block_device_delay_mode = 0;
module_param(block_device_delay_mode, int, 0);
MODULE_PARM_DESC(block_device_delay_mode, "Block device: Delay mode: 0 = no add. delay; 1 = udelay.");

static int block_device_request_statistics_mode = 0;
module_param(block_device_request_statistics_mode, int, 0);
MODULE_PARM_DESC(block_device_request_statistics_mode, "Block device: Request statistics mode: 0 = no statistics per request (default); "
		"1 = yes (maintaining statistics (including FTL statistics if FTL is used) per each external request ).");

static int block_device_max_req_stat_entries = 0;
module_param(block_device_max_req_stat_entries, int, 0);
MODULE_PARM_DESC(block_device_max_req_stat_entries, "Block device: Maximal number of request's statistics, that will be stored and could be read via proc files."
		"This parameter is only valid in combination with block_device_request_statistics_mode != 0.");

static uint block_device_read_sector_latency_us = 25;
module_param(block_device_read_sector_latency_us, uint, 0);
MODULE_PARM_DESC(block_device_read_sector_latency_us, "Block device: Latency of READ PAGE operation (in microseconds).");

static uint block_device_program_sector_latency_us = 250;
module_param(block_device_program_sector_latency_us, uint, 0);
MODULE_PARM_DESC(block_device_program_sector_latency_us, "Block device: Latency of PROGRAM PAGE operation (in microseconds).");

static uint block_device_erase_block_latency_us = 700;
module_param(block_device_erase_block_latency_us, uint, 0);
MODULE_PARM_DESC(block_device_erase_block_latency_us, "Block device: Latency of ERASE BLOCK operation (in microseconds).");

static uint block_device_oob_size = 64;
module_param(block_device_oob_size, uint, 0);
MODULE_PARM_DESC(block_device_oob_size, "Block device: Size of OOB area of a sector (in bytes).");

static uint block_device_read_oob_latency_us = 0;
module_param(block_device_read_oob_latency_us, uint, 0);
MODULE_PARM_DESC(block_device_read_oob_latency_us, "Block device: Latency of READ METADATA operation (in microseconds).");

static uint block_device_program_oob_latency_us = 0;
module_param(block_device_program_oob_latency_us, uint, 0);
MODULE_PARM_DESC(block_device_program_oob_latency_us, "Block device: Latency of PROGRAM METADATA operation (in microseconds).");

static uint block_device_ftl_mode = 0;
module_param(block_device_ftl_mode, uint, 0);
MODULE_PARM_DESC(block_device_ftl_mode, "Block device: FTL type: 0 = none; 1 - dftl_original.");

/*********************************END of BLOCK DEVICE*******************************/

/****************************************CHAR DEVICE********************************/
static uint char_device_major_number = 0;
module_param(char_device_major_number, uint, 0);
MODULE_PARM_DESC(char_device_major_number, "Char device: The major number.");

static uint char_device_sector_size = PAGE_SIZE;
module_param(char_device_sector_size, uint, 0);
MODULE_PARM_DESC(char_device_sector_size, "Char device: The size of the sector (NAND page = read/write unit) (in bytes).");

static uint char_device_num_blocks = 0;
module_param(char_device_num_blocks, uint, 0);
MODULE_PARM_DESC(char_device_num_blocks, "Char device: Size of device in NAND blocks (erase units), i.e., total number of blocks in device.");

static uint char_device_sectors_per_block = 64;
module_param(char_device_sectors_per_block, uint, 0);
MODULE_PARM_DESC(char_device_sectors_per_block, "Char device: Sectors (NAND pages) per NAND block.");

static int char_device_delay_mode = 0;
module_param(char_device_delay_mode, int, 0);
MODULE_PARM_DESC(char_device_delay_mode, "Char device: Delay mode: 0 = no add. delay; 1 = udelay.");

static int char_device_request_statistics_mode = 0;
module_param(char_device_request_statistics_mode, int, 0);
MODULE_PARM_DESC(char_device_request_statistics_mode, "Char device: Request statistics mode: 0 = no statistics per request (default); "
		"1 = yes (maintaining statistics (including FTL statistics if FTL is used) per each external request ).");

static int char_device_max_req_stat_entries = 0;
module_param(char_device_max_req_stat_entries, int, 0);
MODULE_PARM_DESC(char_device_max_req_stat_entries, "Char device: Maximal number of request's statistics, that will be stored and could be read via proc files."
		"This parameter is only valid in combination with char_device_request_statistics_mode != 0.");

static uint char_device_read_sector_latency_us = 25;
module_param(char_device_read_sector_latency_us, uint, 0);
MODULE_PARM_DESC(char_device_read_sector_latency_us, "Char device: Latency of READ PAGE operation (in microseconds).");

static uint char_device_program_sector_latency_us = 250;
module_param(char_device_program_sector_latency_us, uint, 0);
MODULE_PARM_DESC(char_device_program_sector_latency_us, "Char device: Latency of PROGRAM PAGE operation (in microseconds).");

static uint char_device_erase_block_latency_us = 700;
module_param(char_device_erase_block_latency_us, uint, 0);
MODULE_PARM_DESC(char_device_erase_block_latency_us, "Char device: Latency of ERASE BLOCK operation (in microseconds).");

static uint char_device_oob_size = 64;
module_param(char_device_oob_size, uint, 0);
MODULE_PARM_DESC(char_device_oob_size, "Char device: Size of OOB area of a sector (in bytes).");

static uint char_device_read_oob_latency_us = 0;
module_param(char_device_read_oob_latency_us, uint, 0);
MODULE_PARM_DESC(char_device_read_oob_latency_us, "Char device: Latency of READ METADATA operation (in microseconds).");

static uint char_device_program_oob_latency_us = 0;
module_param(char_device_program_oob_latency_us, uint, 0);
MODULE_PARM_DESC(char_device_program_oob_latency_us, "Char device: Latency of PROGRAM METADATA operation (in microseconds).");

static uint char_device_ftl_mode = 0;
module_param(char_device_ftl_mode, uint, 0);
MODULE_PARM_DESC(char_device_ftl_mode, "Char device: FTL type: 0 = none; 1 - dftl_original.");


/*********************************END of CHAR DEVICE*******************************/

/******************************End of parameters' loading***************************/

static struct flashsim_module *FlashSim = NULL;

struct page * lookup_page(struct page **pages, uint idx)
{
	struct page *pg = pages[idx];

	if (!pg || pg->index != idx) {
		print_error("ERROR: Inconsistency in internal memory!");
		return NULL;
	}

	return pg;
}

static struct page * __init flashsim_alloc_page (struct page **pages, uint idx)
{
	struct page *page;
	gfp_t gfp_flags;

	gfp_flags = GFP_NOIO | __GFP_ZERO | __GFP_HIGHMEM;

	page = alloc_page(gfp_flags);
	if (!page)
		return NULL;

	pages[idx] = page;
	page->index = idx;

	return page;
}

int __init flashsim_alloc_pages (struct page **pages, const uint count)
{
	uint i;
	for (i = 0; i < count; i++) {
		if (!flashsim_alloc_page(pages, i)) {
			printf_error("Unable to allocate page number %u. Page size is %lu", i, PAGE_SIZE);
			return -ENOMEM;
		}
	}
	return 0;
}

void flashsim_free_pages(struct page **pages, uint count)
{
	uint i;

	if (!pages)
		return;

	for (i = 0; i < count; i++) {
		if (pages[i])
			__free_page(pages[i]);
	}
}

/*
 * Offset and Length are related here to oob (!not to oob_store_page which contains a certain oob)
 */
int access_sector_oob(int device_type, int psn, void * buffer, int offset, size_t length, int rw, int simulate_delay)
{
	struct flashsim_device *device;
	uint page_idx;
	struct page *oob_store_page;
	void *mapped_oob_store_page;
	t_time request_arrival;

	if (simulate_delay)
		set_current_time(&request_arrival);

	if (device_type == BLOCK_DEVICE)
		device = &FlashSim->block_dev.dev_core;
	else if (device_type == CHAR_DEVICE)
		device = &FlashSim->char_dev.dev_core;
	else {
		print_error("Invalid OOB access command!");
		return -1;
	}

	page_idx = psn / device->params.oobs_per_page;
	oob_store_page = lookup_page(device->oob_pages, page_idx);
	if (!oob_store_page)
		return -1;

	mapped_oob_store_page = kmap_atomic(oob_store_page, KM_USER1);
	//transform the offset of oob to offset of oob_store_page
	offset += (psn % device->params.oobs_per_page) * device->params.oob_size;
	if (rw == READ)
		memcpy(buffer, mapped_oob_store_page + offset, length);
	else
		memcpy(mapped_oob_store_page + offset, buffer, length);
	kunmap_atomic(mapped_oob_store_page, KM_USER1);

	if (simulate_delay && device->params.delay_mode)
		simulate_request_latency(device, rw, 1, SECTOR_OOB_AREA, &request_arrival);

	return 0;
}

int access_oob_ioctl(int device_type, unsigned long arg)
{
	struct access_oob params;
	void *buffer;
	int retval = 0;

	if (copy_from_user(&params, (void __user *) arg, sizeof(struct access_oob)))
		return -EINVAL;

	printf_debug("access_oob_ioctl (begin): psn=%d, rw=%d, delay=%d, offset=%d, length=%d",
			params.psn, params.rw, params.simulate_delay, params.offset, params.length);

	 buffer = kzalloc(params.length, GFP_ATOMIC);
	 if (!buffer)
		 return -ENOMEM;

	if (params.rw == WRITE) {
		retval = copy_from_user(buffer, (void __user *) params.buffer, params.length);
		if (retval) {
			kfree(buffer);
			return retval;
		}
	}

	access_sector_oob(device_type, params.psn, buffer, params.offset, params.length, params.rw, params.simulate_delay);

	if (params.rw == READ)
		retval = copy_to_user((void __user *) params.buffer, buffer, params.length);

	kfree(buffer);
	return retval;
}

void calculate_oob_alignment(struct device_params *params, struct request_alignment *request, int start_psn, int sector_count)
{
	request->start_page_idx = start_psn / params->oobs_per_page;
	request->end_page_idx = (start_psn + sector_count - 1) / params->oobs_per_page;
	request->start_page_offset = (start_psn % params->oobs_per_page) * params->oob_size;
	request->end_page_offset = ((((start_psn + sector_count - 1) % params->oobs_per_page) + 1) * params->oob_size) - 1;
	request->sector_count = sector_count;
}

void calculate_request_alignment(struct device_params *params, struct request_alignment *request, size_t count, loff_t *f_pos)
{
	int start_sector_idx;
	int end_sector_idx;

	//Internal structure
	request->start_page_idx = (*f_pos) >> PAGE_SHIFT;
	request->end_page_idx = (*f_pos + count - 1) >> PAGE_SHIFT;
	request->start_page_offset = (*f_pos) % PAGE_SIZE;
	request->end_page_offset = (*f_pos + count - 1) % PAGE_SIZE;

	//NAND structure
	start_sector_idx = (*f_pos) / params->sector_size;
	end_sector_idx = (*f_pos + count - 1) / params->sector_size;
	request->sector_count = end_sector_idx - start_sector_idx + 1;
}

int calculate_page_info(struct page **pages, struct request_alignment *request, struct page_info *page_info, uint page_idx)
{
	page_info->page = lookup_page(pages, page_idx);
	if (!page_info->page) {
		printk(MSG_PREFIX "ERROR: Page %u not found!", page_idx);
		return -1;
	}

	page_info->page_address = kmap_atomic(page_info->page, KM_USER1);

	if (request->start_page_idx == request->end_page_idx) {
		page_info->page_offset = request->start_page_offset;
		page_info->page_count = request->end_page_offset - request->start_page_offset + 1;
	}
	else if (page_idx == request->start_page_idx) {
		page_info->page_offset = request->start_page_offset;
		page_info->page_count = PAGE_SIZE - request->start_page_offset;
	}
	else if (page_idx == request->end_page_idx) {
		page_info->page_offset = 0;
		page_info->page_count = request->end_page_offset + 1;
	}
	else {
		page_info->page_offset = 0;
		page_info->page_count = PAGE_SIZE;
	}

	return 0;
}

static int erase_block_oobs(struct flashsim_device *device, int block_index)
{
	int i;
	int start_psn;
	struct request_alignment request;
	struct page_info cur_page;

	start_psn = block_index * device->params.sectors_per_block;
	calculate_oob_alignment(&device->params, &request, start_psn, device->params.sectors_per_block);

	for (i = request.start_page_idx; i <= request.end_page_idx; i++) {
		if(calculate_page_info(device->oob_pages, &request, &cur_page, i))
			return -EIO;
		memset((cur_page.page_address + cur_page.page_offset), 0, cur_page.page_count);
		kunmap_atomic(cur_page.page_address, KM_USER1);
	}

	return 0;
}

static int erase_block_core(struct flashsim_device *device, int block_index)
{
	int i;
	int blk_size;
	loff_t start_pos;
	struct request_alignment request;
	struct page_info cur_page;

	if (block_index < 0 || block_index >= device->params.num_blocks)
		return -EINVAL;

	blk_size = device->params.sector_size * device->params.sectors_per_block;
	start_pos = (loff_t)block_index * (loff_t)blk_size;
	calculate_request_alignment(&device->params, &request, blk_size, &start_pos);

	for (i = request.start_page_idx; i <= request.end_page_idx; i++) {
		if(calculate_page_info(device->data_pages, &request, &cur_page, i))
			return -EIO;
		memset((cur_page.page_address + cur_page.page_offset), 0, cur_page.page_count);
		kunmap_atomic(cur_page.page_address, KM_USER1);
	}

	if (device->params.oob_size)
		return erase_block_oobs(device, block_index);

	return 0;
}

int erase_block_ioctl(struct flashsim_device *device, unsigned long arg)
{
	int block_index, retval;
	t_time request_arrival;

	set_current_time(&request_arrival);

	if (copy_from_user(&block_index, (void __user *) arg, sizeof(int)))
		return -EINVAL;

	retval = erase_block_core(device, block_index);
	if (retval)
		return retval;

	if (device->params.delay_mode)
		simulate_request_latency(device, ERASE_BLOCK, 0, SECTOR_DATA_AREA, &request_arrival);

	return 0;
}

int erase_block_internal(struct flashsim_device *device, int block_index)
{
	int retval;
	t_time request_arrival;

	set_current_time(&request_arrival);

	retval = erase_block_core(device, block_index);
	if (retval)
		return retval;

	if (device->params.delay_mode)
		simulate_request_latency(device, ERASE_BLOCK, 0, SECTOR_DATA_AREA, &request_arrival);

	return 0;
}

int set_delay_mode_ioctl(struct flashsim_device *device, unsigned long arg)
{
	int delay_mode = -1;

	if (copy_from_user(&delay_mode, (void __user *) arg, sizeof(int)))
		return -EINVAL;

	if (delay_mode != 0 && delay_mode != 1)
		return -EINVAL;

	device->params.delay_mode = delay_mode;
	return 0;
}

static int __init flashsim_init(void)
{
	int retval;
	struct device_params *block_dev_params;
	struct device_params *char_dev_params;

	FlashSim = vzalloc(sizeof(*FlashSim));
	if (!FlashSim) {
		retval = -ENOMEM;
		print_error("Not enough memory for allocating flashsim_module!");
		goto out;
	}

	//--------------------------block device params----------------------------
	block_dev_params = &FlashSim->block_dev.dev_core.params;

	block_dev_params->device_type = BLOCK_DEVICE;
	block_dev_params->major_number = block_device_major_number;
	block_dev_params->sector_size = block_device_sector_size;
	block_dev_params->num_blocks = block_device_num_blocks;
	block_dev_params->sectors_per_block = block_device_sectors_per_block;
	block_dev_params->read_sector_latency_us = block_device_read_sector_latency_us;
	block_dev_params->program_sector_latency_us = block_device_program_sector_latency_us;
	block_dev_params->erase_block_latency_us = block_device_erase_block_latency_us;
	block_dev_params->delay_mode = block_device_delay_mode;
	block_dev_params->request_statistics_mode = block_device_request_statistics_mode;
	FlashSim->block_dev.dev_core.dev_stat.req_stat_store.max_req_stat_entries = block_device_max_req_stat_entries;

	block_dev_params->num_sectors = block_device_num_blocks * block_device_sectors_per_block;
	block_dev_params->size = (u64)block_dev_params->num_sectors * (u64)block_device_sector_size;
	block_dev_params->num_pages = block_dev_params->size >> PAGE_SHIFT;
	if (block_dev_params->size % PAGE_SIZE)
		block_dev_params->num_pages++;

	//OOB
	block_dev_params->oob_size = block_device_oob_size;
	//To simplify operations with oob, they will be completely fit into pages
	//i.e., there could be some free (unused) space at the end of oob_pages
	if (block_dev_params->oob_size) {
		block_dev_params->oobs_per_page = PAGE_SIZE / block_dev_params->oob_size;
		block_dev_params->num_oob_pages = block_dev_params->num_sectors / block_dev_params->oobs_per_page;
		if (block_dev_params->num_sectors % block_dev_params->oobs_per_page)
			block_dev_params->num_oob_pages++;
		block_dev_params->program_oob_latency_us = block_device_program_oob_latency_us;
		block_dev_params->read_oob_latency_us = block_device_read_oob_latency_us;
	}
	else {
		block_dev_params->oobs_per_page = block_dev_params->num_oob_pages = 0;
		block_dev_params->program_oob_latency_us = block_dev_params->read_oob_latency_us = 0;
	}

	block_dev_params->ftl_mode = block_device_ftl_mode;

	//------------------------End of block device params-----------------------

	//--------------------------char device params----------------------------
	char_dev_params = &FlashSim->char_dev.dev_core.params;

	char_dev_params->device_type = CHAR_DEVICE;
	char_dev_params->major_number = char_device_major_number;
	char_dev_params->sector_size = char_device_sector_size;
	char_dev_params->num_blocks = char_device_num_blocks;
	char_dev_params->sectors_per_block = char_device_sectors_per_block;
	char_dev_params->read_sector_latency_us = char_device_read_sector_latency_us;
	char_dev_params->program_sector_latency_us = char_device_program_sector_latency_us;
	char_dev_params->erase_block_latency_us = char_device_erase_block_latency_us;
	char_dev_params->delay_mode = char_device_delay_mode;
	char_dev_params->request_statistics_mode = char_device_request_statistics_mode;
	FlashSim->char_dev.dev_core.dev_stat.req_stat_store.max_req_stat_entries = char_device_max_req_stat_entries;

	char_dev_params->num_sectors = char_device_num_blocks * char_device_sectors_per_block;
	char_dev_params->size = (u64)char_dev_params->num_sectors * (u64)char_device_sector_size;
	char_dev_params->num_pages = char_dev_params->size >> PAGE_SHIFT;
	if (char_dev_params->size % PAGE_SIZE)
		char_dev_params->num_pages++;

	//OOB
	char_dev_params->oob_size = char_device_oob_size;
	if (char_dev_params->oob_size) {
		char_dev_params->oobs_per_page = PAGE_SIZE / char_dev_params->oob_size;
		char_dev_params->num_oob_pages = char_dev_params->num_sectors / char_dev_params->oobs_per_page;
		if (char_dev_params->num_sectors % char_dev_params->oobs_per_page)
			char_dev_params->num_oob_pages++;
		char_dev_params->program_oob_latency_us = char_device_program_oob_latency_us;
		char_dev_params->read_oob_latency_us = char_device_read_oob_latency_us;
	}
	else {
		char_dev_params->oobs_per_page = char_dev_params->num_oob_pages = 0;
		char_dev_params->program_oob_latency_us = char_dev_params->read_oob_latency_us = 0;
	}

	char_dev_params->ftl_mode = char_device_ftl_mode;

	//------------------------End of char device params-----------------------

	if (block_dev_params->size) {
		retval = init_block_device(&FlashSim->block_dev);
		if (retval)
			goto out_free;
	}

	if (char_dev_params->size) {
		retval = init_char_device(&FlashSim->char_dev);
		if (retval) {
			if (block_dev_params->size)
				goto out_free_block_device;
			else
				goto out_free;
		}
	}

	if (block_dev_params->size == 0 && char_dev_params->size == 0) {
		print_error("No device is specified!");
		goto out_free;
	}

	return 0;

out_free_block_device:
	free_block_device(&FlashSim->block_dev);
out_free:
	vfree(FlashSim);
out:
	return retval;
}

static void __exit flashsim_exit(void)
{
	if (FlashSim->block_dev.dev_core.params.size)
		free_block_device(&FlashSim->block_dev);
	if (FlashSim->char_dev.dev_core.params.size)
		free_char_device(&FlashSim->char_dev);
	vfree(FlashSim);
}

module_init(flashsim_init);
module_exit(flashsim_exit);


