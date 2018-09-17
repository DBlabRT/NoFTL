#include "flashsim_common.h"
#include "dftl_original.h"

void write_request_statistics(struct flashsim_device *device, struct request_info *req_info, t_time *request_start_time)
{
	int page_index;
	int page_offset;
	void * page_addr;
	struct page *page;
	struct request_statistics_store *req_stat_store = &device->dev_stat.req_stat_store;

	if (device->params.request_statistics_mode && !req_stat_store->req_stat_overflow) {
		if (req_info->request_sequence_number < req_stat_store->max_req_stat_entries) {
			page_index = req_info->request_sequence_number / req_stat_store->req_stat_entries_per_page;
			page_offset = (req_info->request_sequence_number % req_stat_store->req_stat_entries_per_page) * req_stat_store->req_stat_entry_size;
			page = lookup_page(req_stat_store->req_stat_pages, page_index);
			if (!page)
				return;

			page_addr = kmap_atomic(page, KM_USER1);
			req_info->duration = get_duration_in_us_till_now(request_start_time);
			memcpy(page_addr + page_offset, req_info, sizeof(struct request_info));
			if (device->params.ftl_mode)
				memcpy((page_addr + page_offset + sizeof(struct request_info)), &device->dftl->req_stat, sizeof(struct request_ftl_statistics));
			kunmap_atomic(page_addr, KM_USER1);
		}
		else
			req_stat_store->req_stat_overflow = 1;
	}
}

static void read_request_statistics(struct flashsim_device *device, int request_sequence_number, struct request_info *req_info, struct request_ftl_statistics *ftl_stat)
{
	int page_index;
	int page_offset;
	void * page_addr;
	struct page *page;
	struct request_statistics_store *req_stat_store = &device->dev_stat.req_stat_store;

	if (device->params.request_statistics_mode && (request_sequence_number < req_stat_store->max_req_stat_entries)) {
		page_index = request_sequence_number / req_stat_store->req_stat_entries_per_page;
		page_offset = (request_sequence_number % req_stat_store->req_stat_entries_per_page) * req_stat_store->req_stat_entry_size;
		page = lookup_page(req_stat_store->req_stat_pages, page_index);
		if (!page)
			return;

		page_addr = kmap_atomic(page, KM_USER1);
		memcpy(req_info, page_addr + page_offset, sizeof(struct request_info));
		if (device->params.ftl_mode)
			memcpy(ftl_stat, (page_addr + page_offset + sizeof(struct request_info)), sizeof(struct request_ftl_statistics));
		kunmap_atomic(page_addr, KM_USER1);
	}
}

static int reset_device_statistics(struct flashsim_device *device)
{
	int i;
	struct page *page;
	void *page_data;
	struct request_statistics_store *req_stat_store = &device->dev_stat.req_stat_store;

	device->dev_stat.read_cursor = 0;
	device->dev_stat.external_io_couter = 0;
	device->dev_stat.external_read_couter = 0;
	device->dev_stat.external_write_couter = 0;
	device->dev_stat.num_time_overflowed_requests[SECTOR_DATA_AREA] = 0;
	device->dev_stat.num_time_overflowed_requests[SECTOR_OOB_AREA] = 0;
	req_stat_store->req_stat_overflow = 0;

	//This hard version of clean up for statistical pages is actually not needed, since reading and writing statistics is coupled with io_sequence_number
	//=> after setting io_sequence_number = 0 the data will be sequentially rewritten and no old data could be accessed
	if (device->params.request_statistics_mode) {
		for (i = 0; i < req_stat_store->num_req_stat_pages; i++) {
			page = lookup_page(req_stat_store->req_stat_pages, i);
			if (!page)
				return -EFAULT;
			page_data = kmap_atomic(page, KM_USER1);
			memset(page_data, 0, PAGE_SIZE);
			kunmap_atomic(page_data, KM_USER1);
		}
	}

	if (device->params.ftl_mode)
		reset_dftl_statistics(device->dftl);

	return 0;
}

static int append_to_page_buffer(char *page_buffer, int *offset, const char *format, ...)
{
	va_list args;
	int len;

	if ((*offset) < PAGE_SIZE) {
		va_start(args, format);
		len = vsnprintf(page_buffer + (*offset), PAGE_SIZE - (*offset), format, args);
		va_end(args);
		if ((*offset) + len < PAGE_SIZE) {
			(*offset) += len;
			return 0;
		}
	}
	(*offset) = PAGE_SIZE;
	return -ENOMEM;
}

static int print_device_params(char *page_buffer, int *offset, struct flashsim_device *device)
{
	struct device_params *params = &device->params;

	append_to_page_buffer(page_buffer, offset, "*******************************************      DEVICE PARAMETERS     ******************************************\n");
	append_to_page_buffer(page_buffer, offset, "Type: %s\n", params->device_type == BLOCK_DEVICE ? "BLOCK" : "CHAR");
	append_to_page_buffer(page_buffer, offset, "Sector size: %u\n", params->sector_size);
	append_to_page_buffer(page_buffer, offset, "Sectors per block: %u\n", params->sectors_per_block);
	append_to_page_buffer(page_buffer, offset, "Blocks: %u\n", params->num_blocks);
	append_to_page_buffer(page_buffer, offset, "Sectors: %u\n", params->num_sectors);
	if (params->delay_mode) {
		append_to_page_buffer(page_buffer, offset, "READ latency (us): %u\n", params->read_sector_latency_us);
		append_to_page_buffer(page_buffer, offset, "WRITE latency (us): %u\n", params->program_sector_latency_us);
		append_to_page_buffer(page_buffer, offset, "ERASE latency (us): %u\n", params->erase_block_latency_us);
		append_to_page_buffer(page_buffer, offset, "READ OOB latency (us): %u\n", params->read_oob_latency_us);
		append_to_page_buffer(page_buffer, offset, "WRITE OOB latency (us): %u\n", params->program_oob_latency_us);
	}
	append_to_page_buffer(page_buffer, offset, "DELAY mode: %s\n", params->delay_mode ? "Simulate delay" : "No delay");
	append_to_page_buffer(page_buffer, offset, "STATISTICS mode: %s\n", params->request_statistics_mode ? "Statistics per each request" : "General device statistics");
	if (params->request_statistics_mode)
		append_to_page_buffer(page_buffer, offset, "Max stat entries: %d\n", device->dev_stat.req_stat_store.max_req_stat_entries);
	append_to_page_buffer(page_buffer, offset, "Memory for data (MB): %lu\n", params->num_pages * PAGE_SIZE / 1024 / 1024);
	append_to_page_buffer(page_buffer, offset, "Memory for metadata (MB): %lu\n", params->num_oob_pages * PAGE_SIZE / 1024 / 1024);
	if (params->request_statistics_mode)
		append_to_page_buffer(page_buffer, offset, "Memory for statistics (MB): %lu\n", device->dev_stat.req_stat_store.num_req_stat_pages * PAGE_SIZE / 1024 / 1024);

	append_to_page_buffer(page_buffer, offset, "FTL mode: %s\n", params->ftl_mode ? "DFTL" : "NoFTL");
	if (params->ftl_mode) {
		append_to_page_buffer(page_buffer, offset, "DFTL - EXTRA BLOCKS: %d\n", device->dftl->params.num_extra_blocks);
		append_to_page_buffer(page_buffer, offset, "DFTL - CACHE LEVEL 1 (max no. of entries): %d\n", device->dftl->params.cmt_real_max_entries);
		append_to_page_buffer(page_buffer, offset, "DFTL - CACHE LEVEL 2 (max no. of entries): %d\n", device->dftl->params.cmt_ghost_max_entries);
		append_to_page_buffer(page_buffer, offset, "DFTL - GMT entries per sector: %d\n", device->dftl->params.gmt_entries_per_page);
		append_to_page_buffer(page_buffer, offset, "DFTL - no. MAP SECTORS: %d\n", device->dftl->params.num_valid_map_sectors);
		append_to_page_buffer(page_buffer, offset, "DFTL - MIN no. of MAP BLOCKS: %d\n", device->dftl->params.min_num_map_blocks);
		append_to_page_buffer(page_buffer, offset, "DFTL - no. DATA SECTORS: %d\n", device->dftl->params.num_data_sectors);
		append_to_page_buffer(page_buffer, offset, "DFTL - GTD size (bytes): %d\n", device->dftl->params.gtd_size);
	}

	append_to_page_buffer(page_buffer, offset, "*******************************************  END of DEVICE PARAMETERS  ******************************************\n");

	//We hope that it will fit into buffer
	return append_to_page_buffer(page_buffer, offset, "\n");
}

static int print_part_of_ftl_general_stat(char *page_buffer, int *offset, int stat_array[2][2], const char * stat_name)
{
	append_to_page_buffer(page_buffer, offset, "\n");
	append_to_page_buffer(page_buffer, offset, "%s REQUESTs...\n", stat_name);
	append_to_page_buffer(page_buffer, offset, "%s[DATA_UNIT][NOT_GC]: %d\n", stat_name, stat_array[DATA_UNIT][NOT_GC]);
	append_to_page_buffer(page_buffer, offset, "%s[DATA_UNIT][GC]: %d\n", stat_name, stat_array[DATA_UNIT][GC]);
	append_to_page_buffer(page_buffer, offset, "%s[DATA_UNIT]: %d\n", stat_name, stat_array[DATA_UNIT][NOT_GC] + stat_array[DATA_UNIT][GC]);
	append_to_page_buffer(page_buffer, offset, "%s[MAP_UNIT][NOT_GC]: %d\n", stat_name, stat_array[MAP_UNIT][NOT_GC]);
	append_to_page_buffer(page_buffer, offset, "%s[MAP_UNIT][GC]: %d\n", stat_name, stat_array[MAP_UNIT][GC]);
	append_to_page_buffer(page_buffer, offset, "%s[MAP_UNIT]: %d\n", stat_name, stat_array[MAP_UNIT][NOT_GC] + stat_array[MAP_UNIT][GC]);
	append_to_page_buffer(page_buffer, offset, "%s[GC]: %d\n", stat_name, stat_array[MAP_UNIT][GC] + stat_array[DATA_UNIT][GC]);
	append_to_page_buffer(page_buffer, offset, "%s[NOT_GC]: %d\n", stat_name, stat_array[MAP_UNIT][NOT_GC] + stat_array[DATA_UNIT][NOT_GC]);

	return append_to_page_buffer(page_buffer, offset, "%s: %d\n", stat_name, stat_array[MAP_UNIT][NOT_GC] + stat_array[DATA_UNIT][NOT_GC] +
			stat_array[MAP_UNIT][GC] + stat_array[DATA_UNIT][GC]);
}

static int print_general_statistics(char *page_buffer, int *offset, struct flashsim_device *device)
{
	struct dftl_stat *dftl_stat;

	append_to_page_buffer(page_buffer, offset, "*******************************************     GENERAL STATISTICS     ******************************************\n");
	append_to_page_buffer(page_buffer, offset, "IOs performed: %lu\n", device->dev_stat.external_io_couter);
	append_to_page_buffer(page_buffer, offset, "READs performed: %lu\n", device->dev_stat.external_read_couter);
	append_to_page_buffer(page_buffer, offset, "WRITEs performed: %lu\n", device->dev_stat.external_write_couter);
	append_to_page_buffer(page_buffer, offset, "Overflowed data requests: %d\n", device->dev_stat.num_time_overflowed_requests[SECTOR_DATA_AREA]);
	append_to_page_buffer(page_buffer, offset, "Overflowed oob requests: %d\n", device->dev_stat.num_time_overflowed_requests[SECTOR_OOB_AREA]);
	append_to_page_buffer(page_buffer, offset, "\n");

	if (device->params.ftl_mode) {
		dftl_stat = &device->dftl->stat;

		append_to_page_buffer(page_buffer, offset, "FTL general statistics...\n");
		append_to_page_buffer(page_buffer, offset, "CACHE HIT: %d\n", dftl_stat->cache_hit);
		append_to_page_buffer(page_buffer, offset, "CACHE EVICT: %d\n", dftl_stat->cache_evict);
		append_to_page_buffer(page_buffer, offset, "FLASH HIT: %d\n", dftl_stat->flash_hit);
		append_to_page_buffer(page_buffer, offset, "LAZY UPDATE (no. of saved WRITEs): %d\n", dftl_stat->delay_flash_update);
		append_to_page_buffer(page_buffer, offset, "BATCH UPDATE (no. of saved WRITEs): %d\n", dftl_stat->save_map_updates_via_batch_update);
		append_to_page_buffer(page_buffer, offset, "BLIND READ: %d\n", dftl_stat->blind_read);

		print_part_of_ftl_general_stat(page_buffer, offset, dftl_stat->read, "READ SECTOR");
		print_part_of_ftl_general_stat(page_buffer, offset, dftl_stat->write, "WRITE SECTOR");
		print_part_of_ftl_general_stat(page_buffer, offset, dftl_stat->oob_read, "READ OOB");
		print_part_of_ftl_general_stat(page_buffer, offset, dftl_stat->oob_write, "WRITE OOB");
		print_part_of_ftl_general_stat(page_buffer, offset, dftl_stat->oob_read_without_delay, "READ OOB W/O DELAY");
		print_part_of_ftl_general_stat(page_buffer, offset, dftl_stat->oob_write_without_delay, "WRITE OOB W/O DELAY");
		append_to_page_buffer(page_buffer, offset, "Note: READ/WRITE[DATA_UNIT][NOT_GC] are external IO calls!\n");

		append_to_page_buffer(page_buffer, offset, "\n");
		append_to_page_buffer(page_buffer, offset, "ERASE REQUESTs...\n");
		append_to_page_buffer(page_buffer, offset, "ERASE[DATA_UNIT]: %d\n", dftl_stat->erase[DATA_UNIT]);
		append_to_page_buffer(page_buffer, offset, "ERASE[MAP_UNIT]: %d\n", dftl_stat->erase[MAP_UNIT]);
		append_to_page_buffer(page_buffer, offset, "ERASE: %d\n", dftl_stat->erase[DATA_UNIT] + dftl_stat->erase[MAP_UNIT]);
	}

	append_to_page_buffer(page_buffer, offset, "******************************************* END of GENERAL STATISTICS  ******************************************\n");

	return append_to_page_buffer(page_buffer, offset, "\n");
}

static int print_request_statistics_header(char *page_buffer, int *offset, struct flashsim_device *device)
{
	char * underline = "|____________|____|__________|______|____________|";
	char * underline2 = "_______|_______|_______|_______|_______|__________|____________|";

	append_to_page_buffer(page_buffer, offset, "*******************************************    REQUEST STATISTICS      ******************************************\n\n");

	if (device->dev_stat.req_stat_store.req_stat_overflow)
		append_to_page_buffer(page_buffer, offset, "                  !!! Statistics store was overflowed. Overflowed requests (%lu) were not stored !!!\n\n",
				device->dev_stat.external_io_couter - (unsigned long)device->dev_stat.req_stat_store.max_req_stat_entries);
	append_to_page_buffer(page_buffer, offset, "| %-10s | %-2s | %-8s | %-4s | %-10s |", "IO #", "RW", "LSN", "SIZE", "T.DURATION");

	if (device->params.ftl_mode)
		append_to_page_buffer(page_buffer, offset, " %-5s | %-5s | %-5s | %-5s | %-5s | %-8s | %-10s |", "READ", "WRITE", "OOB_R", "OOB_W", "ERASE", "PSN", "F.DURATION");

	append_to_page_buffer(page_buffer, offset, "\n");

	return append_to_page_buffer(page_buffer, offset, "%s%s\n", underline, device->params.ftl_mode ? underline2 : "");
}

static int print_single_request_statistics(char *page_buffer, int *offset, int request_idx, struct flashsim_device *device)
{
	struct request_info req_info;
	struct request_ftl_statistics ftl_stat;

	read_request_statistics(device, request_idx, &req_info, &ftl_stat);

	append_to_page_buffer(page_buffer, offset, "| %-10lu | %-2s | %-8d | %-4d | %-10d |",
			req_info.request_sequence_number,
			req_info.rw == WRITE ? "W" : (req_info.rw == ERASE_BLOCK ? "E" : "R"),
			req_info.start_lsn, req_info.num_sectors, req_info.duration);

	if (device->params.ftl_mode) {
		append_to_page_buffer(page_buffer, offset, " %-5u | %-5u | %-5u | %-5u | %-5u | %-8d | %-10d |",
				ftl_stat.sector_read_num, ftl_stat.sector_write_num, ftl_stat.sector_oob_read_num, ftl_stat.sector_oob_write_num,
				ftl_stat.block_erase_num, ftl_stat.psn, ftl_stat.ftl_duration);
	}
	return append_to_page_buffer(page_buffer, offset, "\n");
}

static ssize_t statistics_read(struct file *filp, char __user *buffer, size_t length, loff_t *offset)
{
	int retval;
	char * page_buffer;
	int buffer_offset = 0;
	int buffer_prev_offset = 0;
	struct flashsim_device *device = filp->private_data;
	int read_cursor_backup = device->dev_stat.read_cursor;
	int num_saved_requests = (int) min(device->dev_stat.external_io_couter, (unsigned long)device->dev_stat.req_stat_store.max_req_stat_entries);

	if (length != PAGE_SIZE) {
		print_error("Incorrect reading of statistics. PAGE_SIZE-d granularity!");
		return -EINVAL;
	}

	if (device->dev_stat.read_cursor &&
			(device->dev_stat.read_cursor == num_saved_requests || !num_saved_requests || !device->params.request_statistics_mode)) {
		device->dev_stat.read_cursor = 0;
		return 0; //EOF
	}

	page_buffer = vzalloc(PAGE_SIZE);
	if (!page_buffer) {
		print_error("Failed to allocate temporal buffer while printing statistics!");
		return -ENOMEM;
	}

	//Print device parameters and general statistics
	if (device->dev_stat.read_cursor == 0) {
		print_device_params(page_buffer, &buffer_offset, device);
		retval = print_general_statistics(page_buffer, &buffer_offset, device);
		if (retval) {
			print_error("Failed to print first statistics' page: buffer full -> splitting needed!");
			goto out;
		}
		if (!device->params.request_statistics_mode || !num_saved_requests)
			device->dev_stat.read_cursor = 1; //next read -> EOF.
		buffer_prev_offset = buffer_offset;
	}

	//request statistics
	if (device->params.request_statistics_mode) {
		//request statistics header
		if (device->dev_stat.read_cursor == 0)
			print_request_statistics_header(page_buffer, &buffer_offset, device);
		while (device->dev_stat.read_cursor < num_saved_requests) {
			retval = print_single_request_statistics(page_buffer, &buffer_offset, device->dev_stat.read_cursor, device);
			if (retval) {
				if (device->dev_stat.read_cursor == 0) {
					print_error("Failed to print first statistics' page: buffer full -> splitting needed!");
					goto out;
				}
				else
					break;
			}
			device->dev_stat.read_cursor++;
			buffer_prev_offset = buffer_offset;
		}
	}

	if (copy_to_user(buffer, page_buffer, buffer_prev_offset)) {
		retval = -EFAULT;
		device->dev_stat.read_cursor = read_cursor_backup;
	}
	else
		retval = buffer_prev_offset; //number of successfully written bytes

out:
	vfree(page_buffer);
	return retval;
}

static int statistics_open(struct inode *inode, struct file *filp)
{
	struct device_statistics *device_stat;
	struct flashsim_device *device;

	device_stat = container_of(inode->i_cdev, struct device_statistics, statistics_char_device);
	device = container_of(device_stat, struct flashsim_device, dev_stat);

	filp->private_data = device;

	return 0;
}

static long statistics_ioctl(struct file *filp, unsigned int cmd, unsigned long arg)
{
	struct flashsim_device *device = filp->private_data;

	switch(cmd) {
		case RESET_STAT:
			return reset_device_statistics(device);
	}

	return -ENOTTY;
}

static struct file_operations statistics_device_fops = {
	.owner 			=	THIS_MODULE,
	.open			=	statistics_open,
	.read 			=	statistics_read,
	.unlocked_ioctl	=	statistics_ioctl
};

static char * get_statistics_device_name(struct flashsim_device *device)
{
	char * device_name;

	if (device->params.device_type == BLOCK_DEVICE)
		device_name = BLOCK_DEVICE_NAME "_stat";
	else
		device_name = CHAR_DEVICE_NAME "_stat";

	return device_name;
}

static int __init register_statistics(struct flashsim_device *device)
{
	int retval;
	dev_t stat_char_device_id;

	retval = alloc_chrdev_region(&stat_char_device_id, 0, 1, get_statistics_device_name(device));

	if (retval) {
		print_error("Failed to register character device for statistics!");
		return retval;
	}

	device->dev_stat.statistics_char_device_major = MAJOR(stat_char_device_id);

	return 0;
}

int __init init_device_statistics(struct flashsim_device *device)
{
	int retval = 0;
	struct request_statistics_store *req_stat_store = &device->dev_stat.req_stat_store;

	print_msg("Registering statistics char device ...");

	retval = register_statistics(device);
	if (retval)
		return -EBUSY;

	cdev_init(&device->dev_stat.statistics_char_device, &statistics_device_fops);
	device->dev_stat.statistics_char_device.owner = THIS_MODULE;
	device->dev_stat.statistics_char_device.ops = &statistics_device_fops;
	retval = cdev_add(&device->dev_stat.statistics_char_device, MKDEV(device->dev_stat.statistics_char_device_major, 0), 1);
	if (retval) {
		print_error("Can not add statistics char device to the system!");
		return retval;
	}
	printf_msg("Statistics char device registered successfully with %d major number!", device->dev_stat.statistics_char_device_major);

	device->dev_stat.read_cursor = 0;
	device->dev_stat.external_io_couter = 0;
	device->dev_stat.external_read_couter = 0;
	device->dev_stat.external_write_couter = 0;
	device->dev_stat.num_time_overflowed_requests[SECTOR_DATA_AREA] = 0;
	device->dev_stat.num_time_overflowed_requests[SECTOR_OOB_AREA] = 0;

	if (device->params.request_statistics_mode) {
		req_stat_store->req_stat_overflow = 0;
		req_stat_store->req_stat_entry_size = sizeof(struct request_info) + (device->params.ftl_mode ? sizeof(struct request_ftl_statistics) : 0);
		req_stat_store->req_stat_entries_per_page = PAGE_SIZE / req_stat_store->req_stat_entry_size;
		req_stat_store->num_req_stat_pages = req_stat_store->max_req_stat_entries / req_stat_store->req_stat_entries_per_page;
		if (req_stat_store->max_req_stat_entries % req_stat_store->req_stat_entries_per_page)
			req_stat_store->num_req_stat_pages++;

		if (req_stat_store->num_req_stat_pages) {
			print_msg("Allocate memory for device statistics ...");
			req_stat_store->req_stat_pages = vzalloc(req_stat_store->num_req_stat_pages * sizeof(*req_stat_store->req_stat_pages));
			if (!req_stat_store->req_stat_pages) {
				print_error("Not enough memory for allocating array of page pointers for statistical store!");
				return -ENOMEM;
			}
			retval = flashsim_alloc_pages(req_stat_store->req_stat_pages, req_stat_store->num_req_stat_pages);
			if (retval)
				return retval; //freeing of memory should be called externally

			print_msg("Allocate memory for device statistics: successfully!");
		}
	}

	return retval;
}

void free_device_statistics(struct flashsim_device *device)
{
	struct request_statistics_store *req_stat_store = &device->dev_stat.req_stat_store;

	print_msg("Freeing device statistics ...");

	cdev_del(&device->dev_stat.statistics_char_device);
	unregister_chrdev_region(MKDEV (device->dev_stat.statistics_char_device_major, 0), 1);

	if (req_stat_store->req_stat_pages) {
		flashsim_free_pages(req_stat_store->req_stat_pages, req_stat_store->num_req_stat_pages);
		vfree(req_stat_store->req_stat_pages);
	}

	print_msg("Freeing device statistics: successfully!");
}
