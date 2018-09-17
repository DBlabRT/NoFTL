#include <linux/hdreg.h>

#include "flashsim_common.h"

#define KERNEL_SECTOR_SHIFT					9
#define KERNEL_SECTOR_SIZE					(1 << KERNEL_SECTOR_SHIFT)
#define PAGE_KERNEL_SECTORS_SHIFT			(PAGE_SHIFT - KERNEL_SECTOR_SHIFT)
#define PAGE_KERNEL_SECTORS					(1 << PAGE_KERNEL_SECTORS_SHIFT)
#define PAGE_IDX(kernel_sector) 			(kernel_sector >> PAGE_KERNEL_SECTORS_SHIFT)
#define PAGE_BYTE_OFFSET(kernel_sector)		((kernel_sector & (PAGE_KERNEL_SECTORS-1)) << KERNEL_SECTOR_SHIFT)

static void copy_to_flashsim(struct flashsim_block_device *block_dev, const void *src, sector_t kernel_sector, uint n)
{
	struct page *page;
	void *dst;
	uint offset = PAGE_BYTE_OFFSET(kernel_sector);
	size_t copy;

	copy = min_t(size_t, n, PAGE_SIZE - offset);
	page = lookup_page(block_dev->dev_core.data_pages, PAGE_IDX(kernel_sector));
	if (!page)
		return;

	dst = kmap_atomic(page, KM_USER1);
	memcpy(dst + offset, src, copy);
	kunmap_atomic(dst, KM_USER1);

	if (copy < n) {
		src += copy;
		kernel_sector += copy >> KERNEL_SECTOR_SHIFT;
		copy = n - copy;
		page = lookup_page(block_dev->dev_core.data_pages, PAGE_IDX(kernel_sector));
		if (!page)
			return;
		dst = kmap_atomic(page, KM_USER1);
		memcpy(dst, src, copy);
		kunmap_atomic(dst, KM_USER1);
	}
}

void copy_to_flashsim_internal(struct flashsim_block_device *block_dev, const void *src, uint psn)
{
	sector_t kernel_sector;
	t_time request_arrival;

	set_current_time(&request_arrival);

	//TODO: different page sizes. make_request could handle different sector_sizes (and block device too)
	//but OS then transforms big requests into different segments - each segment is equal to or less then a PAGE_SIZE
	if (block_dev->dev_core.params.sector_size != PAGE_SIZE) {
		print_error("UNIMPLEMENTED = Invalid sector size of block device!");
		BUG_ON(1);
	}

	kernel_sector = psn * block_dev->kernel_sectors_per_flashsim_sector;
	copy_to_flashsim(block_dev, src, kernel_sector, block_dev->dev_core.params.sector_size);

	if (block_dev->dev_core.params.delay_mode)
		simulate_request_latency(&block_dev->dev_core, WRITE, 1, SECTOR_DATA_AREA, &request_arrival);
}

static void copy_from_flashsim(struct flashsim_block_device *block_dev, void *dst, sector_t kernel_sector, uint n)
{
	struct page *page;
	void *src;
	uint offset = PAGE_BYTE_OFFSET(kernel_sector);
	size_t copy;

	copy = min_t(size_t, n, PAGE_SIZE - offset);
	page = lookup_page(block_dev->dev_core.data_pages, PAGE_IDX(kernel_sector));
	if (!page)
		return;

	src = kmap_atomic(page, KM_USER1);
	memcpy(dst, src + offset, copy);
	kunmap_atomic(src, KM_USER1);

	if (copy < n) {
		dst += copy;
		kernel_sector += copy >> KERNEL_SECTOR_SHIFT;
		copy = n - copy;
		page = lookup_page(block_dev->dev_core.data_pages, PAGE_IDX(kernel_sector));
		if (!page)
			return;
		src = kmap_atomic(page, KM_USER1);
		memcpy(dst, src, copy);
		kunmap_atomic(src, KM_USER1);
	}
}

void copy_from_flashsim_internal (struct flashsim_block_device *block_dev, void *dst, uint psn)
{
	sector_t kernel_sector;
	t_time request_arrival;

	set_current_time(&request_arrival);

	//TODO: different page sizes. make_request could handle different sector_sizes (and block device too)
	//but OS then transforms big requests into different segments - each segment is equal to or less then a PAGE_SIZE
	if (block_dev->dev_core.params.sector_size != PAGE_SIZE) {
		print_error("UNIMPLEMENTED = Invalid sector size of block device!");
		BUG_ON(1);
	}

	kernel_sector = psn * block_dev->kernel_sectors_per_flashsim_sector;
	copy_from_flashsim(block_dev, dst, kernel_sector, block_dev->dev_core.params.sector_size);

	if (block_dev->dev_core.params.delay_mode)
		simulate_request_latency(&block_dev->dev_core, READ, 1, SECTOR_DATA_AREA, &request_arrival);
}

static int do_bvec(struct flashsim_block_device *block_dev, struct page *page, uint len, uint off, int rw, sector_t kernel_sector)
{
	void *mem;

	mem = kmap_atomic(page, KM_USER0);
	if (rw == READ || rw == READA) {
		copy_from_flashsim(block_dev, mem + off, kernel_sector, len);
		flush_dcache_page(page);
	} else {
		flush_dcache_page(page);
		copy_to_flashsim(block_dev, mem + off, kernel_sector, len);
	}
	kunmap_atomic(mem, KM_USER0);
	return 0;
}

static int get_physical_kernel_sector(struct flashsim_block_device *block_dev, int lsn, int rw)
{
	int psn;

	if (block_dev->dev_core.params.ftl_mode == DFTL_ORIGINAL) {
		//printf_msg("FLASHSIM_TRACE: %lu %d %d\n", block_dev->io_sequence_number++, logical_kernel_sector, rw);

		psn = lsn_to_psn(block_dev->dev_core.dftl, lsn, rw);

		if (psn >= 0)
			return (psn * block_dev->kernel_sectors_per_flashsim_sector);
		else
			return psn; //error_code from FTL
	}

	return -1;
}

static void make_request(struct request_queue *queue, struct bio *bio)
{
	int i;
	int cpu;
	int err = 0;
	sector_t sector;
	t_time request_arrival;
	t_time request_execution_start;
	struct bio_vec *bvec;
	struct request_info req_info;
	struct block_device *bdev = bio->bi_bdev;
	struct flashsim_block_device *block_dev = bdev->bd_disk->private_data;
	unsigned long request_arrival_jiffies;
	unsigned long total_duration_jiffies;

	set_current_time(&request_arrival);
	request_arrival_jiffies = jiffies;
	spin_lock(&block_dev->lock); //for now device is a single chip => no concurrency

	req_info.request_sequence_number = block_dev->dev_core.dev_stat.external_io_couter++;
	req_info.rw = bio_rw(bio);
	if (req_info.rw == WRITE)
		block_dev->dev_core.dev_stat.external_write_couter++;
	else
		block_dev->dev_core.dev_stat.external_read_couter++;
	req_info.num_sectors = bio->bi_size / block_dev->dev_core.params.sector_size;
	req_info.start_lsn = bio->bi_sector / block_dev->kernel_sectors_per_flashsim_sector;

	if (bio->bi_sector + (bio->bi_size >> KERNEL_SECTOR_SHIFT) > get_capacity(bdev->bd_disk)) {
		err = -EFAULT;
		print_error("I/O out of disk boundary!");
		goto out;
	}

	//FTL mapping
	if (block_dev->dev_core.params.ftl_mode) {
		if (req_info.num_sectors != 1) {
			err = -EINVAL;
			print_error("While using FTL only requests of 1 logical sector are allowed!");
			goto out;
		}
		sector = get_physical_kernel_sector(block_dev, req_info.start_lsn, req_info.rw);
		if (sector < 0) {
			err = sector; //Error code
			print_error("FTL transformation failed!");
			goto out;
		}
	}
	else
		sector = bio->bi_sector;

	if (block_dev->dev_core.params.ftl_mode)
		set_current_time(&request_execution_start);

	//update diskstat
	cpu = part_stat_lock();
	part_stat_inc(cpu, bdev->bd_part, ios[req_info.rw]);
	part_stat_add(cpu, bdev->bd_part, sectors[req_info.rw], bio_sectors(bio));
	part_stat_unlock();

	bio_for_each_segment(bvec, bio, i) {
		uint len = bvec->bv_len;
		err = do_bvec(block_dev, bvec->bv_page, len, bvec->bv_offset, req_info.rw, sector);
		if (err) {
			err = -EIO;
			print_error("Internal error during IO request!");
			break;
		}
		sector += len >> KERNEL_SECTOR_SHIFT;
	}

	if (!err) {
		if (block_dev->dev_core.params.request_statistics_mode)
			write_request_statistics(&block_dev->dev_core, &req_info, &request_arrival);
		if (block_dev->dev_core.params.delay_mode)
			simulate_request_latency(&block_dev->dev_core, req_info.rw, req_info.num_sectors, SECTOR_DATA_AREA,
					(block_dev->dev_core.params.ftl_mode ? &request_execution_start : &request_arrival));
	}

out:
	bio_endio(bio, err);

	//update diskstat
	cpu = part_stat_lock();
	total_duration_jiffies = jiffies - request_arrival_jiffies;
	part_stat_add(cpu, bdev->bd_part, ticks[req_info.rw], total_duration_jiffies);
	part_stat_add(cpu, bdev->bd_part, io_ticks, total_duration_jiffies);
	//part_round_stats(cpu, bdev->bd_part);
	part_inc_in_flight(bdev->bd_part, req_info.rw);
	part_stat_unlock();

	spin_unlock(&block_dev->lock);
}

static int ioctl_get_geo (struct flashsim_block_device *block_dev, unsigned long arg)
{
	/*
	 * TODO: check this settings
	 * Get geometry: since we are a virtual device, we have to make
	 * up something plausible.  So we claim 16 sectors, four heads,
	 * and calculate the corresponding number of cylinders.  We set the
	 * start of data at sector four.
	 */

	struct hd_geometry geo;
	geo.cylinders = (block_dev->dev_core.params.size & ~0x3f) >> 6;
	geo.heads = 4;
	geo.sectors = 16;
	geo.start = 4;
	if (copy_to_user((void __user *) arg, &geo, sizeof(geo)))
		return -EFAULT;
	return 0;
}

static int block_device_ioctl (struct block_device *bdev, fmode_t mode, unsigned int cmd, unsigned long arg)
{
	struct flashsim_block_device *block_dev = bdev->bd_disk->private_data;

	switch(cmd) {
		case HDIO_GETGEO:
			return ioctl_get_geo(block_dev, arg);
		case ERASE_BLOCK:
			return erase_block_ioctl(&block_dev->dev_core, arg);
		case ACCESS_OOB:
			return access_oob_ioctl(BLOCK_DEVICE, arg);
		case SET_DELAY_MODE:
			return set_delay_mode_ioctl(&block_dev->dev_core, arg);
	}

	//TODO HDIO_GETGEO, BLKGETSIZE, BLKGETSIZE64, BLKFLSBUF, BLKPBSZGET, BLKBSZGET, BLKSSZGET, "own commands (get stat, erase, ...)"
	return -ENOTTY;
}

static const struct block_device_operations block_device_fops = {
	.owner =		THIS_MODULE,
	.ioctl = 		block_device_ioctl
};

static int __init register_block_device(struct flashsim_block_device *block_device)
{
	uint result;
	print_msg("Registering block device ...");
	result = register_blkdev(block_device->dev_core.params.major_number, BLOCK_DEVICE_NAME);
	if (result < 0) {
		print_error("Unable to get major number for block device");
		return -EBUSY;
	}
	//if we have requested any free major number, then store the returned value = found number
	else if (block_device->dev_core.params.major_number == 0)
		block_device->dev_core.params.major_number = result;

	printf_msg("Block device registered successfully with %u major number!", block_device->dev_core.params.major_number);
	return 0;
}

static void unregister_block_device(struct flashsim_block_device *block_device)
{
	print_msg("Unregistering block device ...");
	unregister_blkdev(block_device->dev_core.params.major_number, BLOCK_DEVICE_NAME);
	print_msg("Unregistering block device: successfully!");
}

int __init init_block_device(struct flashsim_block_device *block_device)
{
	int retval;
	int num_data_sectors;
	sector_t disk_capacity;

	print_msg("Initialization of block device ...");

	retval = register_block_device(block_device);
	if (retval)
		return retval;

	spin_lock_init(&block_device->lock);

	block_device->kernel_sectors_per_flashsim_sector = block_device->dev_core.params.sector_size / KERNEL_SECTOR_SIZE;

	//----------------------QUEUE SETUP------------------------------
	print_msg("Initialization of block device queue ...");
	block_device->queue = blk_alloc_queue(GFP_KERNEL);
	if (!block_device->queue) {
		retval = -ENOMEM;
		print_error("Not enough memory for allocating block device queue!");
		goto out_free;
	}
	blk_queue_make_request(block_device->queue, make_request);
	//TODO Queue parameters
	blk_queue_logical_block_size(block_device->queue, block_device->dev_core.params.sector_size);
	blk_queue_max_hw_sectors(block_device->queue, 8);
	/*blk_queue_bounce_limit(block_device->queue, BLK_BOUNCE_ANY);
	block_device->queue->limits.discard_granularity = PAGE_SIZE;
	block_device->queue->limits.max_discard_sectors = UINT_MAX;
	block_device->queue->limits.discard_zeroes_data = 1;
	queue_flag_set_unlocked(QUEUE_FLAG_DISCARD, block_device->queue);*/
	print_msg("Initialization of block device queue: successfully!");
	//------------------END OF QUEUE SETUP----------------------------

	print_msg("Allocating main memory ...");
	block_device->dev_core.data_pages = vzalloc(block_device->dev_core.params.num_pages * sizeof(*block_device->dev_core.data_pages));
	if (!block_device->dev_core.data_pages) {
		retval = -ENOMEM;
		print_error("Not enough memory for allocating array of page pointers!");
		goto out_free;
	}
	retval = flashsim_alloc_pages(block_device->dev_core.data_pages, block_device->dev_core.params.num_pages);
	if (retval)
		goto out_free;
	print_msg("Allocating main memory: successfully!");

	print_msg("Allocate memory for oob_store ...");
	if (block_device->dev_core.params.num_oob_pages) {
		block_device->dev_core.oob_pages = vzalloc(block_device->dev_core.params.num_oob_pages * sizeof(*block_device->dev_core.oob_pages));
		if (!block_device->dev_core.oob_pages) {
			retval = -ENOMEM;
			print_error("Not enough memory for allocating array of page pointers for OOB store!");
			goto out_free;
		}
		retval = flashsim_alloc_pages(block_device->dev_core.oob_pages, block_device->dev_core.params.num_oob_pages);
		if (retval)
			goto out_free;
	}
	print_msg("Allocating oob memory: successfully!");

	retval = init_device_statistics(&block_device->dev_core);
	if (retval)
		goto out_free;

	//-----------------------FTL-------------------------------------
	print_msg("Initializing FTL scheme ...");
	if (block_device->dev_core.params.ftl_mode == DFTL_ORIGINAL) {
		block_device->dev_core.dftl = init_dftl(block_device, BLOCK_DEVICE, &num_data_sectors);
		if (!block_device->dev_core.dftl) {
			retval = -1;
			print_error("Initialization of DFTL failed!");
			goto out_free;
		}
	}
	print_msg("Initializing FTL scheme: successfully!");

	//----------------------gendisk-----------------------------------
	print_msg("Initialization of gendisk ...");
	block_device->disk = alloc_disk(BLOCK_DEVICE_MINORS);
	if (!block_device->disk) {
		retval = -ENOMEM;
		print_error("Not enough memory for allocating gendisk!");
		goto out_free;
	}
	block_device->disk->major = block_device->dev_core.params.major_number;
	block_device->disk->first_minor = 0;
	block_device->disk->fops = &block_device_fops;
	block_device->disk->private_data = block_device;
	block_device->disk->queue = block_device->queue;
	snprintf (block_device->disk->disk_name, 32, BLOCK_DEVICE_NAME);

	//size of the disk in terms of KERNEL_SECTORS
	if (block_device->dev_core.params.ftl_mode == DFTL_ORIGINAL)
		disk_capacity = ((sector_t)num_data_sectors * (sector_t)block_device->dev_core.params.sector_size)/KERNEL_SECTOR_SIZE;
	else
		disk_capacity = block_device->dev_core.params.size/KERNEL_SECTOR_SIZE;
	set_capacity(block_device->disk, disk_capacity);
	add_disk(block_device->disk);
	print_msg("Initialization of gendisk: successfully!");
	//------------------end of gendisk---------------------------------

	print_msg("Initialization of block device: successfully!");

	return 0;

out_free:
	free_block_device(block_device);
	return retval;
}

void free_block_device(struct flashsim_block_device *block_device)
{
	print_msg("Freeing block device ...");

	if (block_device->disk) {
		del_gendisk(block_device->disk);
		put_disk(block_device->disk);
	}

	if (block_device->queue)
		blk_cleanup_queue(block_device->queue);

	unregister_block_device(block_device);

	free_device_statistics(&block_device->dev_core);

	free_dftl(block_device->dev_core.dftl);

	flashsim_free_pages(block_device->dev_core.oob_pages, block_device->dev_core.params.num_oob_pages);
	vfree(block_device->dev_core.oob_pages);

	flashsim_free_pages(block_device->dev_core.data_pages, block_device->dev_core.params.num_pages);
	vfree(block_device->dev_core.data_pages);

	print_msg("Freeing block device: successfully!");
}
