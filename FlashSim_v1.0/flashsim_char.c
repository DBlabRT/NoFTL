#include "flashsim_common.h"

ssize_t char_device_read_internal (struct flashsim_char_device * char_device, void * buffer, size_t count, loff_t *f_pos)
{
	uint i, copied = 0;
	struct request_alignment request;
	struct page_info cur_page;
	t_time request_arrival;

	set_current_time(&request_arrival);
	//TODO: for now - for internal requests we are not going to use any kind of locking
	//because the locking is already done in "caller" function
	if (*f_pos >= char_device->dev_core.params.size)
		return 0; //EOF

	if (*f_pos + count >= char_device->dev_core.params.size)
		count = char_device->dev_core.params.size - *f_pos;

	calculate_request_alignment(&char_device->dev_core.params, &request, count, f_pos);

	for (i = request.start_page_idx; i <= request.end_page_idx; i++) {
		if(calculate_page_info(char_device->dev_core.data_pages, &request, &cur_page, i))
			return -EIO;

		memcpy(buffer + copied, (cur_page.page_address + cur_page.page_offset), cur_page.page_count);
		kunmap_atomic(cur_page.page_address, KM_USER1);
		copied += cur_page.page_count;
	}

	*f_pos += count;

	if (char_device->dev_core.params.delay_mode)
		simulate_request_latency(&char_device->dev_core, READ, request.sector_count, SECTOR_DATA_AREA, &request_arrival);

	return count;
}

static ssize_t char_device_read (struct file *filp, char __user *buf, size_t count, loff_t *f_pos)
{
	int retval;
	uint i, copied = 0;
	void * tmp_buffer;
	struct flashsim_char_device *char_device = filp->private_data;
	struct request_alignment request;
	struct page_info cur_page;
	t_time request_arrival;

	set_current_time(&request_arrival);
	//TODO: for concurrency use the rw semaphores
	if (down_interruptible (&char_device->sem))
		return -ERESTARTSYS;
	if (*f_pos >= char_device->dev_core.params.size) {
		retval = 0; //EOF
		goto out;
	}
	if (*f_pos + count >= char_device->dev_core.params.size)
		count = char_device->dev_core.params.size - *f_pos;

	calculate_request_alignment(&char_device->dev_core.params, &request, count, f_pos);

	tmp_buffer = vzalloc(count);
	if (!tmp_buffer) {
		retval = -ENOMEM;
		goto out;
	}

	for (i = request.start_page_idx; i <= request.end_page_idx; i++) {
		if(calculate_page_info(char_device->dev_core.data_pages, &request, &cur_page, i)) {
			retval = -EIO;
			goto out_free_tmp_buffer;
		}

		memcpy(tmp_buffer + copied, (cur_page.page_address + cur_page.page_offset), cur_page.page_count);
		kunmap_atomic(cur_page.page_address, KM_USER1);
		copied += cur_page.page_count;
	}
	if (copy_to_user(buf, tmp_buffer, count)) {
		retval = -EFAULT;
		goto out_free_tmp_buffer;
	}
	*f_pos += count;
	retval = count;

	//TODO statistics for CHAR device
	//if (char_device->params.request_statistics_mode)
		//write_request_statistics(&char_device->dev_core, &req_info, &request_arrival);
	if (char_device->dev_core.params.delay_mode)
		simulate_request_latency(&char_device->dev_core, READ, request.sector_count, SECTOR_DATA_AREA, &request_arrival);

out_free_tmp_buffer:
	vfree(tmp_buffer);
out:
	up (&char_device->sem);
	return retval;
}

ssize_t char_device_write_internal (struct flashsim_char_device * char_device, const void * buffer, size_t count, loff_t *f_pos)
{
	uint i, copied = 0;
	struct request_alignment request;
	struct page_info cur_page;
	t_time request_arrival;

	set_current_time(&request_arrival);

	//TODO: for now - for internal requests we are not going to use any kind of locking
	//because the locking is already done in "caller" function

	if (*f_pos >= char_device->dev_core.params.size || (*f_pos + count) >= char_device->dev_core.params.size)
		return -ENOMEM; //EOF

	calculate_request_alignment(&char_device->dev_core.params, &request, count, f_pos);

	for (i = request.start_page_idx; i <= request.end_page_idx; i++) {
		if(calculate_page_info(char_device->dev_core.data_pages, &request, &cur_page, i))
			return -EIO;

		memcpy((cur_page.page_address + cur_page.page_offset), (buffer + copied), cur_page.page_count);
		kunmap_atomic(cur_page.page_address, KM_USER1);
		copied += cur_page.page_count;
	}
	*f_pos += count;
	if (char_device->dev_core.params.delay_mode)
		simulate_request_latency(&char_device->dev_core, WRITE, request.sector_count, SECTOR_DATA_AREA, &request_arrival);

	return count;
}

static ssize_t char_device_write (struct file *filp, const char __user *buf, size_t count, loff_t *f_pos)
{
	int retval;
	uint i, copied = 0;
	void * tmp_buffer;
	struct flashsim_char_device *char_device = filp->private_data;
	struct request_alignment request;
	struct page_info cur_page;
	t_time request_arrival;

	set_current_time(&request_arrival);
	//TODO: for concurrency use the rw semaphores
	if (down_interruptible (&char_device->sem))
		return -ERESTARTSYS;
	if (*f_pos >= char_device->dev_core.params.size || (*f_pos + count) >= char_device->dev_core.params.size) {
		retval = -ENOMEM; //EOF
		goto out;
	}
	calculate_request_alignment(&char_device->dev_core.params, &request, count, f_pos);

	tmp_buffer = vzalloc(count);
	if (!tmp_buffer) {
		retval = -ENOMEM;
		goto out;
	}
	if (copy_from_user(tmp_buffer, buf, count)) {
		retval = -EFAULT;
		goto out_free_tmp_buffer;
	}
	for (i = request.start_page_idx; i <= request.end_page_idx; i++) {
		if(calculate_page_info(char_device->dev_core.data_pages, &request, &cur_page, i)) {
			retval = -EIO;
			goto out_free_tmp_buffer;
		}
		memcpy((cur_page.page_address + cur_page.page_offset), (tmp_buffer + copied), cur_page.page_count);
		kunmap_atomic(cur_page.page_address, KM_USER1);
		copied += cur_page.page_count;
	}
	*f_pos += count;
	retval = count;

	//TODO statistics for CHAR device
	//if (char_device->params.request_statistics_mode)
		//write_request_statistics(&char_device->dev_core, &req_info, &request_arrival);
	if (char_device->dev_core.params.delay_mode)
		simulate_request_latency(&char_device->dev_core, WRITE, request.sector_count, SECTOR_DATA_AREA, &request_arrival);

out_free_tmp_buffer:
	vfree(tmp_buffer);
out:
	up (&char_device->sem);
	return retval;
}

static long char_device_ioctl (struct file *filp, unsigned int cmd, unsigned long arg)
{
	struct flashsim_char_device *char_device = filp->private_data;

	switch(cmd) {
		case ERASE_BLOCK:
			return erase_block_ioctl(&char_device->dev_core, arg);
		case ACCESS_OOB:
			return access_oob_ioctl(CHAR_DEVICE, arg);
		case SET_DELAY_MODE:
			return set_delay_mode_ioctl(&char_device->dev_core, arg);
	}

	return -ENOTTY;
}

static loff_t char_device_llseek(struct file *filp, loff_t off, int whence)
{
	struct flashsim_char_device *char_device = filp->private_data;
	loff_t newpos;

	switch(whence) {
	  case 0: /* SEEK_SET */
		newpos = off;
		break;

	  case 1: /* SEEK_CUR */
		newpos = filp->f_pos + off;
		break;

	  case 2: /* SEEK_END */
		newpos = char_device->dev_core.params.size;
		break;

	  default: /* can't happen */
		return -EINVAL;
	}
	if (newpos < 0 || newpos >= char_device->dev_core.params.size)
		return -EINVAL;
	filp->f_pos = newpos;
	return newpos;
}

static int char_device_open (struct inode *inode, struct file *filp)
{
	struct flashsim_char_device *dev;
	dev = container_of(inode->i_cdev, struct flashsim_char_device, disk);

	filp->private_data = dev;

	return 0;
}

struct file_operations char_device_fops = {
	.owner 			=	THIS_MODULE,
	.open			=	char_device_open,
	.llseek			= 	char_device_llseek,
	.read 			=	char_device_read,
	.write 			=	char_device_write,
	.unlocked_ioctl	=	char_device_ioctl
};

static int __init register_char_device(struct flashsim_char_device *char_device)
{
	int retval;
	dev_t char_device_id = MKDEV(char_device->dev_core.params.major_number, 0);

	PRINT_MSG("Registering char device ...");
	if (char_device_id)
		retval = register_chrdev_region(char_device_id, 1, CHAR_DEVICE_NAME);
	else
		retval = alloc_chrdev_region(&char_device_id, 0, 1, CHAR_DEVICE_NAME);

	if (retval) {
		PRINT_MSG("ERROR: Unable to get major number for char device");
		return retval;
	}

	char_device->dev_core.params.major_number = MAJOR(char_device_id);

	printk(MSG_PREFIX "Char device registered successfully with %u major number!", char_device->dev_core.params.major_number);
	return 0;
}

static void unregister_char_device(struct flashsim_char_device *char_device)
{
	PRINT_MSG("Unregistering char device ...");
	unregister_chrdev_region(MKDEV (char_device->dev_core.params.major_number, 0), 1);
	PRINT_MSG("Unregistering char device: successfully!");
}

int __init init_char_device (struct flashsim_char_device *char_device)
{
	int retval;

	print_msg("Initializing char device ...");

	retval = register_char_device(char_device);
	if (retval)
		return retval;

	sema_init(&char_device->sem, 1);

	print_msg("Allocating main memory ...");
	char_device->dev_core.data_pages = vzalloc(char_device->dev_core.params.num_pages * sizeof(*char_device->dev_core.data_pages));
	if (!char_device->dev_core.data_pages) {
		retval = -ENOMEM;
		print_error("ERROR: Not enough memory for allocating array of page pointers!");
		goto out_free;
	}
	retval = flashsim_alloc_pages(char_device->dev_core.data_pages, char_device->dev_core.params.num_pages);
	if (retval)
		goto out_free;
	print_msg("Allocating main memory: successfully!");

	print_msg("Allocate memory for oob_store ...");
	if (char_device->dev_core.params.num_oob_pages) {
		char_device->dev_core.oob_pages = vzalloc(char_device->dev_core.params.num_oob_pages * sizeof(*char_device->dev_core.oob_pages));
		if (!char_device->dev_core.oob_pages) {
			retval = -ENOMEM;
			print_error("Not enough memory for allocating array of page pointers for OOB store!");
			goto out_free;
		}
		retval = flashsim_alloc_pages(char_device->dev_core.oob_pages, char_device->dev_core.params.num_oob_pages);
		if (retval)
			goto out_free;
	}
	print_msg("Allocating oob memory: successfully!");

	retval = init_device_statistics(&char_device->dev_core);
	if (retval)
		goto out_free;

	cdev_init(&char_device->disk, &char_device_fops);
	char_device->disk.owner = THIS_MODULE;
	char_device->disk.ops = &char_device_fops;
	retval = cdev_add(&char_device->disk, MKDEV(char_device->dev_core.params.major_number, 0), 1);

	if (retval) {
		print_error("Can not add char device to the system");
		goto out_free;
	}

	print_msg("Initializing char device: successfully!");
	return 0;

out_free:
	free_char_device(char_device);
	return retval;
}

void free_char_device(struct flashsim_char_device *char_device)
{
	print_msg("Freeing char device ...");

	cdev_del(&char_device->disk);
	unregister_char_device(char_device);

	free_device_statistics(&char_device->dev_core);

	flashsim_free_pages(char_device->dev_core.oob_pages, char_device->dev_core.params.num_oob_pages);
	vfree(char_device->dev_core.oob_pages);

	flashsim_free_pages(char_device->dev_core.data_pages, char_device->dev_core.params.num_pages);
	vfree(char_device->dev_core.data_pages);

	print_msg("Freeing char device: successfully!");
}
