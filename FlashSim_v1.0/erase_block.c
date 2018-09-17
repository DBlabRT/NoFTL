#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <sys/ioctl.h>

/*
 * This code is defined in flashsim_common.h
 * If you like to use another code, be sure to use new value here
 */
#define ERASE_BLOCK	0x6b01

int erase_block(int device_fd, int blk_num)
{
	if (ioctl(device_fd, ERASE_BLOCK, &blk_num) == -1) {
	  printf("ERROR: erase_block command failed: %s\n", strerror(errno));
	  return -1;
	}

	return 0;
}

main(int argc, char * argv[])
{
	if (argc != 3) {
		printf ("Invalid arguments! \n "
				"1st argument = disk_file_name (e.g., /dev/flashsim_block or /dev/flashsim_char) \n"
				"2nd argument = index of block that should be erased");
		exit(0);
	}

	char * device_name = argv[1];
	int blk_num = atoi(argv[2]);

	int device_fd = open(device_name, 0);
	if (device_fd == -1) {
		printf("ERROR during openning a device file!");
		exit(-1);
	}
	
	erase_block(device_fd, blk_num);
	close(device_fd);
}
