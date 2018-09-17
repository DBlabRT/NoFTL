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
#define SET_DELAY_MODE	0x6b04

#define BLOCK_DEVICE_PATH			"/dev/flashsim_block"
#define CHAR_DEVICE_PATH			"/dev/flashsim_char"

int set_delay_mode(int device_fd, int delay_mode)
{
	if (ioctl(device_fd, SET_DELAY_MODE, &delay_mode) == -1) {
	  printf("SET_DELAY_MODE command failed: %s\n", strerror(errno));
	  return -1;
	}

	 printf("Delays were turned %s successfully!\n", delay_mode ? "on" : "off");

	return 0;
}

main(int argc, char * argv[])
{
	int device;
	int action;
	int device_fd;

	printf("Select device: \n"
		   "		1 = BLOCK DEVICE\n"
		   "		2 = CHAR DEVICE\n"
		   "		3 = EXIT \n");
	scanf("%d", &device);
	if (device != 1 && device != 2)
		return;

	device_fd = open(device == 1 ? BLOCK_DEVICE_PATH : CHAR_DEVICE_PATH, O_RDONLY, 0);
	if (device_fd == -1) {
		printf("Failed to open a device file! %s\n", strerror(errno));
		return;
	}
	
	printf("Select action: \n"
				   "		1 = TURN DELAYS ON \n"
				   "		2 = TURN DELAYS OFF \n"
				   "		3 = EXIT \n");
	scanf("%d", &action);

	if (action == 1)
		return set_delay_mode(device_fd, 1);
	else if (action == 2)
		return set_delay_mode(device_fd, 0);
	else
		return;
}
