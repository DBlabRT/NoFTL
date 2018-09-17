#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <sys/ioctl.h>

int populate_device(int device_fd, int num_sectors)
{
	int page_size = getpagesize();
	void * buffer;
	int io_result;
	int lsn;

	posix_memalign(&buffer, page_size, page_size);
	if (buffer == NULL) {
		printf("Failed to allocate memory for temporary buffer!\n");
		return -ENOMEM;
	}

	lsn = 0;
	while (num_sectors) {
    		io_result = pwrite(device_fd, buffer, page_size, (long)lsn*(long)page_size);
		if (io_result != page_size) {
			printf("Request #%d failed! %s\n", lsn, strerror(errno));
			return -1;
		}		
		lsn++;
		num_sectors--;
	}
	
	return 0;
}

main(int argc, char * argv[])
{
	int device_fd;
	int num_sectors;
	char device_path[250];
	char trace_file_path[250];

	printf("Enter device path: ");
	scanf("%s", device_path);

	printf("Enter num of written sectors: ");
	scanf("%d", &num_sectors);

	device_fd = open(device_path, O_RDWR|O_SYNC|O_DIRECT, 0600);
	if (device_fd == -1) {
		printf("Failed to open device file! %s\n", strerror(errno));
		return;
	}

	if (populate_device(device_fd, num_sectors))
		printf("Warm-up failed!\n");
	else
		printf("Warm-up finished successfully!\n");
	close(device_fd);

	return 0;
}
