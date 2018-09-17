#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <sys/ioctl.h>

int play_trace_file(FILE *trace_file, int device_fd)
{
	int size;
	int lsn;
	int rw;
	unsigned long trace_io_counter = 0;
	unsigned long req_seq_number;
	char line [256];
	int page_size = getpagesize();
	void * buffer;
	int io_result;

	posix_memalign(&buffer, page_size, page_size);
	if (buffer == NULL) {
		printf("Failed to allocate memory for temporary buffer!\n");
		return -ENOMEM;
	}

	while (fgets (line, sizeof(line), trace_file) != NULL) {
		//fputs (line, stdout);

        sscanf(line, "%lu %d %d %d", &req_seq_number, &lsn, &rw, &size);
        if (req_seq_number != trace_io_counter++) {
        	printf("ERROR: Trace inconsistency!");
        	return -1;
        }

	//printf("#=%lu, lsn=%d, rw=%d, size=%d\n", req_seq_number, lsn, rw, size);

        do {
            if (rw == 0) //WRITE
            	io_result = pwrite(device_fd, buffer, page_size, (long)lsn*(long)page_size);
            else
            	io_result = pread(device_fd, buffer, page_size, (long)lsn*(long)page_size);

			if (io_result != page_size) {
				printf("Request #%lu failed! %s\n", req_seq_number, strerror(errno));
				return -1;
			}

			size--;
			lsn++;
        } while (size > 0);
	}
	return 0;
}

main(int argc, char * argv[])
{
	int device_fd;
	FILE *trace_file;
	char device_path[250];
	char trace_file_path[250];

	printf("Enter device path: ");
	scanf("%s", device_path);

	printf("Enter trace file path: ");
	scanf("%s", trace_file_path);

	device_fd = open(device_path, O_RDWR|O_SYNC|O_DIRECT, 0600);
	if (device_fd == -1) {
		printf("Failed to open device file! %s\n", strerror(errno));
		return;
	}

	trace_file = fopen (trace_file_path, "r");
	if (trace_file == NULL) {
		printf("Failed to open trace file!\n");
		close(device_fd);
		return -ENOENT;
	}

	if (play_trace_file(trace_file, device_fd))
		printf("Failed on replaying the warm-up trace!\n");
	else
		printf("Trace file was proceeded successfully!\n");

	fclose (trace_file);
	close(device_fd);

	return 0;
}
