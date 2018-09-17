#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <sys/ioctl.h>

//These three constants must be identical with ones in flashsim_common.h
#define RESET_STAT							0x6b03
#define BLOCK_DEVICE_STAT_MODULE			"flashsim_block_stat"
#define CHAR_DEVICE_STAT_MODULE				"flashsim_char_stat"

void save_statistics(int device_fd, const char * save_to)
{
	int statistics_fd = open(save_to, O_WRONLY | O_CREAT | O_TRUNC/*, S_IROTH | S_IWOTH*/);
	if (statistics_fd == -1) {
		printf("Failed to create statistics file! %s\n", strerror(errno));
		return;
	}

	int page_size = getpagesize();
	void * buffer;
	posix_memalign(&buffer, page_size, page_size);
	if (buffer == NULL) {
		printf("Failed to allocate memory for temporary buffer!\n");
		close(statistics_fd);
		return;
	}

	int read_result;
	while(1) {
		memset(buffer, 0, page_size);
		read_result = read(device_fd, buffer, page_size);
		if (read_result == 0) {
			printf("Statistics file %s were written successfully!\n", save_to);
			break;
		}
		else if (read_result < 0) {
			printf("Failed to read statistics device! %s\n", strerror(errno));
			break;
		}
		int write_result = write(statistics_fd, buffer, read_result);
		if (write_result != read_result) {
			printf("Failed to write to statistics file! %s\n", strerror(errno));
			break;
		}
	}
	free(buffer);
	close(statistics_fd);
}

void reset_statistics(int device_fd)
{
	if (ioctl(device_fd, RESET_STAT) == -1)
		printf("Failed to reset statistics! %s\n", strerror(errno));
	else
		printf("Statistics was reset!\n");
}

main(int argc, char * argv[])
{
	int device;
	int action;
	char device_path[250];
	char save_to[250];
	int device_fd;

	while (1) {
		device = 0;
		action = 0;
		save_to[0] = '\0';
		device_path[0] = '\0';
		device_fd = -1;

		printf("Select statistics: \n"
			   "		1 = BLOCK DEVICE STATISTICS\n"
			   "		2 = CHAR DEVICE STATISTICS\n"
			   "		3 = EXIT \n");
		scanf("%d", &device);
		if (device == 1) {
			strcpy(device_path, "/dev/" BLOCK_DEVICE_STAT_MODULE);
			strcpy(save_to, "/tmp/" BLOCK_DEVICE_STAT_MODULE);
		}
		else if (device == 2) {
			strcpy(device_path, "/dev/" CHAR_DEVICE_STAT_MODULE);
			strcpy(save_to, "/tmp/" CHAR_DEVICE_STAT_MODULE);
		}
		else
			return;

		device_fd = open(device_path, O_RDONLY, 0);
		if (device_fd == -1) {
			printf("Failed to open a device file! %s\n", strerror(errno));
			continue;
		}

		printf("Select action: \n"
			   "		1 = SAVE TO \"%s\" \n"
			   "		2 = SAVE AS \n"
			   "		3 = RESET \n"
			   "		4 = EXIT \n", save_to);
		scanf("%d", &action);

		switch (action) {
			case 1:
				save_statistics(device_fd, save_to);
				break;
			case 2:
				printf("Save statistics as:\n");
				scanf("%s", save_to);
				save_statistics(device_fd, save_to);
				break;
			case 3:
				reset_statistics(device_fd);
				break;
			default:
				break;
		}
		close(device_fd);
	}
}
