#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <sys/ioctl.h>

int convert_fio_trace_file(FILE *fio_trace_file, FILE *new_trace_file)
{
	int size_in_sectors;
	int size_in_bytes;
	unsigned long long offset;
	unsigned long long trace_io_counter = 0;
	int rw;
	int lsn;
	char line [256];
	char device_name [128];
	char operation_name [128];
	int page_size = getpagesize();

	while (fgets (line, sizeof(line), fio_trace_file) != NULL) {
		//fputs (line, stdout);

		sscanf(line, "%s %s", device_name, operation_name);
		if (!strcmp(device_name, "fio") || !strcmp(operation_name, "add")
				|| !strcmp(operation_name, "open") || !strcmp(operation_name, "close"))
			continue;

		sscanf(line, "%s %s %llu %d", device_name, operation_name, &offset, &size_in_bytes);

		if ((offset % page_size) != 0 || (size_in_bytes % page_size) != 0)
        	return -1;

        size_in_sectors = size_in_bytes / page_size;
        lsn = offset / page_size;
        rw = strcmp(operation_name, "write") == 0 ? 0 /*WRITE*/: 1 /*READ*/;

        if (lsn == 0) //leave the first sector unwritten (otherwise shoreMT will treat any data as header and result in error)
        	lsn = 1;

        do {
        	fprintf(new_trace_file, "%llu %d %d %d\n", trace_io_counter++, lsn, rw, 1);
			size_in_sectors--;
			lsn++;
        } while (size_in_sectors > 0);
	}
	return 0;
}

main(int argc, char * argv[])
{
	FILE *fio_trace_file;
	FILE *new_trace_file;
	char fio_trace_file_path[250];
	char new_trace_file_path[250];

	printf("Enter FIO trace file path: ");
	scanf("%s", fio_trace_file_path);

	printf("Enter path for new trace file: ");
	scanf("%s", new_trace_file_path);

	fio_trace_file = fopen (fio_trace_file_path, "r");
	if (fio_trace_file == NULL) {
		printf("Failed to open FIO trace file!\n");
		return -ENOENT;
	}

	new_trace_file = fopen (new_trace_file_path, "w");
	if (new_trace_file == NULL) {
		printf("Failed to create new trace file!\n");
		fclose(fio_trace_file);
		return -ENOENT;
	}

	if (convert_fio_trace_file(fio_trace_file, new_trace_file))
		printf("Failed to convert FIO trace file!\n");
	else
		printf("Trace file was converted successfully!\n");

	fclose (fio_trace_file);
	fclose(new_trace_file);

	return 0;
}
