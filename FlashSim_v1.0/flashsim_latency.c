#include "flashsim_common.h"

void set_current_time(t_time *time)
{
	getnstimeofday(time);
}

int get_duration_in_us(t_time *end, t_time *start)
{
	t_time time_difference;
	int duration;

	time_difference = timespec_sub(*end, *start);
	duration = timespec_to_ns(&time_difference) / 1000;
	duration += 1; //in order not to deal with % or do_div, we just add to result 1 microsecond -> in such a way we compensate nanoseconds lost by "/"

	return duration;
}

int get_duration_in_us_till_now(t_time *start)
{
	int duration;
	t_time request_execution_end;

	set_current_time(&request_execution_end);
	duration = get_duration_in_us(&request_execution_end, start);

	return duration;
}

static int calculate_desired_request_duration(struct flashsim_device *device, int rw, int sector_count, int data_type)
{
	int desired_request_duration = 0;

	if (data_type == SECTOR_DATA_AREA) {
		switch (rw)
		{
			case WRITE:
				desired_request_duration = device->params.program_sector_latency_us * sector_count;
				break;
			case READ:
			case READA:
				desired_request_duration = device->params.read_sector_latency_us * sector_count;
				break;
			case ERASE_BLOCK:
				desired_request_duration = device->params.erase_block_latency_us;
				break;
			default:
				desired_request_duration = 0;
				break;
		}
	}
	else {
		switch (rw)
		{
			case WRITE:
				desired_request_duration = device->params.program_oob_latency_us;
				break;
			case READ:
				desired_request_duration = device->params.read_oob_latency_us;
				break;
			default:
				desired_request_duration = 0;
				break;
		}
	}

	return desired_request_duration;
}

void simulate_request_latency(struct flashsim_device *device, int rw, int sector_count, int data_type, t_time *request_start_time)
{
	int original_elapsed_us;
	int should_elapsed_us;

	if (device->params.delay_mode) {
		should_elapsed_us = calculate_desired_request_duration(device, rw, sector_count, data_type);
		original_elapsed_us = get_duration_in_us_till_now(request_start_time);

		if (original_elapsed_us < should_elapsed_us)
			udelay(should_elapsed_us - original_elapsed_us);
		else if (should_elapsed_us > 0)
			device->dev_stat.num_time_overflowed_requests[data_type]++;
	}
}
