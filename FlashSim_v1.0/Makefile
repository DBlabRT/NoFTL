ifneq ($(KERNELRELEASE),)

obj-m += flashsim.o
flashsim-objs 	:= flashsim_module.o flashsim_char.o flashsim_block.o flashsim_latency.o flashsim_statistics.o dftl_original.o    

else

KERNELDIR ?= /usr/src/linux-headers-3.2.0-44-generic
PWD       := $(shell pwd)

default:
	$(MAKE) -C $(KERNELDIR) M=$(PWD) modules
	gcc erase_block.c -o erase_block
	gcc statistics.c -o statistics
	gcc trace_player.c -o trace_player
	gcc dftl_external/dftl_external.c -g -o dftl_external/dftl_external
	gcc warm_uper.c -o warm_uper
	gcc set_delay_mode.c -o set_delay_mode
	gcc fio_trace_converter.c -o fio_trace_converter

endif

clean:
	rm -rf *.o *.ko *.symvers *.mod.c .*.cmd modules.order erase_block statistics trace_player dftl_external/dftl_external warm_uper set_delay_mode fio_trace_converter
