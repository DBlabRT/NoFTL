/*
 * sdisk_nand_t class is to be used for simulations with raw NAND devices (simulators)
 * It provides the same interface as sdisk_unix_t
 * Functions: make, convert_flags, path, fd, open, close, pread, pwrite are copied as they are from sdisk_unix_t
 * Other functions are not supported (implemented)
 * The main of this class is to leave the original sdisk_unix_t without changes but ensure that
 * the simulator is "accessed" only via supported set of functions (pwrite, pread, erase)
 * Inheritance from sdisk_unix_t would be a alternative for copy-paste but would require changes in base class
 */
#ifndef NAND_DISK_H
#define NAND_DISK_H

#include "w_defines.h"

/*
 * This code is defined in flashsim_common.h
 */
#define ERASE_BLOCK					0x6b01
#define ACCESS_OOB					0x6b02
#define FLASHSIM_BLOCK_DEV_PATH 	"/dev/flashsim_block"
#define FLASHSIM_CHAR_DEV_PATH		"/dev/flashsim_char"

class sdisk_nand_t : public sdisk_t {
    int _fd;
    char _path[256];
    enum { FD_NONE = -1 };

public:
    sdisk_nand_t(const char *path);
    ~sdisk_nand_t();

    static w_rc_t make(const char *name, int flags, int mode, sdisk_t *&disk);
    static int convert_flags(int);
    const char * path() const { return &_path[0]; }
    int fd() const { return _fd; }
    static bool isFlashsimDevice(const char *path);

    w_rc_t open(const char *name, int flags, int mode);
    w_rc_t close();

    w_rc_t erase_block(int blk_num);
    w_rc_t read_oob(int psn, int offset, int length, void *buf, bool with_delay);
    w_rc_t write_oob(int psn, int offset, int length, void *buf, bool with_delay);

    w_rc_t read(void *buf, int count, int &done);
    w_rc_t write(const void *buf, int count, int &done);

    w_rc_t readv(const iovec_t *iov, int iovcnt, int &done);
    w_rc_t writev(const iovec_t *iov, int iovcnt, int &done);

    w_rc_t pread(void *buf, int count, fileoff_t pos, int &done);
    w_rc_t pwrite(const void *buf, int count, fileoff_t pos, int &done);

    w_rc_t seek(fileoff_t pos, int origin, fileoff_t &newpos);
    w_rc_t truncate(fileoff_t size);
    w_rc_t sync();
    w_rc_t stat(filestat_t &st);

private:
    w_rc_t clean_os_pagecache();

    struct access_oob {
    	int psn;
    	int offset;
    	int length;
    	int rw;
    	int simulate_delay;
    	void * buffer;
    };
};

#endif
