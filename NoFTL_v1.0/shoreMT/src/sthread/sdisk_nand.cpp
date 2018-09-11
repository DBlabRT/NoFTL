#include "w_defines.h"

#if defined(linux) && !defined(_GNU_SOURCE)
/*
 *  XXX this done to make O_DIRECT available as an I/O choice.
 *  Unfortunately, it needs to pollute the other headers, otw
 *  the features will be set and access won't be possible
 */
#define _GNU_SOURCE
#endif

#include <w.h>
#include <sthread.h>
#include <sthread_stats.h>
#include <sdisk.h>
#include <sdisk_nand.h>
#include "os_fcntl.h"
#include <cerrno>
#include <sys/stat.h>
#include <sys/uio.h>
#include <os_interface.h>
#include <sys/ioctl.h>

const int stBADFD 	= sthread_base_t::stBADFD;
const int stINVAL 	= sthread_base_t::stINVAL;
const int oobREAD 	= 0;
const int oobWRITE 	= 1;

int sdisk_nand_t::convert_flags(int sflags)
{
    int    flags = 0;

    /* 1 of n */
    switch (modeBits(sflags)) {
    case OPEN_RDWR:
        flags |= O_RDWR;
        break;
    case OPEN_WRONLY:
        flags |= O_WRONLY;
        break;
    case OPEN_RDONLY:
        flags |= O_RDONLY;
        break;
    }

    /* m of n */
    /* could make a data driven flag conversion, :-) */
    if (hasOption(sflags, OPEN_CREATE))
        flags |= O_CREAT;
    if (hasOption(sflags, OPEN_TRUNC))
        flags |= O_TRUNC;
    if (hasOption(sflags, OPEN_EXCL))
        flags |= O_EXCL;
#ifdef O_SYNC
    if (hasOption(sflags, OPEN_SYNC))
        flags |= O_SYNC;
#endif
    if (hasOption(sflags, OPEN_APPEND))
        flags |= O_APPEND;
#ifdef O_DIRECT
    if (hasOption(sflags, OPEN_RAW))
        flags |= O_DIRECT;
#endif

    return flags;
}

sdisk_nand_t::sdisk_nand_t(const char *path) : _fd(FD_NONE)
{
	if (!isFlashsimDevice(path))
		w_assert0(0);

	strncpy(&_path[0], path, 255);
    _path[255] = '0';
}

sdisk_nand_t::~sdisk_nand_t()
{
    if (_fd != FD_NONE)
        W_COERCE(close());
}

bool sdisk_nand_t::isFlashsimDevice(const char *path)
{
	return !strcmp(path, FLASHSIM_BLOCK_DEV_PATH) || !strcmp(path, FLASHSIM_CHAR_DEV_PATH);
}

w_rc_t sdisk_nand_t::make(const char *name, int flags, int mode, sdisk_t *&disk)
{
    sdisk_nand_t *ud;
    w_rc_t e;

    disk = 0;    /* default value*/
    
    ud = new sdisk_nand_t(name);
    if (!ud)
        return RC(fcOUTOFMEMORY);

    e = ud->open(name, flags, mode);
    if (e.is_error()) {
        delete ud;
        return e;
    }

    disk = ud;
    return RCOK;
}

w_rc_t sdisk_nand_t::open(const char *name, int flags, int mode)
{
    if (_fd != FD_NONE)
        return RC(stBADFD);    /* XXX in use */

    //To be sure
    flags |= O_DIRECT;
    flags |= O_SYNC;

    _fd = ::os_open(name, convert_flags(flags), mode);
    if (_fd == -1) {
        w_rc_t rc = RC(fcOS);
        RC_APPEND_MSG(rc, << "Offending file: " << name);
        return rc;
    }

    return RCOK;
}

w_rc_t sdisk_nand_t::close()
{
    if (_fd == FD_NONE)
        return RC(stBADFD);    /* XXX closed */

    int n = ::os_close(_fd);
    if (n == -1)
        return RC(fcOS);

    _fd = FD_NONE;
    return RCOK;
}

w_rc_t sdisk_nand_t::erase_block(int blk_num)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    if (ioctl(_fd, ERASE_BLOCK, &blk_num) == -1)
    	return RC(fcOS);

    return RCOK;
}

w_rc_t sdisk_nand_t::read_oob(int psn, int offset, int length, void *buf, bool with_delay)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    struct access_oob params;

	params.psn = psn;
	params.offset = offset;
	params.length = length;
	params.rw = oobREAD;
	params.simulate_delay = with_delay;
	params.buffer = buf;

   if(ioctl(_fd, ACCESS_OOB, &params))
	   return RC(fcOS);

    return RCOK;
}

w_rc_t sdisk_nand_t::write_oob(int psn, int offset, int length, void *buf, bool with_delay)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    struct access_oob params;

	params.psn = psn;
	params.offset = offset;
	params.length = length;
	params.rw = oobWRITE;
	params.simulate_delay = with_delay;
	params.buffer = buf;

   if(ioctl(_fd, ACCESS_OOB, &params))
	   return RC(fcOS);

    return RCOK;
}

/*
 * This of course add a bit of performance bottleneck
 * But we really want to have only DBMS buffer and avoid OS caching
 * Probably specifying the concrete (previous) range in posi_fadvise would be more efficient,
 * but then we need to keep track of previous touched pages (which could be a problem if we have several threads)
 * This is for example done in tool pagecache-management.sh but it shoes no results (cleaning pages only from beginning to current position)
 * So far this is applied for all tests - it is OK to pay for this additional overhead but have ONLY DBMS buffer
*/
w_rc_t sdisk_nand_t::clean_os_pagecache()
{
	w_rc_t rc;

	//Check if this is necessary
	if (fdatasync(_fd)) {
		rc = RC(fcOS);
		RC_APPEND_MSG(rc, << "FDATASYNC FAILED: "  << _path);
		return rc;
	}

	if (posix_fadvise(_fd, 0, 0, POSIX_FADV_DONTNEED)) {
		rc = RC(fcOS);
		RC_APPEND_MSG(rc, << "POSIX FADVISE DONTNEED FAILED: "  << _path);
		return rc;
	}

	return RCOK;
}

w_rc_t sdisk_nand_t::pread(void *buf, int count, fileoff_t pos, int &done)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

	if (count > SM_PAGESIZE || pos < 0)
    	return RC(stINVAL);

    INC_STH_STATS(num_io);

    clean_os_pagecache();

    int n = ::os_pread(_fd, buf, count, pos);
    if (n == -1)
        return RC(fcOS);

    done = n;

    return RCOK;
}

w_rc_t sdisk_nand_t::pwrite(const void *buf, int count, fileoff_t pos, int &done)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

	if (count > SM_PAGESIZE || pos < 0)
    	return RC(stINVAL);

    INC_STH_STATS(num_io);

    clean_os_pagecache();

    int n = ::os_pwrite(_fd, buf, count, pos);
    if (n == -1)
        return RC(fcOS);
#if defined(USING_VALGRIND)
    if(RUNNING_ON_VALGRIND)
    {
        check_valgrind_errors(__LINE__, __FILE__);
    }
#endif

    done = n;

    return RCOK;
}

w_rc_t sdisk_nand_t::read(void */*buf*/, int /*count*/, int &/*done*/)
{
	return RC(fcNOTIMPLEMENTED);
}

w_rc_t sdisk_nand_t::write(const void */*buf*/, int /*count*/, int &/*done*/)
{
	return RC(fcNOTIMPLEMENTED);
}

w_rc_t sdisk_nand_t::readv(const iovec_t */*iov*/, int /*iovcnt*/, int &/*done*/)
{
	return RC(fcNOTIMPLEMENTED);
}

w_rc_t sdisk_nand_t::writev(const iovec_t */*iov*/, int /*iovcnt*/, int &/*done*/)
{
	return RC(fcNOTIMPLEMENTED);
}

w_rc_t sdisk_nand_t::seek(fileoff_t /*pos*/, int /*origin*/, fileoff_t &/*newpos*/)
{
	return RC(fcNOTIMPLEMENTED);
}

w_rc_t sdisk_nand_t::truncate(fileoff_t /*size*/)
{
	return RC(fcNOTIMPLEMENTED);
}

w_rc_t sdisk_nand_t::sync()
{
	return RCOK;
}

w_rc_t sdisk_nand_t::stat(filestat_t &st)
{
    if (_fd == FD_NONE)
        return RC(stBADFD);

    os_stat_t    sys;
    int n = os_fstat(_fd, &sys);
    if (n == -1)
        return RC(fcOS);

    st.st_size = sys.st_size;
    st.st_block_size = sys.st_blksize;
    st.st_device_id = sys.st_dev;
    st.st_file_id = sys.st_ino;

    int mode = (sys.st_mode & S_IFMT);
    st.is_file = (mode == S_IFREG);
    st.is_dir = (mode == S_IFDIR);
    st.is_device = (mode == S_IFBLK || mode == S_IFCHR);

    return RCOK;
}
