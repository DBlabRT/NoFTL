## Process this file with automake to produce Makefile.in
include $(top_srcdir)/Makefile.generic

## no-op rule for making the -out files
## this is to avoid automagic makefiles thinking that the -out
## files are executable files to be made from the cpp files of the same
## prefix

AM_CXXFLAGS          +=  \
		 -I$(top_srcdir)/src/fc \
		 -I$(top_srcdir)/src/sthread \
		 -I$(top_srcdir)/src/common  \
		 -I$(top_srcdir)/src/sm 

check_PROGRAMS     = sm_exec$(EXEEXT) 


TESTS = testall

include_HEADERS = query_util.h sm_exec.h sm_query.h sm_hash.h ahhjoin.h sm_tpch.h

sm_exec_SOURCES = query_util.cpp sm_exec.cpp sm_query.cpp sm_hash.cpp ahhjoin.cpp sm_tpch.cpp options.cpp sm_measure.cpp
 

LDADD      = \
	$(top_builddir)/src/sm/libsm.a  \
	$(top_builddir)/src/common/libcommon.a  \
	$(top_builddir)/src/sthread/libsthread.a  \
	$(top_builddir)/src/fc/libfc.a  \
	$(LOCALLDADD)

#file_scan.pure:  
#	$(PURIFY_BIN)/purify $(PURIFY_FLAGS)  -collector=$(PURIFY_COLLECTOR)  $(CXX) -o $@ $(file_scan_SOURCES:.cpp=.o) $(LDADD) $(LIBS)


