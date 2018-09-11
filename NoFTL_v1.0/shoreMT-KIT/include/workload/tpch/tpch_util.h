#ifndef __TPCH_UTIL_H
#define __TPCH_UTIL_H

#include "workload/tpch/tpch_struct.h"

ENTER_NAMESPACE(tpch);

tpch_l_shipmode str_to_shipmode(char* shipmode);
void shipmode_to_str(char* str, tpch_l_shipmode shipmode);

EXIT_NAMESPACE(tpch);
#endif
