
#include "workload/tpch/tpch_struct.h"
#include "workload/tpch/tpch_util.h"
#include <string>

//enum tpch_l_shipmode{REG_AIR, AIR};
ENTER_NAMESPACE(tpch);

tpch_l_shipmode str_to_shipmode(char* shipmode)
{
  tpch_l_shipmode ret;
  if ( !strcmp(shipmode,"REG_AIR") || !strcmp(shipmode,"REG AIR") ) 
    { ret=REG_AIR; }
  else if (!strcmp(shipmode,"AIR"))
    { ret=AIR;  } 
  else if (!strcmp(shipmode,"RAIL"))
    { ret=RAIL;  } 
  else if (!strcmp(shipmode,"TRUCK"))
    { ret=TRUCK; } 
  else if (!strcmp(shipmode,"MAIL"))
    { ret=MAIL;  } 
  else if (!strcmp(shipmode,"FOB"))
    { ret=FOB;  } 
  else //if (strcmp(shipmode,"SHIP")==0)
    { ret=SHIP;  } 

  return ret;
}

void shipmode_to_str(char* str, tpch_l_shipmode shipmode)
{
  
  if ( shipmode == REG_AIR )
    { strcpy(str,"REG AIR"); }
  else if ( shipmode == AIR )
    { strcpy(str,"AIR"); }
  else if ( shipmode == RAIL )
    { strcpy(str,"RAIL"); }
  else if ( shipmode == TRUCK )
    { strcpy(str,"TRUCK"); }
  else if ( shipmode == MAIL )
    { strcpy(str,"MAIL"); }
  else if ( shipmode == FOB )
    { strcpy(str,"FOB"); }
  else //if ( shipmode == SHIP )
    { strcpy(str,"SHIP"); }

}

EXIT_NAMESPACE(tpch);
