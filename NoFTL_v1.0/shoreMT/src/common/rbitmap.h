
#ifndef RBITMAP_H
#define RBITMAP_H

#include "w_defines.h"

#ifdef __GNUG__
#pragma interface
#endif

extern "C" int r_bm_first_set(const u_char* rbm, int size, int start);
extern "C" int r_bm_first_clr(const u_char* rbm, int size, int start);

extern "C" int r_bm_last_set(const u_char *rbm, int size, int start);
extern "C" int r_bm_last_clr(const u_char *rbm, int size, int start);

extern "C" int r_bm_num_set(const u_char* rbm, int size);
extern "C" bool r_bm_any_set (const u_char* rbm, int size);

extern "C" bool r_bm_is_set(const u_char* rbm, long offset);
extern "C" bool r_bm_is_clr(const u_char* rbm, long offset);

extern "C" void r_bm_zero(u_char* rbm, int size);
extern "C" void r_bm_fill(u_char* rbm, int size);

extern "C" void r_bm_set(u_char* rbm, long offset);
extern "C" void r_bm_clr(u_char* rbm, long offset);

extern "C" void r_bm_print(u_char *rbm, int size);

#ifndef DUAL_ASSERT_H
#include "dual_assert.h"
#endif

inline bool r_bm_is_clr(const u_char* rbm, long offset)
{
    return !r_bm_is_set(rbm, offset);
}

inline int r_bm_num_clr(const u_char* rbm, int size)
{
    return size - r_bm_num_set(rbm, size);
}

#endif          /*</std-footer>*/
