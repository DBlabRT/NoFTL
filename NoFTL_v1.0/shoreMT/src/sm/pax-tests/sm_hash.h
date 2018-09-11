/////////////////////////////////////////////////////////////////////////////
// in memory hash table used in hash join.
////////////////////////////////////////////////////////////////////////////

#ifndef SM_HASH_H
#define SM_HASH_H

#include "w_dynarray.h"
#include "query_util.h"

class hash_table_t {
  public:
    hash_table_t(uint4_t size);
    ~hash_table_t();

    typedef dynarray<void*> val_array_t;	

    struct table_entry_t 
    {
        char* key;
        val_array_t val_array;
        table_entry_t* next;
        table_entry_t* prev;

        table_entry_t() 
	{
            key = NULL;
            next = this;
            prev = this;
        }
    };

    typedef table_entry_t bucket_t;

    void		reset();
    void		add(const char* key, void* val);
    val_array_t*	find_match(const char* key);

    uint4_t	m_size;
    table_entry_t*	m_table; 

    uint4_t	hash_val(const char* key);

    friend class ahhjoin_c;
};

class hash_func_c 
{
  
  private:
    int		ht_size ; // the size of hash table.
    char*	buf;

  public:
    hash_func_c(int size) 
    {
        ht_size = size ;
        buf = new char[1024];
    }
  
    ~hash_func_c() 
    {
        delete [] buf ;
    }

    // hash the tuple on the given column.
    uint4_t operator()(const file_info_t& finfo, const char* frec, int col);
    uint4_t operator()(const file_info_t& finfo, const precord_t& prec, int col);
    uint4_t operator()(const file_info_t& finfo, const hpl_record_t& hpl_rec, int col);
    uint4_t operator()(int int_val);
    uint4_t operator()(double double_val);
    uint4_t operator()(char* str_val);
    uint4_t operator()(const char* str_val);
};

#endif
  

