/////////////////////////////////////////////////////////
// memory hash table to be used in hash join.
///////////////////////////////////////////////////////

#include "sm_hash.h"

// import the hash function from  stringhash.C
w_base_t::uint4_t hash(const char *p) {
	//FUNC(hash(char *));
	const char *c=p;

	int len;
	int	i; int	b; int m=1 ; unsigned long sum = 0;

	c++;
	len = strlen(c);

	for(i=0; i<len; i++) {
		// compute push-button signature
		// of a sort (has to include the rest of the
		// ascii characters
		switch(*c) {
#define CASES(a,b,c,d,e,f) case a: case b: case c: case d: case e: case f:
			CASES('a','b','c','A','B','C')
				b = 2; break;
			CASES('d','e','f','D','E','F')
				b = 3; break;
			CASES('g','h','i','G','H','I')
				b = 4; break;
			CASES('j','k','l','J','K','L')
				b = 5; break;
			CASES('m','n','o','M','N','O')
				b = 6; break;
			CASES('p','r','s','P','R','S')
				b = 7; break;
			CASES('t','u','v','T','U','V')
				b = 8; break;
			CASES('w','x','y','W','X','Y')
				b = 9; break;
			default:
				b = 1;
				break;
		}
		sum += (unsigned) (b * m);
		m = m * 10;
	}
	//DUMP(end of hash(char *));
	return sum;
}



hash_table_t::hash_table_t (uint4_t size)
{
    m_size = size;
    m_table = new table_entry_t [m_size];
}

hash_table_t::~hash_table_t()
{
    this->reset();
    delete [] m_table;
}

uint4_t hash_table_t::hash_val(const char* key)
	//hash was defined in stringhash.C
{ return hash(key) % m_size; }

void hash_table_t::add(const char * key, void* val)
{
    uint4_t hval = hash_val(key);
    table_entry_t*  bucket = &m_table[hval];

    // see if the key is already in the table.
    table_entry_t* entry = NULL;
    for (entry=bucket->next; entry!=bucket; entry = entry->next)
        if (strcmp(key, entry->key) == 0) break;
  
    if (entry == bucket) {
        // the key is not in the bucket.
        entry = new table_entry_t;
        int len = strlen(key);
        entry->key = new char[len+1];
        strncpy(entry->key, key,len);
        entry->key[len] = '\0';

        entry->next = bucket->next;
        entry->prev = bucket;
        bucket->next->prev = entry;
        bucket->next= entry;
    }

    entry->val_array.push_back(val);
}


hash_table_t::val_array_t* hash_table_t::find_match(const char* key)
{
    uint4_t hval = hash_val(key);
    table_entry_t* bucket = &m_table[hval];
    table_entry_t* entry;
  
    for (entry = bucket->next; entry!=bucket; entry = entry->next)
        if (strcmp(key, entry->key) == 0) break;
  
    // find the match
    if (entry != bucket)
        return &entry->val_array;
    else // not found
        return NULL;
}
  
void hash_table_t::reset()
{
    table_entry_t* entry;
    uint4_t bukInx;

    for (bukInx =0; bukInx < m_size; bukInx++) 
    {
    	for (entry = m_table[bukInx].next; 
	     entry != &m_table[bukInx]; 
	     entry=entry->next) 
	{
	    w_assert3(entry!=NULL);
	    /* I don't think this does anything and I am not sure why it's here anyway
    	    for (i=0; i < entry->val_array.getsize(); i++)
	        delete entry->val_array[i];
    	    entry->val_array.reset();
	    */
    	    delete [] entry->key;
    	    entry->prev->next = entry->next;
    	    entry->next->prev = entry->prev;
    	    delete entry;
        }
    }
}

uint4_t hash_func_c::operator()(const file_info_t& finfo, const char* frec, int col)
{

    const char* field_ptr = FIELD_START(finfo,frec,col);

    switch(finfo.field_type[col]) 
    {
      case SM_INTEGER:
        return this->operator()(*(int*)field_ptr);
      
      case SM_DATE8:
      case SM_FLOAT:
        return this->operator()(*(double*)field_ptr);

      case SM_DATE:
      case SM_CHAR:
      case SM_VARCHAR:
        // string is terminated
        return this->operator()(field_ptr);
      default:
        cerr << "Bad type found in column " << col << endl;
        w_assert3(0);
        return 0;
    }
}

uint4_t hash_func_c::operator()(const file_info_t& finfo, const precord_t& prec, int col)
{
  switch(finfo.field_type[col]) 
    {
    case SM_INTEGER:
      return this->operator()(*(int*) prec.data[col]);
      
    case SM_DATE8:
    case SM_FLOAT:
      return this->operator()(*(double*) prec.data[col]);

    case SM_DATE:
    case SM_CHAR:
    case SM_VARCHAR:
      return this->operator()(prec.data[col]) ;
      
    default:
      cerr << "Bad type found in column " << col << endl;
      w_assert3(0) ;
      return 0 ;
    }
}

uint4_t hash_func_c::operator()(const file_info_t& finfo, const hpl_record_t& hpl_rec, int col)
{
  switch(finfo.field_type[col])
    {
    case SM_INTEGER:
      return this->operator()(*(int*) hpl_rec.data[col]);

    case SM_DATE8:
    case SM_FLOAT:
      return this->operator()(*(double*) hpl_rec.data[col]);

    case SM_DATE:
    case SM_CHAR:
    case SM_VARCHAR:
      return this->operator()(hpl_rec.data[col]) ;

    default:
      cerr << "Bad type found in column " << col << endl;
      w_assert3(0) ;
      return 0 ;
    }
}

uint4_t hash_func_c::operator()(int int_val)
{
  sprintf(buf, "%d", int_val) ;
  return this->operator()(buf) ;
}

uint4_t hash_func_c::operator()(double double_val)
{
  sprintf(buf, "%f", double_val);
  return this->operator()(buf) ;
}

uint4_t hash_func_c::operator()(const char* str_val)
{
  uint4_t h = 0;
  // HPL:
  //for ( ; *str_val; ++str_val) h = 5*h + *str_val;
  for (int counter=0 ; (*str_val) && (counter < 2); ++str_val, counter++) h = 5*h + *str_val;
  return h % ht_size ;
}

uint4_t hash_func_c::operator()(char* str_val)
{
  uint4_t h = 0;
  // HPL:
  //for ( ; *str_val; ++str_val) h = 5*h + *str_val;
  for (int counter=0 ; (*str_val) && (counter < 2); ++str_val, counter++) h = 5*h + *str_val;
  return  h % ht_size ;
}

#ifdef __GNUG__
template class dynarray<void*>;
#endif

