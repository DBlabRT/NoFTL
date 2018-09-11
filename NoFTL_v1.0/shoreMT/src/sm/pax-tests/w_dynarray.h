/*<std-header orig-src='shore' incl-file-exclusion='W_DYNARRAY_H'>

 $Id: w_dynarray.h,v 3.0 2000/09/18 21:04:44 natassa Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#ifndef W_DYNARRAY_H
#define W_DYNARRAY_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifndef W_BASE_H
#include "w_base.h"
#endif

template <class T>
class dynarray {

  public :
  dynarray(const unsigned mincap = 128)
    {
      w_assert3(mincap > 0);
      capacity = mincap; size = 0 ; 
      data = new T[mincap];
      if (!data) W_FATAL(fcOUTOFMEMORY) ; 
    }

  dynarray(const dynarray& other) ;

  ~dynarray()
    {
      delete [] data;
    }

  void reset()
    {
      size = 0 ; 
    }
     
  const unsigned getsize() const
    {
      return size;
    }

  const dynarray & operator = (const dynarray & other);

  T & operator [] (const unsigned i) const
    {
      w_assert3(i < size) ; 
      return data[i];
    }

  T & last()
  {
      if (size)
	  return data[size-1];
      else
	  return data[0]; // will be garbage value
  }

    void push_back(T el) ;
    void push_back1(T &el) ;

    void set_size(unsigned int s);

    void set_size(unsigned int s, T val);

    void pop_back()
    {
	if (size > 0) size--;
    }
 
  private :
  unsigned size;
  unsigned capacity;
  T * data;

  void set_capacity(unsigned int c);

};

template <class T>
dynarray<T>::dynarray(const dynarray<T>& other) 
{
  capacity = other.size ; 
  size = other.size ; 
  
  data = new T[size] ; 
  if (!data) W_FATAL(fcOUTOFMEMORY) ; 

  unsigned i;

  for (i=0; i<size; i++)
	data[i] = other.data[i];

}

template <class T> 
const dynarray<T> & dynarray<T>::operator = (const dynarray<T> & other)
{
  unsigned i;
  if ( capacity < other.size ) 
  {
      capacity = other.size; 

      delete [] data ;  
      data = new T[capacity];
      if (!data) W_FATAL(fcOUTOFMEMORY) ; 
  }
  size = other.size;
  for (i=0; i<size; i++)
      data[i] = other.data[i];
  return *this;
}

template <class T>
void dynarray<T>::push_back(T el)
{
   if (size >= capacity) 
   {
      T *tvec = new T[capacity << 1];
      if (!tvec) W_FATAL(fcOUTOFMEMORY) ; 

      unsigned i;
      for (i=0; i<capacity; i++)
	  tvec[i] = data[i];

      delete [] data;

      data = tvec;
      capacity = capacity << 1;
   }

   data[size++]= el;
} 


template <class T>
void dynarray<T>::push_back1(T &el)
{
   if (size >= capacity) 
   {
      T *tvec = new T[capacity << 1];
      if (!tvec) W_FATAL(fcOUTOFMEMORY) ; 

      unsigned i;
      for (i=0; i<capacity; i++)
	  tvec[i] = data[i];

      delete [] data;

      data = tvec;
      capacity = capacity << 1;
   }

   data[size++]= el;
} 

template <class T>
void dynarray<T>::set_capacity(unsigned int c)
{
    if (c>capacity)
    {
	T *tarr = new T[c];
	if (!tarr) W_FATAL(fcOUTOFMEMORY) ; 
	unsigned i;
	for (i=0; i<capacity; i++)
	    tarr[i] = data[i];
	delete [] data;
	data = tarr;
	capacity = c;
    }
}

template <class T>
void dynarray<T>::set_size(unsigned int s)
{
    set_capacity(s);
    T tmpelem=0;
    unsigned i;
    
    for (i = s; i < size; i++)
	data[i] = tmpelem;  // necessary for proper initialization
    size = s;
}


template <class T>
void dynarray<T>::set_size(unsigned int s, T val)
{
    set_capacity(s);
    T tmpelem=0;
    unsigned i;
    
    for (i = 0; i < s; i++)
	data[i] = val;

    for (i = s; i < size; i++)
	data[i] = tmpelem;  // necessary for proper initialization
    size = s;
}


/*<std-footer incl-file-exclusion='W_DYNARRAY_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
