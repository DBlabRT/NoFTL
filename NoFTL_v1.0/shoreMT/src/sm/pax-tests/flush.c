#include <stdio.h>
#include <iostream.h>
#include <sys/file.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <assert.h>

//
// Windows stuff
//
// typedef long mode_t;
// typedef void* caddr_t;
// typedef long uid_t;
// typedef long gid_t;
// typedef unsigned pid_t;
// typedef unsigned int uint;
// typedef unsigned short u_short;

// all in bytes
const int Alloc_cutoff = 32 * 1024;
const int Alloc_begin = 1024 * 1024 * 32;
#define MaxNameLength 256
// STATUS_CHUNK should be determined according to TempFileSize
#define STATUS_CHUNK (0x1<<8)

int main(int argc, char *argv[])
{
  int n = Alloc_begin, sum;
  char buf[BUFSIZ];

  char *p;
  off_t size = 0, newOffset;
  int   bytesRead, src, count=0;

   
  if ( argc != 2 && argc != 3)   {
      fprintf(stderr, "Correct usage is: %s new_filename [-y (to create new file)]\n", argv[0]);
      exit(1); 
  }

  assert(strlen(argv[1]) < MaxNameLength-5);


  /* ------------------------------------------------------- */
  /* Eat up as much virtual memory as possible; up to Alloc_cutoff*/
  
  sum = 0;
  do {
      char *q;
      while ((caddr_t) (p = (char *) sbrk(n)) == (caddr_t) -1 
	     && n > Alloc_cutoff) {
	  n /= 2;
      }

      sum += n;
      fprintf(stderr, "allocated now %d bytes\n", sum);
      for (q = p; q < p + n; q += 512) {
	  *q = 3;
      }
  } while (n > Alloc_cutoff);
  
  /* ------------------------------------------------------- */
  if (argc >= 3 && argv[2][2] == 'y')
  {
    const int TmpFileSize = 32 * 1024 * 1024;
    char systemline[MaxNameLength];
    int i;

    sprintf(systemline, "yes | rm -f %s", argv[1]);
    system(systemline);


    fprintf(stderr, "Creating <%s> \n", argv[1]);
  
    src = open(argv[1], O_RDWR | O_CREAT);
    if (src == -1) {
      perror("open");
      fprintf(stderr, "Hopefully file %s does not already exist.\n", argv[1]);
      return 1;
    }

    memset(buf, 0, BUFSIZ);
    sprintf(buf, "this is junk\n");
  
    for (i = 0; TmpFileSize > i * BUFSIZ; i++) {
      write(src, buf, BUFSIZ);
    }
    close(src);
  
  }
  src = open(argv[1], O_RDONLY);
  if (src == -1) {
    perror("open");
    fprintf(stderr, "Hopefully file %s does already exist.\n", argv[1]);
    return 1;
  }

  /* ------------------------------------------------------- */
  /* Flush the Swap Space */

  newOffset = lseek(src, 0, SEEK_SET);
  if (newOffset != 0) {
      fprintf(stderr, "Unable to seek\n"); 
      exit(1); 
  }      

  fprintf(stderr, "Reading <%s> forwards\n", argv[1]);
  while ( 1 )
   {
      bytesRead = read(src, buf, BUFSIZ); 
      
      if ( bytesRead <= 0 )
      {
	 fprintf(stderr,"\nForward scan complete. Read %u bytes\n", size); 
	 break; 
      }
      size+=bytesRead;

      count++;
      if (!(count%STATUS_CHUNK))
      {
	 fprintf(stderr, ".");
	 count = 0;
      }      
   }
   
   count = 0;
   fprintf(stderr, "Reading <%s> backwards\n", argv[1]);   
   size -= BUFSIZ;
   while ( size >= 0)
   {
      newOffset = lseek(src, size, SEEK_SET);
      if ( newOffset != size )
      {
	 fprintf(stderr, "Unable to seek\n"); 
	 exit(1); 
      }      

      bytesRead = read(src, buf, BUFSIZ); 
      
      if ( bytesRead != BUFSIZ )
      {
	 fprintf(stderr, "Unable to read\n"); 
	 exit(1); 
      }      

      count++;
      if (!(count%STATUS_CHUNK))
      {
	 fprintf(stderr, ".");
	 count = 0;
      }
      size-=BUFSIZ;

   }

   fprintf(stderr,"\nBackwards scan complete\n");
   close(src); 

  return 0;
}

