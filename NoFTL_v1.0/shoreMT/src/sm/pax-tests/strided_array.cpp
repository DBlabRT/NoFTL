#include <iostream.h>
#include <ctype.h>
#include <stdlib.h>

#define arrsize 8192
#define rowsize 8

void main(int argc, char** argv)
{
    int i, j, distance = atoi(argv[1]), notimes = atoi(argv[2]), sum1, sum2;
    double arr[rowsize][arrsize]; // 8 M doubles

    cerr << sizeof(double) << "Distance = " << distance << ", no times = " << notimes << endl;
    for (i=0; i<arrsize; ++i) // set to 0
     for (j=0; j<rowsize; ++j)
    	arr[j][i] = 1;
    
    cerr << "init done." << endl;
    // read
    for (i=0; i<notimes; ++i)
    {
      sum1 = sum2 = 0;
      for (j=0; j<arrsize; ++j)
      {
    	sum1 += arr[0][j];
	sum2 += arr[distance][j];
      }
    }
    cerr << "program done." << endl;
}
