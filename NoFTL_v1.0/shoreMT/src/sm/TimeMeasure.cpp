/* ------------------------------------------------------------------------------- */
/* -- Copyright (c) 2011, 2012 Databases and Distributed Systems Department,    -- */
/* -- Technical University of Darmstadt, subject to the terms and conditions    -- */
/* -- given in the file COPYRIGHT.  All Rights Reserved.                        -- */
/* ------------------------------------------------------------------------------- */
/* -- Ilia Petrov, Todor Ivanov, Veselin Marinov                                -- */
/* ------------------------------------------------------------------------------- */

#include "TimeMeasure.h"
#include <iostream>
#include <fstream>
using namespace std;

TimeMeasure::TimeMeasure()
{
	filename = "timemeasure.csv";
}

TimeMeasure::~TimeMeasure() {
	// do nothing
}

TimeMeasure::TimeMeasure(string myfilename)
{
	filename = myfilename;
}


void TimeMeasure::startTimer()
{
	// Get time accurate to ~microseconds (much less accurate in reality)
	gettimeofday(&start, NULL);

}

void TimeMeasure::stopTimer()
{
	gettimeofday(&end, NULL);
}

double TimeMeasure::getElapsedTime()
{
	// See how much time passed between beginning and end
	differUsec = (end.tv_sec*1000000+end.tv_usec)-(start.tv_sec*1000000+start.tv_usec);

	return differUsec;
}

void TimeMeasure::printData()
{
	// See how much time passed between beginning and end
	differUsec = getElapsedTime();

	size_t length;
	char filenamebuffer[30];
	length=filename.copy(filenamebuffer,filename.size(),0);
	filenamebuffer[length]='\0';
	ofstream myfile(filenamebuffer, ios::app);
	if (myfile.is_open())
	{
		//myfile <<"Time(sec.): "<<differSec<<"; Time(us): "<<differUsec<<"; All accesses:"<<accesses<<"; Single access time(ns):"<<(differUsec/accesses)*1000<<"; Inc_step: "<<incr_step<<"; Data(kylobytes): "<<sizeof(array)/1024<<endl;
		myfile << differUsec << endl;
		myfile.close();
	}
	else cout << "Unable to open file";
}
