/* ------------------------------------------------------------------------------- */
/* -- Copyright (c) 2011, 2012 Databases and Distributed Systems Department,    -- */
/* -- Technical University of Darmstadt, subject to the terms and conditions    -- */
/* -- given in the file COPYRIGHT.  All Rights Reserved.                        -- */
/* ------------------------------------------------------------------------------- */
/* -- Ilia Petrov, Todor Ivanov, Veselin Marinov                                -- */
/* ------------------------------------------------------------------------------- */

#ifndef TIMEMEASURE_H_
#define TIMEMEASURE_H_

#include <sys/time.h>
#include <time.h>
#include <string>
using namespace std;

class TimeMeasure {
private:
	timeval start, end;
	double differUsec;
	string filename;
public:
	TimeMeasure();
	virtual ~TimeMeasure();

	TimeMeasure(string myfilename);

	void startTimer();

	void stopTimer();

	double getElapsedTime();

	void printData();
};
#endif /* TIMEMEASURE_H_ */
