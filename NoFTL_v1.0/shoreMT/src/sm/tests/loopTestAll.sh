#!/bin/bash
for (( i = 1; i <= $1; i++ )); do
	echo "========================"
	echo " Running for $i-th time"
	echo "========================"
	make check
done
