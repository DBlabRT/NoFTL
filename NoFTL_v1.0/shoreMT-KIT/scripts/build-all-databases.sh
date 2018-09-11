#!/bin/bash
#
# Recreates the directories, builds and closes the databases

STAMP=$(date +"%F-%Hh%Mm%Ss")
echo "[$STAMP]" | tee -a building-db

### EDIT if moving to a different machine
CPUS=16;

echo "Assuming a $CPUS hw contexts machine" | tee -a building-db


## Creates a directory if this does not exist
check_dir ()
{
    # Dir name
    DIR=$1

    if [ -d $DIR ] 
        then        
        return
    else
        echo "Creating $DIR"
        mkdir $DIR
    fi
}


## Clears the contents of a directory
clear_dir ()
{
    # Dir name
    DIR=$1

    if [ -d $DIR ] 
        then        
        echo "Deleting $DIR"
        rm $DIR/*
    else
        echo "Creating $DIR"
        mkdir $DIR
    fi
}


## 
load_db ()
{
    if [ $# -lt 3 ]; then
        echo "Not enough parameters" >& 2
        return;
    fi

    STAMP=$(date +"%F-%Hh%Mm%Ss")
    echo "[$STAMP]" | tee -a building-db

    # System (baseline)
    SYSTEM=$1

    # Design (normal)
    DESIGN=$2

    # Configuration (tm1-16,...)
    CONF=$3

    # Configuration file
    CONFFILE="shore.conf.$SYSTEM.$CPUS"
    if [ $# -ge 4 ]; then
        CONFFILE=$4
        echo "Trying custom conf file $CONFFILE"
    fi

    if [ -e $CONFFILE ] 
        then
        echo "Using config file $CONFFILE" | tee -a building-db
        else
        $CONFFILE=shore.conf
        echo "!!! POSSIBLE ERROR: using default config file" | tee -a building-db
    fi

    echo "XXX $CONFFILE $SYSTEM $DESIGN $CONF XXX"  | tee -a building-db

    # Clear the contents of the corresponding log dir
    cat $CONFFILE | grep $CONF- | grep logdir | cut -d' ' -f 3
    LOGDIR=`cat $CONFFILE | grep $CONF- | grep logdir | cut -d' ' -f 3`
    echo "Clearing $LOGDIR"
    clear_dir $LOGDIR

    echo "./shore_kits -f $CONFFILE -s $SYSTEM -d $DESIGN -c $CONF -x -r" | tee -a building-db
    (echo quit | LD_LIBRARY_PATH="/home/ipandis/apps/lib64/" ./shore_kits -f $CONFFILE -s $SYSTEM -d $DESIGN -c $CONF -x -r ) 2>&1 | tee -a building-db

# (echo quit | LD_LIBRARY_PATH="/export/home/ipandis/apps/readline/;/usr/sfw/lib/sparcv9" ./shore_kits -f $CONFFILE -s $SYSTEM -d $DESIGN -c $CONF -x -r ) 2>&1 | tee -a building-db
}


### Make sure that the databases directory exists
check_dir databases


### The various databases that will be created
load_db baseline normal tm1-10 

# load_db baseline normal tm1-16
# load_db baseline normal tm1-64
# load_db baseline normal tpcb-20
# load_db baseline normal tpcb-100
# load_db baseline normal tpcc-20
# load_db baseline normal tpcc-100