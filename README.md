## About
	NoFTL - a storage architecture which allows the DBMS to operate directly on a raw Flash memory, i.e., without intermediate layers such as file system and FTL (Flash Translation Layer used by modern Flash-SSDs). NoFTL was first presented at the VLDB'13 conference with the demo paper [NoFTL: Database Systems on FTL-less Flash Storage](http://www.vldb.org/pvldb/vol6/p1278-petrov.pdf). The NoFTL-v1.0 project is the source code, which was used by that demonstration.

## History
	NoFTL is a fork of [Shore-MT](https://bitbucket.org/shoremt/), which itself is derived from Shore. The latter was developed in the early 90's by researchers in the University of Wisconsin-Madison, whereas the former was a continuation of the project at Carnegie Mellon University and, later on, EPFL. Shore-MT focuses on improving scalability in multi-core CPUs. Several published techniques in the database literature are based on Shore/Shore-MT, including recent and ongoing research.
	

## Project structure
	The NoFTL project consists of two main parts: Shore-MT and FlashSim. Shore-MT in turn is devided into two sub-projects: 1) shore-mt - storage engine, and 2) shore-kits - test suite, which allows to run various benchmarks. FlashSim is an emulator of Flash memory, which is implemented as a device driver in Linux kernel. 
	
	
## Papers

## Configure & Build & Install
#### shoreMT
		More detailed information can be found here: https://bitbucket.org/shoremt/shore-mt/wiki/Home  
		1) cd NoFTL/NoFTL_v1.0/shoreMT
		2) set ShoreMT and NoFTL parameters in config/shore.def file (e.g., "FTL_SECTORS_PER_BLOCK 64", this should match parameters in NoFTL/FlashSim_v1.0/install)
		3) ./bootstrap
		4) ./configure
		5) make
		6) no installation required		
		
#### shoreMT-KIT
		More detailed information can be found here: https://bitbucket.org/shoremt/shore-kits/wiki/Quick%20Start
		1) cd NoFTL/NoFTL_v1.0/shoreMT-KIT
		2) set ShoreMT-KIT parameters in shore.conf file (e.g., tpcc-100-device = /dev/flashsim_block to use Flash emulator as a device)
		3) ./autogen.sh
		4) ./configure --enable-shore6 SHORE_HOME=../shoreMT/
		5) make
		6) no installation required
		
#### FlashSim
		1) cd NoFTL/FlashSim_v1.0
		1) set FlashSim parameters in install file
		2) make
		3) sudo ./install

## Testing
	cd shoreMT-KIT
	sudo ./shore_kits -c tpcc-100
