#Distributed Data Structure - Distributed Queue
##Dependencies:
* gcc 4.9 =<
* MPI compiler and library. Tested with MPICH
* git
* gsl and gsl-devel library

##Fedora 23 Installation:
```bash
dnf install -y git mpich mpich-devel gcc gsl gsl-devel
module load mpi/mpich-x86_64
git clone https://github.com/Lirt/Distributed-Data-Structure.git
```

##Build:
```bash
make install
make all
```

##Test:
```bash
bin/queue_tester_sequential -q 1 -a 10000000
bin/queue_tester_insert_performance -q 1 -a 10000000
bin/queue_tester_remove_performance -q 1 -a 10000000
bin/queue_tester_rand_computation -q 8 -c 8 -p 8 -a 10000000 --local-balance-type=pair -s 1000000
```

For testing with libhoard library use "LD_PRELOAD=/usr/lib/libhoard.so" before command eg.
```bash
LD_PRELOAD=/usr/lib/libhoard.so bin/queue_tester_rand_computation -q 8 -c 8 -p 8 -a 100000000 --local-balance-type=pair -s 5000000
LD_PRELOAD=/usr/lib/libhoard.so mpirun -np 2 bin/queue_tester_rand_computation -q 4 -c 4 -p 4 -a 100000000 --local-balance-type=pair -s 5000000
```