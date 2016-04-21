#!/bin/bash -x

dir=/tmp/distributed_queue
logfile_tmp=$dir/log_debug

dt=$(date '+%d_%m_%Y_%H_%M_%S')
resultfile=$dir/test_results_no_qw$dt.txt
touch $resultfile

queues=(2 4 8 16 32 64)
durations=(10)
local_balance=false
local_balance_type=(all fair)
#producers=
#consumers
#

process_results='pid=$(cat $dir/dds.pid)
			logfile=$logfile_tmp$pid
			echo "pid=$pid"
			echo $cmd >> $resultfile
			echo "pid=$pid" >> $resultfile
			grep -E "ERROR" $logfile >> $resultfile
			grep -E "T\[[0-9]+\]\: Inserted" $logfile >> $resultfile
			grep -E "T\[[0-9]+\]\: Removed" $logfile >> $resultfile
			grep -E "Total removed items" $logfile >> $resultfile
			grep -E "Total realtime spent in load balancer" $logfile >> $resultfile
			grep -E "Total realtime spent in global size" $logfile >> $resultfile
			grep -E "Total realtime spent in global balancer" $logfile >> $resultfile
			grep -E "Final realtime program time" $logfile >> $resultfile
			grep -E -A6 "STATISTICS\:" $logfile >> $resultfile
			echo "----" >> $resultfile
			echo "" >> $resultfile
			echo "" >> $resultfile'

for q in ${queues[@]}; do
	for d in ${durations[@]}; do
		for lt in ${local_balance_type[@]}; do

			cmd="bin/queue_tester_rand_computation -d $d -q $q -l $local_balance -p $q -c $q --local-balance-type=$lt"
			$cmd
			sleep 2
			$process_results

			cmd="bin/queue_tester_rand_computation -d $d -q $q -l $local_balance -p $q -c $(expr $q / 2) --local-balance-type=$lt"
			$cmd
			sleep 2
			$process_results

			cmd="bin/queue_tester_rand_computation -d $d -q $q -l $local_balance -p $q -c 1 --local-balance-type=$lt"
			$cmd
			sleep 2
			$process_results

	done
done

exit

./bin/queue_tester_rand_computation -d 10 -q 1 -l false --q1-ins-ratio=1 --q1-rm-ratio=1

./bin/queue_tester_rand_computation -d 10 -q 2 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q1-rm-ratio=1 --q2-rm-ratio=1 --local-balance-type=all
./bin/queue_tester_rand_computation -d 10 -q 2 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q1-rm-ratio=1 --q2-rm-ratio=1 --local-balance-type=pair

./bin/queue_tester_rand_computation -d 10 -q 2 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=1 --local-balance-type=all
./bin/queue_tester_rand_computation -d 10 -q 2 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=1 --local-balance-type=pair

./bin/queue_tester_rand_computation -d 10 -q 4 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=1 --q2-rm-ratio=1 --q3-rm-ratio=1 --q4-rm-ratio=1 --local-balance-type=all
./bin/queue_tester_rand_computation -d 10 -q 4 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=1 --q3-rm-ratio=1 --q4-rm-ratio=1 --local-balance-type=all
./bin/queue_tester_rand_computation -d 10 -q 4 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=0 --q3-rm-ratio=1 --q4-rm-ratio=1 --local-balance-type=all
./bin/queue_tester_rand_computation -d 10 -q 4 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=1 --q2-rm-ratio=1 --q3-rm-ratio=1 --q4-rm-ratio=1 --local-balance-type=pair
./bin/queue_tester_rand_computation -d 10 -q 4 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=1 --q3-rm-ratio=1 --q4-rm-ratio=1 --local-balance-type=pair
./bin/queue_tester_rand_computation -d 10 -q 4 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=0 --q3-rm-ratio=1 --q4-rm-ratio=1 --local-balance-type=pair
./bin/queue_tester_rand_computation -d 10 -q 4 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=0 --q3-rm-ratio=0 --q4-rm-ratio=1 --local-balance-type=pair

./bin/queue_tester_rand_computation -d 10 -q 8 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=1 --q3-rm-ratio=1 --q4-rm-ratio=1 --local-balance-type=all
./bin/queue_tester_rand_computation -d 10 -q 8 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=0 --q3-rm-ratio=1 --q4-rm-ratio=1 --local-balance-type=all
./bin/queue_tester_rand_computation -d 10 -q 8 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=0 --q3-rm-ratio=0 --q4-rm-ratio=1 --local-balance-type=all
./bin/queue_tester_rand_computation -d 10 -q 8 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=0 --q3-rm-ratio=0 --q4-rm-ratio=0 --local-balance-type=all
./bin/queue_tester_rand_computation -d 10 -q 8 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=1 --q3-rm-ratio=1 --q4-rm-ratio=1 --local-balance-type=pair
./bin/queue_tester_rand_computation -d 10 -q 8 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=0 --q3-rm-ratio=1 --q4-rm-ratio=1 --local-balance-type=pair
./bin/queue_tester_rand_computation -d 10 -q 8 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=0 --q3-rm-ratio=0 --q4-rm-ratio=1 --local-balance-type=pair
./bin/queue_tester_rand_computation -d 10 -q 8 -l false --q1-ins-ratio=1 --q2-ins-ratio=1 --q3-ins-ratio=1 --q4-ins-ratio=1 --q1-rm-ratio=0 --q2-rm-ratio=0 --q3-rm-ratio=0 --q4-rm-ratio=0 --local-balance-type=pair
######
./bin/queue_tester_insert_performance -q 1 -a 100000000 #TEST WITH DIFFERENT INSERT METHODS
./bin/queue_tester_insert_performance -q 2 -a 100000000
./bin/queue_tester_insert_performance -q 4 -a 100000000
./bin/queue_tester_insert_performance -q 8 -a 100000000
./bin/queue_tester_insert_performance -q 16 -a 100000000
./bin/queue_tester_insert_performance -q 32 -a 100000000
./bin/queue_tester_insert_performance -q 1 -d 7
./bin/queue_tester_insert_performance -q 2 -d 7
./bin/queue_tester_insert_performance -q 4 -d 7
./bin/queue_tester_insert_performance -q 8 -d 7
./bin/queue_tester_insert_performance -q 16 -d 7
./bin/queue_tester_insert_performance -q 32 -d 7


./bin/queue_tester_remove_performance -q 1 -a 100000000 --local-balance-type=all #TEST WITH DIFFERENT INSERT METHODS
./bin/queue_tester_remove_performance -q 2 -a 100000000 --local-balance-type=all
./bin/queue_tester_remove_performance -q 4 -a 100000000 --local-balance-type=all
./bin/queue_tester_remove_performance -q 8 -a 100000000 --local-balance-type=all
./bin/queue_tester_remove_performance -q 16 -a 100000000 --local-balance-type=all
./bin/queue_tester_remove_performance -q 32 -a 100000000 --local-balance-type=all
./bin/queue_tester_remove_performance -q 64 -a 100000000 --local-balance-type=all
./bin/queue_tester_remove_performance -q 1 -a 100000000 --local-balance-type=pair #TEST WITH DIFFERENT INSERT METHODS
./bin/queue_tester_remove_performance -q 2 -a 100000000 --local-balance-type=pair
./bin/queue_tester_remove_performance -q 4 -a 100000000 --local-balance-type=pair
./bin/queue_tester_remove_performance -q 8 -a 100000000 --local-balance-type=pair
./bin/queue_tester_remove_performance -q 16 -a 100000000 --local-balance-type=pair
./bin/queue_tester_remove_performance -q 32 -a 100000000 --local-balance-type=pair
./bin/queue_tester_remove_performance -q 64 -a 100000000 --local-balance-type=pair


./bin/queue_tester_rand_computation -d 7 -q 1 -l false -p 1 -c 1 --local-balance-type=all
./bin/queue_tester_rand_computation -d 7 -q 2 -l false -p 2 -c 2 --local-balance-type=all
./bin/queue_tester_rand_computation -d 7 -q 2 -l false -p 2 -c 1 --local-balance-type=all
./bin/queue_tester_rand_computation -d 7 -q 4 -l false -p 4 -c 4 --local-balance-type=all
./bin/queue_tester_rand_computation -d 7 -q 4 -l false -p 4 -c 2 --local-balance-type=all
./bin/queue_tester_rand_computation -d 7 -q 4 -l false -p 4 -c 1 --local-balance-type=all
./bin/queue_tester_rand_computation -d 7 -q 8 -l false -p 8 -c 8 --local-balance-type=all
./bin/queue_tester_rand_computation -d 7 -q 8 -l false -p 8 -c 4 --local-balance-type=all
./bin/queue_tester_rand_computation -d 7 -q 8 -l false -p 8 -c 1 --local-balance-type=all
./bin/queue_tester_rand_computation -d 7 -q 16 -l false -p 16 -c 16 --local-balance-type=all
./bin/queue_tester_rand_computation -d 7 -q 16 -l false -p 16 -c 8 --local-balance-type=all
./bin/queue_tester_rand_computation -d 7 -q 16 -l false -p 16 -c 1 --local-balance-type=all
