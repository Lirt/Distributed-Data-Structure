#!/bin/bash -x

dir=/tmp/distributed_queue
logfile_tmp=$dir/log_debug_

dt=$(date '+%d_%m_%Y_%H_%M_%S')
resultfile=$dir/test_results_.txt

#process_results='pid=$(cat $dir/dds.pid)
#			logfile=$logfile_tmp$pid
#			echo "pid=$pid"
#			echo $cmd >> $resultfile
#			echo "pid=$pid" >> $resultfile
#			grep -E "ERROR" $logfile >> $resultfile
#			grep -E "T\[[0-9]+\]\: Inserted" $logfile >> $resultfile
#			grep -E "T\[[0-9]+\]\: Removed" $logfile >> $resultfile
#			grep -E "Total removed items" $logfile >> $resultfile
#			grep -E "Total realtime spent in load balancer" $logfile >> $resultfile
#			grep -E "Total realtime spent in global size" $logfile >> $resultfile
#			grep -E "Total realtime spent in global balancer" $logfile >> $resultfile
#			grep -E "Final realtime program time" $logfile >> $resultfile
#			grep -E -A6 "STATISTICS\:" $logfile >> $resultfile
#			echo "----" >> $resultfile
#			echo "" >> $resultfile
#			echo "" >> $resultfile'

process_results='pid=$(cat $dir/dds.pid)
			logfile=$logfile_tmp$pid
			echo $cmd >> $resultfile
			echo "pid=$pid" >> $resultfile
			grep -E "Final realtime program time" $logfile >> $resultfile'

amounts=(40000000 300000000 1200000000)
#TEST 1 - sequential program
test=t1
for a in ${amounts[@]}; do
	for i in $(seq 1 7); do
		q=1
		size=$(echo "80000000/$q"|bc)
		resultfile=$dir/test_results_$test_$i.txt
		touch $resultfile

		cmd="LD_PRELOAD=/usr/lib/libhoard.so:/usr/local/lib/libgsl.so.19 bin/queue_tester_sequential -q $q -a $a"
		echo $cmd
		sleep 3
		$process_results
	done
done


#TEST 2 - insert performance
test=t2
queues=(1 2 4 8 16 32)
for q in ${queues[@]}; do
	for i in $(seq 1 7); do 
		resultfile=$dir/test_results_$test_q$q_$i.txt
		touch $resultfile

		cmd="LD_PRELOAD=/usr/lib/libhoard.so:/usr/local/lib/libgsl.so.19 bin/queue_tester_insert_performance -q $q -a 80000000"
		echo $cmd
		$cmd
		sleep 3
	done
done

#TEST 3 - remove performance
test=t3
queues=(1 2 4 8 16 32 64 128)
for q in ${queues[@]}; do
	for i in $(seq 1 7); do 
		resultfile=$dir/test_results_$test_q$q_$i.txt
		touch $resultfile

		cmd="LD_PRELOAD=/usr/lib/libhoard.so:/usr/local/lib/libgsl.so.19 bin/queue_tester_remove_performance -q $q -a 80000000"
		echo $cmd
		$cmd
		sleep 3
	done
done

#TEST 4 - random computation consumer and producer = queues
test=t4
queues=(1 2 4 8)
for q in ${queues[@]}; do
	for a in ${amounts[@]}; do
		for i in $(seq 1 7); do

			c=$q
			p=$q
			size=$(echo "80000000/$q"|bc)
			resultfile=$dir/test_results_$test_q$q_c$c_p$p_$i.txt
			touch $resultfile

			cmd="LD_PRELOAD=/usr/lib/libhoard.so:/usr/local/lib/libgsl.so.19 bin/queue_tester_rand_computation -q $q -c $c -p $p -a $a --local-balance-type=pair -s $size"
			echo $cmd
			$cmd
			sleep 3
			$process_results

		done
	done
done

#TEST 5 - random computation consumer = 1
test=t5
queues=(2 4 8)
for q in ${queues[@]}; do
	for a in ${amounts[@]}; do
		for i in $(seq 1 7); do

			c=1
			p=$q
			size=$(echo "80000000/$q"|bc)
			resultfile=$dir/test_results_$test_q$q_c$c_p$p_$i.txt
			touch $resultfile

			cmd="LD_PRELOAD=/usr/lib/libhoard.so:/usr/local/lib/libgsl.so.19 bin/queue_tester_rand_computation -q $q -c $c -p $p -a $a \ 
			--local-balance-type=pair -s $size"
			echo $cmd
			$cmd
			sleep 3
			$process_results

		done
	done
done

#TEST 6 - random computation consumer = 2
test=t6
queues=(4 8)
for q in ${queues[@]}; do
	for a in ${amounts[@]}; do
		for i in $(seq 1 7); do

			c=2
			p=$q
			size=$(echo "80000000/$q"|bc)
			resultfile=$dir/test_results_$test_q$q_c$c_p$p_$i.txt
			touch $resultfile

			cmd="LD_PRELOAD=/usr/lib/libhoard.so:/usr/local/lib/libgsl.so.19 bin/queue_tester_rand_computation -q $q -c $c -p $p -a $a \ 
			--local-balance-type=pair -s $size"
			echo $cmd
			$cmd
			sleep 3
			$process_results

		done
	done
done

#TEST 7 - random computation more consumers
test=t7
for a in ${amounts[@]}; do
	for i in $(seq 1 7); do

			q=2
			c=2
			p=1
			size=$(echo "80000000/$q"|bc)
			resultfile=$dir/test_results_$test_q$q_c$c_p$p_$i.txt
			touch $resultfile

			cmd="LD_PRELOAD=/usr/lib/libhoard.so:/usr/local/lib/libgsl.so.19 bin/queue_tester_rand_computation -q $q -c $c -p $p -a $a \ 
			--local-balance-type=pair -s $size"
			echo $cmd
			$cmd
			sleep 3
			$process_results

			q=4
			c=4
			p=2
			size=$(echo "80000000/$q"|bc)
			resultfile=$dir/test_results_$test_q$q_c$c_p$p_$i.txt
			touch $resultfile

			cmd="LD_PRELOAD=/usr/lib/libhoard.so:/usr/local/lib/libgsl.so.19 bin/queue_tester_rand_computation -q $q -c $c -p $p -a $a \ 
			--local-balance-type=pair -s $size"
			echo $cmd
			$cmd
			sleep 3
			$process_results

		done
	done
done

#Test 8 - multiple processes
test=t8
for a in ${amounts[@]}; do
	for i in $(seq 1 7); do
		q=1
		c=1
		p=1
		np=2
		size=$(echo "80000000/($q*$np)"|bc)
		cmd="LD_PRELOAD=/usr/lib/libhoard.so:/usr/local/lib/libgsl.so.19 mpirun -np $np bin/queue_tester_rand_computation_debug -q $q -c $c -p $p -a $a --local-balance-type=pair -s $size"
		echo $cmd
		$cmd
		sleep 3

	done
done

test=t9
for a in ${amounts[@]}; do
	for i in $(seq 1 7); do
		q=2
		c=1
		p=2
		np=2
		size=$(echo "80000000/($q*$np)"|bc)
		cmd="LD_PRELOAD=/usr/lib/libhoard.so:/usr/local/lib/libgsl.so.19 mpirun -np $np bin/queue_tester_rand_computation_debug -q $q -c $c -p $p -a $a --local-balance-type=pair -s $size"
		echo $cmd
		$cmd
		sleep 3

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
