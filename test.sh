#!/bin/bash -x

dir=/tmp/distributed_queue
logfile_tmp=$dir/log_debug

dt=$(date '+%d_%m_%Y_%H_%M_%S')
resultfile=$dir/test_results_no_qw$dt.txt
touch $resultfile

queues=(1 2 4 8)
durations=(30 120 300)
q_ratios1=(1 1 1 1)
q_ratios2=(10 1 1 1)
q_ratios3=(10 18 1 1)
q_ratios4=(14 14 14 14)
q_ratios=(${q_ratios1[@]} ${q_ratios2[@]} ${q_ratios3[@]} {q_ratios4[@]})
local_balance=false
#computation-load=(0 10 16 21)

###
#LOCAL BALANCING OFF
COUNT=${#q_ratios[@]}
for q in ${queues[@]}; do
	for d in ${durations[@]}; do
		for ((i=0; i<$COUNT/4; i++)) do
			
			bin/queue_tester_rand_computation -d $d -q $q -l $local_balance \
			--q1-ratio=${q_ratios[$i*4]} --q2-ratio=${q_ratios[($i*4) + 1]} \
			--q3-ratio=${q_ratios[($i*4) + 2]} --q4-ratio=${q_ratios[($i*4) + 3]}
			sleep 2

			pid=$(cat $dir/dds.pid)
			echo "pid=$pid"
			logfile=$logfile_tmp$pid
			echo "bin/queue_tester_rand_computation -d $d -q $q -l $local_balance \
--q1-ratio=${q_ratios[$i*4]} --q2-ratio=${q_ratios[($i*4) + 1]} \
--q3-ratio=${q_ratios[($i*4) + 2]} --q4-ratio=${q_ratios[($i*4) + 3]}" >> $resultfile
			echo "pid=$pid" >> $resultfile
			grep -E "ERROR" $logfile >> $resultfile
			grep -E "T\[[0-9]+\]\: Inserted" $logfile >> $resultfile
			grep -E "T\[[0-9]+\]\: Removed" $logfile >> $resultfile
			grep -E "Total removed items" $logfile >> $resultfile
			grep -E "Total realtime spent in load balancer" $logfile >> $resultfile
			grep -E "Total realtime spent in global size" $logfile >> $resultfile
			grep -E "Final realtime program time" $logfile >> $resultfile
			grep -E "Final process time" $logfile >> $resultfile
			grep -E -A6 "STATISTICS\:" $logfile >> $resultfile
			echo "----" >> $resultfile
			echo "" >> $resultfile
			echo "" >> $resultfile

		done
	done
done

####
#LOCAL BALANCING ON
#EQUAL BALANCE
##
dt=$(date '+%d_%m_%Y_%H_%M_%S')
resultfile=$dir/test_results_qw_static$dt.txt
touch $resultfile

queues=(2 4 8)
local_thresholds_static=(3000 12000 50000 100000)
local_balance=true
local-threshold-type=static
lb_types=( all pair )

for q in ${queues[@]}; do
	for d in ${durations[@]}; do
		for lbt in ${lb_types[@]}; do
			for lt in ${local_thresholds_static[@]}; do
				for ((i=0; i<$COUNT/4; i++)) do

					echo "bin/queue_tester_rand_computation -d $d -q $q -l $local_balance --local-threshold-type=static \
--local-threshold-static=$lt --local-balance-type=$lb_type --q1-ratio=${q_ratios[$i*4]} \
--q2-ratio=${q_ratios[($i*4) + 1]} --q3-ratio=${q_ratios[($i*4) + 2]} \
--q4-ratio=${q_ratios[($i*4) + 3]}"

					bin/queue_tester_rand_computation -d $d -q $q -l $local_balance --local-threshold-type=static \
					--local-threshold-static=$lt --local-balance-type=$lb_type --q1-ratio=${q_ratios[$i*4]} \
					--q2-ratio=${q_ratios[($i*4) + 1]} --q3-ratio=${q_ratios[($i*4) + 2]} \
					--q4-ratio=${q_ratios[($i*4) + 3]}

					pid=$(cat $dir/dds.pid)
					echo "pid=$pid"
					logfile=$logfile_tmp$pid
					echo "bin/queue_tester_rand_computation -d $d -q $q -l $local_balance --local-threshold-type=static \
--local-threshold-static=$lt --local-balance-type=$lb_type --q1-ratio=${q_ratios[$i*4]} \
--q2-ratio=${q_ratios[($i*4) + 1]} --q3-ratio=${q_ratios[($i*4) + 2]} \
--q4-ratio=${q_ratios[($i*4) + 3]}" >> $resultfile
					grep -E "ERROR" $logfile >> $resultfile
					grep -E "T\[[0-9]+\]\: Inserted" $logfile >> $resultfile
					grep -E "T\[[0-9]+\]\: Removed" $logfile >> $resultfile
					grep -E "Total realtime spent in load balancer" $logfile >> $resultfile
					grep -E "Total realtime spent in global size" $logfile >> $resultfile
					grep -E "Total removed items" $logfile >> $resultfile
					grep -E "Final realtime program time" $logfile >> $resultfile
					grep -E "Final process time" $logfile >> $resultfile
					grep -E -A6 "STATISTICS\:" $logfile >> $resultfile
					echo "----" >> $resultfile
					echo "" >> $resultfile
					echo "" >> $resultfile
				done
			done
		done
	done
done


