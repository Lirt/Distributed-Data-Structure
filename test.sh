#!/bin/bash

dir=/tmp/distributed_queue
logfile_tmp=$dir/log_debug
resultfile=$dir/test_results.txt
touch $resultfile

queues=(1 2 4 8)
durations=(5 10 30 120 300 1200)
local_thresholds_static=(10000 20000 50000 100000)
q_ratios1=(1 1 1 1)
q_ratios2=(0 1 1 1)
q_ratios3=(0 0 1 1)
q_ratios=(${q_ratios1[@]} ${q_ratios2[@]} ${q_ratios3[@]})

#echo "qr ${#q_ratios[@]} ${q_ratios[@]}"

###
#LOCAL BALANCING OFF

local_balance=false
COUNT=${#q_ratios[@]}
for q in ${queues[@]}; do
	for d in ${durations[@]}; do
		for ((i=0; i<$COUNT/4; i++)) do
			#echo "bin/queue_tester_callback_2 -d $d -q $q -l $local_balance \
			#--q1-ratio=${q_ratios[$i*4]} --q2-ratio=${q_ratios[($i*4) + 1]} \
			#--q3-ratio=${q_ratios[($i*4) + 2]} --q4-ratio=${q_ratios[($i*4) + 3]}"
			bin/queue_tester_callback_2 -d $d -q $q -l $local_balance \
			--q1-ratio=${q_ratios[$i*4]} --q2-ratio=${q_ratios[($i*4) + 1]} \
			--q3-ratio=${q_ratios[($i*4) + 2]} --q4-ratio=${q_ratios[($i*4) + 3]}
			sleep 2

			pid=$(cat $dir/dds.pid)
			echo "pid=$pid"
			logfile=$logfile_tmp$pid
			echo "bin/queue_tester_callback_2 -d $d -q $q -l $local_balance \
--q1-ratio=${q_ratios[$i*4]} --q2-ratio=${q_ratios[($i*4) + 1]} \
--q3-ratio=${q_ratios[($i*4) + 2]} --q4-ratio=${q_ratios[($i*4) + 3]}" >> $resultfile
			echo "pid=$pid" >> $resultfile
			grep -E "ERROR" $logfile >> $resultfile
			grep -E "T\[[0-9]+\]\: Inserted" $logfile >> $resultfile
			grep -E "T\[[0-9]+\]\: Removed" $logfile >> $resultfile
			grep -E "Total removed items" $logfile >> $resultfile
			grep -E "Final realtime program time" $logfile >> $resultfile
			grep -E "Final process time" $logfile >> $resultfile
			grep -E -A6 "STATISTICS\:" $logfile >> $resultfile
			echo "" >> $resultfile
			echo "" >> $resultfile
		done
	done
done

###
#LOCAL BALANCING ON

local_balance=true
for q in ${queues[@]}; do
	for d in ${durations[@]}; do
		for lt in ${local_thresholds_static[@]}; do
			for ((i=0; i<$COUNT/4; i++)) do
				#echo "bin/queue_tester_callback_2 -d $d -q $q -l $local_balance --local-threshold-type=static --local-threshold-static=${lt} \
				#--q1-ratio=${q_ratios[$i*4]} --q2-ratio=${q_ratios[($i*4) + 1]} \
				#--q3-ratio=${q_ratios[($i*4) + 2]} --q4-ratio=${q_ratios[($i*4) + 3]}"
				bin/queue_tester_callback_2 -d $d -q $q -l $local_balance --local-threshold-type=static --local-threshold-static=${lt} \
				--q1-ratio=${q_ratios[$i*4]} --q2-ratio=${q_ratios[($i*4) + 1]} \
				--q3-ratio=${q_ratios[($i*4) + 2]} --q4-ratio=${q_ratios[($i*4) + 3]}

				pid=$(cat $dir/dds.pid)
				logfile=$logfile_tmp$pid
				echo "bin/queue_tester_callback_2 -d $d -q $q -l $local_balance \
	--q1-ratio=${q_ratios[$i*4]} --q2-ratio=${q_ratios[($i*4) + 1]} \
	--q3-ratio=${q_ratios[($i*4) + 2]} --q4-ratio=${q_ratios[($i*4) + 3]}" >> $resultfile
				grep -E "ERROR" $logfile >> $resultfile
				grep -E "T\[[0-9]+\]\: Inserted" $logfile >> $resultfile
				grep -E "T\[[0-9]+\]\: Removed" $logfile >> $resultfile
				grep -E "Total removed items" $logfile >> $resultfile
				grep -E "Final realtime program time" $logfile >> $resultfile
				grep -E "Final process time" $logfile >> $resultfile
				grep -E -A6 "STATISTICS\:" $logfile >> $resultfile
				echo "" >> $resultfile
				echo "" >> $resultfile
			done
		done
	done
done


