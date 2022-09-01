#!/bin/bash

#kvstore="rocksdb"; ycsb="ycsbrocks"
kvstore="remixdb"; ycsb="ycsbremix"
#kvstore="pebblesdb"; ycsb="ycsbpebbles"
thrcount=28
opcount=$((2000*1000*1000))
#opcount=20000000
sleeptime=60 # time interval between runs. 

RES_DIR="db_test/${kvstore}/res_`date "+%Y%m%d%H%M"`"

array=(a b c d e f)

main () {
  do_ycsb_all 
  # do_ycsb_mergeloadandrun
}

reset_filesystem () {
  sudo umount /mnt/${kvstore}_test 2> /dev/null
  sudo umount /dev/nvme0n1 2> /dev/null

  sudo /sbin/mkfs.ext4 /dev/nvme0n1 -F
  sudo mount /dev/nvme0n1 /mnt/${kvstore}_test
  sudo sh -c "/usr/bin/echo 3 > /proc/sys/vm/drop_caches"
}

do_ycsb_all () { 
  mkdir -p $RES_DIR
   
  echo -en "" > ${RES_DIR}/size.dat
  
  sudo umount /mnt/${kvstore}_test 2> /dev/null

  sudo rm -rf /mnt/${kvstore}_test
  sudo mkdir /mnt/${kvstore}_test

  reset_filesystem 

  do_ycsb load putonly 

  do_ycsb run a
  do_ycsb run b
  do_ycsb run c
  do_ycsb run f
  do_ycsb run d
    
  reset_filesystem

  do_ycsb load putonly 

  do_ycsb run e $((200*1000*1000))

  sudo umount /dev/nvme0n1
}


do_ycsb_mergeloadandrun () {
  mkdir -p $RES_DIR
   
  echo -en "" > ${RES_DIR}/size.dat
  for i in "${array[@]}"
  do
    sudo rm -rf /mnt/${kvstore}_test
    sudo mkdir /mnt/${kvstore}_test

    sudo /sbin/mkfs.ext4 /dev/nvme0n1 -F
    sudo mount /dev/nvme0n1 /mnt/${kvstore}_test
    sudo sh -c "/usr/bin/echo 3 > /proc/sys/vm/drop_caches"


    #do_ycsb load putonly 

    #do_ycsb run $i
    
    do_ycsb_load_run $i

    sudo umount /mnt/${kvstore}_test
  done
}

do_ycsb () {
  type="$1"; workload="$2"

  if [ -z $3 ]; then 
    testcount=${opcount}
  else
    testcount=$3
  fi

  printf "Start: %s %s %s %s\n" ${type} ${workload} ${opcount} ${testcount} 

  sudo ./${ycsb} -${type} -db ${kvstore} -P workloads/workload${workload} -P ${kvstore}/${kvstore}.properties \
    -p threadcount=${thrcount} -p recordcount=${opcount} -p operationcount=${testcount} -s \
    >> ${RES_DIR}/${kvstore}_${type}_${workload}.dat

  echo -en "${type}_${workload}  " >> ${RES_DIR}/size.dat
  sudo df -Th /dev/nvme0n1 >> ${RES_DIR}/size.dat
  sudo cp /mnt/${kvstore}_test/LOG ${RES_DIR}/LOG_${type}_${workload}

  printf "Done : %s %s %s %s\n" ${type} ${workload} ${opcount} ${testcount} 

  sleep ${sleeptime}
  sudo sh -c "/usr/bin/echo 3 > /proc/sys/vm/drop_caches"
}

do_ycsb_load_run () {
  workload="$1"

  sudo ./${ycsb} -load -run -db ${kvstore} -P workloads/workload${workload} -P ${kvstore}/${kvstore}.properties \
    -p threadcount=${thrcount} -p recordcount=${opcount} -p operationcount=${opcount} -s \
    >> ${RES_DIR}/${kvstore}_load_and_run_${workload}.dat

  echo -en "${workload}  " >> ${RES_DIR}/size.dat
  sudo df -Th /dev/nvme0n1 >> ${RES_DIR}/size.dat
  sudo cp /mnt/${kvstore}_test/LOG ${RES_DIR}/LOG_load_and_run_${workload}
}

main

