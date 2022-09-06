#!/bin/bash

#kvstore="rocksdb"; ycsb="ycsbrocks"
kvstore="remixdb"; ycsb="ycsbremix"
thrcount=28
#opcount=$((2000*1000*1000))
opcount=2000000

RES_DIR="oom-test/res_`date "+%Y%m%d%H%M"`"

main () {
  mkdir $RES_DIR

  sudo rm -rf /mnt/${kvstore}_test
  sudo mkdir /mnt/${kvstore}_test

  sudo /sbin/mkfs.ext4 /dev/nvme0n1 -F
  sudo mount /dev/nvme0n1 /mnt/${kvstore}_test
  sudo sh -c "/usr/bin/echo 3 > /proc/sys/vm/drop_caches"

  #./mem_monitoring.sh > ${RES_DIR}/mem.dat &
  #MEM_MON_PID="$!"

  echo -en "" > ${RES_DIR}/size.dat

  do_ycsb load putonly 

  do_ycsb run a 

  #kill -9 ${MEM_MON_PID} 

  sudo umount /mnt/${kvstore}_test
  #sudo /sbin/mkfs.ext4 /dev/nvme0n1 -F
}

do_ycsb () {
  type="$1"; workload="$2"

  sudo ./${ycsb} -${type} -db ${kvstore} -P workloads/workload${workload} -P ${kvstore}/${kvstore}.properties \
    -p threadcount=${thrcount} -p recordcount=${opcount} -p operationcount=${opcount} -s \
    > ${RES_DIR}/${kvstore}_${type}${workload}.dat

  echo -en "${type}${workload}  " >> ${RES_DIR}/size.dat
  sudo df -Th /dev/nvme0n1 >> ${RES_DIR}/size.dat
  sudo cp /mnt/${kvstore}_test/LOG ${RES_DIR}/LOG_${type}${workload}
}

main

