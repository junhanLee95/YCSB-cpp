#!/bin/bash

sudo rm -rf result
sudo rm -rf /mnt/remixdb_test
mkdir result
sudo mkdir /mnt/remixdb_test
sudo /sbin/mkfs.ext4 /dev/nvme0n1 -F
sudo mount /dev/nvme0n1 /mnt/remixdb_test 
sudo sh -c "/usr/bin/echo 3 > /proc/sys/vm/drop_caches"
sudo ./ycsbremix -load -db remixdb -P workloads/workloadputonly -P remixdb/remixdb.properties -p threadcount=4 -p recordcount=2000000000 -s > result/remixdb_result.txt
sudo df -Th >> result/size.txt
sudo ./ycsbremix -run -db remixdb -P workloads/workloada -P remixdb/remixdb.properties -p threadcount=4 -p recordcount=2000000000 -p operationcount=2000000000 -s >> result/remixdb_result.txt
sudo ./ycsbremix -run -db remixdb -P workloads/workloadb -P remixdb/remixdb.properties -p threadcount=4 -p recordcount=2000000000 -p operationcount=2000000000 -s >> result/remixdb_result.txt
sudo ./ycsbremix -run -db remixdb -P workloads/workloadc -P remixdb/remixdb.properties -p threadcount=4 -p recordcount=2000000000 -p operationcount=2000000000 -s >> result/remixdb_result.txt
sudo ./ycsbremix -run -db remixdb -P workloads/workloadf -P remixdb/remixdb.properties -p threadcount=4 -p recordcount=2000000000 -p operationcount=2000000000 -s >> result/remixdb_result.txt
sudo ./ycsbremix -run -db remixdb -P workloads/workloadd -P remixdb/remixdb.properties -p threadcount=4 -p recordcount=2000000000 -p operationcount=2000000000 -s >> result/remixdb_result.txt

sudo cp /mnt/remixdb_test/LOG /home/ceph/limsh/YCSB-cpp/result
sudo mv result/LOG result/LOG_1

sudo umount /mnt/remixdb_test
sudo /sbin/mkfs.ext4 /dev/nvme0n1 -F
sudo mount /dev/nvme0n1 /mnt/remixdb_test
sudo sh -c "/usr/bin/echo 3 > /proc/sys/vm/drop_caches"
sudo ./ycsbremix -load -db remixdb -P workloads/workloadputonly -P remixdb/remixdb.properties -p threadcount=4 -p recordcount=2000000000 -s >> result/remixdb_result.txt
sudo ./ycsbremix -run -db remixdb -P workloads/workloade -P remixdb/remixdb.properties -p threadcount=4 -p recordcount=2000000000 -p operationcount=200000000 -s >> result/remixdb_result.txt
sudo umount /mnt/remixdb_test

sudo cp /mnt/remixdb_test/LOG /home/ceph/limsh/YCSB-cpp/result
