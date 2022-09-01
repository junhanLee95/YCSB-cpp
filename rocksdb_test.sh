#!/bin/bash

sudo rm -rf result
sudo rm -rf /mnt/rocksdb_test
mkdir result
sudo mkdir /mnt/rocksdb_test
sudo /sbin/mkfs.ext4 /dev/nvme0n1 -F
sudo mount /dev/nvme0n1 /mnt/rocksdb_test
sudo sh -c "/usr/bin/echo 3 > /proc/sys/vm/drop_caches"
sudo ./ycsbrocks -load -db rocksdb -P workloads/workloadputonly -P rocksdb/rocksdb.properties -p threadcount=28 -p recordcount=2000000000 -s > result/rocksdb_result.txt
sudo df -Th >> result/size.txt
sudo ./ycsbrocks -run -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p threadcount=28 -p recordcount=2000000000 -p operationcount=2000000000  -s >> result/rocksdb_result.txt
sudo ./ycsbrocks -run -db rocksdb -P workloads/workloadb -P rocksdb/rocksdb.properties -p threadcount=28 -p recordcount=2000000000 -p operationcount=2000000000  -s >> result/rocksdb_result.txt
sudo ./ycsbrocks -run -db rocksdb -P workloads/workloadc -P rocksdb/rocksdb.properties -p threadcount=28 -p recordcount=2000000000 -p operationcount=2000000000  -s >> result/rocksdb_result.txt
sudo ./ycsbrocks -run -db rocksdb -P workloads/workloadf -P rocksdb/rocksdb.properties -p threadcount=28 -p recordcount=2000000000 -p operationcount=2000000000  -s >> result/rocksdb_result.txt
sudo ./ycsbrocks -run -db rocksdb -P workloads/workloadd -P rocksdb/rocksdb.properties -p threadcount=28 -p recordcount=2000000000 -p operationcount=2000000000  -s >> result/rocksdb_result.txt

sudo cp /mnt/rocksdb_test/LOG /home/ceph/limsh/YCSB-cpp/result
sudo mv result/LOG result/LOG_1

sudo umount /mnt/rocksdb_test
sudo /sbin/mkfs.ext4 /dev/nvme0n1 -F
sudo mount /dev/nvme0n1 /mnt/rocksdb_test
sudo sh -c "/usr/bin/echo 3 > /proc/sys/vm/drop_caches"
sudo ./ycsbrocks -load -db rocksdb -P workloads/workloadputonly -P rocksdb/rocksdb.properties -p threadcount=28 -p recordcount=2000000000  -s >> result/rocksdb_result.txt
sudo ./ycsbrocks -run -db rocksdb -P workloads/workloade -P rocksdb/rocksdb.properties -p threadcount=28 -p recordcount=2000000000 -p operationcount=200000000  -s >> result/rocksdb_result.txt
sudo cp /mnt/rocksdb_test/LOG /home/ceph/limsh/YCSB-cpp/result

sudo umount /mnt/rocksdb_test
