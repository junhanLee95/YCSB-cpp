
while :; do 
  ps -eo user,pid,ppid,rss,size,vsize,pmem,pcpu,time,cmd --sort -rss | grep ycsb | grep -v grep  
  sleep 1
done
