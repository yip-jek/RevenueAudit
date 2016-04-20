export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

my_dir=/home/ng3ra/RevenueAudit

nohup ${my_dir}/bin/HAcquire 172.16.1.181 10000 test1 &

