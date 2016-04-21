export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

my_dir=/home/ng3ra/RevenueAudit

#nohup ${my_dir}/bin/HAcquire 172.16.1.181 10000 test1 &

#   Process     daemon_flag CCM_ID     cfg_file       启动批号|指标ID|采集ID|
../bin/HAcquire    1        3333333333 ../etc/acq.cfg 00001\|00123\|00004\|

