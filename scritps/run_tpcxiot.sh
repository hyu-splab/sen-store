#/bin/sh

BUILD_PATH="../build"
DATA_PATH="/data/test01"

echo "======= START TPCx-IoT for SEN-STORE ======="

$BUILD_PATH/db_bench --db=$DATA_PATH --threads=320 --num=10000000 --benchmarks="TPCxIoT"


echo "============= END TPCx-IoT ================="


