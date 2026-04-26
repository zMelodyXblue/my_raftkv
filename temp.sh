
./scripts/cmake_build.sh  
#cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo -DPIN_SYSTEM_PROTOBUF=ON
#cd build
#cmake --build . --target raftkv_server raftkv_cli -j$(nproc)
#cd ..
./scripts/build_raft_redis_proxy.sh 

./scripts/start_cluster.sh 
./bin/raftkv_redis_proxy --peers 127.0.0.1:50050,127.0.0.1:50051,127.0.0.1:50052 --port 6379


redis-cli -p 6379 PING
redis-cli -p 6379 SET foo bar #→ OK
redis-cli -p 6379 GET foo #→ "bar"
#redis-benchmark -h 127.0.0.1 -p 6379 -t set,get -c 50 -n 10000 -d 256
#redis-benchmark -p 6379 -t set,get -c 50 -n 10000 -d 4096 --csv > docs/bench_result.csv
redis-benchmark -p 6379 -t set,get -c 50 -n 10000 -d 4096 2>&1 | tee docs/bench/bench_full.txt

./scripts/stop_cluster.sh 