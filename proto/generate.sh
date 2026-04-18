mkdir -p /home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv/src/proto

cd /home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv

/usr/bin/protoc --proto_path=proto --cpp_out=src/proto --grpc_out=src/proto \
--plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin proto/raft.proto

/usr/bin/protoc --proto_path=proto --cpp_out=src/proto --grpc_out=src/proto \
--plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin proto/kv.proto