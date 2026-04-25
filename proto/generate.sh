mkdir -p src/proto


/usr/bin/protoc --proto_path=proto --cpp_out=src/proto --grpc_out=src/proto \
--plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin proto/raft.proto

/usr/bin/protoc --proto_path=proto --cpp_out=src/proto --grpc_out=src/proto \
--plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin proto/kv.proto