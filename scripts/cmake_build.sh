mkdir build
cd build
cmake ..                                                                                                                                         
cmake --build . --target raftkv_server -j$(nproc)
cmake --build . --target raftkv_cli -j$(nproc)  