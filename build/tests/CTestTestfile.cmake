# CMake generated Testfile for 
# Source directory: /home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv/tests
# Build directory: /home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv/build/tests
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(smoke "/home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv/build/bin/test_smoke")
set_tests_properties(smoke PROPERTIES  _BACKTRACE_TRIPLES "/home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv/tests/CMakeLists.txt;12;add_test;/home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv/tests/CMakeLists.txt;0;")
add_test(persister "/home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv/build/bin/test_persister")
set_tests_properties(persister PROPERTIES  _BACKTRACE_TRIPLES "/home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv/tests/CMakeLists.txt;21;add_test;/home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv/tests/CMakeLists.txt;0;")
add_test(election "/home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv/build/bin/test_election")
set_tests_properties(election PROPERTIES  _BACKTRACE_TRIPLES "/home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv/tests/CMakeLists.txt;31;add_test;/home/zmagex/5.Projects/raft_based_kv_storage/my_raftkv/tests/CMakeLists.txt;0;")
