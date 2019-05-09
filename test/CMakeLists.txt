# we mostly prefer protocol-level integration tests but have some unit tests
add_executable(ks_unit_tests ks_unit_tests.cpp db_url_test.cpp ../src/db_url.cpp basic_uint128_t_test.cpp sql_functions_test.cpp)
add_test(unit_tests          ks_unit_tests)

# the main tests require ruby (and various extra gems).  to run the suite, run
#   cmake .. && CTEST_OUTPUT_ON_FAILURE=1 make test
add_test(prepare_for_test        env BUNDLE_GEMFILE=../../test/Gemfile bundle install --quiet)
add_test(protocol_version_test   env BUNDLE_GEMFILE=../../test/Gemfile bundle exec ruby ../../test/protocol_version_test.rb)
add_test(snapshot_from_test      env BUNDLE_GEMFILE=../../test/Gemfile bundle exec ruby ../../test/snapshot_from_test.rb)
add_test(schema_from_test        env BUNDLE_GEMFILE=../../test/Gemfile bundle exec ruby ../../test/schema_from_test.rb)
add_test(schema_to_test          env BUNDLE_GEMFILE=../../test/Gemfile bundle exec ruby ../../test/schema_to_test.rb)
add_test(range_from_test         env BUNDLE_GEMFILE=../../test/Gemfile bundle exec ruby ../../test/range_from_test.rb)
add_test(hash_from_test          env BUNDLE_GEMFILE=../../test/Gemfile bundle exec ruby ../../test/hash_from_test.rb)
add_test(rows_from_test          env BUNDLE_GEMFILE=../../test/Gemfile bundle exec ruby ../../test/rows_from_test.rb)
add_test(filter_from_test        env BUNDLE_GEMFILE=../../test/Gemfile bundle exec ruby ../../test/filter_from_test.rb)
add_test(filter_to_test          env BUNDLE_GEMFILE=../../test/Gemfile bundle exec ruby ../../test/filter_to_test.rb)
add_test(column_types_to_test    env BUNDLE_GEMFILE=../../test/Gemfile bundle exec ruby ../../test/column_types_to_test.rb)
add_test(column_types_from_test  env BUNDLE_GEMFILE=../../test/Gemfile bundle exec ruby ../../test/column_types_from_test.rb)
add_test(sync_to_test            env BUNDLE_GEMFILE=../../test/Gemfile bundle exec ruby ../../test/sync_to_test.rb)

# we also have a performance test utility that is not run as part of the test suite because there's no particular pass/fail criteria
add_executable(ks_bench ks_bench.cpp ../src/xxHash/xxhash.cpp)
target_link_libraries(ks_bench ${OPENSSL_LIBRARIES})
