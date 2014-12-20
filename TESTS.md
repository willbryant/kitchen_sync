Running the test suite
======================

First, compile the software using the [install guide](INSTALL.md) instructions.  You'll need
to build Kitchen Sync for both mysql and postgresql if you'd like to run the test suite.

To run the tests you should install both databases on your local machine.  You'll then need
to create a test databases to use with each:
```
  mysqladmin -u root create ks_test
  createdb --encoding unicode ks_test
```

You can override these database names, and if necessary the usernames and passwords it will
use, by setting the variables `ENDPOINT_DATABASE_NAME`, `ENDPOINT_DATABASE_USERNAME`, and
`ENDPOINT_DATABASE_PASSWORD`.

If you need to use different usernames etc. for the two databases, you can instead use:
`MYSQL_DATABASE_NAME` and `POSTGRESQL_DATABASE_NAME`;
`MYSQL_DATABASE_USERNAME` and `POSTGRESQL_DATABASE_USERNAME`;
and `MYSQL_DATABASE_PASSWORD` and `POSTGRESQL_DATABASE_PASSWORD`.

You can also override the host or port that it uses if you want to test against a particular
instance; see `test_helper.rb` for more.

You'll also need to install Ruby 2.0 or later, and the bundle of gems (Kitchen Sync is written
in pure C++, but the test suite is in Ruby):

```
  cd test
  bundle install
  cd -
```

To run the suite, change to the kitchen_sync directory where you checked out the files, then:

```
  cd build
  cmake .. && make test
```

The normal cmake behavior is to report failures without giving any actual information about
them.  To get cmake to output on failures, use `CTEST_OUTPUT_ON_FAILURE`:
```
  cmake .. && CTEST_OUTPUT_ON_FAILURE=1 make test
```

You can also inspect the full test output in `Testing/Temporary/LastTest.log`.
