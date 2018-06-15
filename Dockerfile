# this dockerfile builds kitchen sync on ubuntu and runs the test suite.

# ubuntu 16.04 is considered the oldest currently supported distro.
FROM ubuntu:16.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
	DEBIAN_FRONTEND=noninteractive apt-get upgrade -y && \
	DEBIAN_FRONTEND=noninteractive apt-get install -y build-essential cmake libboost-dev libssl-dev \
		mysql-server libmysqlclient-dev postgresql-9.5 libpq-dev \
		git ruby ruby-dev && \
	rm -f /etc/apt/apt.conf.d/20auto-upgrades && \
	apt-get clean -y && \
	rm -rf /var/cache/apt/archives/*

RUN gem install bundler --no-ri --no-rdoc

WORKDIR /tmp
COPY test/Gemfile Gemfile
COPY test/Gemfile.lock Gemfile.lock
RUN bundle install --path ~/gems

ADD CMakeLists.txt /tmp/CMakeLists.txt
ADD cmake /tmp/cmake
ADD src /tmp/src

WORKDIR /tmp/build
RUN cmake .. && make

ADD test /tmp/test
RUN echo 'starting postgresql' && \
	service postgresql start && \
	echo 'creating postgresql database' && \
	su postgres -c 'createdb --encoding unicode --template template0 ks_test' && \
	echo 'creating postgresql user' && \
	su postgres -c 'createuser --superuser root' && \
	echo 'starting mysql' && \
	mkdir -p /var/run/mysqld && \
	chown mysql:mysql /var/run/mysqld && \
	(/usr/sbin/mysqld --skip-grant-tables &) && \
	echo 'waiting for mysql to start' && \
	mysqladmin --wait=30 ping && \
	echo 'creating mysql database' && \
	mysqladmin create ks_test && \
	echo 'installing test gems' && \
	BUNDLE_GEMFILE=../test/Gemfile bundle install --path ~/gems && \
	echo 'preparing for tests' && \
	ls -al /tmp/build && \
	echo 'running tests' && \
	CTEST_OUTPUT_ON_FAILURE=1 POSTGRESQL_DATABASE_USERNAME=root make test
