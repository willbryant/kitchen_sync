# this dockerfile builds kitchen sync on ubuntu and runs the test suite.

# ubuntu 12.04 is considered the oldest currently supported distro.
FROM ubuntu:12.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
	DEBIAN_FRONTEND=noninteractive apt-get upgrade -y && \
	DEBIAN_FRONTEND=noninteractive apt-get install -y build-essential cmake libboost-dev libssl-dev \
		mysql-server libmysqlclient-dev wget \
		git python-software-properties && \
	(wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -) && \
	echo "deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main" >>/etc/apt/sources.list && \
	apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql-9.3 libpq-dev && \
	rm -f /etc/apt/apt.conf.d/20auto-upgrades && \
	apt-get clean -y && \
	rm -rf /var/cache/apt/archives/*

# suppress warnings
RUN sed -i 's/key_buffer/key_buffer_size/' /etc/mysql/my.cnf
RUN sed -i 's/myisam-recover/myisam-recover-options/' /etc/mysql/my.cnf

RUN add-apt-repository -y "deb http://ppa.launchpad.net/powershop/ppa/ubuntu precise main" && \
    DEBIAN_FRONTEND=noninteractive apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y ruby2 && \
    apt-get clean -y && \
    rm -rf /var/cache/apt/archives/* /var/lib/apt/lists/*

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
RUN service postgresql start && \
	su postgres -c 'createdb --encoding unicode --template template0 ks_test' && \
	su postgres -c 'createuser --superuser root' && \
	(/usr/sbin/mysqld --skip-grant-tables &) && \
	mysqladmin --silent --wait=30 ping && \
	mysqladmin create ks_test && \
	BUNDLE_GEMFILE=../test/Gemfile bundle install --path ~/gems && \
	ls -al /tmp/build && \
	CTEST_OUTPUT_ON_FAILURE=1 POSTGRESQL_DATABASE_USERNAME=root make test
