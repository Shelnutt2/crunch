#!/usr/bin/env bash

set -e -x
mkdir tmp
shopt -s extglob
mv !(tmp) tmp # Move everything but tmp
wget https://downloads.mariadb.org/interstitial/mariadb-10.2.10/source/mariadb-10.2.10.tar.gz \
&& tar xf mariadb-10.2.10.tar.gz \
&& mv tmp mariadb-10.2.10/storage/crunch \
&& cd mariadb-10.2.10 \
&& mkdir build \
&& cd build \
&& cmake -GNinja -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_MROONGA=NO -DPLUGIN_SPIDER=NO -DPLUGIN_SPHINX=NO -DPLUGIN_FEDERATED=NO -DPLUGIN_FEDERATEDX=NO -DPLUGIN_CONNECT=NO -DCMAKE_BUILD_TYPE=Debug .. \
&& ninja \
&& if ! ./mysql-test/mysql-test-run.pl  --suite=crunch --debug; then cat ./mysql-test/var/log/mysqld.1.err && false; fi;
cat ./mysql-test/var/log/mysqld.1.err
set +e +x
