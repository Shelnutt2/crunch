#!/bin/bash

curl -O https://capnproto.org/capnproto-c++-0.6.1.tar.gz &&
tar zxf capnproto-c++-0.6.1.tar.gz &&
cd capnproto-c++-0.6.1 &&
./configure &&
make -j6 check &&
sudo make install &&
cd ../ &&
sudo rm -rf capnproto-c++-0.6.1