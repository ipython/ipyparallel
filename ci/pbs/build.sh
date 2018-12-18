#!/bin/bash
cd /src/pbspro
./autogen.sh
./configure -prefix=/opt/pbs
make dist
mkdir /root/rpmbuild /root/rpmbuild/SOURCES /root/rpmbuild/SPECS
cp pbspro-*.tar.gz /root/rpmbuild/SOURCES
cp pbspro.spec /root/rpmbuild/SPECS
cp pbspro-rpmlintrc /root/rpmbuild/SOURCES
cd /root/rpmbuild/SPECS
rpmbuild -ba pbspro.spec
