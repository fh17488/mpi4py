#!/bin/bash

echo "Running post-install script"
sudo yum install -y python36 python36-pip python36-devel
sudo pip-3.6 install pymysql
sudo pip-3.6 install numpy
sudo env MPICC=/opt/amazon/openmpi/bin/mpicc pip-3.6 install mpi4py
