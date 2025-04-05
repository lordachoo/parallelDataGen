#!/bin/bash
# $0 : This
# $1 : Output Directory
# This is an example. localhost can be changed to be different nodes that have access to a shared storage area. Needs to be passwordless ssh

ssh localhost "/usr/bin/python3 /mmfs1/data/scripts/parallelDataGen/parallelDataGen -n 1000 -s 10 -t 4 --node-id 0 --node-count 3 $1" & 
ssh localhost "/usr/bin/python3 /mmfs1/data/scripts/parallelDataGen/parallelDataGen -n 1000 -s 10 -t 4 --node-id 1 --node-count 3 $1" &
ssh localhost "/usr/bin/python3 /mmfs1/data/scripts/parallelDataGen/parallelDataGen -n 1000 -s 10 -t 4 --node-id 2 --node-count 3 $1" &
