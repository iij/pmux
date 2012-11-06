# Pmux: pipeline multiplexer

Pmux is a lightweight file-based MapReduce system, written in Ruby.
Applying the philosophy of Unix pipeline processing to distributed
computing on a GlusterFS cluster, pmux provides a tool capable of
handling large amounts of data stored in files.

## Requirements
 * ruby 1.8.7, 1.9.2 or higher
 * msgpack-rpc
 * net-ssh, net-scp
 * gflocator
 * GlusterFS 3.3.0 or higher, native client (FUSE)

## Install

on all GlusterFS server nodes:

    gem install pmux

on the GlusterFS client node

    gem install pmux
    gem install gflocator
    sudo gflocator

Each of GlusterFS server nodes, SSH with authentication key is required.

## Usage

show status

    $ pmux --status
    host0.example.com: pmux 0.1.0, num_cpu=8, ruby 1.9.3

show status of remote machine

    $ pmux --status -h host1.example.com
    host1.example.com: pmux 0.1.0, num_cpu=2, ruby 1.9.3

distributed grep

    $ pmux --mapper="grep PATTERN" /glusterfs/xxx/*.log
