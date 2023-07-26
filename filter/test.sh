#! /usr/bin/env bash

WORKING_DIR=/Users/mike/Code/github.com/mna/pigeon
PIGEON=${WORKING_DIR}/bin/pigeon
SPC_DIR=.

#echo $'package main\n' > ${SPC_DIR}/filter.go

cat ${SPC_DIR}/filter.peg | ${PIGEON} > ${SPC_DIR}/filter.go

go test -v
