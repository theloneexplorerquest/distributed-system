#!/bin/bash

#for i in {1..100}
#do
#    echo "Running test iteration $i"
#    if ! go test -run TestBasicAgree3B -race; then
#        echo "Test iteration $i failed"
#        exit 1
#    fi
#done
#echo "All tests passed"
#
#for i in {1..100}
#do
#    echo "Running test iteration $i"
#    if ! go test -run TestRPCBytes3B -race; then
#        echo "Test iteration $i failed"
#        exit 1
#    fi
#done
#echo "All tests passed"
#
#for i in {1..100}
#do
#    echo "Running test iteration $i"
#    if ! go test -run TestFollowerFailure3B -race; then
#        echo "Test iteration $i failed"
#        exit 1
#    fi
#done
#echo "All tests passed"
#
#for i in {1..100}
#do
#    echo "Running test iteration $i"
#    if ! go test -run TestLeaderFailure3B -race; then
#        echo "Test iteration $i failed"
#        exit 1
#    fi
#done
#echo "All tests passed"

#for i in {1..100}
#do
#    echo "Running test iteration $i"
#    if ! go test -run TestFailAgree3B -race; then
#        echo "Test iteration $i failed"
#        exit 1
#    fi
#done
#echo "All tests passed"
#
#for i in {1..100}
#do
#    echo "Running test iteration $i"
#    if ! go test -run TestFailNoAgree3B -race; then
#        echo "Test iteration $i failed"
#        exit 1
#    fi
#done
#echo "All tests passed"

for i in {1..100}
do
    echo "Running test iteration $i"
#    if ! go test -run TestFigure8Unreliable3C -race; then
#        echo "Test iteration $i failed"
##        exit 1
#    fi
#    if ! go test -run TestReliableChurn3C -race; then
#        echo "Test iteration $i failed"
##        exit 1
#    fi
    if ! go test -run 3C -race; then
        echo "Test iteration $i failed"
#        exit 1
    fi
done
echo "All tests passed"
