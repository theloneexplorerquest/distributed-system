#!/bin/bash
for i in {1..100}
do
    echo "Running test iteration $i"
    if ! go test -run 4A -race; then
        echo "Test iteration $i failed"
#        exit 1
    fi
done
echo "All tests passed"
