#!/bin/bash


# Clean and make the project
cd /mnt/nvme/project/code
make clean
make

# Create the test directory
cd /mnt/nvme
mkdir -p smalltests
cd smalltests

# Remove old files
rm ./input*
rm ./output*
rm ./trace*
rm ./ExternalSort.exe

# Copy the executable to the test directory
cp /mnt/nvme/project/code/ExternalSort.exe .
cp -r /mnt/nvme/project/code/.vscode/ .

# Run small tests
./ExternalSort.exe -c 0 -s 1024 -v -o trace-c0-s1024
./ExternalSort.exe -c 1 -s 1024 -v -o trace-c1-s1024
./ExternalSort.exe -c 3 -s 1024 -v -o trace-c3-s1024
./ExternalSort.exe -c 7 -s 1024 -v -o trace-c3-s1024
./ExternalSort.exe -c 10 -s 1024 -v -o trace-c10-s1024
./ExternalSort.exe -c 29 -s 1024 -v -o trace-c29-s1024
./ExternalSort.exe -c 100 -s 1024 -v -o trace-c100-s1024
./ExternalSort.exe -c 576 -s 1024 -v -o trace-c576-s1024
./ExternalSort.exe -c 1000 -s 1024 -v -o trace-c1000-s1024


