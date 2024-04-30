#!/bin/bash


# Clean and make the project
cd /mnt/nvme/project/code
make clean
make

# Create the test directory
dirname="test50G"
cd /mnt/nvme
mkdir -p $dirname
cd $dirname

# Remove old files
rm ./input*
rm ./output*
rm ./trace*
rm ./ExternalSort.exe

# Copy the executable to the test directory
cp /mnt/nvme/project/code/ExternalSort.exe .
cp /mnt/nvme/project/code/track.sh .
cp /mnt/nvme/project/code/plot.py .
cp -r /mnt/nvme/project/code/.vscode/ .

# Run small tests
./ExternalSort.exe -c 53000000 -s 1024 -v -o trace-c53000000-s1024 &
sleep 1

# Run the tracking script
./track.sh memcpu_50G.csv


