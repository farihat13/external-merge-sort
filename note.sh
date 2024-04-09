#!/bin/bash

if [ "$1" == "tareq" ]; then
    git config user.name "Md. Tareq Mahmood"
    git config user.email "tareq.py@gmail.com"
elif [ "$1" == "fariha" ]; then
    git config user.name "Fariha Tabassum Islam"
    git config user.email  "fariha.t13@gmail.com"
else
    echo "Invalid user"
fi