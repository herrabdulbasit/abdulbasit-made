#!/bin/bash

if command -v pip3 &> /dev/null
then
    PIP_COMMAND="pip3"
elif command -v pip &> /dev/null
then
    PIP_COMMAND="pip"
else
    echo "Neither pip nor pip3 is installed. Please install Python pip."
    exit 1
fi

# Use the detected pip command to install packages
$PIP_COMMAND install pandas sqlalchemy requests opendatasets fuzzywuzzy

python3 "$(dirname "$0")/pipeline.py"