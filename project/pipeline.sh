#!/bin/bash

# Some systems use the pip command and some use pip3. This handles that
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


$PIP_COMMAND install pandas sqlalchemy requests opendatasets fuzzywuzzy

# This is to make sure that no matter from where this script is called, this will call the pipeline.py 
# from its own directory
python3 "$(dirname "$0")/pipeline.py"