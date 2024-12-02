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

# Install required packages
$PIP_COMMAND install pytest pandas sqlalchemy requests opendatasets fuzzywuzzy python-Levenshtein

# Run the tests
pytest -v spec.py 