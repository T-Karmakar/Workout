#!/bin/bash

# Output file path
OUTPUT_FILE="/desired/location/dataframe_output.txt"

# Read input from Scala application and append to file
while IFS= read -r line; do
    echo "$line" >> "$OUTPUT_FILE"
done
