#!/bin/bash

# Define the output file
output_file="dummy_data.txt"

# Define the number of rows
num_rows=6666600

# Create or empty the output file if it exists
> $output_file

# Generate and write data to the file
for ((i=1; i<=num_rows; i++))
do
  echo "record_$i|some_random_text_$i|sample string|more rows|data" >> $output_file

  # Optional: Print progress every 10,000 rows
  if (( i % 10000 == 0 )); then
    echo "$i rows written..."
  fi
done

echo "Data generation completed. $num_rows rows written to $output_file."