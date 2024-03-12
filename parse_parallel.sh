#!/bin/bash

years=(2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019)
months=(01 02 03 04 05 06 07 08 09 10 11 12)
log_file="error_log.txt"

total_jobs=$(( ${#years[@]} * ${#months[@]} ))
completed_jobs=0

# Function to handle errors
handle_error() {
    local year="$1"
    local month="$2"
    local error_message="$3"
    echo "Error occurred for year: $year, month: $month"
    echo "Error message: $error_message"
    echo "Year: $year, Month: $month, Error: $error_message" >> "$log_file"
}

# Clear previous log file
> "$log_file"


for year in "${years[@]}"; do
    for month in "${months[@]}"; do
        file_path="/${year}_${month}_parsed.txt"
        if [ -s "$file_path" ]; then
            echo "Skipping job for year: $year, month: $month. File $file_path already exists and is not empty."
        else
            python parse_reddit.py "$year" "$month" 2>> "$log_file" &
            echo "Started job for year: $year, month: $month"
        fi
        completed_jobs=$((completed_jobs + 1))
        echo "Completed $completed_jobs/$total_jobs jobs"
    done
done

# Wait for all instances to finish
wait

# Print error log, if any
if [[ -s "$log_file" ]]; then
    echo "Errors occurred. Check $log_file for details."
else
    echo "All jobs completed successfully."
fi
