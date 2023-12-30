#!/bin/bash
# This Bash Script doing job to do extract, transform
# and load batch data from multiple source into single file

# Unzip data
tar -xzf tolldata.tgz

# Extract data from csv file
cut -d"," -f1-4 vehicle-data.csv > csv_data.csv

# Extract data from tsv file
cut -f5-7 tollplaza-data.tsv | tr "\t" "," > tsv_data.csv

# Extract data from fixed width file
cut -c59-67 payment-data.txt | tr " " "," > fixed_width_data.csv

# Consolidate data
paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv

# Transform data
awk '$5 = toupper($5)' < extracted_data.csv > transformed_data.csv