#!/bin/bash

tar -xzf tolldata.tgz

cut -d"," -f1-4 vehicle-data.csv > csv_data.csv

cut -f5-7 tollplaza-data.tsv | tr "\t" "," > tsv_data.csv

cut -c59-67 payment-data.txt | tr " " "," > fixed_width_data.csv

paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv

awk '$5 = toupper($5)' < extracted_data.csv > transformed_data.csv