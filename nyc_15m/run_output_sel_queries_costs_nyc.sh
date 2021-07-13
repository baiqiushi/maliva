#!/usr/bin/env bash

printf "==================================================================================\n"
printf "          Sel queries costs\n"
printf "==================================================================================\n"
printf "\"Sample Size\",    \"sel_1\",    \"sel_2\",    \"sel_3\",    \"sel_4\",    \"sel_5\",    \"sel_6\",    \"sel_7\"\n"
python3 ../smart_output_sel_queries_costs_nyc.py -lsf labeled_sel_nyc_600k_queries_nyc.csv -xt 600k
printf "\n\n"

# write to csv file
printf "\"Sample Size\",    \"sel_1\",    \"sel_2\",    \"sel_3\",    \"sel_4\",    \"sel_5\",    \"sel_6\",    \"sel_7\"\n" > sel_queries_costs_nyc.csv
python3 ../smart_output_sel_queries_costs_nyc.py -lsf labeled_sel_nyc_600k_queries_nyc.csv -xt 600k >> sel_queries_costs_nyc.csv
