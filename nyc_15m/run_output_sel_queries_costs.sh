#!/usr/bin/env bash

printf "==================================================================================\n"
printf "          Sel queries costs\n"
printf "==================================================================================\n"
printf "\"Sample Size\",    \"sel_1\",    \"sel_2\",    \"sel_3\",    \"sel_4\",    \"sel_5\",    \"sel_6\",    \"sel_7\"\n"
python3 ../core/smart_output_sel_queries_costs.py -d 3 -lsf labeled_sel_nyc_600k_queries.csv -xt 600k
printf "\n\n"

# write to csv file
printf "\"Sample Size\",    \"sel_1\",    \"sel_2\",    \"sel_3\",    \"sel_4\",    \"sel_5\",    \"sel_6\",    \"sel_7\"\n" > sel_queries_costs.csv
python3 ../core/smart_output_sel_queries_costs.py -d 3 -lsf labeled_sel_nyc_600k_queries.csv -xt 600k >> sel_queries_costs.csv
