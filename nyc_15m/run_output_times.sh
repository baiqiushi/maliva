#!/usr/bin/env bash

# time_budget
tb=3.0

printf "==================================================================================\n"
printf "        Time (seconds) on labeled_queries_tb${tb}_test_gn[1~4].csv\n"
printf "==================================================================================\n"

printf -- "----------------------------------------------------------------------------------\n"
printf "    Using Accurate-QTE \n"
printf -- "----------------------------------------------------------------------------------\n"
printf "\"Number of viable plans\",    \"Baseline\",    \"MDP (Accurate-QTE) total\",    \"MDP (Accurate-QTE) query\",    \"MDP (Accurate-QTE) plan\"\n"
for gn in 1 2 3 4; do
  # unit_cost
  uc=0.04
  python3 ../core/smart_output_best_qe_results.py -d 3 \
                                                  -lf labeled_queries_tb${tb}_test_gn${gn}.csv \
                                                  -lef labeled_queries_tb${tb}_test_evaluated_uc${uc}.v0.csv \
                                                  -tb ${tb} \
                                                  -xt ${gn}
done
printf "\n\n"
printf -- "----------------------------------------------------------------------------------\n"
printf "    Using Approximate-QTE \n"
printf -- "----------------------------------------------------------------------------------\n"
printf "\"Number of viable plans\",    \"Baseline\",    \"MDP (Approximate-QTE) total\",    \"MDP (Approximate-QTE) query\",    \"MDP (Approximate-QTE) plan\"\n"
for gn in 1 2 3 4; do
  # sample_table
  st='600k'
  python3 ../core/smart_output_best_qe_results.py -d 3 \
                                                  -lf labeled_queries_tb${tb}_test_gn${gn}.csv \
                                                  -lef labeled_queries_tb${tb}_test_evaluated_${st}.v2.csv \
                                                  -tb ${tb} \
                                                  -xt ${gn}
done
printf "\n"
