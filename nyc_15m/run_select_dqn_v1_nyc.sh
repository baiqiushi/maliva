#!/usr/bin/env bash

# time_budget
tb=3.0

printf -- "----------------------------------------------------------------------\n"
printf "  Start experiment selecting DQN v1 for time_budget ${tb} on nyc_15m\n"
date
printf -- "----------------------------------------------------------------------\n"

# sample_table
st='150k'
printf "==========> Sample Table = ${st}\n"

python3 -u ../smart_select_dqn_nyc.py -tf labeled_queries_nyc_tb${tb}_train.csv \
                                      -vf labeled_queries_nyc_tb${tb}_validate.csv \
                                      -tb ${tb} \
                                      -nr 100 \
                                      --batch_size 1024 \
                                      --eps_decay 0.001 \
                                      --memory_size 1000000 \
                                      -mf dqn_tb${tb}_${st}.v1.model \
                                      -v 1 \
                                      -tr \
                                      -nt 5 \
                                      -tllsf labeled_sel_nyc_150k_queries_nyc.csv \
                                      -vllsf labeled_sel_nyc_150k_queries_nyc.csv \
                                      -tlsqf sel_nyc_150k_queries_nyc.csv \
                                      -vlsqf sel_nyc_150k_queries_nyc.csv \
                                      -scf sel_queries_costs_nyc.csv \
                                      -qmp . \
                                      -sp 0 \
                                      2>&1 | tee select_dqn_tb${tb}_${st}.v1.log &

printf -- "----------------------------------------------------------------------\n"
printf "  End experiment selecting DQNs v1 for time_budget ${tb} on nyc_15m\n"
date
printf -- "----------------------------------------------------------------------\n"
