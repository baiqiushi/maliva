#!/usr/bin/env bash

# time_budget
tb=3.0

printf -- "----------------------------------------------------------------------\n"
printf "  Start experiment selecting DQN v2 for time_budget ${tb} on nyc_15m\n"
date
printf -- "----------------------------------------------------------------------\n"

# sample_table
st='600k'
printf "==========> Sample Table = ${st}\n"

python3 -u ../core/smart_select_dqn.py -d 3 \
                                       -tf labeled_queries_tb${tb}_train.csv \
                                       -vf labeled_queries_tb${tb}_validate.csv \
                                       -tb ${tb} \
                                       -nr 100 \
                                       --batch_size 32 \
                                       --eps_decay 0.001 \
                                       --memory_size 1000 \
                                       -mf dqn_tb${tb}_${st}.v2.model \
                                       -v 2 \
                                       -tr \
                                       -nt 5 \
                                       -tllsf labeled_sel_nyc_${st}_queries.csv \
                                       -vllsf labeled_sel_nyc_${st}_queries.csv \
                                       -tlsqf sel_nyc_${st}_queries.csv \
                                       -vlsqf sel_nyc_${st}_queries.csv \
                                       -scf sel_queries_costs.csv \
                                       -qmp . \
                                       2>&1 | tee select_dqn_tb${tb}_${st}.v2.log

printf -- "----------------------------------------------------------------------\n"
printf "  End experiment selecting DQNs v2 for time_budget ${tb} on nyc_15m\n"
date
printf -- "----------------------------------------------------------------------\n"
