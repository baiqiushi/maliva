#!/usr/bin/env bash

# time_budget
tb=3.0

printf -- "----------------------------------------------------------------------\n"
printf "  Start experiment selecting DQN v0 for time_budget ${tb} on nyc_15m\n"
date
printf -- "----------------------------------------------------------------------\n"

# unit_cost
uc=0.04
printf "==========> Unit cost = ${uc}\n"

python3 -u ../smart_select_dqn_nyc.py -tf labeled_queries_nyc_tb${tb}_train.csv \
                                      -vf labeled_queries_nyc_tb${tb}_validate.csv \
                                      -tb ${tb} \
                                      -uc ${uc} \
                                      -nr 100 \
                                      --batch_size 32 \
                                      --eps_decay 0.001 \
                                      --memory_size 1000 \
                                      -mf dqn_tb${tb}_uc${uc}.v0.model \
                                      -v 0 \
                                      -tr \
                                      -nt 5 \
                                      2>&1 | tee select_dqn_tb${tb}_uc${uc}.v0.log

printf -- "----------------------------------------------------------------------\n"
printf "  End experiment selecting DQN v0 for time_budget ${tb} on nyc_15m\n"
date
printf -- "----------------------------------------------------------------------\n"
