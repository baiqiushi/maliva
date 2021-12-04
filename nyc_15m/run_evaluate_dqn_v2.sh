#!/usr/bin/env bash

# time_budget
tb=3.0

# sample_table
st='600k'

# test queries
python3 -u ../core/smart_evaluate_dqn.py -d 3 \
                                         -mf dqn_tb${tb}_${st}.v2.model \
                                         -lf labeled_queries_tb${tb}_test.csv \
                                         -tb ${tb} \
                                         -ef labeled_queries_tb${tb}_test_evaluated_${st}.v2.csv \
                                         -v 2 \
                                         -llsf labeled_sel_nyc_${st}_queries.csv \
                                         -lsqf sel_nyc_${st}_queries.csv \
                                         -scf sel_queries_costs.csv \
                                         -qmp .
