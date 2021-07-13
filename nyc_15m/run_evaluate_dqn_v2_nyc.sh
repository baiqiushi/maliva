#!/usr/bin/env bash

# time_budget
tb=3.0

# sample_table
st='600k'

# test queries
python3 -u ../smart_evaluate_dqn_nyc.py -mf dqn_tb${tb}_${st}.v2.model \
                                        -lf labeled_queries_nyc_tb${tb}_test.csv \
                                        -tb ${tb} \
                                        -ef labeled_queries_nyc_tb${tb}_test_evaluated_${st}.v2.csv \
                                        -v 2 \
                                        -llsf labeled_sel_nyc_${st}_queries_nyc.csv \
                                        -lsqf sel_nyc_${st}_queries_nyc.csv \
                                        -scf sel_queries_costs_nyc.csv \
                                        -qmp .

# train queries
python3 -u ../smart_evaluate_dqn_nyc.py -mf dqn_tb${tb}_${st}.v2.model \
                                        -lf labeled_queries_nyc_tb${tb}_train.csv \
                                        -tb ${tb} \
                                        -ef labeled_queries_nyc_tb${tb}_train_evaluated_${st}.v2.csv \
                                        -v 2 \
                                        -llsf labeled_sel_nyc_${st}_queries_nyc.csv \
                                        -lsqf sel_nyc_${st}_queries_nyc.csv \
                                        -scf sel_queries_costs_nyc.csv \
                                        -qmp .
