#!/usr/bin/env bash

# time_budget
tb=3.0

# unit_cost
uc=0.04

# test queries
python3 -u ../core/smart_evaluate_dqn.py -d 3 \
                                         -mf dqn_tb${tb}_uc${uc}.v0.model \
                                         -lf labeled_queries_tb${tb}_test.csv \
                                         -tb ${tb} \
                                         -uc ${uc} \
                                         -ef labeled_queries_tb${tb}_test_evaluated_uc${uc}.v0.csv \
                                         -v 0
