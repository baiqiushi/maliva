#!/usr/bin/env bash

# collect selectivities for queries_nyc
python3 -u ../smart_collect_sel_queries_nyc.py -if ./queries_nyc.csv -t nyc_150k 2>&1 | tee collect_sel_nyc_150k_queries_nyc.log
python3 -u ../smart_collect_sel_queries_nyc.py -if ./queries_nyc.csv -t nyc_600k 2>&1 | tee collect_sel_nyc_600k_queries_nyc.log

# use 1500k sample table sels for query_estimator training
python3 -u ../smart_collect_sel_queries_nyc.py -if ./queries_nyc.csv -t nyc_1500k 2>&1 | tee collect_sel_nyc_1500k_queries_nyc.log
