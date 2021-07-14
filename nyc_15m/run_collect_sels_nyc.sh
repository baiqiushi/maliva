#!/usr/bin/env bash

# collect selectivities for queries_nyc on 600k sample table for query_estimator estimating query times online
python3 -u ../smart_collect_sel_queries_nyc.py -if ./queries_nyc.csv -t nyc_600k 2>&1 | tee collect_sel_nyc_600k_queries_nyc.log

# collect selectivities for queries_nyc on 1500k sample table for query_estimator training
python3 -u ../smart_collect_sel_queries_nyc.py -if ./queries_nyc.csv -t nyc_1500k 2>&1 | tee collect_sel_nyc_1500k_queries_nyc.log
