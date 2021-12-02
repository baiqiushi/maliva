#!/usr/bin/env bash

# collect selectivities for queries on 600k sample table for query_estimator estimating query times online
python3 -u ../core/smart_collect_queries_sels.py -ds "nyc" -if ./queries.csv -t nyc_600k 2>&1 | tee collect_sel_nyc_600k_queries.log

# collect selectivities for queries on 1500k sample table for query_estimator training
python3 -u ../core/smart_collect_queries_sels.py -ds "nyc" -if ./queries.csv -t nyc_1500k 2>&1 | tee collect_sel_nyc_1500k_queries.log
