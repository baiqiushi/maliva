#!/usr/bin/env bash

#### label selectivity queries on sample tables for queries_nyc
python3 -u ../smart_label_sel_queries_nyc.py -if ./queries_nyc.csv -run 3 -t nyc_600k 2>&1 | tee label_sel_nyc_600k_queries_nyc.log
