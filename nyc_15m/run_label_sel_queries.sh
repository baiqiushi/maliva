#!/usr/bin/env bash

#### label selectivity queries on sample tables for queries
python3 -u ../core/smart_label_sel_queries.py -ds "nyc" -if ./queries.csv -run 3 -t nyc_600k 2>&1 | tee label_sel_nyc_600k_queries.log
