#!/usr/bin/env bash

python3 -u ../smart_label_queries_nyc.py -if queries_nyc.csv 2>&1 | tee label_queries_nyc.log
