#!/usr/bin/env bash

python3 -u ../core/smart_label_queries.py -ds "nyc" -if queries.csv 2>&1 | tee label_queries.log
