#!/usr/bin/env bash

python3 -u ../core/smart_generate_queries_nyc.py -n 400 -of queries.csv 2>&1 | tee generate_queries.log
