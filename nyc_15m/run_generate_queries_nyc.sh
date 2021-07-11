#!/usr/bin/env bash

python3 -u ../smart_generate_queries_nyc.py -n 400 -of queries_nyc.csv 2>&1 | tee generate_queries_nyc.log
