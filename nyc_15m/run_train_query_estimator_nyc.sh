#!/usr/bin/env bash

# train query estimator using sel of nyc_1500k
python3 -u ../smart_train_query_estimator_nyc.py -sf sel_nyc_1500k_queries_nyc.csv \
                                                 -lf labeled_queries_nyc.csv \
                                                 -op . \
                                                 2>&1 | tee train_query_estimator_1500k_queries_nyc.log
