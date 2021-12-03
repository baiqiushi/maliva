#!/usr/bin/env bash

# train query estimator using sel of nyc_1500k
python3 -u ../core/smart_train_query_estimator.py -d 3 \
                                                  -sf sel_nyc_1500k_queries.csv \
                                                  -lf labeled_queries.csv \
                                                  -op . \
                                                  2>&1 | tee train_query_estimator_nyc_1500k.log
