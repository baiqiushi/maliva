#!/usr/bin/env bash

# time_budget
tb=3.0

python3 -u ../core/smart_select_queries.py -d 3 \
                                           -lf labeled_queries.csv \
                                           -tb ${tb} \
                                           -sf labeled_queries_tb${tb}.csv

# total number of rows of the possible viable queries file
length=$(<"labeled_queries_tb${tb}.csv" wc -l)
echo "Total number of queries = ${length}"

# split into 3 partitions:
# left 1/3 for train
head -n $((length/3)) labeled_queries_tb${tb}.csv > labeled_queries_tb${tb}_train.csv
# middle 1/6 for validate
head -n $((length/2)) labeled_queries_tb${tb}.csv | tail -n +$((length/3+1))  > labeled_queries_tb${tb}_validate.csv
# right 1/2 for test
tail -n +$((length/2+1)) labeled_queries_tb${tb}.csv > labeled_queries_tb${tb}_test.csv

# generate test workloads with varying "# of viable plans" (good plan number)
for gn in 1 2 3 4; do
  python3 -u ../core/smart_select_queries.py -d 3 \
                                             -lf labeled_queries_tb${tb}_test.csv \
                                             -tb ${tb} \
                                             -gn ${gn} \
                                             -sf labeled_queries_tb${tb}_test_gn${gn}.csv
done
