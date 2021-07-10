import argparse
import config
import csv
import math
import os.path
import progressbar
import random
import time
from postgresql import PostgreSQL
from smart_util import Util


###########################################################
#  smart_label_sel_queries_nyc.py
#
# Purpose:
#   label running time of all selectivity probing queries for given queries
#
# Arguments:
#   -if  / --in_file        input file that holds the queries
#   -run / --run            run how many times (default: 3)
#   -t   / --table          table name on which to run the selectivity probing queries
#
# Dependencies:
#   python3.6+
#   pip install progressbar
#
# Requirements:
#   TODO - generalize to other DBs
#   1) PostgreSQL 9.6+
#   2) schema:
#        nyc_500m (
#            id                        bigint primary key,
#        [x] pickup_datetime           timestamp,
#            dropoff_datetime          timestamp,
#            passenger_count           int,
#        [x] trip_distance             numeric,
#        [x] pickup_coordinates        point,
#            dropoff_coordinates       point,
#            payment_type              text,
#            fare_amount               numeric,
#            extra                     numeric,
#            mta_tax                   numeric,
#            tip_amount                numeric,
#            tolls_amount              numeric,
#            improvement_surcharge     numeric,
#            total_amount              numeric
#        )
#      indexes:
#        btree (pickup_datetime)
#        btree (trip_distance)
#        gist (pickup_coordinates)
#   3) input queries csv file:
#      id, start_time, end_time, trip_distance_start, trip_distance_end, lng0, lat0, lng1, lat1
#      ...
#
# Output:
#   query performance on different selectivity probing queries for each query:
#     selectivity of filtering combinations on [pickup_datetime, trip_distance, pickup_coordinates] dimensions
#     in combination order of 3 bits decimal: (1 - on / 0 - not on),
#       001, 010, 011, 100, 101, 110, 111.
#     example:
#       001 - probing query for selectivity of filtering on (pickup_coordinates)
#       101 - probing query for selectivity of filtering on (pickup_datetime & pickup_coordinates)
#   format (csv):
#     id, start_time, end_time, trip_distance_start, trip_distance_end, lng0, lat0, lng1, lat1,
#     time(001), time(010), time(011), time(100), time(101), time(110), time(111)
#     ...
#   file name: labeled_sel_[table]_[in_file]
#              labeled_sel_std_[table]_[in_file]
#
###########################################################


# TODO - Move this function inside each DB's adapter
# time probing query of selectivity for given filtering combination on given query
# @param _db - handle to database util
# @param _query - {id: 1,
#                  start_time: 2010-01-30 23:31:00,
#                  end_time; 2010-01-31 23:31:00,
#                  trip_distance_start: 0.7,
#                  trip_distance_end: 5.9
#                  lng0: -73.996117,
#                  lat0: 40.741193,
#                  lng1: -73.981511,
#                  lat1: 40.763931}
# @param _fc - 1 ~ 7, represents selectivity of filtering combination on dimensions
#                     [pickup_datetime, trip_distance, pickup_coordinates]
#                e.g., 001 - selectivity of filtering on (pickup_coordinates)
#                      101 - selectivity of filtering on (pickup_datetime & pickup_coordinates)
# @param _table - table name on which to run the selectivity probing query
# @return - float, running time of probing query of selectivity for given filtering combination on given query
def time_sel_query(_db, _query, _fc, _table):

    sql = "SELECT count(1) " \
          "  FROM " + _table + " t " \
          " WHERE 1=1"

    # set up filtering on sample table if filtering combination is 1 ~ 7
    if 1 <= _fc <= 7:
        # translate the _fc number into binary bits array
        # e.g., 6 -> 0,0,0,0,0,1,1,0
        fc_bits = [int(x) for x in '{:08b}'.format(_fc)]

        # pickup_datetime
        if fc_bits[5] == 1:
            sql = sql + " AND t.pickup_datetime between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'"

        # trip_distance
        if fc_bits[6] == 1:
            sql = sql + " AND t.trip_distance between " + _query["trip_distance_start"] + " and " + \
                  _query["trip_distance_end"]

        # pickup_coordinates
        if fc_bits[7] == 1:
            sql = sql + " AND t.pickup_coordinates <@ box '((" + _query["lng0"] + "," + _query["lat0"] + "),(" + \
                  _query["lng1"] + "," + _query["lat1"] + "))'"

    start = time.time()
    _db.query(sql)
    end = time.time()
    return end - start


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Label selectivity probing queries for benchmark queries.")
    parser.add_argument("-if", "--in_file", help="in_file: input file that holds the queries", required=True)
    parser.add_argument("-run", "--run", help="run: run how many times", required=False, type=int, default=3)
    parser.add_argument("-t", "--table", help="table: table name on which to run the selectivity probing queries",
                        required=True)
    args = parser.parse_args()

    in_file = args.in_file
    number_of_runs = args.run
    table = args.table

    # initialize DB handle
    postgresql = PostgreSQL(
        config.PostgresConf.hostname,
        config.PostgresConf.username,
        config.PostgresConf.password,
        config.NYC.database
    )

    print("start labeling selectivity probing queries ...")

    # 1. read queries into memory
    queries = Util.load_queries_file_nyc(in_file)
    print("loaded ", len(queries), " queries into memory.")
    # Build map <id, query>
    queries_map = {}
    for query in queries:
        queries_map[query["id"]] = query

    # extract path and filename from in_file
    in_path, in_filename = os.path.split(in_file)

    # 2. run queries to collect timings for selectivity probing queries
    #    - run [number_of_runs] in total, for each run:
    #        - enumerate all tuples of (query_id, fc_id) into a list
    #        - shuffle the list,
    #        - loop the list, for each combination (query_id, fc_id):
    #            - run probing query of selectivity for filtering combination (fc_id) for query[query_id]
    print("start running queries against DB.")
    runs = []
    start_runs = time.time()
    for ri in range(0, number_of_runs):
        print("---------- run " + str(ri + 1) + " ----------")
        # each run is a map of query["id"] -> [0.0, 0.0, ..., 0.0] (7 zeros)
        list_of_query_ids = [query_id for query_id in sorted(queries_map)]
        run = {}
        for query_id in list_of_query_ids:
            run[query_id] = [0.0] * 7
        # populate the combination list
        queries_fcs = []
        for query_id in sorted(queries_map):
            for fc_id in range(1, 8):
                combination = (query_id, fc_id)
                queries_fcs.append(combination)

        # shuffle the combination list
        random.shuffle(queries_fcs)

        # run combination list
        start_run = time.time()
        # show progress bar
        bar = progressbar.ProgressBar(maxval=len(queries_fcs),
                                      widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
        bar.start()
        qp = 0
        for query_fc in queries_fcs:
            query = queries_map[query_fc[0]]
            fc = query_fc[1]
            run[query["id"]][fc - 1] = time_sel_query(postgresql, query, fc, table)
            qp += 1
            bar.update(qp)
        bar.finish()

        end_run = time.time()
        print("run [", ri + 1, "] is done, time: " + str(end_run - start_run) + " seconds.")
        runs.append(run)
        # write each run to a csv file for archiving
        archive_out_file = in_path + "/" + in_filename + ".sel." + str(ri+1)
        with open(archive_out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query_id in sorted(run):
                row = [query_id]
                row.extend(run[query_id])
                csv_writer.writerow(row)
    end_runs = time.time()
    print("all ", number_of_runs, " runs are done, time: " + str(end_runs - start_runs) + " seconds.")

    # 4. compute avarage and standard deviation of timings of each query each fc in all runs
    for query in queries:
        for fc in range(1, 8):
            sum_of_runs = 0.0
            for run in runs:
                sum_of_runs += run[query["id"]][fc - 1]
            avg_of_runs = sum_of_runs / number_of_runs
            sqr_sum_diffs = 0.0
            for run in runs:
                sqr_sum_diffs += (run[query["id"]][fc - 1] - avg_of_runs) ** 2
            std_of_runs = math.sqrt(sqr_sum_diffs / len(runs))
            query["time_" + str(fc)] = avg_of_runs
            query["time_" + str(fc) + "_std"] = std_of_runs

    # 5. write labeled sel queries to output file named labeled_sel_[table]_[in_file]
    #    & write std of sel queries to labeled_sel_std_[table]_[in_file]
    queries = sorted(queries, key=lambda k: k["id"])
    out_avg_file = in_path + "/labeled_sel_" + table + "_" + in_filename
    with open(out_avg_file, "w") as csv_out:
        csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for query in queries:
            row = [query["id"],
                   query["start_time"],
                   query["end_time"],
                   query["trip_distance_start"],
                   query["trip_distance_end"],
                   query["lng0"],
                   query["lat0"],
                   query["lng1"],
                   query["lat1"],
                   query["time_1"],
                   query["time_2"],
                   query["time_3"],
                   query["time_4"],
                   query["time_5"],
                   query["time_6"],
                   query["time_7"]]
            csv_writer.writerow(row)
    out_std_file = in_path + "/labeled_sel_std_" + table + "_" + in_filename
    with open(out_std_file, "w") as csv_out:
        csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for query in queries:
            row = [query["id"],
                   query["start_time"],
                   query["end_time"],
                   query["trip_distance_start"],
                   query["trip_distance_end"],
                   query["lng0"],
                   query["lat0"],
                   query["lng1"],
                   query["lat1"],
                   query["time_1_std"],
                   query["time_2_std"],
                   query["time_3_std"],
                   query["time_4_std"],
                   query["time_5_std"],
                   query["time_6_std"],
                   query["time_7_std"]]
            csv_writer.writerow(row)

