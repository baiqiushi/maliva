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
#  smart_label_queries_nyc.py
#
# Purpose:
#   label running time of all possible hinting plans for given queries
#
# Arguments:
#   -if  / --in_file        input file that holds the queries
#   -run / --run            run how many times
#   -oo  / --original_only  tag query performance for the original plan only
#   -ho  / --hint_only      tag query performance for the hinted plans only
#   -sp  / --start_plan     tag query performance for a range of plan ids, start id
#   -ep  / --end_plan       tag query performance for a range of plan ids, end id
#
# Dependencies:
#   python3.6+
#   pip install progressbar
#
# Requirements:
#   TODO - generalize to other DBs
#   1) PostgreSQL 9.6+
#   2) schema:
#        nyc_15m (
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
#   query performance on different hinted plans: hint using/not using index on
#     [pickup_datetime, trip_distance, pickup_coordinates] dimensions
#     in combination order of 3 bits decimal: (1 - using / 0 - not using),
#       000, 001, 010, 011, 100, 101, 110, 111.
#     example:
#       000 - (special case) No hint at all, its original plan
#       101 - using pickup_datetime and pickup_coordinates indexes, not using trip_distance index
#             /*+ BitmapScan(t idx_nyc_500m_pickup_datetime idx_nyc_500m_pickup_coordinates) */
#   format (csv):
#     id, start_time, end_time, trip_distance_start, trip_distance_end, lng0, lat0, lng1, lat1,
#     time(000), time(001), time(010), time(011), time(100), time(101), time(110), time(111)
#     ...
#   file name: labeled_[in_file]
#              labeled_std_[in_file]
#
###########################################################


# TODO - Move this function inside each DB's adapter
# time given query using given plan
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
# @param _plan - 0 ~ 7, represents plan of using/not using index on dimensions
#                       [pickup_datetime, trip_distance, pickup_coordinates]
#                e.g., 000 - original query without hints
#                      001 - hint using idx_nyc_500m_pickup_coordinates index only
# @return - time (seconds) of running this query using this plan
def time_query(_db, _query, _plan):

    sql = "SELECT id, " \
          "       pickup_coordinates " \
          "  FROM " + config.NYC.table + " t " \
          " WHERE t.pickup_datetime between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'" \
          "   AND t.trip_distance between " + _query["trip_distance_start"] + " and " + _query["trip_distance_end"] + \
          "   AND t.pickup_coordinates <@ box '((" + _query["lng0"] + "," + _query["lat0"] + "),(" + _query["lng1"] + "," + _query["lat1"] + "))'"

    # generate hint if plan is 1 ~ 7
    if 1 <= _plan <= 7:
        # translate the _plan number into binary bits array
        # e.g., 6 -> 0,0,0,0,0,1,1,0
        plan_bits = [int(x) for x in '{:08b}'.format(_plan)]

        hint = "/*+ BitmapScan(t"

        # pick_datetime
        if plan_bits[5] == 1:
            hint = hint + " " + config.NYC.index_on_pickup_datetime

        # trip_distance
        if plan_bits[6] == 1:
            hint = hint + " " + config.NYC.index_on_trip_distance

        # pickup_coordinates
        if plan_bits[7] == 1:
            hint = hint + " " + config.NYC.index_on_pickup_coordinates

        hint = hint + ") */"
        sql = hint + sql

    start = time.time()
    _db.query(sql)
    end = time.time()
    return end - start


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Label benchmark queries.")
    parser.add_argument("-if", "--in_file", help="in_file: input file that holds the queries", required=True)
    parser.add_argument("-run", "--run", help="run: run how many times", required=False, type=int, default=3)
    parser.add_argument("-oo", "--original_only", help="original_only: label query performance for the original plan only", required=False, action="store_true")
    parser.add_argument("-ho", "--hint_only", help="hint_only: label query performance for the hinted plans only", required=False, action="store_true")
    parser.add_argument("-sp", "--start_plan", help="start_plan: label query performance for a range of plan ids, start id", required=False, type=int, default=0)
    parser.add_argument("-ep", "--end_plan", help="end_plan: label query performance for a range of plan ids, end id", required=False, type=int, default=7)
    args = parser.parse_args()

    in_file = args.in_file
    number_of_runs = args.run
    original_only = args.original_only
    hint_only = args.hint_only
    sp = args.start_plan
    ep = args.end_plan

    # range of plan ids to label
    plan_id_start = sp
    plan_id_end = ep
    if original_only:
        plan_id_start = 0
        plan_id_end = 0
    elif hint_only:
        plan_id_start = 1
        plan_id_end = 7

    # initialize DB handle
    postgresql = PostgreSQL(
        config.PostgresConf.hostname,
        config.PostgresConf.username,
        config.PostgresConf.password,
        config.NYC.database,
        config.PostgresConf.timeout
    )

    print("start labeling queries ...")

    # 1. read queries into memory
    queries = Util.load_queries_file_nyc(in_file)
    print("loaded ", len(queries), " queries into memory.")
    # Build map <id, query>
    queries_map = {}
    for query in queries:
        queries_map[query["id"]] = query

    # 2. read output files if it exists
    out_avg_file = "labeled_" + in_file
    if os.path.isfile(out_avg_file):
        print("[" + out_avg_file + "] exists, loading labeled avg queries into memory.")
        with open(out_avg_file, "r") as csv_in:
            csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
            for row in csv_reader:
                qid = int(row[0])
                queries_map[qid]["time_0"] = float(row[8])
                queries_map[qid]["time_1"] = float(row[9])
                queries_map[qid]["time_2"] = float(row[10])
                queries_map[qid]["time_3"] = float(row[11])
                queries_map[qid]["time_4"] = float(row[12])
                queries_map[qid]["time_5"] = float(row[13])
                queries_map[qid]["time_6"] = float(row[14])
                queries_map[qid]["time_7"] = float(row[15])
        print("[" + out_avg_file + "] loaded into memory.")

    out_std_file = "labeled_std_" + in_file
    if os.path.isfile(out_std_file):
        print("[" + out_std_file + "] exists, loading labeled std queries into memory.")
        with open(out_std_file, "r") as csv_in:
            csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
            for row in csv_reader:
                qid = int(row[0])
                queries_map[qid]["time_0_std"] = float(row[8])
                queries_map[qid]["time_1_std"] = float(row[9])
                queries_map[qid]["time_2_std"] = float(row[10])
                queries_map[qid]["time_3_std"] = float(row[11])
                queries_map[qid]["time_4_std"] = float(row[12])
                queries_map[qid]["time_5_std"] = float(row[13])
                queries_map[qid]["time_6_std"] = float(row[14])
                queries_map[qid]["time_7_std"] = float(row[15])
        print("[" + out_std_file + "] loaded into memory.")

    # 3. run queries to collect timings for plans
    #    - run [number_of_runs] in total, for each run:
    #        - enumerate all tuples of (query_id, plan_id) into a list
    #        - shuffle the list,
    #        - loop the list, for each combination (query_id, plan_id):
    #            - run query[query_id] using plan[plan_id]
    print("start running queries against DB.")
    runs = []
    start_runs = time.time()
    for ri in range(0, number_of_runs):
        print("---------- run " + str(ri + 1) + " ----------")
        # each run is a map of query["id"] -> [0.0, 0.0, ..., 0.0] (at most 8 zeros)
        list_of_query_ids = [query_id for query_id in sorted(queries_map)]
        run = {}
        for query_id in list_of_query_ids:
            run[query_id] = [0.0] * 8
        # populate the combination list
        queries_plans = []
        for query_id in sorted(queries_map):
            for plan_id in range(plan_id_start, plan_id_end + 1):
                combination = (query_id, plan_id)
                queries_plans.append(combination)

        # shuffle the combination list
        random.shuffle(queries_plans)

        # run combination list
        start_run = time.time()
        # show progress bar
        bar = progressbar.ProgressBar(maxval=len(queries_plans),
                                      widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
        bar.start()
        qp = 0
        for query_plan in queries_plans:
            query = queries_map[query_plan[0]]
            plan = query_plan[1]
            run[query["id"]][plan] = time_query(postgresql, query, plan)
            qp += 1
            bar.update(qp)
        bar.finish()

        end_run = time.time()
        print("run [", ri + 1, "] is done, time: " + str(end_run - start_run) + " seconds.")
        runs.append(run)
        # write each run to a csv file for archiving
        archive_out_file = in_file + "." + str(ri+1)
        with open(archive_out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query_id in sorted(run):
                row = [query_id]
                row.extend(run[query_id])
                csv_writer.writerow(row)
    end_runs = time.time()
    print("all ", number_of_runs, " runs are done, time: " + str(end_runs - start_runs) + " seconds.")

    # 4. compute avarage and standard deviation of timings of each query each plan in all runs
    for query in queries:
        for plan in range(0, 8):
            sum_of_runs = 0.0
            for run in runs:
                sum_of_runs += run[query["id"]][plan]
            avg_of_runs = sum_of_runs / number_of_runs
            sqr_sum_diffs = 0.0
            for run in runs:
                sqr_sum_diffs += (run[query["id"]][plan] - avg_of_runs) ** 2
            std_of_runs = math.sqrt(sqr_sum_diffs / len(runs))
            # only update output result for targeting plans
            if plan_id_start <= plan <= plan_id_end:
                query["time_" + str(plan)] = avg_of_runs
                query["time_" + str(plan) + "_std"] = std_of_runs
            # other plans init to 0.0 or keep as before
            elif "time_" + str(plan) not in query.keys():
                query["time_" + str(plan)] = 0.0
                query["time_" + str(plan) + "_std"] = 0.0

    # 5. write labeled queries to output file named labeled_[in_file] & labeled_std_[in_file]
    queries = sorted(queries, key=lambda k: k["id"])
    out_avg_file = "labeled_" + in_file
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
                   query["time_0"],
                   query["time_1"],
                   query["time_2"],
                   query["time_3"],
                   query["time_4"],
                   query["time_5"],
                   query["time_6"],
                   query["time_7"]]
            csv_writer.writerow(row)
    out_std_file = "labeled_std_" + in_file
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
                   query["time_0_std"],
                   query["time_1_std"],
                   query["time_2_std"],
                   query["time_3_std"],
                   query["time_4_std"],
                   query["time_5_std"],
                   query["time_6_std"],
                   query["time_7_std"]]
            csv_writer.writerow(row)

