import argparse
import config
import csv
import os.path
import progressbar
import time
from postgresql import PostgreSQL
from smart_util import Util


###########################################################
#  smart_collect_sel_queries_nyc.py
#
# Purpose:
#   collect all possible combination of selectivities for given queries
#
# Arguments:
#   -if / --in_file        input file that holds the queries
#   -t  / --table          table name on which to collect selectivities
#
# Dependencies:
#   python3.6+
#   pip install progressbar
#
# Requirements:
#   TODO - generalize to other DBs
#   1) PostgreSQL 9.6+
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
#   3) input queries csv file:
#      id, start_time, end_time, trip_distance_start, trip_distance_end, lng0, lat0, lng1, lat1
#      ...
#
# Output:
#   all possible filtering combinations of selectivities for each query:
#     filtering on [pickup_datetime, trip_distance, pickup_coordinates] dimensions
#     in combination order of 3 bits decimal: (1 - on / 0 - not on),
#       001, 010, 011, 100, 101, 110, 111.
#     example:
#       001 - selectivity of filtering on (pickup_coordinates)
#       101 - selectivity of filtering on (pickup_datetime & pickup_coordinates)
#   format (csv):
#     id, start_time, end_time, trip_distance_start, trip_distance_end, lng0, lat0, lng1, lat1,
#     sel(001), sel(010), sel(011), sel(100), sel(101), sel(110), sel(111)
#     ...
#   file name: sel_[table]_[in_file]
#
###########################################################


# TODO - Move this function inside each DB's adapter
# collect size of given table
# @param _db - handle to database util
# @param _table - table name to collect size of
# @return - int, size of the table
def size_table(_db, _table):

    sql = "SELECT count(1) " \
          "  FROM " + _table + ""

    size = _db.query(sql)  # [(1,)]
    size = size[0][0]
    return int(size)


# TODO - Move this function inside each DB's adapter
# collect selectivity for given filtering combination on given query
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
# @param _table - table name on which to collect selectivity
# @param _table_size - the size of the table provided above
# @return - float, selectivity [0, 1] of given filtering combination on given query
def sel_query(_db, _query, _fc, _table, _table_size):

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

    sel = _db.query(sql)  # [(1,)]
    sel = sel[0][0]
    return float(sel) / float(_table_size)


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Collect selectivities for benchmark queries.")
    parser.add_argument("-if", "--in_file", help="in_file: input file that holds the queries", required=True)
    parser.add_argument("-t", "--table", help="table: table name on which to collect selectivities", required=True)
    args = parser.parse_args()

    in_file = args.in_file
    table = args.table

    # initialize DB handle
    postgresql = PostgreSQL(
        config.PostgresConf.hostname,
        config.PostgresConf.username,
        config.PostgresConf.password,
        config.NYC.database
    )

    # 0. collect the size of the table provided
    table_size = size_table(postgresql, table)
    print("size of table [" + table + "] = " + str(table_size))

    print("start collecting selectivities for queries ...")

    # 1. read queries into memory
    queries = Util.load_queries_file_nyc(in_file)
    print("loaded ", len(queries), " queries into memory.")

    # extract path and filename from in_file
    in_path, in_filename = os.path.split(in_file)

    # 2. collect selectivities queries
    print("start running probing queries against DB ...")
    start = time.time()
    # show progress bar
    bar = progressbar.ProgressBar(maxval=len(queries),
                                      widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()
    # loop queries
    for index, query in enumerate(queries):
        # loop possible filtering combinations (001 ~ 111)
        for fc in range(1, 8):
            query["sel_" + str(fc)] = sel_query(postgresql, query, fc, table, table_size)
        bar.update(index + 1)
    bar.finish()
    end = time.time()
    print("running probing queries against DB is done, time: " + str(end - start) + " seconds.")

    # 3. write queries selectivities to output file named sel_[table]_[in_file]
    outfile = in_path + "/sel_" + table + "_" + in_filename
    with open(outfile, "w") as csv_out:
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
                   query["sel_1"],
                   query["sel_2"],
                   query["sel_3"],
                   query["sel_4"],
                   query["sel_5"],
                   query["sel_6"],
                   query["sel_7"]]
            csv_writer.writerow(row)

