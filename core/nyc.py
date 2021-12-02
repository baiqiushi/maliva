import csv
import os.path
import time
from smart_util import Util


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
class NYC:
    database = "nyc"
    table = "nyc_15m"
    table_size = 14863778
    index_on_pickup_datetime = "idx_nyc_15m_pickup_datetime"
    index_on_trip_distance = "idx_nyc_15m_trip_distance"
    index_on_pickup_coordinates = "idx_nyc_15m_pickup_coordinates"
    min_pickup_datetime = "2010-01-01 00:00:00"
    max_pickup_datetime = "2010-01-31 23:59:59"
    max_pickup_datetime_zoom = 14  # (2010-01-31 - 2010-01-01) = 31 days = 8928 (x5 mins), log2(8928) = 13.12
    min_trip_distance = 0
    max_trip_distance = 50
    max_trip_distance_zoom = 9  # (50 - 0) * 10 = 500 (x0.1 mi), log2(500) = 8.96
    min_pickup_coordinates_lng = -74.35
    min_pickup_coordinates_lat = 40.45
    max_pickup_coordinates_lng = -73.65
    max_pickup_coordinates_lat = 40.95
    max_pickup_coordinates_zoom = 11
    indexes = [
        index_on_pickup_datetime,
        index_on_trip_distance,
        index_on_pickup_coordinates
    ]

    # time given query using given plan
    # @param _db - handle to database util
    # @param _dimension - dimension of queries
    #                     TODO - dimension is not used for NYC dataset, only support 3D.
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
    @staticmethod
    def time_query(_db, _dimension, _query, _plan):

        sql = "SELECT id, " \
              "       pickup_coordinates " \
              "  FROM " + NYC.table + " t " \
              " WHERE t.pickup_datetime between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'" \
              "   AND t.trip_distance between " + str(_query["trip_distance_start"]) + " and " + str(_query["trip_distance_end"]) + \
              "   AND t.pickup_coordinates <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                                   "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'"

        # generate hint if plan is 1 ~ 7
        if 1 <= _plan <= 7:
            # translate the _plan number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            plan_bits = [int(x) for x in '{:08b}'.format(_plan)]

            hint = "/*+ BitmapScan(t"

            # pick_datetime
            if plan_bits[5] == 1:
                hint = hint + " " + NYC.index_on_pickup_datetime

            # trip_distance
            if plan_bits[6] == 1:
                hint = hint + " " + NYC.index_on_trip_distance

            # pickup_coordinates
            if plan_bits[7] == 1:
                hint = hint + " " + NYC.index_on_pickup_coordinates

            hint = hint + ") */"
            sql = hint + sql

        start = time.time()
        _db.query(sql)
        end = time.time()
        return end - start

    # time probing query of selectivity for given filtering combination on given query
    # @param _db - handle to database util
    # @param _dimension - dimension of queries
    #                     TODO - dimension is not used for NYC dataset, only support 3D.
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
    @staticmethod
    def time_sel_query(_db, _dimension, _query, _fc, _table):

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
                sql = sql + " AND t.trip_distance between " + str(_query["trip_distance_start"]) + " and " + str(_query["trip_distance_end"])

            # pickup_coordinates
            if fc_bits[7] == 1:
                sql = sql + " AND t.pickup_coordinates <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                                               "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'"

        start = time.time()
        _db.query(sql)
        end = time.time()
        return end - start

    # collect selectivity for given filtering combination on given query
    # @param _db - handle to database util
    # @param _dimension - dimension of queries
    #                     TODO - dimension is not used for NYC dataset, only support 3D.
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
    @staticmethod
    def sel_query(_db, _dimension, _query, _fc, _table, _table_size):

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
                sql = sql + " AND t.trip_distance between " + str(_query["trip_distance_start"]) + " and " + str(_query["trip_distance_end"])

            # pickup_coordinates
            if fc_bits[7] == 1:
                sql = sql + " AND t.pickup_coordinates <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                                               "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'"

        sel = _db.query(sql)  # [(1,)]
        sel = sel[0][0]
        return float(sel) / float(_table_size)

    # load queries into memory
    # TODO - dimension is not used for NYC dataset, only support 3D.
    @staticmethod
    def load_queries_file(dimension, in_file):
        queries = []
        if os.path.isfile(in_file):
            with open(in_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    query = {"id": int(row[0]),
                             "start_time": row[1],
                             "end_time": row[2],
                             "trip_distance_start": float(row[3]),
                             "trip_distance_end": float(row[4]),
                             "lng0": float(row[5]),
                             "lat0": float(row[6]),
                             "lng1": float(row[7]),
                             "lat1": float(row[8])
                             }
                    queries.append(query)
        else:
            print("[" + in_file + "] does NOT exist! Exit!")
            exit(0)
        return queries
    
    # construct a SQL string for a given query with given dimension on given table
    # @param _query - {id: 1,
    #                  start_time: 2010-01-30 23:31:00,
    #                  end_time; 2010-01-31 23:31:00,
    #                  trip_distance_start: 0.7,
    #                  trip_distance_end: 5.9
    #                  lng0: -73.996117,
    #                  lat0: 40.741193,
    #                  lng1: -73.981511,
    #                  lat1: 40.763931}
    # @param _dimension - dimension of queries
    # @param _table - table name
    # @return - SQL string
    def construct_sql_str(_query, _dimension=3, _table=None):

        if _table is None:
            _table = NYC.table
        
        if _dimension != 3:
            print("Given dimension " + str(_dimension) + " is not supported in NYC.construct_sql_str() yet!")
            exit(0)
        
        sql = "SELECT id, " \
              "       pickup_coordinates " \
              "  FROM " + _table + " t " \
              " WHERE t.pickup_datetime between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'" \
              "   AND t.trip_distance between " + str(_query["trip_distance_start"]) + " and " + str(_query["trip_distance_end"]) + \
              "   AND t.pickup_coordinates <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                                   "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'"

        return sql
    
    # construct a hint string for given dimension and plan id
    #   assume the table alias is "t"
    # @param _dimension - dimension of queries
    # @param _plan - 0 ~ 2 ** _dimension - 1
    # @return - hint string
    def construct_hint_str(_dimension=3, _plan=0):
        
        if _dimension != 3:
            print("Given dimension " + str(_dimension) + " is not supported in NYC.construct_hint_str() yet!")
            exit(0)
        
        if _plan < 0 or _plan > Util.num_of_plans(_dimension):
            print("Given plan id " + str(_plan) + " is not invalid!")
            exit(0)

        # generate hint if plan is 1 ~ 7
        if 1 <= _plan <= 7:
            # translate the _plan number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            plan_bits = [int(x) for x in '{:08b}'.format(_plan)]

            hint = "/*+ BitmapScan(t"

            # pick_datetime
            if plan_bits[5] == 1:
                hint = hint + " " + NYC.index_on_pickup_datetime

            # trip_distance
            if plan_bits[6] == 1:
                hint = hint + " " + NYC.index_on_trip_distance

            # pickup_coordinates
            if plan_bits[7] == 1:
                hint = hint + " " + NYC.index_on_pickup_coordinates

            hint = hint + ") */"

        return hint

