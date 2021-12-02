import csv
import os.path
import time
from smart_util import Util


# Requirements:
#   TODO - generalize to other DBs
#   1) PostgreSQL 9.6+
#   2) schema:
#        tweets (
#          id                      int64,
#      [1] create_at               timestamp,
#      [2] text                    text,
#      [3] coordinate              point,
#          user_description        text,
#          user_create_at          date,
#      [4] user_followers_count    int,
#          user_friends_count      int,
#      [5] user_statues_count      int
#        )
#      indexes:
#        gin   (text)
#        btree (create_at)
#        gist  (coordinate)
#        btree (user_followers_count)
#        btree (user_statues_count)
class Twitter:

    # Profile
    database = "twitter"
    table = "tweets_100m"
    table_size = 100000000
    index_on_text = "idx_tweets_100m_text"
    index_on_time = "idx_tweets_100m_create_at"
    index_on_space = "idx_tweets_100m_coordinate"
    index_on_user_followers_count = "idx_tweets_100m_user_followers_count"
    index_on_user_statues_count = "idx_tweets_100m_user_statues_count"
    min_create_at = "2015-11-17 21:33:25"
    max_create_at = "2017-01-09 18:23:57"  # 3d_tweets_100m = 2019-09-07 04:16:11
    max_temporal_zoom = 9  # 3d_tweets_100m = 11
    min_lng = -125.0011
    min_lat = 24.9493
    max_lng = -66.9326
    max_lat = 49.5904
    max_spatial_zoom = 18
    min_user_followers_count = -9
    max_user_followers_count = 28760320  # 10m = 23317727
    max_user_followers_count_zoom = 25  # log2(28760320 - (-9)) = 24.77
    min_user_statues_count = -1
    max_user_statues_count = 2653135
    max_user_statues_count_zoom = 22  # log2(2653135 - (-1)) = 21.33
    indexes = [
        index_on_text,
        index_on_time,
        index_on_space, 
        index_on_user_followers_count, 
        index_on_user_statues_count
    ]
    sample_ratios = [0.00032, 0.0016, 0.008, 0.04, 0.2]  # 0.032%, 0.16%, 0.8%, 4%, 20%

    # time given query using given plan
    # @param _db - handle to database util
    # @param _dimension - dimension of queries
    # @param _query - query object
    # @param _plan - plan id
    # @return - time (seconds) of running this query using this plan
    @staticmethod
    def time_query(_db, _dimension, _query, _plan):
        if _dimension == 3:
            return Twitter.time_query_3d(_db, _query, _plan)
        elif _dimension == 4:
            return Twitter.time_query_4d(_db, _query, _plan)
        elif _dimension == 5:
            return Twitter.time_query_5d(_db, _query, _plan)
        else:
            print("Given dimension " + str(_dimension) + " is not supported in Twitter.time_query() yet!")
            exit(0)

    # time given query using given plan
    # @param _db - handle to database util
    # @param _query - {id: 1,
    #                  keyword: hurricane,
    #                  start_time: 2015-12-01T00:00:00.000Z,
    #                  end_time; 2016-01-01T00:00:00.000Z,
    #                  lng0: -77.119759,
    #                  lat0: 38.791645,
    #                  lng1: -76.909395,
    #                  lat1: 38.99511}
    # @param _plan - 0 ~ 7, represents plan of using/not using index on dimensions [text, create_at, coordinate]
    #                e.g., 000 - original query without hints
    #                      001 - hint using idx_tweets_coordinate index only
    # @return - time (seconds) of running this query using this plan
    @staticmethod
    def time_query_3d(_db, _query, _plan):

        sql = "SELECT id, " \
              "       coordinate " \
              "  FROM " + Twitter.table + " t " \
              " WHERE to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')" \
              "   AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'" \
              "   AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                           "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'"

        # generate hint if plan is 1 ~ 7
        if 1 <= _plan <= 7:
            # translate the _plan number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            plan_bits = [int(x) for x in '{:08b}'.format(_plan)]

            hint = "/*+ BitmapScan(t"

            # text
            if plan_bits[5] == 1:
                hint = hint + " " + Twitter.index_on_text

            # time
            if plan_bits[6] == 1:
                hint = hint + " " + Twitter.index_on_time

            # space
            if plan_bits[7] == 1:
                hint = hint + " " + Twitter.index_on_space

            hint = hint + ") */"
            sql = hint + sql

        start = time.time()
        _db.query(sql)
        end = time.time()
        return end - start

    # time given query using given plan
    # @param _db - handle to database util
    # @param _query - {id: 1,
    #                  keyword: hurricane,
    #                  start_time: 2015-12-01T00:00:00.000Z,
    #                  end_time; 2016-01-01T00:00:00.000Z,
    #                  lng0: -77.119759,
    #                  lat0: 38.791645,
    #                  lng1: -76.909395,
    #                  lat1: 38.99511,
    #                  user_followers_count_start: 30,
    #                  user_followers_count_end: 200}
    # @param _plan - 0 ~ 15, represents plan of using/not using index on dimensions [text, create_at, coordinate, ufc]
    #                e.g., 0000 - original query without hints
    #                      0010 - hint using idx_tweets_coordinate index only
    # @return - time (seconds) of running this query using this plan
    @staticmethod
    def time_query_4d(_db, _query, _plan):

        sql = "SELECT id, " \
              "       coordinate " \
              "  FROM " + Twitter.table + " t " \
              " WHERE to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')" \
              "   AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'" \
              "   AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                           "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'" \
              "   AND t.user_followers_count between " + str(_query["user_followers_count_start"]) + \
                                               " and " + str(_query["user_followers_count_end"])

        # generate hint if plan is 1 ~ 15
        if 1 <= _plan <= 15:
            # translate the _plan number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            plan_bits = [int(x) for x in '{:08b}'.format(_plan)]

            hint = "/*+ BitmapScan(t"

            # text
            if plan_bits[4] == 1:
                hint = hint + " " + Twitter.index_on_text

            # create_at
            if plan_bits[5] == 1:
                hint = hint + " " + Twitter.index_on_time

            # coordinate
            if plan_bits[6] == 1:
                hint = hint + " " + Twitter.index_on_space

            # user_followers_count
            if plan_bits[7] == 1:
                hint = hint + " " + Twitter.index_on_user_followers_count

            hint = hint + ") */"
            sql = hint + sql

        start = time.time()
        _db.query(sql)
        end = time.time()
        return end - start

    # time given query using given plan
    # @param _db - handle to database util
    # @param _query - {id: 1,
    #                  keyword: hurricane,
    #                  start_time: 2015-12-01T00:00:00.000Z,
    #                  end_time; 2016-01-01T00:00:00.000Z,
    #                  lng0: -77.119759,
    #                  lat0: 38.791645,
    #                  lng1: -76.909395,
    #                  lat1: 38.99511,
    #                  user_followers_count_start: 30,
    #                  user_followers_count_end: 200,
    #                  user_statues_count_start: 1000,
    #                  user_statues_count_end: 1500}
    # @param _plan - 0 ~ 31, represents plan of using/not using index on dimensions [text, create_at, coordinate, ufc, usc]
    #                e.g., 00000 - original query without hints
    #                      00100 - hint using idx_tweets_coordinate index only
    # @return - time (seconds) of running this query using this plan
    @staticmethod
    def time_query_5d(_db, _query, _plan):

        sql = "SELECT id, " \
              "       coordinate " \
              "  FROM " + Twitter.table + " t " \
              " WHERE to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')" \
              "   AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'" \
              "   AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                           "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'" \
              "   AND t.user_followers_count between " + str(_query["user_followers_count_start"]) + \
                                               " and " + str(_query["user_followers_count_end"]) + \
              "   AND t.user_statues_count between " + str(_query["user_statues_count_start"]) + \
                                             " and " + str(_query["user_statues_count_end"])

        # generate hint if plan is 1 ~ 31
        if 1 <= _plan <= 31:
            # translate the _plan number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            plan_bits = [int(x) for x in '{:08b}'.format(_plan)]

            hint = "/*+ BitmapScan(t"

            # text
            if plan_bits[3] == 1:
                hint = hint + " " + Twitter.index_on_text

            # create_at
            if plan_bits[4] == 1:
                hint = hint + " " + Twitter.index_on_time

            # coordinate
            if plan_bits[5] == 1:
                hint = hint + " " + Twitter.index_on_space

            # user_followers_count
            if plan_bits[6] == 1:
                hint = hint + " " + Twitter.index_on_user_followers_count

            # user_statues_count
            if plan_bits[7] == 1:
                hint = hint + " " + Twitter.index_on_user_statues_count

            hint = hint + ") */"
            sql = hint + sql

        start = time.time()
        _db.query(sql)
        end = time.time()
        return end - start

    # time probing query of selectivity for given filtering combination on given query
    # @param _db - handle to database util
    # @param _dimension - dimension of queries
    # @param _query - query object
    # @param _fc - filtering combination id
    # @param _table - table name on which to run the selectivity probing query
    # @return - time (seconds) of probing query of selectivity for given filtering combination on given query
    @staticmethod
    def time_sel_query(_db, _dimension, _query, _fc, _table):
        if _dimension == 3:
            return Twitter.time_sel_query_3d(_db, _query, _fc, _table)
        elif _dimension == 4:
            return Twitter.time_sel_query_4d(_db, _query, _fc, _table)
        elif _dimension == 5:
            return Twitter.time_sel_query_5d(_db, _query, _fc, _table)
        else:
            print("Given dimension " + str(_dimension) + " is not supported in Twitter.time_sel_query() yet!")
            exit(0)

    # time probing query of selectivity for given filtering combination on given query
    # @param _db - handle to database util
    # @param _query - {id: 1,
    #                  keyword: hurricane,
    #                  start_time: 2015-12-01T00:00:00.000Z,
    #                  end_time; 2016-01-01T00:00:00.000Z,
    #                  lng0: -77.119759,
    #                  lat0: 38.791645,
    #                  lng1: -76.909395,
    #                  lat1: 38.99511}
    # @param _fc - 1 ~ 7, represents selectivity of filtering combination on dimensions [text, create_at, coordinate]
    #                e.g., 001 - selectivity of filtering on (coordinate)
    #                      101 - selectivity of filtering on (text & coordinate)
    # @param _table - table name on which to run the selectivity probing query
    # @return - time (seconds) of probing query of selectivity for given filtering combination on given query
    @staticmethod
    def time_sel_query_3d(_db, _query, _fc, _table):

        sql = "SELECT count(1) " \
              "  FROM " + _table + " t " \
                                   " WHERE 1=1"

        # set up filtering on sample table if filtering combination is 1 ~ 7
        if 1 <= _fc <= 7:
            # translate the _fc number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            fc_bits = [int(x) for x in '{:08b}'.format(_fc)]

            # text
            if fc_bits[5] == 1:
                sql = sql + " AND to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')"

            # time
            if fc_bits[6] == 1:
                sql = sql + " AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'"

            # space
            if fc_bits[7] == 1:
                sql = sql + " AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                                       "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'"

        start = time.time()
        _db.query(sql)
        end = time.time()
        return end - start

    # time probing query of selectivity for given filtering combination on given query
    # @param _db - handle to database util
    # @param _query - {id: 1,
    #                  keyword: hurricane,
    #                  start_time: 2015-12-01T00:00:00.000Z,
    #                  end_time; 2016-01-01T00:00:00.000Z,
    #                  lng0: -77.119759,
    #                  lat0: 38.791645,
    #                  lng1: -76.909395,
    #                  lat1: 38.99511,
    #                  user_followers_count_start: 30,
    #                  user_followers_count_end: 200}
    # @param _fc - 1 ~ 15, represents selectivity of filtering combination on dimensions [text, create_at, coordinate, ufc]
    #                e.g., 0010 - selectivity of filtering on (coordinate)
    #                      1010 - selectivity of filtering on (text & coordinate)
    # @param _table - table name on which to run the selectivity probing query
    # @return - float, running time of probing query of selectivity for given filtering combination on given query
    @staticmethod
    def time_sel_query_4d(_db, _query, _fc, _table):

        sql = "SELECT count(1) " \
              "  FROM " + _table + " t " \
                                   " WHERE 1=1"

        # set up filtering on sample table if filtering combination is 1 ~ 15
        if 1 <= _fc <= 15:
            # translate the _fc number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            fc_bits = [int(x) for x in '{:08b}'.format(_fc)]

            # text
            if fc_bits[4] == 1:
                sql = sql + " AND to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')"

            # time
            if fc_bits[5] == 1:
                sql = sql + " AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'"

            # space
            if fc_bits[6] == 1:
                sql = sql + " AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                                       "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'"

            # user_followers_count
            if fc_bits[7] == 1:
                sql = sql + " AND t.user_followers_count between " + str(_query["user_followers_count_start"]) + \
                                                           " and " + str(_query["user_followers_count_end"])

        start = time.time()
        _db.query(sql)
        end = time.time()
        return end - start

    # time probing query of selectivity for given filtering combination on given query
    # @param _db - handle to database util
    # @param _query - {id: 1,
    #                  keyword: hurricane,
    #                  start_time: 2015-12-01T00:00:00.000Z,
    #                  end_time; 2016-01-01T00:00:00.000Z,
    #                  lng0: -77.119759,
    #                  lat0: 38.791645,
    #                  lng1: -76.909395,
    #                  lat1: 38.99511,
    #                  user_followers_count_start: 30,
    #                  user_followers_count_end: 200,
    #                  user_statues_count_start: 1000,
    #                  user_statues_count_end: 1500}
    # @param _fc - 1 ~ 31, represents selectivity of filtering combination on dimensions
    #                      [text, create_at, coordinate, ufc, usc]
    #                e.g., 00100 - selectivity of filtering on (coordinate)
    #                      10100 - selectivity of filtering on (text & coordinate)
    # @param _table - table name on which to run the selectivity probing query
    # @return - float, running time of probing query of selectivity for given filtering combination on given query
    @staticmethod
    def time_sel_query_5d(_db, _query, _fc, _table):

        sql = "SELECT count(1) " \
              "  FROM " + _table + " t " \
                                   " WHERE 1=1"

        # set up filtering on sample table if filtering combination is 1 ~ 31
        if 1 <= _fc <= 31:
            # translate the _fc number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            fc_bits = [int(x) for x in '{:08b}'.format(_fc)]

            # text
            if fc_bits[3] == 1:
                sql = sql + " AND to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')"

            # time
            if fc_bits[4] == 1:
                sql = sql + " AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'"

            # space
            if fc_bits[5] == 1:
                sql = sql + " AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                                       "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'"

            # user_followers_count
            if fc_bits[6] == 1:
                sql = sql + " AND t.user_followers_count between " + str(_query["user_followers_count_start"]) + \
                                                           " and " + str(_query["user_followers_count_end"])

            # user_statues_count
            if fc_bits[7] == 1:
                sql = sql + " AND t.user_statues_count between " + str(_query["user_statues_count_start"]) + \
                                                         " and " + str(_query["user_statues_count_end"])

        start = time.time()
        _db.query(sql)
        end = time.time()
        return end - start

    # collect selectivity for given filtering combination on given query
    # @param _db - handle to database util
    # @param _dimension - dimension of queries
    # @param _query - query object
    # @param _fc - filtering combination id
    # @param _table - table name on which to collect selectivity
    # @param _table_size - the size of the table provided above
    # @return - time (seconds) of probing query of selectivity for given filtering combination on given query
    @staticmethod
    def sel_query(_db, _dimension, _query, _fc, _table, _table_size):
        if _dimension == 3:
            return Twitter.sel_query_3d(_db, _query, _fc, _table, _table_size)
        elif _dimension == 4:
            return Twitter.sel_query_4d(_db, _query, _fc, _table, _table_size)
        elif _dimension == 5:
            return Twitter.sel_query_5d(_db, _query, _fc, _table, _table_size)
        else:
            print("Given dimension " + str(_dimension) + " is not supported in Twitter.sel_query() yet!")
            exit(0)

    # collect selectivity for given filtering combination on given query
    # @param _db - handle to database util
    # @param _query - {id: 1,
    #                  keyword: hurricane,
    #                  start_time: 2015-12-01T00:00:00.000Z,
    #                  end_time; 2016-01-01T00:00:00.000Z,
    #                  lng0: -77.119759,
    #                  lat0: 38.791645,
    #                  lng1: -76.909395,
    #                  lat1: 38.99511}
    # @param _fc - 1 ~ 7, represents selectivity of filtering combination on dimensions [text, create_at, coordinate]
    #                e.g., 001 - selectivity of filtering on (coordinate)
    #                      101 - selectivity of filtering on (text & coordinate)
    # @param _table - table name on which to collect selectivity
    # @param _table_size - the size of the table provided above
    # @return - float, selectivity [0, 1] of given filtering combination on given query
    @staticmethod
    def sel_query_3d(_db, _query, _fc, _table, _table_size):

        sql = "SELECT count(1) " \
              "  FROM " + _table + " t " \
                                   " WHERE 1=1"

        # set up filtering on sample table if filtering combination is 1 ~ 7
        if 1 <= _fc <= 7:
            # translate the _fc number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            fc_bits = [int(x) for x in '{:08b}'.format(_fc)]

            # text
            if fc_bits[5] == 1:
                sql = sql + " AND to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')"

            # time
            if fc_bits[6] == 1:
                sql = sql + " AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'"

            # space
            if fc_bits[7] == 1:
                sql = sql + " AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                                       "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'"

        sel = _db.query(sql)  # [(1,)]
        sel = sel[0][0]
        return float(sel) / float(_table_size)

    # collect selectivity for given filtering combination on given query
    # @param _db - handle to database util
    # @param _query - {id: 1,
    #                  keyword: hurricane,
    #                  start_time: 2015-12-01T00:00:00.000Z,
    #                  end_time; 2016-01-01T00:00:00.000Z,
    #                  lng0: -77.119759,
    #                  lat0: 38.791645,
    #                  lng1: -76.909395,
    #                  lat1: 38.99511,
    #                  user_followers_count_start: 30,
    #                  user_followers_count_end: 200}
    # @param _fc - 1 ~ 15, represents selectivity of filtering combination on dimensions [text, create_at, coordinate, ufc]
    #                e.g., 0010 - selectivity of filtering on (coordinate)
    #                      1010 - selectivity of filtering on (text & coordinate)
    # @param _table - table name on which to collect selectivity
    # @param _table_size - the size of the table provided above
    # @return - float, selectivity [0, 1] of given filtering combination on given query
    @staticmethod
    def sel_query_4d(_db, _query, _fc, _table, _table_size):

        sql = "SELECT count(1) " \
              "  FROM " + _table + " t " \
                                   " WHERE 1=1"

        # set up filtering on sample table if filtering combination is 1 ~ 15
        if 1 <= _fc <= 15:
            # translate the _fc number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            fc_bits = [int(x) for x in '{:08b}'.format(_fc)]

            # text
            if fc_bits[4] == 1:
                sql = sql + " AND to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')"

            # create_at
            if fc_bits[5] == 1:
                sql = sql + " AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'"

            # coordinate
            if fc_bits[6] == 1:
                sql = sql + " AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                                       "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'"

            # user_followers_count
            if fc_bits[7] == 1:
                sql = sql + " AND t.user_followers_count between " + str(_query["user_followers_count_start"]) + \
                                                           " and " + str(_query["user_followers_count_end"])

        sel = _db.query(sql)  # [(1,)]
        sel = sel[0][0]
        return float(sel) / float(_table_size)

    # collect selectivity for given filtering combination on given query
    # @param _db - handle to database util
    # @param _query - {id: 1,
    #                  keyword: hurricane,
    #                  start_time: 2015-12-01T00:00:00.000Z,
    #                  end_time; 2016-01-01T00:00:00.000Z,
    #                  lng0: -77.119759,
    #                  lat0: 38.791645,
    #                  lng1: -76.909395,
    #                  lat1: 38.99511,
    #                  user_followers_count_start: 30,
    #                  user_followers_count_end: 200,
    #                  user_statues_count_start: 1000,
    #                  user_statues_count_end: 1500}
    # @param _fc - 1 ~ 31, represents selectivity of filtering combination on dimensions
    #                      [text, create_at, coordinate, ufc, usc]
    #                e.g., 00100 - selectivity of filtering on (coordinate)
    #                      10100 - selectivity of filtering on (text & coordinate)
    # @param _table - table name on which to collect selectivity
    # @param _table_size - the size of the table provided above
    # @return - float, selectivity [0, 1] of given filtering combination on given query
    @staticmethod
    def sel_query_5d(_db, _query, _fc, _table, _table_size):

        sql = "SELECT count(1) " \
              "  FROM " + _table + " t " \
                                   " WHERE 1=1"

        # set up filtering on sample table if filtering combination is 1 ~ 31
        if 1 <= _fc <= 31:
            # translate the _fc number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            fc_bits = [int(x) for x in '{:08b}'.format(_fc)]

            # text
            if fc_bits[3] == 1:
                sql = sql + " AND to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')"

            # create_at
            if fc_bits[4] == 1:
                sql = sql + " AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'"

            # coordinate
            if fc_bits[5] == 1:
                sql = sql + " AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                                       "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'"

            # user_followers_count
            if fc_bits[6] == 1:
                sql = sql + " AND t.user_followers_count between " + str(_query["user_followers_count_start"]) + \
                                                           " and " + str(_query["user_followers_count_end"])

            # user_statues_count
            if fc_bits[7] == 1:
                sql = sql + " AND t.user_statues_count between " + str(_query["user_statues_count_start"]) + \
                                                         " and " + str(_query["user_statues_count_end"])

        sel = _db.query(sql)  # [(1,)]
        sel = sel[0][0]
        return float(sel) / float(_table_size)

    # load queries into memory
    @staticmethod
    def load_queries_file(dimension, in_file):
        queries = []
        if os.path.isfile(in_file):
            with open(in_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    query = {"id": int(row[0]),
                             "keyword": row[1],
                             "start_time": row[2],
                             "end_time": row[3],
                             "lng0": float(row[4]),
                             "lat0": float(row[5]),
                             "lng1": float(row[6]),
                             "lat1": float(row[7])
                             }
                    if dimension >= 4:
                        query["user_followers_count_start"] = int(row[8])
                        query["user_followers_count_end"] = int(row[9])
                    if dimension >= 5:
                        query["user_statues_count_start"] = int(row[10])
                        query["user_statues_count_end"] = int(row[11])
                    queries.append(query)
        else:
            print("[" + in_file + "] does NOT exist! Exit!")
            exit(0)
        return queries
    
    # dump queries into file
    @staticmethod
    def dump_queries_file(dimension, out_file, queries):
        with open(out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query in queries:
                row = [
                    query["id"], 
                    query["keyword"], 
                    query["start_time"], 
                    query["end_time"], 
                    query["lng0"],
                    query["lat0"],
                    query["lng1"],
                    query["lat1"]
                ]
                if dimension >= 4:
                    row.append(query["user_followers_count_start"])
                    row.append(query["user_followers_count_end"])
                if dimension >= 5:
                    row.append(query["user_statues_count_start"])
                    row.append(query["user_statues_count_end"])
                csv_writer.writerow(row)

    # construct a SQL string for a given query with given dimension on given table
    # @param _query - {id: 1,
    #                  keyword: hurricane,
    #                  start_time: 2015-12-01T00:00:00.000Z,
    #                  end_time; 2016-01-01T00:00:00.000Z,
    #                  lng0: -77.119759,
    #                  lat0: 38.791645,
    #                  lng1: -76.909395,
    #                  lat1: 38.99511[,
    #                  user_followers_count_start: 30,
    #                  user_followers_count_end: 200,
    #                  user_statues_count_start: 1000,
    #                  user_statues_count_end: 1500]}
    # @param _dimension - dimension of queries
    # @param _table - table name
    # @return - SQL string
    def construct_sql_str(_query, _dimension=3, _table=None):

        if _table is None:
            _table = Twitter.table
        
        if _dimension < 3 or _dimension > 5:
            print("Given dimension " + str(_dimension) + " is not supported in Twitter.construct_sql_str() yet!")
            exit(0)
        
        sql = "SELECT id, " \
            "       coordinate[0], coordinate[1] " \
            "  FROM " + _table + " t " \
            " WHERE to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')" \
            "   AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'" \
            "   AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                        "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'"
        
        if _dimension >= 4:
            sql = sql + "   AND t.user_followers_count between " + str(_query["user_followers_count_start"]) + \
                                                         " and " + str(_query["user_followers_count_end"])
        if _dimension >= 5:
            sql = sql + "   AND t.user_statues_count between " + str(_query["user_statues_count_start"]) + \
                                                       " and " + str(_query["user_statues_count_end"])

        return sql
    
    # construct a hint string for given dimension and plan id
    #   assume the table alias is "t"
    # @param _dimension - dimension of queries
    # @param _plan - 0 ~ 2 ** _dimension - 1
    # @return - hint string
    def construct_hint_str(_dimension=3, _plan=0):
        
        if _dimension < 3 or _dimension > 5:
            print("Given dimension " + str(_dimension) + " is not supported in Twitter.construct_hint_str() yet!")
            exit(0)
        
        if _plan < 0 or _plan > Util.num_of_plans(_dimension):
            print("Given plan id " + str(_plan) + " is not invalid!")
            exit(0)

        if _plan > 0:
            hint = "/*+ BitmapScan(t"
            for hint_id in range(0, _dimension):
                if Util.use_index(hint_id, _plan, _dimension):
                    hint = hint + " " + Twitter.indexes[hint_id]
            hint = hint + ") */"

        return hint
    
    # time given query using given sampling plan
    # @param _db - handle to database util
    # @param _dimension - dimension of queries
    # @param _query - {id: 1,
    #                  keyword: hurricane,
    #                  start_time: 2015-12-01T00:00:00.000Z,
    #                  end_time; 2016-01-01T00:00:00.000Z,
    #                  lng0: -77.119759,
    #                  lat0: 38.791645,
    #                  lng1: -76.909395,
    #                  lat1: 38.99511[,
    #                  user_followers_count_start: 30,
    #                  user_followers_count_end: 200,
    #                  user_statues_count_start: 1000,
    #                  user_statues_count_end: 1500]}
    # @param _card - cardinality of the given _query
    # @param _plan - 0 ~ _dimension * len(sample_ratios) - 1, 
    #                  represents sampling plan of using index on one of the dimensions [text, create_at, coordinate, ufc, usc]
    #                  and put a limit k = cardinality(_query) * one of the sample ratios [6.25%, 12.5%, 25%, 50%, 75%]
    #                e.g., 0 - hint using idx_tweets_text and limit 6.25%
    #                      6 - hint using idx_tweets_create_at and limit 12.5%
    # @return - (time (seconds) of running this query using this plan, result of this query using this plan)
    @staticmethod
    def time_sampling_query(_db, _dimension, _query, _card, _plan):

        if _dimension < 3 or _dimension > 5:
            print("Given dimension " + str(_dimension) + " is not supported in Twitter.time_sampling_query() yet!")
            exit(0)
        
        if _plan < 0 or _plan > _dimension * len(Twitter.sample_ratios) - 1:
            print("Given plan id " + str(_plan) + " is not invalid!")
            exit(0)

        sql = Twitter.construct_sql_str(_query, _dimension)

        # generate hint
        hint_idx = Util.hint_id_of_sampling_plan(len(Twitter.sample_ratios), _plan)
        hint = "/*+ BitmapScan(t " + Twitter.indexes[hint_idx] + ") */"
        sql = hint + sql

        # generate limit-k
        sample_ratio_idx = Util.sample_ratio_id_of_sampling_plan(len(Twitter.sample_ratios), _plan)
        sample_k = round(_card * Twitter.sample_ratios[sample_ratio_idx])
        sql = sql + " limit " + str(sample_k)

        start = time.time()
        result = _db.query(sql)
        end = time.time()
        return end - start, result

