import csv
import os.path
import time
from smart_util import Util


# Requirements:
#   TODO - generalize to other DBs
#   1) PostgreSQL 9.6+
#   2) schema:
#        -- tweets
#        CREATE TABLE tweets_100m
#        (
#           id                      bigint NOT NULL,
#       [x] create_at               timestamp,
#       [x] text                    text,
#       [x] coordinate              point,
#           user_id                 bigint
#        );
#        -- users
#        CREATE TABLE users_100m
#        (
#           id                 bigint PRIMARY KEY,
#           description        text,
#           create_at          date,
#       [x] followers_count    int,
#           friends_count      int,
#       [x] statues_count      int
#        );
#   3) indexes:
#        gin   (tweets_100m.text)
#        btree (tweets_100m.create_at)
#        gist  (tweets_100m.coordinate)
#        btree (tweets_100m.user_id)
#        btree (user_100m.id)
#        btree (user_100m.followers_count)
#        btree (user_100m.statues_count)
class TwitterJoin:

    # Profile
    database = "twitter2"
    tweets_table = "tweets_100m"
    users_table = "users_100m"
    tweets_table_size = 100000000
    users_table_size = 3815312
    index_on_text = "idx_tweets_100m_text"
    index_on_time = "idx_tweets_100m_create_at"
    index_on_space = "idx_tweets_100m_coordinate"
    index_on_user_followers_count = "idx_users_100m_followers_count"
    index_on_user_statues_count = "idx_users_100m_statues_count"
    min_create_at = "2015-11-17 21:33:25"
    max_create_at = "2017-01-09 18:23:57"
    max_temporal_zoom = 9
    min_lng = -125.0011
    min_lat = 24.9493
    max_lng = -66.9326
    max_lat = 49.5904
    max_spatial_zoom = 18
    min_user_followers_count = -1
    max_user_followers_count = 28691017
    max_user_followers_count_zoom = 25  # log2(28691017 - (-9)) = 24.77
    min_user_statues_count = -1
    max_user_statues_count = 2649730
    max_user_statues_count_zoom = 22  # log2(2649730 - (-1)) = 21.33
    indexes = [
        index_on_text,
        index_on_time,
        index_on_space, 
        index_on_user_followers_count, 
        index_on_user_statues_count
    ]
    num_of_joins = 3  # NestLoop, HashJoin, MergeJoin

    # time given query using given plan
    # @param _db - handle to database util
    # @param _dimension - dimension of queries
    # @param _query - query object
    # @param _plan - plan id
    # @return - time (seconds) of running this query using this plan
    @staticmethod
    def time_query(_db, _dimension, _query, _plan):
        if _dimension == 3:
            return TwitterJoin.time_query_3d(_db, _query, _plan)
        elif _dimension == 4:
            return TwitterJoin.time_query_4d(_db, _query, _plan)
        elif _dimension == 5:
            return TwitterJoin.time_query_5d(_db, _query, _plan)
        else:
            print("Given dimension " + str(_dimension) + " is not supported in TwitterJoin.time_query() yet!")
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
    # @param _plan - 0 ~ 2^3*3-1, represents plan of using/not using index on dimensions [text, create_at, coordinate] 
    #                                            and using one of the three join methods [NestLoop, HashJoin, MergeJoin]
    #                for a given _plan, if _plan in [0 ~ 7], it indicates no hint on join method but each bit indicates using index on dimensions:
    #                   e.g., 000 - original query without hints
    #                         001 - hint using idx_tweets_coordinate index only
    #                similarly, if _plan in [8 ~ 15], it indicates all using [HashJoin], then we do (_plan = _plan - 8) and each bit indicates using index on dimensions:
    #                   e.g., 000 - original query without hints
    #                         001 - hint using idx_tweets_coordinate index only
    #                finally, if _plan in [16 ~ 23], it indicates all using [MergeJoin], then we do (_plan = _plan - 16) and each bit indicates using index on dimensions:
    #                   e.g., 000 - original query without hints
    #                         001 - hint using idx_tweets_coordinate index only
    # @return - time (seconds) of running this query using this plan
    @staticmethod
    def time_query_3d(_db, _query, _plan):

        _dimension = 3
        num_of_plans = Util.num_of_plans(_dimension, TwitterJoin.num_of_joins)

        sql = "SELECT t.id, " \
              "       t.coordinate " \
              "  FROM " + TwitterJoin.tweets_table + " t, " + TwitterJoin.users_table + " u " + \
              " WHERE to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')" \
              "   AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'" \
              "   AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                           "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'" \
              "   AND t.user_id = u.id "

        # generate hint if plan is 1 ~ num_of_plans
        if 1 <= _plan <= num_of_plans:
            hint = " /*+ "
            _plan, _join_method = Util.reduce_join_method(_plan, _dimension)
            if _join_method == 1:
                hint = hint + " NestLoop(t u) "
            if  _join_method == 2:
                hint = hint + " HashJoin(t u) "
            if _join_method == 3:
                hint = hint + " MergeJoin(t u) "

            # add BitmapScan hint if offset plan is 1 ~ 7
            if 1 <= _plan <= 7:
                # translate the _plan number into binary bits array
                # e.g., 6 -> 0,0,0,0,0,1,1,0
                plan_bits = [int(x) for x in '{:08b}'.format(_plan)]

                # there is at least one hint on table t
                if plan_bits[5] + plan_bits[6] + plan_bits[7] >= 1:
                    hint = hint + " BitmapScan(t"
                    # text
                    if plan_bits[5] == 1:
                        hint = hint + " " + TwitterJoin.index_on_text
                    # create_at
                    if plan_bits[6] == 1:
                        hint = hint + " " + TwitterJoin.index_on_time
                    # coordinate
                    if plan_bits[7] == 1:
                        hint = hint + " " + TwitterJoin.index_on_space
                    hint = hint + ")"

            hint = hint + " */"
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
    # @param _plan - 0 ~ 2^4*3-1, represents plan of using/not using index on dimensions [text, create_at, coordinate, ufc] 
    #                                            and using one of the three join methods [NestLoop, HashJoin, MergeJoin]
    #                for a given _plan, if _plan in [0 ~ 15], it indicates no hint on join method but each bit indicates using index on dimensions:
    #                   e.g., 00000 - original query without hints
    #                         00100 - hint using idx_tweets_coordinate index only
    #                similarly, if _plan in [16 ~ 31], it indicates all using [HashJoin], then we do (_plan = _plan - 16) and each bit indicates using index on dimensions:
    #                   e.g., 00000 - original query without hints
    #                         00100 - hint using idx_tweets_coordinate index only
    #                finally, if _plan in [32 ~ 47], it indicates all using [MergeJoin], then we do (_plan = _plan - 32) and each bit indicates using index on dimensions:
    #                   e.g., 00000 - original query without hints
    #                         00100 - hint using idx_tweets_coordinate index only
    # @return - time (seconds) of running this query using this plan
    @staticmethod
    def time_query_4d(_db, _query, _plan):

        sql = "SELECT t.id, " \
              "       t.coordinate " \
              "  FROM " + TwitterJoin.tweets_table + " t, " + TwitterJoin.users_table + " u " + \
              " WHERE to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')" \
              "   AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'" \
              "   AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                           "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'" \
              "   AND u.followers_count between " + str(_query["user_followers_count_start"]) + \
                                          " and " + str(_query["user_followers_count_end"]) + \
              "   AND t.user_id = u.id "

        # generate hint if plan is 1 ~ 47
        if 1 <= _plan <= 47:
            hint = " /*+ "
            # if 1 <= _plan <= 15:
            #     hint = hint + " NestLoop(t u) "
            if 16 <= _plan <= 31:
                hint = hint + " HashJoin(t u) "
                _plan = _plan - 16
            if 32 <= _plan <= 47:
                hint = hint + " MergeJoin(t u) "
                _plan = _plan - 32

            # add BitmapScan hint if offset plan is 1 ~ 15
            if 1 <= _plan <= 15:
                # translate the _plan number into binary bits array
                # e.g., 6 -> 0,0,0,0,0,1,1,0
                plan_bits = [int(x) for x in '{:08b}'.format(_plan)]

                # there is at least one hint on table t
                if plan_bits[4] + plan_bits[5] + plan_bits[6] >= 1:
                    hint = hint + " BitmapScan(t"
                    # text
                    if plan_bits[4] == 1:
                        hint = hint + " " + TwitterJoin.index_on_text
                    # create_at
                    if plan_bits[5] == 1:
                        hint = hint + " " + TwitterJoin.index_on_time
                    # coordinate
                    if plan_bits[6] == 1:
                        hint = hint + " " + TwitterJoin.index_on_space
                    hint = hint + ")"

                # use user_followers_count index on table u
                if plan_bits[7] == 1:
                    hint = hint + " BitmapScan(u"
                    hint = hint + " " + TwitterJoin.index_on_user_followers_count
                    hint = hint + ")"

            hint = hint + " */"
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
    # @param _plan - 0 ~ 2^5*3-1, represents plan of using/not using index on dimensions [text, create_at, coordinate, ufc, usc] 
    #                                            and using one of the three join methods [NestLoop, HashJoin, MergeJoin]
    #                for a given _plan, if _plan in [0 ~ 31], it indicates no hint on join method but each bit indicates using index on dimensions:
    #                   e.g., 00000 - original query without hints
    #                         00100 - hint using idx_tweets_coordinate index only
    #                similarly, if _plan in [32 ~ 63], it indicates all using [HashJoin], then we do (_plan = _plan - 32) and each bit indicates using index on dimensions:
    #                   e.g., 00000 - original query without hints
    #                         00100 - hint using idx_tweets_coordinate index only
    #                finally, if _plan in [64 ~ 95], it indicates all using [MergeJoin], then we do (_plan = _plan - 64) and each bit indicates using index on dimensions:
    #                   e.g., 00000 - original query without hints
    #                         00100 - hint using idx_tweets_coordinate index only
    # @return - time (seconds) of running this query using this plan
    @staticmethod
    def time_query_5d(_db, _query, _plan):

        sql = "SELECT t.id, " \
              "       t.coordinate " \
              "  FROM " + TwitterJoin.tweets_table + " t, " + TwitterJoin.users_table + " u " + \
              " WHERE to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')" \
              "   AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'" \
              "   AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                           "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'" \
              "   AND u.followers_count between " + str(_query["user_followers_count_start"]) + \
                                          " and " + str(_query["user_followers_count_end"]) + \
              "   AND u.statues_count between " + str(_query["user_statues_count_start"]) + \
                                        " and " + str(_query["user_statues_count_end"]) + \
              "   AND t.user_id = u.id "

        # generate hint if plan is 1 ~ 95
        if 1 <= _plan <= 95:
            hint = " /*+ "
            # if 1 <= _plan <= 31:
            #     hint = hint + " NestLoop(t u) "
            if 32 <= _plan <= 63:
                hint = hint + " HashJoin(t u) "
                _plan = _plan - 32
            if 64 <= _plan <= 95:
                hint = hint + " MergeJoin(t u) "
                _plan = _plan - 64

            # add BitmapScan hint if offset plan is 1 ~ 31
            if 1 <= _plan <= 31:
                # translate the _plan number into binary bits array
                # e.g., 6 -> 0,0,0,0,0,1,1,0
                plan_bits = [int(x) for x in '{:08b}'.format(_plan)]

                # there is at least one hint on table t
                if plan_bits[3] + plan_bits[4] + plan_bits[5] >= 1:
                    hint = hint + " BitmapScan(t"
                    # text
                    if plan_bits[3] == 1:
                        hint = hint + " " + TwitterJoin.index_on_text
                    # create_at
                    if plan_bits[4] == 1:
                        hint = hint + " " + TwitterJoin.index_on_time
                    # coordinate
                    if plan_bits[5] == 1:
                        hint = hint + " " + TwitterJoin.index_on_space
                    hint = hint + ")"

                # there is at least one hint on table u
                if plan_bits[6] + plan_bits[7] >= 1:
                    hint = hint + " BitmapScan(u"
                    # user_followers_count
                    if plan_bits[6] == 1:
                        hint = hint + " " + TwitterJoin.index_on_user_followers_count
                    # user_statues_count
                    if plan_bits[7] == 1:
                        hint = hint + " " + TwitterJoin.index_on_user_statues_count
                    hint = hint + ")"

            hint = hint + " */"
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
            return TwitterJoin.time_sel_query_3d(_db, _query, _fc, _table)
        else:
            print("Given dimension " + str(_dimension) + " is not supported in TwitterJoin.time_sel_query() yet!")
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
            return TwitterJoin.sel_query_3d(_db, _query, _fc, _table, _table_size)
        else:
            print("Given dimension " + str(_dimension) + " is not supported in TwitterJoin.sel_query() yet!")
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
    
    # TODO - only support 3d queries now
    # construct a SQL string for a given query
    # @param _query - {id: 1,
    #                  keyword: hurricane,
    #                  start_time: 2015-12-01T00:00:00.000Z,
    #                  end_time; 2016-01-01T00:00:00.000Z,
    #                  lng0: -77.119759,
    #                  lat0: 38.791645,
    #                  lng1: -76.909395,
    #                  lat1: 38.99511}
    # @param _dimension - dimension of queries
    # @return - SQL string
    def construct_sql_str(_query, _dimension=3):

        if _dimension != 3:
            print("Given dimension " + str(_dimension) + " is not supported in TwitterJoin.construct_sql_str() yet!")
            exit(0)

        sql = "SELECT t.id, " \
              "       t.coordinate " \
              "  FROM " + TwitterJoin.tweets_table + " t, " + TwitterJoin.users_table + " u " + \
              " WHERE to_tsvector('english', t.text)@@to_tsquery('english', '" + _query["keyword"] + "')" \
              "   AND t.create_at between '" + _query["start_time"] + "' and '" + _query["end_time"] + "'" \
              "   AND t.coordinate <@ box '((" + str(_query["lng0"]) + "," + str(_query["lat0"]) + ")," \
                                           "(" + str(_query["lng1"]) + "," + str(_query["lat1"]) + "))'" \
              "   AND t.user_id = u.id "

        return sql
    
    # construct a hint string for given dimension and plan id
    # @param _dimension - dimension of queries
    # @param _plan - 0 ~ 2 ** _dimension - 1
    # @return - hint string
    def construct_hint_str(_dimension=3, _plan=0):
        
        if _dimension != 3:
            print("Given dimension " + str(_dimension) + " is not supported in TwitterJoin.construct_hint_str() yet!")
            exit(0)

        num_of_plans = Util.num_of_plans(_dimension, TwitterJoin.num_of_joins)

        # generate hint if plan is 1 ~ num_of_plans
        if 1 <= _plan <= num_of_plans:
            hint = " /*+ "
            _plan, _join_method = Util.reduce_join_method(_plan, _dimension)
            if _join_method == 1:
                hint = hint + " NestLoop(t u) "
            if  _join_method == 2:
                hint = hint + " HashJoin(t u) "
            if _join_method == 3:
                hint = hint + " MergeJoin(t u) "

            # add BitmapScan hint if offset plan is 1 ~ 7
            if 1 <= _plan <= 7:
                # translate the _plan number into binary bits array
                # e.g., 6 -> 0,0,0,0,0,1,1,0
                plan_bits = [int(x) for x in '{:08b}'.format(_plan)]

                # there is at least one hint on table t
                if plan_bits[5] + plan_bits[6] + plan_bits[7] >= 1:
                    hint = hint + " BitmapScan(t"
                    # text
                    if plan_bits[5] == 1:
                        hint = hint + " " + TwitterJoin.index_on_text
                    # create_at
                    if plan_bits[6] == 1:
                        hint = hint + " " + TwitterJoin.index_on_time
                    # coordinate
                    if plan_bits[7] == 1:
                        hint = hint + " " + TwitterJoin.index_on_space
                    hint = hint + ")"

            hint = hint + " */"

        return hint
