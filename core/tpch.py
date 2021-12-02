import csv
import os.path
import time
from smart_util import Util


# Requirements:
#   TODO - generalize to other DBs
#   1) PostgreSQL 9.6+
#   2) schema:
#        lineitem_300m (
#            L_ORDERKEY       INTEGER NOT NULL,
#            L_PARTKEY        INTEGER NOT NULL,
#            L_SUPPKEY        INTEGER NOT NULL,
#            L_LINENUMBER     INTEGER NOT NULL,
#            L_QUANTITY       DECIMAL(15,2) NOT NULL,
#        [x] L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
#            L_DISCOUNT       DECIMAL(15,2) NOT NULL,
#            L_TAX            DECIMAL(15,2) NOT NULL,
#            L_RETURNFLAG     CHAR(1) NOT NULL,
#            L_LINESTATUS     CHAR(1) NOT NULL,
#        [x] L_SHIPDATE       DATE NOT NULL,
#            L_COMMITDATE     DATE NOT NULL,
#        [x] L_RECEIPTDATE    DATE NOT NULL,
#            L_SHIPINSTRUCT   CHAR(25) NOT NULL,
#            L_SHIPMODE       CHAR(10) NOT NULL,
#            L_COMMENT        VARCHAR(44) NOT NULL
#        )
#      indexes:
#        btree (L_EXTENDEDPRICE)
#        btree (L_SHIPDATE)
#        btree (L_RECEIPTDATE)
class TPCH:
    database = "tpch"
    table = "lineitem_300m"
    table_size = 300005811
    index_on_extended_price = "idx_lineitem_300m_l_extendedprice"
    index_on_ship_date = "idx_lineitem_300m_l_shipdate"
    index_on_receipt_date = "idx_lineitem_300m_l_receiptdate"
    min_extended_price = 900.51
    max_extended_price = 104949.50
    max_extended_price_zoom = 20  # (104949.50 - 900.51) * 10 = 1040489.9 (0.1 unit price), log2(1040489.9) = 19.98
    min_ship_date = "1992-01-02"
    max_ship_date = "1998-12-01"
    max_ship_date_zoom = 12  # (1998-12-01 - 1992-01-02) = 2526 days, log2(2526) = 11.3
    min_receipt_date = "1992-01-03"
    max_receipt_date = "1998-12-31"
    max_receipt_date_zoom = 12  # (1998-12-31 - 1992-01-03) = 2555 days, log2(2555) = 11.3
    indexes = [
        index_on_extended_price,
        index_on_ship_date,
        index_on_receipt_date
    ]

    # time given query using given plan
    # @param _db - handle to database util
    # @param _dimension - dimension of queries
    #                     TODO - dimension is not used for TPCH dataset, only support 3D.
    # @param _query - {id: 1,
    #                  extended_price_start: 3800,
    #                  extended_price_end: 9000,
    #                  ship_date_start: 1995-11-01,
    #                  ship_date_end: 1995-12-01,
    #                  receipt_date_start: 1996-01-01,
    #                  receipt_date_end: 1996-01-31}
    # @param _plan - 0 ~ 7, represents plan of using/not using index on dimensions
    #                       [L_EXTENDEDPRICE, L_SHIPDATE, L_RECEIPTDATE]
    #                e.g., 000 - original query without hints
    #                      001 - hint using idx_lineitem_300m_L_RECEIPTDATE index only
    # @return - time (seconds) of running this query using this plan
    def time_query(_db, _dimension, _query, _plan):

        sql = "SELECT L_QUANTITY, " \
            "       L_DISCOUNT" \
            "  FROM " + TPCH.table + " t " \
            " WHERE t.L_EXTENDEDPRICE between " + _query["extended_price_start"] + \
            "       and " + _query["extended_price_end"] + \
            "   AND t.L_SHIPDATE between '" + _query["ship_date_start"] + "' and '" + _query["ship_date_end"] + "'" \
            "   AND t.L_RECEIPTDATE between '" + _query["receipt_date_start"] + "' " \
            "       and '" + _query["receipt_date_end"] + "'"

        # generate hint if plan is 1 ~ 7
        if 1 <= _plan <= 7:
            # translate the _plan number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            plan_bits = [int(x) for x in '{:08b}'.format(_plan)]

            hint = "/*+ BitmapScan(t"

            # L_EXTENDEDPRICE
            if plan_bits[5] == 1:
                hint = hint + " " + TPCH.index_on_extended_price

            # L_SHIPDATE
            if plan_bits[6] == 1:
                hint = hint + " " + TPCH.index_on_ship_date

            # L_RECEIPTDATE
            if plan_bits[7] == 1:
                hint = hint + " " + TPCH.index_on_receipt_date

            hint = hint + ") */"
            sql = hint + sql

        start = time.time()
        _db.query(sql)
        end = time.time()
        return end - start
    
    # time probing query of selectivity for given filtering combination on given query
    # @param _db - handle to database util
    # @param _dimension - dimension of queries
    #                     TODO - dimension is not used for TPCH dataset, only support 3D.
    # @param _query - {id: 1,
    #                  extended_price_start: 3800,
    #                  extended_price_end: 9000,
    #                  ship_date_start: 1995-11-01,
    #                  ship_date_end: 1995-12-01,
    #                  receipt_date_start: 1996-01-01,
    #                  receipt_date_end: 1996-01-31}
    # @param _fc - 1 ~ 7, represents selectivity of filtering combination on dimensions
    #                     [L_EXTENDEDPRICE, L_SHIPDATE, L_RECEIPTDATE]
    #                e.g., 001 - selectivity of filtering on (L_RECEIPTDATE)
    #                      101 - selectivity of filtering on (L_EXTENDEDPRICE & L_RECEIPTDATE)
    # @param _table - table name on which to run the selectivity probing query
    # @return - float, running time of probing query of selectivity for given filtering combination on given query
    def time_sel_query(_db, _dimension, _query, _fc, _table):

        sql = "SELECT count(1) " \
            "  FROM " + _table + " t " \
            " WHERE 1=1"

        # set up filtering on sample table if filtering combination is 1 ~ 7
        if 1 <= _fc <= 7:
            # translate the _fc number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            fc_bits = [int(x) for x in '{:08b}'.format(_fc)]

            # L_EXTENDEDPRICE
            if fc_bits[5] == 1:
                sql = sql + " AND t.L_EXTENDEDPRICE between " + _query["extended_price_start"] + " and " + \
                    _query["extended_price_end"]

            # L_SHIPDATE
            if fc_bits[6] == 1:
                sql = sql + " AND t.L_SHIPDATE between '" + _query["ship_date_start"] + "' and '" + \
                    _query["ship_date_end"] + "'"

            # L_RECEIPTDATE
            if fc_bits[7] == 1:
                sql = sql + " AND t.L_RECEIPTDATE between '" + _query["receipt_date_start"] + "' and '" + \
                    _query["receipt_date_end"] + "'"

        start = time.time()
        _db.query(sql)
        end = time.time()
        return end - start
    
    # collect selectivity for given filtering combination on given query
    # @param _db - handle to database util
    # @param _dimension - dimension of queries
    #                     TODO - dimension is not used for TPCH dataset, only support 3D.
    # @param _query - {id: 1,
    #                  extended_price_start: 3800,
    #                  extended_price_end: 9000,
    #                  ship_date_start: 1995-11-01,
    #                  ship_date_end: 1995-12-01,
    #                  receipt_date_start: 1996-01-01,
    #                  receipt_date_end: 1996-01-31}
    # @param _fc - 1 ~ 7, represents selectivity of filtering combination on dimensions
    #                     [L_EXTENDEDPRICE, L_SHIPDATE, L_RECEIPTDATE]
    #                e.g., 001 - selectivity of filtering on (L_RECEIPTDATE)
    #                      101 - selectivity of filtering on (L_EXTENDEDPRICE & L_RECEIPTDATE)
    # @param _table - table name on which to collect selectivity
    # @param _table_size - the size of the table provided above
    # @return - float, selectivity [0, 1] of given filtering combination on given query
    def sel_query(_db, _dimension, _query, _fc, _table, _table_size):

        sql = "SELECT count(1) " \
            "  FROM " + _table + " t " \
            " WHERE 1=1"

        # set up filtering on sample table if filtering combination is 1 ~ 7
        if 1 <= _fc <= 7:
            # translate the _fc number into binary bits array
            # e.g., 6 -> 0,0,0,0,0,1,1,0
            fc_bits = [int(x) for x in '{:08b}'.format(_fc)]

            # L_EXTENDEDPRICE
            if fc_bits[5] == 1:
                sql = sql + " AND t.L_EXTENDEDPRICE between " + _query["extended_price_start"] + " and " + \
                    _query["extended_price_end"]

            # L_SHIPDATE
            if fc_bits[6] == 1:
                sql = sql + " AND t.L_SHIPDATE between '" + _query["ship_date_start"] + "' and '" + \
                    _query["ship_date_end"] + "'"

            # L_RECEIPTDATE
            if fc_bits[7] == 1:
                sql = sql + " AND t.L_RECEIPTDATE between '" + _query["receipt_date_start"] + "' and '" + \
                    _query["receipt_date_end"] + "'"

        sel = _db.query(sql)  # [(1,)]
        sel = sel[0][0]
        return float(sel) / float(_table_size)
    
    # load queries into memory
    # TODO - dimension is not used for TPCH dataset, only support 3D.
    @staticmethod
    def load_queries_file(dimension, in_file):
        queries = []
        if os.path.isfile(in_file):
            with open(in_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    query = {"id": int(row[0]),
                             "extended_price_start": row[1],
                             "extended_price_end": row[2],
                             "ship_date_start": row[3],
                             "ship_date_end": row[4],
                             "receipt_date_start": row[5],
                             "receipt_date_end": row[6]
                             }
                    queries.append(query)
        else:
            print("[" + in_file + "] does NOT exist! Exit!")
            exit(0)
        return queries
    
    # construct a SQL string for a given query with given dimension on given table
    # @param _query - {id: 1,
    #                  extended_price_start: 3800,
    #                  extended_price_end: 9000,
    #                  ship_date_start: 1995-11-01,
    #                  ship_date_end: 1995-12-01,
    #                  receipt_date_start: 1996-01-01,
    #                  receipt_date_end: 1996-01-31}
    # @param _dimension - dimension of queries
    # @param _table - table name
    # @return - SQL string
    def construct_sql_str(_query, _dimension=3, _table=None):

        if _table is None:
            _table = TPCH.table
        
        if _dimension != 3:
            print("Given dimension " + str(_dimension) + " is not supported in TPCH.construct_sql_str() yet!")
            exit(0)
        
        sql = "SELECT L_QUANTITY, " \
            "       L_DISCOUNT" \
            "  FROM " + TPCH.table + " t " \
            " WHERE t.L_EXTENDEDPRICE between " + str(_query["extended_price_start"]) + \
            "       and " + str(_query["extended_price_end"]) + \
            "   AND t.L_SHIPDATE between '" + _query["ship_date_start"] + "' and '" + _query["ship_date_end"] + "'" \
            "   AND t.L_RECEIPTDATE between '" + _query["receipt_date_start"] + "' " \
            "       and '" + _query["receipt_date_end"] + "'"

        return sql
    
    # construct a hint string for given dimension and plan id
    #   assume the table alias is "t"
    # @param _dimension - dimension of queries
    # @param _plan - 0 ~ 2 ** _dimension - 1
    # @return - hint string
    def construct_hint_str(_dimension=3, _plan=0):
        
        if _dimension != 3:
            print("Given dimension " + str(_dimension) + " is not supported in TPCH.construct_hint_str() yet!")
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

            # L_EXTENDEDPRICE
            if plan_bits[5] == 1:
                hint = hint + " " + TPCH.index_on_extended_price

            # L_SHIPDATE
            if plan_bits[6] == 1:
                hint = hint + " " + TPCH.index_on_ship_date

            # L_RECEIPTDATE
            if plan_bits[7] == 1:
                hint = hint + " " + TPCH.index_on_receipt_date

            hint = hint + ") */"

        return hint
