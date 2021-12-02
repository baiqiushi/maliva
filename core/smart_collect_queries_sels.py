import argparse
import config
import os.path
import progressbar
import time
from postgresql import PostgreSQL
from smart_util import Util


###########################################################
#  smart_collect_queries_sels.py
#
# Purpose:
#   collect all possible combination of selectivities for given queries
#
# Arguments:
#   -ds  / --dataset        dataset to run the queries on. Default: twitter
#   -d   / --dimension      dimension: dimension of the queries. Default: 3
#   -if  / --in_file        input file that holds the queries
#   -t   / --table          table name on which to collect selectivities
#
# Dependencies:
#   python3.7 & pip: https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb-cli3-install-linux.html
#   pip install progressbar
#
# Output:
#   selectivities of all possible filtering combinations for each query:
#     filtering on [d1, d2, ...] dimensions
#     in combination order of d bits decimal: (1 - on / 0 - not on),
#     example:
#       001 - selectivity of filtering on (d3)
#       101 - selectivity of filtering on (d1 & d3)
#   format (csv):
#     id, sel(1), sel(2), ..., sel(2**d-1)
#     ...
#   file name: sel_[table]_[in_file]
#
###########################################################


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Collect selectivities for benchmark queries.")
    parser.add_argument("-ds", "--dataset", help="dataset: dataset to run the queries on. Default: twitter",
                        type=str, required=False, default="twitter")
    parser.add_argument("-d", "--dimension", help="dimension: dimension of the queries. Default: 3", type=int,
                        required=False, default=3)
    parser.add_argument("-if", "--in_file", help="in_file: input file that holds the queries", required=True)
    parser.add_argument("-t", "--table", help="table: table name on which to collect selectivities", required=True)
    args = parser.parse_args()

    dataset = args.dataset
    dimension = args.dimension
    in_file = args.in_file
    table = args.table

    database_config = config.database_configs["postgresql"]
    dataset = config.datasets[dataset]

    # initialize DB handle
    postgresql = PostgreSQL(
        database_config.hostname,
        database_config.username,
        database_config.password,
        dataset.database
    )

    num_of_plans = 2 ** dimension - 1  # plan 0 is the original plan (no hint)

    # 0. collect the size of the table provided
    print("start collecting table size ...")
    table_size = postgresql.size_table(table)
    print("size of table [" + table + "] = " + str(table_size))

    print("start collecting selectivities for queries ...")

    # 1. read queries into memory
    queries = dataset.load_queries_file(dimension, in_file)
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
        for fc in range(1, num_of_plans + 1):
            query["sel_" + str(fc)] = dataset.sel_query(postgresql, dimension, query, fc, table, table_size)
        bar.update(index + 1)
    bar.finish()
    end = time.time()
    print("running probing queries against DB is done, time: " + str(end - start) + " seconds.")

    # 3. write queries selectivities to output file named sel_[table]_[in_file]
    out_file = in_path + "/sel_" + table + "_" + in_filename
    Util.dump_queries_sels_file(dimension, out_file, queries)

