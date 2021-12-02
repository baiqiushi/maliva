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
#  smart_label_sel_queries.py
#
# Purpose:
#   label running time of all selectivity probing queries for given queries
#
# Arguments:
#   -ds  / --dataset        dataset to run the queries on. Default: twitter
#   -d   / --dimension      dimension of the queries. Default: 3
#   -if  / --in_file        input file that holds the queries
#   -run / --run            run how many times (default: 3)
#   -t   / --table          table name on which to run the selectivity probing queries
#
# Dependencies:
#   python3.7 & pip: https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb-cli3-install-linux.html
#   pip install progressbar
#
# Output:
#   query performance on different selectivity probing queries for each query:
#     selectivity of filtering combinations on [d1, d2, ...] dimensions
#     in combination order of d bits decimal: (1 - on / 0 - not on),
#     example:
#       001 - probing query for selectivity of filtering on (d3)
#       101 - probing query for selectivity of filtering on (d1 & d3)
#   format (csv):
#     id, time(1), time(2), ..., time(2**d-1)
#     ...
#   file name: labeled_sel_[table]_[in_file]
#              labeled_sel_std_[table]_[in_file]
#
###########################################################


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Label selectivity probing queries for benchmark queries.")
    parser.add_argument("-ds", "--dataset", help="dataset: dataset to run the queries on. Default: twitter",
                        type=str, required=False, default="twitter")
    parser.add_argument("-d", "--dimension", help="dimension: dimension of the queries. Default: 3", type=int,
                        required=False, default=3)
    parser.add_argument("-if", "--in_file", help="in_file: input file that holds the queries", required=True)
    parser.add_argument("-run", "--run", help="run: run how many times", required=False, type=int, default=3)
    parser.add_argument("-t", "--table", help="table: table name on which to run the selectivity probing queries",
                        required=True)
    args = parser.parse_args()

    dataset = args.dataset
    dimension = args.dimension
    in_file = args.in_file
    num_of_runs = args.run
    table = args.table

    database_config = config.database_configs["postgresql"]
    dataset = config.datasets[dataset]

    num_of_plans = 2 ** dimension - 1  # plan 0 is the original plan (no hint)

    # initialize DB handle
    postgresql = PostgreSQL(
        database_config.hostname,
        database_config.username,
        database_config.password,
        dataset.database
    )

    print("start labeling selectivity probing queries ...")

    # 1. read queries into memory
    queries = dataset.load_queries_file(dimension, in_file)
    print("loaded ", len(queries), " queries into memory.")
    # Build map <id, query>
    queries_map = {}
    for query in queries:
        queries_map[query["id"]] = query

    # extract path and filename from in_file
    in_path, in_filename = os.path.split(in_file)

    # 2. run queries to collect timings for selectivity probing queries
    #    - run [num_of_runs] in total, for each run:
    #        - enumerate all tuples of (query_id, fc_id) into a list
    #        - shuffle the list,
    #        - loop the list, for each combination (query_id, fc_id):
    #            - run probing query of selectivity for filtering combination (fc_id) for query[query_id]
    print("start running queries against DB.")
    runs = []
    start_runs = time.time()
    for ri in range(0, num_of_runs):
        print("---------- run " + str(ri + 1) + " ----------")
        # each run is a map of query["id"] -> [0.0, 0.0, ..., 0.0] (num_of_plans zeros)
        list_of_query_ids = [query_id for query_id in sorted(queries_map)]
        run = {}
        for query_id in list_of_query_ids:
            run[query_id] = [0.0] * num_of_plans
        # populate the combination list
        queries_fcs = []
        for query_id in sorted(queries_map):
            for fc_id in range(1, num_of_plans + 1):
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
            run[query["id"]][fc - 1] = dataset.time_sel_query(postgresql, dimension, query, fc, table)
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
    print("all ", num_of_runs, " runs are done, time: " + str(end_runs - start_runs) + " seconds.")

    # 3. compute average and standard deviation of timings of each query each fc in all runs
    for query in queries:
        for fc in range(1, num_of_plans + 1):
            sum_of_runs = 0.0
            for run in runs:
                sum_of_runs += run[query["id"]][fc - 1]
            avg_of_runs = sum_of_runs / num_of_runs
            sqr_sum_diffs = 0.0
            for run in runs:
                sqr_sum_diffs += (run[query["id"]][fc - 1] - avg_of_runs) ** 2
            std_of_runs = math.sqrt(sqr_sum_diffs / len(runs))
            query["time_" + str(fc)] = avg_of_runs
            query["time_" + str(fc) + "_std"] = std_of_runs

    # 4.0 resort queries by id
    queries = sorted(queries, key=lambda k: k["id"])

    # 4.1 write labeled sel queries to output file named labeled_sel_[table]_[in_file]
    out_avg_file = in_path + "/labeled_sel_" + table + "_" + in_filename
    Util.dump_labeled_sel_queries_file(dimension, out_avg_file, queries)

    # 4.2 write std of labeled sel queries to output file named labeled_sel_std_[table]_[in_file]
    out_std_file = in_path + "/labeled_sel_std_" + table + "_" + in_filename
    Util.dump_labeled_sel_std_queries_file(dimension, out_std_file, queries)

