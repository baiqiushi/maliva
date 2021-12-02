import argparse
import config
import csv
import math
import progressbar
import random
import time
from postgresql import PostgreSQL
from smart_util import Util


###########################################################
#  smart_label_queries.py
#
# Purpose:
#   label running time of all possible hinting plans for given queries
#
# Arguments:
#   -ds  / --dataset        dataset to run the queries on. Default: twitter
#   -d   / --dimension      dimension of the queries. Default: 3
#   -if  / --in_file        input file that holds the queries
#   -run / --run            run how many times
#   -oo  / --original_only  tag query performance for the original plan only
#   -ho  / --hint_only      tag query performance for the hinted plans only
#   -sp  / --start_plan     tag query performance for a range of plan ids, start id
#   -ep  / --end_plan       tag query performance for a range of plan ids, end id
#   -nj  / --num_join       number of join methods. Default: 1
#
# Dependencies:
#   python3.7 & pip: https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb-cli3-install-linux.html
#   pip install progressbar
#
# Output:
#   query performance on different hinted plans: hint using/not using index on [d1, d2, ...] dimensions
#     in combination order of d bits decimal: (1 - using / 0 - not using),
#     example:
#       000 - (special case) No hint at all, its original plan
#       101 - using d1 and d3 indexes, not using d2 index
#             /*+ BitmapScan(t idx_tweets_text idx_tweets_coordinate) */
#   format (csv):
#     id, time(0), time(1), time(2), ..., time(2**d-1)
#     ...
#   file name: labeled_[in_file]
#              labeled_std_[in_file]
#
###########################################################


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Label benchmark queries.")
    parser.add_argument("-ds", "--dataset", help="dataset: dataset to run the queries on. Default: twitter",
                        type=str, required=False, default="twitter")
    parser.add_argument("-d", "--dimension", help="dimension: dimension of the queries. Default: 3",
                        type=int, required=False, default=3)
    parser.add_argument("-if", "--in_file", help="in_file: input file that holds the queries", required=True)
    parser.add_argument("-run", "--run", help="run: run how many times", required=False, type=int, default=3)
    parser.add_argument("-oo", "--original_only",
                        help="original_only: label query performance for the original plan only",
                        required=False, action="store_true")
    parser.add_argument("-ho", "--hint_only", help="hint_only: label query performance for the hinted plans only",
                        required=False, action="store_true")
    parser.add_argument("-sp", "--start_plan",
                        help="start_plan: label query performance for a range of plan ids, start id",
                        required=False, type=int, default=-1)
    parser.add_argument("-ep", "--end_plan", help="end_plan: label query performance for a range of plan ids, end id",
                        required=False, type=int, default=-1)
    parser.add_argument("-nj", "--num_join", help="num_join: number of join methods. Default: 1", 
                        required=False, type=int, default=1)
    args = parser.parse_args()

    dataset = args.dataset
    dimension = args.dimension
    in_file = args.in_file
    num_of_runs = args.run
    original_only = args.original_only
    hint_only = args.hint_only
    start_plan = args.start_plan
    end_plan = args.end_plan
    num_of_joins = args.num_join

    database_config = config.database_configs["postgresql"]
    dataset = config.datasets[dataset]

    # range of plan ids to label
    num_of_plans = Util.num_of_plans(dimension, num_of_joins)
    if start_plan >= 0:
        start_plan_id = start_plan
    else:
        start_plan_id = 0
    if end_plan >= 0:
        end_plan_id = end_plan
    else:
        end_plan_id = num_of_plans
    if original_only:
        start_plan_id = 0
        end_plan_id = 0
    elif hint_only:
        start_plan_id = 1
        end_plan_id = num_of_plans

    print("start_plan_id = ", start_plan_id)
    print("end_plan_id = ", end_plan_id)

    # initialize DB handle
    postgresql = PostgreSQL(
        database_config.hostname,
        database_config.username,
        database_config.password,
        dataset.database,
        database_config.timeout
    )

    print("start labeling queries ...")

    # 1. read queries into memory
    queries = dataset.load_queries_file(dimension, in_file)
    print("loaded ", len(queries), " queries into memory.")
    # Build map <id, query>
    queries_map = {}
    for query in queries:
        queries_map[query["id"]] = query

    # 2. run queries to collect timings for plans
    #    - run [num_of_runs] in total, for each run:
    #        - enumerate all tuples of (query_id, plan_id) into a list
    #        - shuffle the list,
    #        - loop the list, for each combination (query_id, plan_id):
    #            - run query[query_id] using plan[plan_id]
    print("start running queries against DB.")
    runs = []
    start_runs = time.time()
    for ri in range(0, num_of_runs):
        print("---------- run " + str(ri + 1) + " ----------")
        # each run is a map of query["id"] -> [0.0, 0.0, ..., 0.0] (at most num_of_plans+1 zeros)
        list_of_query_ids = [query_id for query_id in sorted(queries_map)]
        run = {}
        for query_id in list_of_query_ids:
            run[query_id] = [0.0] * (num_of_plans + 1)
        # populate the combination list
        queries_plans = []
        for query_id in sorted(queries_map):
            for plan_id in range(start_plan_id, end_plan_id + 1):
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
            run[query["id"]][plan] = dataset.time_query(postgresql, dimension, query, plan)
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
    print("all ", num_of_runs, " runs are done, time: " + str(end_runs - start_runs) + " seconds.")

    # 3. compute average and standard deviation of timings of each query each plan in all runs
    for query in queries:
        for plan in range(0, num_of_plans + 1):
            sum_of_runs = 0.0
            for run in runs:
                sum_of_runs += run[query["id"]][plan]
            avg_of_runs = sum_of_runs / num_of_runs
            sqr_sum_diffs = 0.0
            for run in runs:
                sqr_sum_diffs += (run[query["id"]][plan] - avg_of_runs) ** 2
            std_of_runs = math.sqrt(sqr_sum_diffs / len(runs))
            # only update output result for targeting plans
            if start_plan_id <= plan <= end_plan_id:
                query["time_" + str(plan)] = avg_of_runs
                query["time_" + str(plan) + "_std"] = std_of_runs
            # other plans init to 0.0 or keep as before
            elif "time_" + str(plan) not in query.keys():
                query["time_" + str(plan)] = 0.0
                query["time_" + str(plan) + "_std"] = 0.0

    # 4.0 resort queries by id
    queries = sorted(queries, key=lambda k: k["id"])

    # 4.1 write labeled queries to output file named labeled_[in_file]
    out_avg_file = "labeled_" + in_file
    Util.dump_labeled_queries_file(dimension, out_avg_file, queries, num_of_joins)

    # 4.2 write labeled std queries to output file named labeled_std_[in_file]
    out_std_file = "labeled_std_" + in_file
    Util.dump_labeled_std_queries_file(dimension, out_std_file, queries, num_of_joins)

