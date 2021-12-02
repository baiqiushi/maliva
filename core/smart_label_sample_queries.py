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
#  smart_label_sample_queries.py
#
# Purpose:
#   label running time of all possible sampling plans for given queries, 
#   also generate query results files for each sampling plan of each query (for computing qualities in the future).
#
# Arguments:
#   -ds  / --dataset        dataset to run the queries on. Default: twitter
#   -d   / --dimension      dimension of the queries. Default: 3
#   -if  / --in_file        input file that holds the queries
#   -run / --run            run how many times
#   -sf  / --sels_file      input file that holds the sels of queries
#   -st  / --sels_table     table name on which the sels_file was collected
#   -rp  / --results_path   output path to save the results files of each query
#
# Output:
#   labeled_sample_[in_file]/labeled_std_sample_[in_file]:
#     query performance on different sampling plans: hint using index on one of the [d1, d2, ...] dimensions 
#       and limit a k such that k/table_size = one of the [s1, s2, ...] sample ratios
#     example:
#       dimensions = [idx_tweets_100m_text, idx_tweets_100m_create_at, idx_tweets_coordinate]
#       sample ratios = [6.25%, 12.5%, 25%, 50%, 75%]
#       table_size = 100,000,000 (100m)
#       time(d1_s1)[query] = time(/*+ BitmapScan(t idx_tweets_100m_text) */ SQL(query) LIMIT sel(query)*100m)
#     format (csv):
#       id, time(d1_s1), time(d1_s2), ..., time(d|d|_s|s|)
#       ...
#   rp/result_{qid}[_h{hint_id}_s{sample_ratio_id}].csv:
#     format (csv):
#       id, coordinate[0], coordinate[1]
#       ...
#
###########################################################


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Label sampled benchmark queries.")
    parser.add_argument("-ds", "--dataset", help="dataset: dataset to run the queries on. Default: twitter",
                        type=str, required=False, default="twitter")
    parser.add_argument("-d", "--dimension", help="dimension: dimension of the queries. Default: 3",
                        type=int, required=False, default=3)
    parser.add_argument("-if", "--in_file", help="in_file: input file that holds the queries", 
                        type=str, required=True)
    parser.add_argument("-run", "--run", help="run: run how many times", 
                        required=False, type=int, default=3)
    parser.add_argument("-sf", "--sels_file",
                        help="sels_file: input file that holds queries' selectivities",
                        type=str, required=True)
    parser.add_argument("-st", "--sels_table", 
                        help="sels_table: table name on which the sels_file was collected", 
                        type=str, required=True)
    parser.add_argument("-rp", "--result_path",
                        help="result_path: output path to save the results files of each query",
                        type=str, required=True)
    args = parser.parse_args()

    dataset = args.dataset
    dimension = args.dimension
    in_file = args.in_file
    num_of_runs = args.run
    queries_sels_file = args.sels_file
    sels_table = args.sels_table
    result_path = args.result_path

    database_config = config.database_configs["postgresql"]
    dataset = config.datasets[dataset]
    sample_ratios = dataset.sample_ratios
    num_of_sample_ratios = len(sample_ratios)

    # initialize DB handle
    postgresql = PostgreSQL(
        database_config.hostname,
        database_config.username,
        database_config.password,
        dataset.database
    )

    # 0. collect the size of the table provided
    print("start collecting sels table size ...")
    sels_table_size = postgresql.size_table(sels_table)
    print("size of sels table [" + sels_table + "] = " + str(sels_table_size))

    # re-initialize DB handle with timeout
    postgresql = PostgreSQL(
        database_config.hostname,
        database_config.username,
        database_config.password,
        dataset.database,
        database_config.timeout
    )

    # range of plan ids to label
    num_of_sampling_plans = Util.num_of_sampling_plans(dimension, num_of_sample_ratios)

    print("start labeling sampling plans ...")

    # 1(a). read queries into memory
    queries = dataset.load_queries_file(dimension, in_file)
    print("loaded ", len(queries), " queries into memory.")
    # Build map <id, query>
    queries_map = {}
    for query in queries:
        queries_map[query["id"]] = query
    
    # 1(b). read queries' selectivities into memory
    queries_sels = Util.load_queries_sels_file(dimension, queries_sels_file)
   
    # Build queries_card_map <id, query_cardinality>
    queries_card_map = {}
    for query_sels in queries_sels:
        query_id = query_sels["id"]
        query_sel = query_sels["sel_" + str(2 ** dimension - 1)]
        if query_sel == 0.0:
            query_sel = 1.0 / sels_table_size
        queries_card_map[query_id] = query_sel * dataset.table_size


    # 2. run queries to collect timings for plans
    #    - run [num_of_runs] in total, for each run:
    #        - enumerate all tuples of (query_id, plan_id) into a list
    #        - shuffle the list,
    #        - loop the list, for each combination (query_id, plan_id):
    #            - run query[query_id] using plan[plan_id]
    print("start running samping queries against DB.")
    runs = []
    start_runs = time.time()
    for ri in range(0, num_of_runs):
        print("---------- run " + str(ri + 1) + " ----------")
        # each run is a map of query["id"] -> [0.0, 0.0, ..., 0.0] (num_of_sampling_plans zeros)
        list_of_query_ids = [query_id for query_id in sorted(queries_map)]
        run = {}
        for query_id in list_of_query_ids:
            run[query_id] = [0.0] * num_of_sampling_plans
        # populate the combination list
        queries_plans = []
        for query_id in sorted(queries_map):
            for plan_id in range(0, num_of_sampling_plans):
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
            query_id = query_plan[0]
            query = queries_map[query_id]
            plan_id = query_plan[1]
            card = queries_card_map[query_id]
            run[query_id][plan_id], result = dataset.time_sampling_query(postgresql, dimension, query, card, plan_id)
            # dump the result to file only for the first run
            if ri == 0:
                hint_id = Util.hint_id_of_sampling_plan(num_of_sample_ratios, plan_id)
                sample_ratio_id = Util.sample_ratio_id_of_sampling_plan(num_of_sample_ratios, plan_id)
                Util.dump_query_result(result_path, query_id, hint_id, sample_ratio_id, result)
            qp += 1
            bar.update(qp)
        bar.finish()

        end_run = time.time()
        print("run [", ri + 1, "] is done, time: " + str(end_run - start_run) + " seconds.")
        runs.append(run)

        # write each run to a csv file for archiving
        archive_out_file = "labeled_sample_" + in_file + "." + str(ri+1)
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
        for plan_id in range(0, num_of_sampling_plans):
            sum_of_runs = 0.0
            for run in runs:
                sum_of_runs += run[query["id"]][plan_id]
            avg_of_runs = sum_of_runs / num_of_runs
            sqr_sum_diffs = 0.0
            for run in runs:
                sqr_sum_diffs += (run[query["id"]][plan_id] - avg_of_runs) ** 2
            std_of_runs = math.sqrt(sqr_sum_diffs / len(runs))
            query["time_" + str(plan_id)] = avg_of_runs
            query["time_" + str(plan_id) + "_std"] = std_of_runs

    # 4.0 resort queries by id
    queries = sorted(queries, key=lambda k: k["id"])

    # 4.1 write labeled sample queries to output file named labeled_sample_[in_file]
    out_avg_file = "labeled_sample_" + in_file
    Util.dump_labeled_sample_queries_file(dimension, num_of_sample_ratios, out_avg_file, queries)

    # 4.2 write labeled std sample queries to output file named labeled_std_sample_[in_file]
    out_std_file = "labeled_std_sample_" + in_file
    Util.dump_labeled_std_sample_queries_file(dimension, num_of_sample_ratios, out_std_file, queries)

