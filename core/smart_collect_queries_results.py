import argparse
import config
import progressbar
import time
from postgresql import PostgreSQL
from smart_util import Util


###########################################################
#  smart_collect_queries_results.py
#
# Purpose:
#   run the best known plan of given queries and write query results to files 
#     (for computing qualities of sample queries in the future).
#
# Arguments:
#   -ds  / --dataset        dataset to run the queries on. Default: twitter
#   -d   / --dimension      dimension of the queries. Default: 3
#   -if  / --in_file        input file that holds the queries
#   -lf  / --labeled_file   input file that holds labeled queries for selecting best plan
#   -to  / --time_out       number of seconds for a query to timeout. Default: 10
#   -t   / --table          table name on which the results will be collected
#   -rp  / --results_path   output path to save the results files of each query
#
# Output:
#   rp/result_{qid}.csv:
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
    parser.add_argument("-lf", "--labeled_file",
                        help="labeled_file: input file that holds labeled queries for selecting best plan",
                        type=str, required=True)
    parser.add_argument("-to", "--time_out", help="time_out: number of seconds for a query to timeout. Default: 10", 
                        required=False, type=int, default=10)
    parser.add_argument("-t", "--table", 
                        help="table: table name on which the results will be collected", 
                        type=str, required=True)
    parser.add_argument("-rp", "--result_path",
                        help="result_path: output path to save the results files of each query",
                        type=str, required=True)
    args = parser.parse_args()

    dataset = args.dataset
    dimension = args.dimension
    in_file = args.in_file
    labeled_queries_file = args.labeled_file
    time_out = args.time_out
    table = args.table
    result_path = args.result_path

    database_config = config.database_configs["postgresql"]
    dataset = config.datasets[dataset]

    num_of_plans = Util.num_of_plans(dimension)

    # initialize DB handle with timeout
    postgresql = PostgreSQL(
        database_config.hostname,
        database_config.username,
        database_config.password,
        dataset.database,
        time_out * 1000
    )

    print("start collecting queries results ...")

    # 1(a). read queries into memory
    queries = dataset.load_queries_file(dimension, in_file)
    print("loaded ", len(queries), " queries into memory.")
    
    # 1(b). load labeled queries into memory
    labeled_queries = Util.load_labeled_queries_file(dimension, labeled_queries_file)
    # build id -> labeled_query map
    labeled_queries_map = {}
    for labeled_query in labeled_queries:
        labeled_queries_map[labeled_query["id"]] = labeled_query

    # 2. run each query to collect results and write to file
    print("start running queries against DB.")
    running_queries_count = 0
    success_queries_count = 0
    start = time.time()
    # show progress bar
    bar = progressbar.ProgressBar(maxval=len(queries),
                                  widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()
    qp = 0
    for query in queries:
        query_id = query["id"]

        qp += 1
        bar.update(qp)

        # skip query if result file exists
        if Util.exist_query_result(result_path, query_id, -1, -1):
            continue

        # find best plan_id for this query based on labeled_query
        labeled_query = labeled_queries_map[query_id]
        best_plan_id = 0
        best_plan_time = 100.0
        for plan_id in range(1, num_of_plans + 1):
            query_time = labeled_query["time_" + str(plan_id)]
            if query_time < best_plan_time:
                best_plan_time = query_time
                best_plan_id = plan_id
        
        # construct sql for this query with best plan_id on given table
        sql = dataset.construct_sql_str(query, _dimension=dimension, _plan=best_plan_id, _table=table)
        
        # run query againt db
        running_queries_count += 1
        result = postgresql.query(sql)
        
        # dump the result to file if not timeout
        if result[0][0] != "timeout":
            success_queries_count += 1
            Util.dump_query_result(result_path, query_id, -1, -1, result)
    bar.finish()

    end = time.time()
    print("done, time: " + str(end - start) + " seconds.")
    print("====> ran queries: " + str(running_queries_count) + ", successfully got results: " + str(success_queries_count) + ".")

