import argparse
import config
import time
from smart_bao_client import Bao
from smart_util import Util


###########################################################
#  smart_evaluate_bao.py
#
# Purpose:
#   Evaluate bao on given queries
#
# Arguments:
#   -ds  / --dataset         dataset to run the queries on. Default: twitter
#   -d   / --dimension       dimension of the queries. Default: 3
#   -nj  / --num_join        number of join methods. Default: 1
#   -qf  / --queries_file    input file that holds the queries
#   -lf  / --labeled_file    input file that holds the labeled queries
#   -ef  / --evaluated_file  output file that holds the evaluated queries result
#
# Dependencies:
#   Bao server running on the same machine
###########################################################


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Evaluate BAO.")
    parser.add_argument("-ds", "--dataset", 
                        help="dataset: dataset to run the queries on. Default: twitter",
                        type=str, required=False, default="twitter")
    parser.add_argument("-d", "--dimension", 
                        help="dimension of the queries. Default: 3",
                        type=int, required=False, default=3)
    parser.add_argument("-nj", "--num_join", help="num_join: number of join methods. Default: 1", 
                        required=False, type=int, default=1)
    parser.add_argument("-qf", "--queries_file", 
                        help="queries_file: input file that holds the queries", 
                        type=str, required=True)
    parser.add_argument("-lf", "--labeled_file",
                        help="labeled_file: input file that holds labeled queries",
                        type=str, required=True)
    parser.add_argument("-ef", "--evaluated_file",
                        help="evaluated_file: output file that holds the evaluated queries result",
                        type=str, required=True)
    args = parser.parse_args()

    dataset_name = args.dataset
    dimension = args.dimension
    num_of_joins = args.num_join
    queries_file = args.queries_file
    labeled_queries_file = args.labeled_file
    evaluated_queries_file = args.evaluated_file

    database_config = config.database_configs["postgresql"]
    dataset = config.datasets[dataset_name]

    # load queries into memory
    queries = dataset.load_queries_file(dimension, queries_file)
    # Build map <id, query>
    queries_map = {}
    for query in queries:
        queries_map[query["id"]] = query

    # load labeled queries into memory
    labeled_queries = Util.load_labeled_queries_file(dimension, labeled_queries_file, num_of_joins)

    bao_client = Bao(
        database_config,
        dataset,
        dimension,
        num_of_joins
    )

    # evaluate labeled queries one by one
    evaluated_queries = []
    for labeled_query in labeled_queries:
        # construct sql string
        query = queries_map[labeled_query["id"]]
        sql = dataset.construct_sql_str(query)
        # use Bao to select the query plan
        start = time.time()
        plan_id = bao_client.plan_query(sql)
        end = time.time()
        planning_time = end - start
        # collect querying time of the query plan
        querying_time = labeled_query["time_" + str(plan_id)]
        # summarize results
        total_time = planning_time + querying_time
        evaluated_queries.append({
            "id": labeled_query["id"],
            "planning_time": planning_time,
            "querying_time": querying_time,
            "total_time": total_time,
            "win": -1,
            "plans_tried": str(plan_id),
            "reason": "null"
        })

    # output evaluated queries to console
    print("======== Evaluation of BAO ========")
    print(labeled_queries_file)
    print("-----------------------------------")
    print("qid,    planning_time,    querying_time,    total_time,    win,    plans_tried,    reason")
    for query in evaluated_queries:
        print(str(query["id"]) + ",    " + str(query["planning_time"]) + ",    " +
                str(query["querying_time"]) + ",    " + str(query["total_time"]) + ",    " + str(query["win"]) +
                ",    " + query["plans_tried"] + ",    " + query["reason"])
    print("===================================")

    # output evaluated queries to file
    Util.dump_evaluated_queries_file(evaluated_queries_file, evaluated_queries)
    print("evaluated queries saved to file [" + evaluated_queries_file + "].")
