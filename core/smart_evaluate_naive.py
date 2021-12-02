import argparse
from smart_util import Util
from smart_query_estimator import Query_Estimator


###########################################################
#  smart_evaluate_naive.py
#
#  -d  / --dimension       dimension: dimension of the queries. Default: 3
#  -nj / --num_join        number of join methods. Default: 1
#  -lf / --labeled_file    input file that holds labeled queries for evaluation
#  -uc / --unit_cost       time (second) to collect selectivity value for one condition
#  -ef / --evaluated_file  output file that holds the evaluated queries result
#  -v  / --version         version of DQN and environment to use
#  -dbg  / --debug         debug query id
#  ** Only required when version = 1/2:
#  -llsf / --list_labeled_sel_file  list of labeled_sel_queries files for different sample sizes
#  -lsqf / --list_sel_query_file    list of sel_queries files for different sample sizes
#  -qmp  / --qe_model_path          input path to load the models used by Query Estimator
#  -sp   / --sample_pointer         pointer to the sample size to use for the query_estimator. Default: 0
#
# Dependencies:
#   python3.7 & pip: https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb-cli3-install-linux.html
#   pip install torch
#
###########################################################

# Evaluate Naive
#
# @param - dimension: dimension of the queries
# @param - labeled_queries: [list of query objects], each query object being
#          {id, time_0, time_1, ..., time_(2**d-1)}
# @param - unit_cost: float, time (second) to collect selectivity value for one condition
# @param - version: int, version of DQN and environment to use
#
# * Only valid when version == 1/2:
# @param - samples_labeled_sel_queries: [list of [list of sel queries times]], each inside list being
#          [{id, time_sel_1, time_sel_2, ..., time_sel_(2**d-1)}]
#          * the outside list is ordered by the sample sizes ascending (e.g., 5k, 50k, 500k)
# @param - samples_query_sels: [list of [list of query_sels]], each inside list being
#          [{id, sel_1, sel_2, ..., sel_(2**d-1)}]
#          * the outside list is ordered by the sample sizes ascending (e.g., 5k, 50k, 500k)
# @param - query_estimator: object, Query_Estimator class instance
# @param - sample_pointer: int, [0~2], pointer to the sample size to use for the query_estimator. Default: 0
# @param - num_of_joins: int, number of join methods in hints set.
#
# @return - (list of evaluated query objects, win_rate), each query object being
#           {id, planning_time, querying_time, total_time, win(1/0), plans_tried(x_x_x_x), reason}
def evaluate_naive(dimension,
                   labeled_queries,
                   unit_cost,
                   samples_labeled_sel_queries=[],
                   samples_query_sels=[],
                   query_estimator=None,
                   sample_pointer=0,
                   version='1',
                   num_of_joins=1):

    num_of_plans = Util.num_of_plans(dimension, num_of_joins)
    num_of_sels = 2 ** dimension - 1

    if version == '1':
        # store the labeled_sel_queries of given sample_pointer into a hash map with query["id"] as the key
        sample_labeled_sel_queries =  samples_labeled_sel_queries[sample_pointer]
        labeled_sel_queries = {}
        for labeled_sel_query in sample_labeled_sel_queries:
            labeled_sel_queries[labeled_sel_query["id"]] = labeled_sel_query

        # store the queries_sels into a hash map with query["id] as the key
        sample_queries_sels = samples_query_sels[sample_pointer]
        queries_sels = {}
        for query_sels in sample_queries_sels:
            queries_sels[query_sels["id"]] = query_sels

    # evaluate labeled queries one by one
    evaluated_queries = []
    for query in labeled_queries:
        qid = query["id"]

        if version == '0':
            # pay the cost of estimating all the plan's query time
            planning_time = unit_cost * num_of_sels
            
            # querying_time is the min of all plans' querying times
            queryting_times = []
            for plan in range(1, num_of_plans + 1):
                queryting_times.append(query["time_" + str(plan)])
            querying_time = min(queryting_times)
            
            total_time = planning_time + querying_time

            evaluated_queries.append(
                {"id": qid,
                "planning_time": planning_time,
                "querying_time": querying_time,
                "total_time": total_time,
                "win": -1,
                "plans_tried": "_".join(str(x) for x in range(1, num_of_plans + 1)),
                "reason": "null"
                }
            )

        elif version == '1':
            # pay the cost of estimating all the plan's query time
            planning_time = 0.0
            labeled_sel_query = labeled_sel_queries[qid]
            for sel in range(1, num_of_sels + 1):
                planning_time += labeled_sel_query["time_sel_" + str(sel)]

            # estimate all queries and select the fastest plan, 
            # then get the real querying time of the plan
            min_estimate_time = 100.0
            min_estimate_plan = 0
            query_sels = queries_sels[qid]
            for plan in range(1, num_of_plans + 1):
                # get the input vector for Query_Estimator
                xte = []
                sel_ids = Util.sel_ids_of_plan(plan, dimension, num_of_joins)
                x = []
                for sel_id in sel_ids:
                    x.append(query_sels["sel_" + str(sel_id)])
                xte.append(x)
                # estimate query time
                ypr = query_estimator.predict(plan, xte)
                estimate_time = ypr[0, 0]
                if estimate_time < min_estimate_time:
                    min_estimate_time = estimate_time
                    min_estimate_plan = plan
            if min_estimate_plan == 0:
                print("[ERROR] query {" + str(query) + "} finds no fastest plan!")
                exit(0)
            
            # retrieve the real querying time for the selected min_estimate_plan
            querying_time = query["time_" + str(min_estimate_plan)]

            total_time = planning_time + querying_time

            evaluated_queries.append(
                {"id": qid,
                "planning_time": planning_time,
                "querying_time": querying_time,
                "total_time": total_time,
                "win": -1,
                "plans_tried": "_".join(str(x) for x in range(1, num_of_plans + 1)),
                "reason": "null"
                }
            )

    return evaluated_queries


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Evaluate Naive solution.")
    parser.add_argument("-d", "--dimension",
                        help="dimension: dimension of the queries. Default: 3",
                        type=int, required=False, default=3)
    parser.add_argument("-nj", "--num_join", help="num_join: number of join methods. Default: 1", 
                        required=False, type=int, default=1)
    parser.add_argument("-lf", "--labeled_file",
                        help="labeled_file: input file that holds labeled queries for evaluation",
                        type=str, required=True)
    parser.add_argument("-uc", "--unit_cost",
                        help="unit_cost: time (second) to collect selectivity value for one condition",
                        type=float, required=False, default=0.05)
    parser.add_argument("-ef", "--evaluated_file",
                        help="evaluated_file: output file that holds the evaluated queries result",
                        type=str, required=True)
    parser.add_argument("-v", "--version",
                        help="version: version of DQN and environment to use. Default: '1'",
                        type=str, required=False, default='1')
    parser.add_argument("-llsf", "--list_labeled_sel_file",
                        help="list_labeled_sel_file: list of labeled_sel_queries files for different sample sizes",
                        action='append', required=False, default=[])
    parser.add_argument("-lsqf", "--list_sel_query_file",
                        help="list_sel_query_file: list of sel_queries files for different sample sizes",
                        action='append', required=False, default=[])
    parser.add_argument("-qmp", "--qe_model_path",
                        help="qe_model_path: input path to load the models used by Query Estimator",
                        type=str, required=False, default=None)
    parser.add_argument("-sp", "--sample_pointer",
                        help="sample_pointer: pointer to the sample size to use for the query_estimator. Default: 0",
                        type=int, required=False, default=0)
    args = parser.parse_args()

    dimension = args.dimension
    num_of_joins = args.num_join
    labeled_queries_file = args.labeled_file
    unit_cost = args.unit_cost
    evaluated_queries_file = args.evaluated_file
    version = args.version
    sample_pointer = args.sample_pointer

    # load labeled queries into memory
    labeled_queries = Util.load_labeled_queries_file(dimension, labeled_queries_file, num_of_joins)

    # For version = 1/2
    if version == '1' or version == '2':
        list_labeled_sel_file = args.list_labeled_sel_file
        list_sel_query_file = args.list_sel_query_file
        qe_model_path = args.qe_model_path

        # assert required parameters
        if len(list_labeled_sel_file) == 0:
            print("-llsf / --list_labeled_sel_file is required when --version is 2!")
            exit(0)
        if len(list_sel_query_file) == 0:
            print("-lsqf / --list_sel_query_file is required when --version is 2!")
            exit(0)
        if qe_model_path is None:
            print("-qmp / --qe_model_path is required when --version is 2!")
            exit(0)
        if len(list_labeled_sel_file) != len(list_sel_query_file):
            print("lengths of list_labeled_sel_file & list_sel_query_file must be the same when --version is 2!")
            exit(0)

        samples_labeled_sel_queries = Util.load_labeled_sel_queries_files(dimension, list_labeled_sel_file)
        samples_query_sels = Util.load_queries_sels_files(dimension, list_sel_query_file)

        # new a Query Estimator
        query_estimator = Query_Estimator(dimension, num_of_joins)
        # load Query Estimator models from files
        query_estimator.load(qe_model_path)
    else:
        samples_labeled_sel_queries = []
        samples_query_sels = []
        query_estimator = None

    # evaluate Naive solution
    evaluated_queries = evaluate_naive(dimension,
                                       labeled_queries,
                                       unit_cost,
                                       samples_labeled_sel_queries=samples_labeled_sel_queries,
                                       samples_query_sels=samples_query_sels,
                                       query_estimator=query_estimator,
                                       sample_pointer=sample_pointer,
                                       version=version,
                                       num_of_joins=num_of_joins)

    # output evaluated queries to console
    print("======== Evaluation of Naive Solution ========")
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

