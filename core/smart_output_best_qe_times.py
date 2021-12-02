import argparse
from smart_util import Util


###########################################################
#  smart_output_best_qe_times.py
#
# Arguments:
#   -d   / --dimension            dimension of the queries. Default: 3
#   -nj  / --num_join             number of join methods. Default: 1
#   -lf  / --labeled_file         input file that holds labeled queries
#   -lef / --list_evaluated_file  list of evaluated files for different sample sizes
#   -oef / --other_evaluated_file other system's evaluated file list [optional]
#   -tb  / --time_budget          time (second) for a query to be viable
#   -xt  / --x_tic                 the x_tic value for this processed entry
#
# Output:
#   print one line to the stdout:
#   [x_tic],
#   [avg time (basic)],
#   [avg time total (mdp)],
#   [avg time querying (mdp)],
#   [avg time planning (mdp)]
#
###########################################################

if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Process result files.")
    parser.add_argument("-d", "--dimension",
                        help="dimension: dimension of the queries. Default: 3",
                        type=int, required=False, default=3)
    parser.add_argument("-nj", "--num_join", help="num_join: number of join methods. Default: 1", 
                        required=False, type=int, default=1)
    parser.add_argument("-lf", "--labeled_file",
                        help="labeled_file: input file that holds labeled queries",
                        type=str, required=True)
    parser.add_argument("-lef", "--list_evaluated_file",
                        help="list_evaluated_file: list of evaluated files for different sample sizes",
                        action='append', required=False, default=[])
    parser.add_argument("-oef", "--other_evaluated_file",
                        help="other_evaluated_file: list of evaluated files for other systems",
                        action='append', required=False, default=[])
    parser.add_argument("-tb", "--time_budget",
                        help="time_budget: time (second) for a query to be viable",
                        type=float, required=False, default=0)
    parser.add_argument("-xt", "--x_tic", help="x_tic: the x_tic value for this processed entry",
                        type=str, required=True)
    args = parser.parse_args()

    dimension = args.dimension
    num_of_joins = args.num_join
    labeled_queries_file = args.labeled_file
    list_evaluated_queries_files = args.list_evaluated_file
    other_evaluated_queries_files = args.other_evaluated_file
    time_budget = args.time_budget
    x_tic = args.x_tic

    num_of_plans = Util.num_of_plans(dimension, num_of_joins)

    # load [labeled_file] into memory
    labeled_queries = Util.load_labeled_queries_file(dimension, labeled_queries_file, num_of_joins)

    # load [list of evaluated_files] into memory
    samples_evaluated_queries = Util.load_evaluated_queries_files(list_evaluated_queries_files)

    # count good queries with given time_budget in
    # each of the samples_evaluated_queries join labeled_queries (mdp_approach)
    # then pick the best sample_evaluated_queries as the result [evaluated_queries]
    best_qe_mdp_approach_good_queries = 0
    for sample_evaluated_queries in samples_evaluated_queries:
        mdp_approach_good_queries = Util.count_good_queries_mdp_approach(labeled_queries,
                                                                         sample_evaluated_queries,
                                                                         time_budget)
        if mdp_approach_good_queries >= best_qe_mdp_approach_good_queries:
            best_qe_mdp_approach_good_queries = mdp_approach_good_queries
            evaluated_queries = sample_evaluated_queries
    # build map <id, query> for evaluated_queries
    evaluated_queries_map = {}
    for query in evaluated_queries:
        evaluated_queries_map[query["id"]] = query
    
    # 2.[optional] load given evaluated files for other systems.
    other_evaluated_queries_maps = []
    if other_evaluated_queries_files and len(other_evaluated_queries_files) > 0:
        others_evaluated_queries = Util.load_evaluated_queries_files(other_evaluated_queries_files)
        for other_evaluated_queries in others_evaluated_queries:
            other_evaluated_queries_map = {}
            for query in other_evaluated_queries:
                other_evaluated_queries_map[query["id"]] = query
            other_evaluated_queries_maps.append(other_evaluated_queries_map)

    # aggregate times for different methods
    sum_time_queries = {"basic": 0.0, "mdp": 0.0, "mdp_planning": 0.0, "mdp_querying": 0.0}
    if len(other_evaluated_queries_maps) > 0:
        for other_index, other_evaluated_queries_map in enumerate(other_evaluated_queries_maps):
            sum_time_queries["other" + str(other_index)] = 0.0
            sum_time_queries["other" + str(other_index) + "_planning"] = 0.0
            sum_time_queries["other" + str(other_index) + "_querying"] = 0.0
    cnt_time_queries = 0
    for query in labeled_queries:
        id = query["id"]
        # handle the fluctuation error of time labeling
        #   time_0 can not be smaller than the best plan of all plans, 
        #     since original query plan must be one of the hinted plans
        all_plans_query_times = []
        for plan in range(1, num_of_plans + 1):
            all_plans_query_times.append(query["time_" + str(plan)])
        best_plan_query_time = min(all_plans_query_times)
        sum_time_queries["basic"] += max(query["time_0"], best_plan_query_time)
        sum_time_queries["mdp"] += evaluated_queries_map[id]["total_time"]
        sum_time_queries["mdp_planning"] += evaluated_queries_map[id]["planning_time"]
        sum_time_queries["mdp_querying"] += evaluated_queries_map[id]["querying_time"]
        if len(other_evaluated_queries_maps) > 0:
            for other_index, other_evaluated_queries_map in enumerate(other_evaluated_queries_maps):
                sum_time_queries["other" + str(other_index)] += other_evaluated_queries_map[id]["total_time"]
                sum_time_queries["other" + str(other_index) + "_planning"] += other_evaluated_queries_map[id]["planning_time"]
                sum_time_queries["other" + str(other_index) + "_querying"] += other_evaluated_queries_map[id]["querying_time"]
        cnt_time_queries += 1
    if cnt_time_queries == 0:
        cnt_time_queries = 1

    # output processed entry to stdout
    entry = [
        x_tic,
        "{:.2f}".format(sum_time_queries["basic"] / cnt_time_queries),
        "{:.2f}".format(sum_time_queries["mdp"] / cnt_time_queries),
        "{:.2f}".format(sum_time_queries["mdp_querying"] / cnt_time_queries),
        "{:.2f}".format(sum_time_queries["mdp_planning"] / cnt_time_queries)
    ]
    if len(other_evaluated_queries_maps) > 0:
        for other_index, other_evaluated_queries_map in enumerate(other_evaluated_queries_maps):
            entry.append("{:.2f}".format(sum_time_queries["other" + str(other_index)] / cnt_time_queries))
            entry.append("{:.2f}".format(sum_time_queries["other" + str(other_index) + "_querying"] / cnt_time_queries))
            entry.append("{:.2f}".format(sum_time_queries["other" + str(other_index) + "_planning"] / cnt_time_queries))
    print(",  ".join(entry))

