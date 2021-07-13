import argparse
from smart_util import Util


###########################################################
#  smart_output_times_nyc.py
#
# Arguments:
#   -lf / --labeled_file    input file that holds labeled queries
#   -ef / --evaluated_file  input file that holds evaluated labeled queries
#   -tb / --time_budget     time (second) for a query to be viable
#   -xt / --x_tic           the x_tic value for this processed entry
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
    parser.add_argument("-lf", "--labeled_file",
                        help="labeled_file: input file that holds labeled queries",
                        type=str, required=True)
    parser.add_argument("-ef", "--evaluated_file",
                        help="evaluated_file: input file that holds evaluated labeled queries",
                        type=str, required=True)
    parser.add_argument("-tb", "--time_budget",
                        help="time_budget: time (second) for a query to be viable",
                        type=float, required=False, default=0)
    parser.add_argument("-xt", "--x_tic", help="x_tic: the x_tic value for this processed entry",
                        type=str, required=True)
    args = parser.parse_args()

    labeled_queries_file = args.labeled_file
    evaluated_queries_file = args.evaluated_file
    time_budget = args.time_budget
    x_tic = args.x_tic

    # load [labeled_file] into memory
    labeled_queries = Util.load_labeled_queries_file_nyc(labeled_queries_file)

    # load [evaluated_file] into memory
    evaluated_queries = Util.load_evaluated_queries_file(evaluated_queries_file)
    # build map <id, query> for evaluated_queries
    evaluated_queries_map = {}
    for query in evaluated_queries:
        evaluated_queries_map[query["id"]] = query

    # aggregate times for different methods
    sum_time_queries = {"basic": 0.0, "mdp": 0.0, "mdp_planning": 0.0, "mdp_querying": 0.0}
    cnt_time_queries = 0
    for query in labeled_queries:
        id = query["id"]
        sum_time_queries["basic"] += query["time_0"]
        sum_time_queries["mdp"] += evaluated_queries_map[id]["total_time"]
        sum_time_queries["mdp_planning"] += evaluated_queries_map[id]["planning_time"]
        sum_time_queries["mdp_querying"] += evaluated_queries_map[id]["querying_time"]
        cnt_time_queries += 1
    if cnt_time_queries == 0:
        cnt_time_queries = 1

    # output processed entry to stdout
    print(x_tic,
          "{:.2f}".format(sum_time_queries["basic"] / cnt_time_queries),
          "{:.2f}".format(sum_time_queries["mdp"] / cnt_time_queries),
          "{:.2f}".format(sum_time_queries["mdp_querying"] / cnt_time_queries),
          "{:.2f}".format(sum_time_queries["mdp_planning"] / cnt_time_queries),
          sep=",    "
          )

