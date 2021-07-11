import argparse
from smart_util import Util


###########################################################
#  smart_select_queries_nyc.py
#
# Arguments:
#   -lf / --labeled_file    input file that holds labeled queries
#   -tb / --time_budget     time (second) for a query to be viable
#   -ng / --good_plans      the threshold of how many plans of the queries are good
#   -sf / --selected_file   output file that holds labeled queries that are selected
#
# Output:
#   the same format as [labeled_file]
#
###########################################################

if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Select queries.")
    parser.add_argument("-lf", "--labeled_file",
                        help="labeled_file: input file that holds labeled queries",
                        type=str, required=True)
    parser.add_argument("-tb", "--time_budget",
                        help="time_budget: time (second) for a query to be viable",
                        type=float, required=False, default=0)
    parser.add_argument("-gn", "--good_plans",
                        help="good_plans: the threshold of how many plans of the queries are good",
                        type=int, required=False, default=-1)
    parser.add_argument("-sf", "--selected_file",
                        help="selected_file: output file that holds labeled queries that are selected",
                        type=str, required=True)
    args = parser.parse_args()

    labeled_queries_file = args.labeled_file
    time_budget = args.time_budget
    good_plans = args.good_plans
    selected_queries_file = args.selected_file

    # load [labeled_file] into memory
    labeled_queries = Util.load_labeled_queries_file_nyc(labeled_queries_file)

    selected_queries = labeled_queries
    if time_budget > 0 and good_plans == -1:
        selected_queries = Util.select_possible_queries_3d(selected_queries, time_budget)
        print("    selected " + str(len(selected_queries)) + " queries after filtering by [time_budget ≤ " + str(time_budget) + "].")

    if good_plans > -1:
        selected_queries = Util.select_good_plans_queries_3d(selected_queries, time_budget, good_plans)
        print("    selected " + str(len(selected_queries)) + " queries after filtering by [good_plans = " + str(good_plans) + "].")

    # dump selected queries to file
    Util.dump_labeled_queries_file_nyc(selected_queries, selected_queries_file)
    print("selected queries saved to file [" + selected_queries_file + "].")

