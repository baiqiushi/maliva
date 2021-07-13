import argparse
from smart_util import Util


###########################################################
#  smart_output_results_nyc.py
#
# Arguments:
#   -lf / --labeled_file    input file that holds labeled queries
#   -ef / --evaluated_file  input file that holds evaluated labeled queries
#   -uc / --unit_cost       time (second) to collect selectivity value for one condition [optional],
#                           when set, output best approach
#   -tb / --time_budget     time (second) for a query to be viable
#   -xt / --x_tic           the x_tic value for this processed entry
#
# Output:
#   print one line to the stdout:
#   [x_tic]    [mdp approach percentage of viable queries]    [basic approach percentage of viable queries]
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
    parser.add_argument("-uc", "--unit_cost",
                        help="unit_cost: time (second) to collect selectivity value for one condition",
                        type=float, required=False, default=-1)
    parser.add_argument("-tb", "--time_budget",
                        help="time_budget: time (second) for a query to be viable",
                        type=float, required=False, default=0)
    parser.add_argument("-xt", "--x_tic", help="x_tic: the x_tic value for this processed entry",
                        type=str, required=True)
    args = parser.parse_args()

    labeled_queries_file = args.labeled_file
    evaluated_queries_file = args.evaluated_file
    unit_cost = args.unit_cost
    time_budget = args.time_budget
    x_tic = args.x_tic

    # load [labeled_file] into memory
    labeled_queries = Util.load_labeled_queries_file_nyc(labeled_queries_file)

    # load [evaluated_file] into memory
    evaluated_queries = Util.load_evaluated_queries_file(evaluated_queries_file)

    # 1 count good queries with given time_budget in labeled_queries (basic_approach)
    basic_approach_good_queries = Util.count_good_queries_basic_approach(labeled_queries, time_budget)

    # 2 count good queries with given time_budget in evaluated_queries join labeled_queries (mdp_approach)
    mdp_approach_good_queries = Util.count_good_queries_mdp_approach(labeled_queries, evaluated_queries, time_budget)

    # 3 count good queries with given time_budget and unit_cost in labeled_queries (max_possible)
    max_possible_good_queries = Util.count_good_queries_max_possible_3d(labeled_queries, unit_cost, time_budget)

    # 4 output processed entry to stdout
    # output best approach result only when unit_cost is setup
    if unit_cost >= 0:
        print(x_tic,
              "{:.2f}".format(basic_approach_good_queries * 100 / len(labeled_queries)),
              "{:.2f}".format(mdp_approach_good_queries * 100 / len(labeled_queries)),
              "{:.2f}".format(max_possible_good_queries * 100 / len(labeled_queries)),
              sep=",    "
              )
    else:
        print(x_tic,
              "{:.2f}".format(basic_approach_good_queries * 100 / len(labeled_queries)),
              "{:.2f}".format(mdp_approach_good_queries * 100 / len(labeled_queries)),
              sep=",    "
              )

