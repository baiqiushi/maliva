import argparse
from smart_util import Util


###########################################################
#  smart_output_best_qe_results.py
#
# Purpose:
#   Given multiple evaluated_file's, and time budget,
#   select the best qe for all queries (highest viable query percentage),
#   and output the best results based that.
#
# Arguments:
#   -d    / --dimension                    dimension of the queries. Default: 3
#   -nj   / --num_join                     number of join methods. Default: 1
#   -lf   / --labeled_file                 input file that holds labeled queries
#   -lef  / --list_evaluated_file          list of evaluated files for different sample sizes
#   -oef  / --other_evaluated_file         other system's evaluated file list [optional]
#   -uc   / --unit_cost                    time (second) to collect selectivity value for one condition [optional],
#                                            when set, output best approach
#   -tb   / --time_budget                  time (second) for a query to be viable
#   -xt   / --x_tic                        the x_tic value for this processed entry
#
# Output:
#   print one line to the stdout:
#   [x_tic]    [basic approach percentage of viable queries]    [mdp approach percentage of viable queries]
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
    parser.add_argument("-uc", "--unit_cost",
                        help="unit_cost: time (second) to collect selectivity value for one condition",
                        type=float, required=False, default=-1)
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
    unit_cost = args.unit_cost
    time_budget = args.time_budget
    x_tic = args.x_tic

    # load [labeled_file] into memory
    labeled_queries = Util.load_labeled_queries_file(dimension, labeled_queries_file, num_of_joins)

    # load [list of evaluated_files] into memory
    samples_evaluated_queries = Util.load_evaluated_queries_files(list_evaluated_queries_files)

    # 1 count good queries with given time_budget in labeled_queries (basic_approach)
    basic_approach_good_queries = Util.count_good_queries_basic_approach(labeled_queries, time_budget, dimension, num_of_joins)

    # 2 count good queries with given time_budget in
    # each of the samples_evaluated_queries join labeled_queries (mdp_approach)
    # then pick the best sample_evaluated_queries as the result
    #   if samples_evaluated_quality_queries is present, use it.
    best_qe_mdp_approach_good_queries = 0
    for idx, sample_evaluated_queries in enumerate(samples_evaluated_queries):
        mdp_approach_good_queries = Util.count_good_queries_mdp_approach(labeled_queries,
                                                                         sample_evaluated_queries,
                                                                         time_budget)
        if mdp_approach_good_queries >= best_qe_mdp_approach_good_queries:
            best_qe_mdp_approach_good_queries = mdp_approach_good_queries
    
    # 2.[optional] count good queries with given time_budget in
    # the given evaluated files for other systems.
    if other_evaluated_queries_files and len(other_evaluated_queries_files) > 0:
        others_evaluated_queries = Util.load_evaluated_queries_files(other_evaluated_queries_files)
        other_approaches_good_queries = []
        for other_evaluated_queries in others_evaluated_queries:
            other_approach_good_queries = Util.count_good_queries_mdp_approach(labeled_queries, 
                                                                               other_evaluated_queries, 
                                                                               time_budget)
            other_approaches_good_queries.append(other_approach_good_queries)

    # 3 count good queries with given time_budget and unit_cost in labeled_queries (max_possible)
    max_possible_good_queries = Util.count_good_queries_max_possible(dimension, labeled_queries, unit_cost, time_budget, num_of_joins)

    # 4 output processed entry to stdout
    output_results_row = []
    # column 1: x_tic
    output_results_row.append(x_tic)
    # column 2: basic approach
    output_results_row.append("{:.2f}".format(basic_approach_good_queries * 100 / len(labeled_queries)))
    # column 3: best qe mdp approach
    output_results_row.append("{:.2f}".format(best_qe_mdp_approach_good_queries * 100 / len(labeled_queries)))
    # column 4~n-1: other approaches (optional)
    if other_evaluated_queries_files and len(other_evaluated_queries_files) > 0:
        for other_approach_good_queries in other_approaches_good_queries:
            output_results_row.append("{:.2f}".format(other_approach_good_queries * 100 / len(labeled_queries)))
    # column n: output best approach result only when unit_cost is setup
    if unit_cost >= 0:
        output_results_row.append("{:.2f}".format(max_possible_good_queries * 100 / len(labeled_queries)))
    # print output_results_row
    print(",    ".join(output_results_row))

