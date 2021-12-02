import argparse
from smart_util import Util


###########################################################
#  smart_output_best_qe_qualities.py
#
# Purpose:
#   Given multiple evaluated_file's, and time budget,
#   select the best qe for all queries (highest viable query percentage),
#   and output the qualities (avg and std) of the best qe results.
#
# Arguments:
#   -d    / --dimension                    dimension of the queries. Default: 3
#   -nj   / --num_join                     number of join methods. Default: 1
#   -lf   / --labeled_file                 input file that holds labeled queries
#   -leqf / --list_evaluated_quality_file  list of evaluated (with) quality files for different sample sizes [optional]
#   -tb   / --time_budget                  time (second) for a query to be viable
#   -xt   / --x_tic                        the x_tic value for this processed entry
#   -rs   / --result                       output qualities result of lossy queries, default: False
#
# Output:
#   if [result] is False:
#       print one line to the stdout:
#       [x_tic] [avg quality of all] [std quality of all] [avg quality of lossy queries] [std quality of lossy queries]
#   else:
#       print multiple lines to the stdout:
#           query_id,  quality,  sampling_plan_id
#           ...
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
    parser.add_argument("-leqf", "--list_evaluated_quality_file",
                        help="list_evaluated_quality_file: list of evaluated (with) quality files for different sample sizes [optional]",
                        action='append', required=False, default=[])
    parser.add_argument("-tb", "--time_budget",
                        help="time_budget: time (second) for a query to be viable",
                        type=float, required=False, default=0)
    parser.add_argument("-xt", "--x_tic", help="x_tic: the x_tic value for this processed entry",
                        type=str, required=True)
    parser.add_argument("-rs", "--result",
                        help="result: output qualities result of lossy queries, default: False",
                        dest='result', action='store_true')
    parser.set_defaults(result=False)
    args = parser.parse_args()

    dimension = args.dimension
    num_of_joins = args.num_join
    labeled_queries_file = args.labeled_file
    list_evaluated_quality_queries_files = args.list_evaluated_quality_file
    time_budget = args.time_budget
    x_tic = args.x_tic
    result = args.result

    # load [labeled_file] into memory
    labeled_queries = Util.load_labeled_queries_file(dimension, labeled_queries_file, num_of_joins)

    # load [list of evaluated_quality_files] into memory
    samples_evaluated_quality_queries = Util.load_evaluated_queries_files(list_evaluated_quality_queries_files, has_quality=True)

    # 2 count good queries with given time_budget in
    # each of the samples_evaluated_queries join labeled_queries (mdp_approach)
    # then pick the best sample_evaluated_queries as the result, the qe as the best qe.
    # Then compute the qualities using the best qe.
    best_qe_mdp_approach_good_queries = 0
    # avg/std qualities of all labeled_queries 
    best_qe_mdp_approach_total_avg_qualities = 0.0
    best_qe_mdp_approach_total_std_qualities = 0.0
    # avg/std qualities of delta labeled_queries (those queries that can not be viable as lossless)
    best_qe_mdp_approach_delta_avg_qualities = 0.0
    best_qe_mdp_approach_delta_std_qualities = 0.0
    qualities_result = []
    for sample_evaluated_quality_queries in samples_evaluated_quality_queries:
        mdp_approach_good_queries = Util.count_good_queries_mdp_approach(labeled_queries,
                                                                         sample_evaluated_quality_queries,
                                                                         time_budget)
        if mdp_approach_good_queries > best_qe_mdp_approach_good_queries:
            best_qe_mdp_approach_good_queries = mdp_approach_good_queries
            (best_qe_mdp_approach_total_avg_qualities, 
             best_qe_mdp_approach_total_std_qualities,
             best_qe_mdp_approach_delta_avg_qualities,
             best_qe_mdp_approach_delta_std_qualities
            ) = Util.qualities_of_queries_mdp_approach(labeled_queries, 
                                                       sample_evaluated_quality_queries,
                                                       time_budget)
            if result:
                qualities_result = Util.qualities_result_of_queries_mdp_approach(labeled_queries, 
                                                                                 sample_evaluated_quality_queries,
                                                                                 time_budget)

    # 4 output processed entry to stdout
    if result:
        for quality_result in qualities_result:
            output_results_row = []
            # query_id
            output_results_row.append(str(quality_result[0]))
            # quality
            output_results_row.append("{:.4f}".format(quality_result[1]))
            # sampling_plan_id
            output_results_row.append(str(quality_result[2]))
            # print output_results_row
            print(",  ".join(output_results_row))
    else:
        output_results_row = []
        # column 1: x_tic
        output_results_row.append(x_tic)
        # column 2: avg quality of all queries
        output_results_row.append("{:.2f}".format(best_qe_mdp_approach_total_avg_qualities))
        # column 3: std quality of all queries
        output_results_row.append("{:.2f}".format(best_qe_mdp_approach_total_std_qualities))
        # column 4: avg quality of lossy queries
        output_results_row.append("{:.2f}".format(best_qe_mdp_approach_delta_avg_qualities))
        # column 5: std quality of lossy queries
        output_results_row.append("{:.2f}".format(best_qe_mdp_approach_delta_std_qualities))
        # print output_results_row
        print(",    ".join(output_results_row))

