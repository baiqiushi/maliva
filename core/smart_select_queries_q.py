import argparse
from smart_util import Util


###########################################################
#  smart_select_queries_q.py
#
# Purpose:
#  select queries from given labeled_sample_queries and sample_queries_qualities
#    based on given evaluated file,
#      output those queries that are treated as "not_possible" in evaluated file.
#
# Arguments:
#   -d   / --dimension                dimension of the queries. Default: 3
#   -nsr / --num_sample_ratios        number of sample ratios.
#   -lsf / --labeled_sample_file      input file that holds labeled sample queries
#   -sqf / --sample_quality_file      input file that holds sample queries qualities
#   -ef  / --evaluated_file           input file that holds evaluated queries
#   -ols / --out_labeled_sample_file  output file that holds selected labeled sample queries
#   -osq / --out_sample_quality_file  output file that holds selected sample queries qualities
#
###########################################################

if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Select labeled sample queries and sample queries qualities based on given evaluated file.")
    parser.add_argument("-d", "--dimension",
                        help="dimension: dimension of the queries. Default: 3",
                        type=int, required=False, default=3)
    parser.add_argument("-nsr", "--num_sample_ratios", help="num_sample_ratios: number of sample ratios.", 
                        type=int, required=True)
    parser.add_argument("-lsf", "--labeled_sample_file",
                        help="labeled_sample_file: input file that holds labeled sample queries",
                        type=str, required=True)
    parser.add_argument("-sqf", "--sample_quality_file",
                        help="sample_quality_file: input file that holds sample queries qualities",
                        type=str, required=True)
    parser.add_argument("-ef", "--evaluated_file",
                        help="evaluated_file: input file that holds evaluated queries",
                        type=str, required=True)
    parser.add_argument("-ols", "--out_labeled_sample_file",
                        help="out_labeled_sample_file: output file that holds selected labeled sample queries",
                        type=str, required=True)
    parser.add_argument("-osq", "--out_sample_quality_file",
                        help="out_sample_quality_file: output file that holds selected sample queries qualities",
                        type=str, required=True)
    args = parser.parse_args()

    dimension = args.dimension
    num_of_sample_ratios = args.num_sample_ratios
    labeled_sampe_queries_file = args.labeled_sample_file
    sample_queries_qualities_file = args.sample_quality_file
    evaluated_queries_files = args.evaluated_file
    out_labeled_sampe_queries_file = args.out_labeled_sample_file
    out_sample_queries_qualities_file = args.out_sample_quality_file

    # load labeled sample queries into memory
    labeled_sample_queries = Util.load_labeled_sample_queries_file(dimension, num_of_sample_ratios, labeled_sampe_queries_file)
    # build qid -> labeled_sample_query map
    labeled_sample_queries_map = {}
    for query in labeled_sample_queries:
        labeled_sample_queries_map[query["id"]] = query

    # load sample queries qualities into memory
    sample_queries_qualities = Util.load_sample_queries_qualities_file(dimension, num_of_sample_ratios, sample_queries_qualities_file)
    # build qid -> sample_query_quality map
    sample_queries_qualities_map = {}
    for query in sample_queries_qualities:
        sample_queries_qualities_map[query["id"]] = query

    # load evaluated queries into memory
    evaluated_queries = Util.load_evaluated_queries_file(evaluated_queries_files)

    # traverse evaluated queries
    #   for each evaluated query that has "not_possible" as the reason,
    #      add corresponding labeled_sample_query and sample_query_quality into selected list
    selected_labeled_sample_queries = []
    selected_sample_queries_qualitites = []
    for evaluated_query in evaluated_queries:
        query_id = evaluated_query["id"]
        reason = evaluated_query["reason"]
        if reason == "not_possible":
            if query_id in labeled_sample_queries_map:
                selected_labeled_sample_queries.append(labeled_sample_queries_map[query_id])
                if query_id not in sample_queries_qualities_map:
                    print("[Error][smart_select_queries_q.py] query [" + 
                    str(query_id) + "] in [" + labeled_sampe_queries_file + 
                    "] but not in [" + sample_queries_qualities_file + "]!")
                    exit(0)
                selected_sample_queries_qualitites.append(sample_queries_qualities_map[query_id])
    
    print("selected " + str(len(selected_labeled_sample_queries)) + " sample queries based on given evaluated file.")

    Util.dump_labeled_sample_queries_file(dimension, num_of_sample_ratios, out_labeled_sampe_queries_file, selected_labeled_sample_queries)
    print("selected labeled sample queries saved to file [" + out_labeled_sampe_queries_file + "].")
    Util.dump_sample_queries_qualities_file(dimension, num_of_sample_ratios, out_sample_queries_qualities_file, selected_sample_queries_qualitites)
    print("selected sample queries qualities saved to file [" + out_sample_queries_qualities_file + "].")
