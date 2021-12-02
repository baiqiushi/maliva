import argparse
import config
import progressbar
import time
from smart_util import Util


###########################################################
#  smart_generate_queries_for_bao.py
#
# Purpose:
#   generate sql queries on Twitter dataset for bao given queries in our format
#
# Arguments:
#   -ds  / --dataset        dataset to run the queries on. Default: twitter
#   -d   / --dimension      dimension of the queries. Default: 3
#   -if  / --in_file        input file that holds the queries
#   -ff  / --filter_file    input file that holds the queries' ids 
#                             to filter queries from in_file as output 
#                             (must be in labeled_queries format)
#   -op  / --out_path       output path for generated sql query files
#
# Dependencies:
#   python3.7 & pip: https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb-cli3-install-linux.html
#   pip install progressbar
#
# Output:
#   file name: bao_[i].sql
#
###########################################################


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Generate SQL queries for BAO.")
    parser.add_argument("-ds", "--dataset", help="dataset: dataset to run the queries on. Default: twitter",
                        type=str, required=False, default="twitter")
    parser.add_argument("-d", "--dimension", help="dimension of the queries. Default: 3",
                        type=int, required=False, default=3)
    parser.add_argument("-if", "--in_file", help="in_file: input file that holds the queries", required=True)
    parser.add_argument("-ff", "--filter_file", 
                        help="filter_file: input file that holds the queries' ids" \
                             "to filter queries from in_file as output (must be in labeled_queries format)", 
                        required=False, default=None)
    parser.add_argument("-op", "--out_path",
                        help="out_path: output path for generated sql query files",
                        type=str, required=True)
    args = parser.parse_args()

    dataset = args.dataset
    dimension = args.dimension
    in_file = args.in_file
    filter_file = args.filter_file
    out_path = args.out_path
    # trim the last '/'
    if out_path.endswith('/'):
        out_path = out_path[:-1]
    
    dataset = config.datasets[dataset]

    print("start generating queries for BAO ...")

    # 1. read queries into memory
    queries = dataset.load_queries_file(dimension, in_file)
    print("loaded ", len(queries), " queries into memory.")

    # 2. read filter queries into memory (must be in labeled_queries format)
    if filter_file:
        filter_queries = Util.load_labeled_queries_file(dimension, filter_file)
        filter_query_ids = set()
        for filter_query in filter_queries:
            filter_query_ids.add(filter_query["id"])
        filtered_queries = []
        for query in queries:
            if query["id"] in filter_query_ids:
                filtered_queries.append(query)
        queries = filtered_queries

    # 3. loop queries to construct a SQL for each
    start_runs = time.time()
    # show progress bar
    bar = progressbar.ProgressBar(maxval=len(queries),
                                  widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()
    qp = 0
    for query in queries:
        sql_str = dataset.construct_sql_str(query)
        qp += 1
        bar.update(qp)

        # write each query to a bao_[i].sql file
        out_file_name = out_path + "/bao_" + str(query["id"]) + ".sql"
        out_file = open(out_file_name, "w")
        out_file.write(sql_str)
    bar.finish()

    end_runs = time.time()
    print(len(queries), " queries are generated, time: " + str(end_runs - start_runs) + " seconds.")

