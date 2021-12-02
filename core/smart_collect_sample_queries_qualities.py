import argparse
import config
import progressbar
from smart_util import Util


###########################################################
#  smart_collect_sample_queries_qualities.py
#
# Purpose:
#   collect qualities of results of given labeled sample queries.
#
# Arguments:
#   -ds  / --dataset               dataset to run the queries on. Default: twitter
#   -d   / --dimension             dimension of the queries. Default: 3
#   -lsf / --labeled_sample_file   input file that holds labeled sample queries
#   -rp  / --results_path          input path that holds the results files of each sample & original query
#   -of  / --out_file              output file that holds the qualities of sample queries
#
# Output:
#   [out_file]:
#     qualities of queries on different sampling plans: hint using index on one of the [d1, d2, ...] dimensions 
#       and limit a k such that k/table_size = one of the [s1, s2, ...] sample ratios
#     example:
#       di in dimensions = [idx_tweets_100m_text, idx_tweets_100m_create_at, idx_tweets_coordinate]
#       si in sample ratios = [6.25%, 12.5%, 25%, 50%, 75%]
#       quality(d1_s1) = quality(query_result(d1_s1), original_query_result)
#     format (csv):
#       id, quality(d1_s1), quality(d1_s2), ..., quality(d|d|_s|s|)
#       ...
#
###########################################################


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Collect qualities of benchmark sample queries.")
    parser.add_argument("-ds", "--dataset", help="dataset: dataset to run the queries on. Default: twitter",
                        type=str, required=False, default="twitter")
    parser.add_argument("-d", "--dimension", help="dimension: dimension of the queries. Default: 3",
                        type=int, required=False, default=3)
    parser.add_argument("-lsf", "--labeled_sample_file",
                        help="labeled_sample_file: input file that holds labeled sample queries",
                        type=str, required=True)
    parser.add_argument("-rp", "--result_path",
                        help="result_path: input path that holds the results files of each sample & original query",
                        type=str, required=True)
    parser.add_argument("-of", "--out_file",
                        help="out_file: output file that holds the qualities of sample queries",
                        type=str, required=True)
    args = parser.parse_args()

    dataset = args.dataset
    dimension = args.dimension
    labeled_sample_queries_file = args.labeled_sample_file
    result_path = args.result_path
    out_file = args.out_file

    dataset = config.datasets[dataset]
    sample_ratios = dataset.sample_ratios
    num_of_sample_ratios = len(sample_ratios)

    num_of_sampling_plans = Util.num_of_sampling_plans(dimension, num_of_sample_ratios)

    # 1. read labeled sample queries into memory
    labeled_sample_queries = Util.load_labeled_sample_queries_file(dimension, num_of_sample_ratios, labeled_sample_queries_file)
    
    print("start collecting queries qualities.")
    # 2. for each labeled_sample_query,
    #        load the original_query_result from file;
    #        for each sampling_plan_id,
    #            load sample query_result(di_si) from file; 
    #            compute quality between query_result(di_si) and original_query_result
    sample_queries_qualities = []
    # show progress bar
    bar = progressbar.ProgressBar(maxval=len(labeled_sample_queries),
                                  widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()
    qp = 0
    for labeled_sample_query in labeled_sample_queries:
        query_id = labeled_sample_query["id"]
        sample_query_qualities = {"id": query_id}
        # load original_query_result
        original_query_result = Util.load_query_result(result_path, query_id, -1, -1)
        for sampling_plan_id in range(0, num_of_sampling_plans):
            hint_id = Util.hint_id_of_sampling_plan(num_of_sample_ratios, sampling_plan_id)
            sample_ratio_id = Util.sample_ratio_id_of_sampling_plan(num_of_sample_ratios, sampling_plan_id)
            query_result = Util.load_query_result(result_path, query_id, hint_id, sample_ratio_id)
            quality = Util.jaccard_similarity(original_query_result, query_result)
            sample_query_qualities["quality_" + str(sampling_plan_id)] = quality
        sample_queries_qualities.append(sample_query_qualities)
        qp += 1
        bar.update(qp)
    bar.finish()

    # 3. write collected sample queries qualities to [out_file]
    Util.dump_sample_queries_qualities_file(dimension, num_of_sample_ratios, out_file, sample_queries_qualities)
    print("sample queries qualities dumped to [" + out_file + "].")
