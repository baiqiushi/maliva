import argparse
from smart_drawer import Drawer
from smart_util import Util


if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(description="Compare two query result files.")
    parser.add_argument("-rf1", "--result_file1", help="result_file1: query result file 1", 
                        type=str, required=True)
    parser.add_argument("-rf2", "--result_file2", help="result_file2: query result file 2", 
                        type=str, required=True)
    parser.add_argument("-of", "--out_file",
                        help="out_file: output file that holds the images of both queries' results.",
                        type=str, required=False, default=None)
    args = parser.parse_args()

    result_file1 = args.result_file1
    result_file2 = args.result_file2
    out_file = args.out_file

    # load queries' results from files: result_file1 and result_file2
    query_result1 = Util.load_query_result_file(result_file1)
    query_result2 = Util.load_query_result_file(result_file2)

    # draw comparing scatterplots and save to [out_file]
    Drawer.compare_scatterplot(query_result1, query_result2, out_file)

    # compute Jaccard similarity between queries' results
    similarity = Util.jaccard_similarity(query_result1, query_result2)
    print("Jaccard Similarity = " + str(similarity))

