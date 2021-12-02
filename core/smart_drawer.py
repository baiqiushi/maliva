import argparse
import matplotlib.pyplot as plt
from smart_util import Util


class Drawer:

    DOTSIZE = 100
    DOTCOLOR = 'blue'
    MARKER = 'o'

    @staticmethod
    def compare_scatterplot(original_query_result, query_result, out_file=None):
        fig, (ax1, ax2) = plt.subplots(1, 2)
        fig.set_size_inches(15, 5)
        x0 = []
        y0 = []
        for record in original_query_result:
            x0.append(record["coordinate[0]"])
            y0.append(record["coordinate[1]"])
        x1 = []
        y1 = []
        for record in query_result:
            x1.append(record["coordinate[0]"])
            y1.append(record["coordinate[1]"])
        ax1.scatter(x0, y0, s=Drawer.DOTSIZE, c=Drawer.DOTCOLOR, marker=Drawer.MARKER)
        xlim = ax1.get_xlim()
        ylim = ax1.get_ylim()
        ax2.set_xlim(xlim)
        ax2.set_ylim(ylim)
        ax2.scatter(x1, y1, s=Drawer.DOTSIZE, c=Drawer.DOTCOLOR, marker=Drawer.MARKER)
        if out_file is not None:
            plt.savefig(out_file)
        else:
            plt.show()


if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(description="Draw query result.")
    parser.add_argument("-nsr", "--num_sample_ratios", help="num_sample_ratios: number of sample ratios.", 
                        type=int, required=True)
    parser.add_argument("-qid", "--query_id",
                        help="query_id: query id to draw",
                        type=int, required=True)
    parser.add_argument("-sp", "--sample_plan",
                        help="sample_plan: sampling plan id to compare with",
                        type=int, required=True)
    parser.add_argument("-rp", "--result_path",
                        help="result_path: input path that holds the results files of each sample & original query",
                        type=str, required=True)
    parser.add_argument("-of", "--out_file",
                        help="out_file: output file that holds the qualities of sample queries",
                        type=str, required=False, default=None)
    args = parser.parse_args()

    num_of_sample_ratios = args.num_sample_ratios
    query_id = args.query_id
    sampling_plan_id = args.sample_plan
    result_path = args.result_path
    out_file = args.out_file

    # load original_query_result
    original_query_result = Util.load_query_result(result_path, query_id, -1, -1)
    # load sample_query_result
    hint_id = Util.hint_id_of_sampling_plan(num_of_sample_ratios, sampling_plan_id)
    sample_ratio_id = Util.sample_ratio_id_of_sampling_plan(num_of_sample_ratios, sampling_plan_id)
    sample_query_result = Util.load_query_result(result_path, query_id, hint_id, sample_ratio_id)

    # draw comparing scatterplots and save to [out_file]
    Drawer.compare_scatterplot(original_query_result, sample_query_result, out_file)
