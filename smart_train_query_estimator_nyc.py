import argparse
import numpy as np
from smart_query_estimator_3d import Query_Estimator
from smart_util import Util


###########################################################
#  smart_train_query_estimator_nyc.py
#
#  -sf / --sel_file        input file that holds queries' selectivities generated by smart_collect_sel_queries_nyc.py
#  -lf / --labeled_file    input file that holds queries's real running times generated by smart_label_queries_nyc.py
#  -op / --out_path        output path to save the models used by Query Estimator
#
###########################################################


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Train Query Estimator.")
    parser.add_argument("-sf", "--sel_file",
                        help="sel_file: input file that holds queries' selectivities",
                        type=str, required=True)
    parser.add_argument("-lf", "--labeled_file",
                        help="labeled_file: input file that holds queries's real running times",
                        type=str, required=True)
    parser.add_argument("-op", "--out_path",
                        help="out_path: output path to save the models used by Query Estimator",
                        type=str, required=True)
    args = parser.parse_args()

    sel_queries_file = args.sel_file
    labeled_queries_file = args.labeled_file
    out_path = args.out_path

    # 1. read queries' selectivities into memory
    queries_sels = Util.load_sel_queries_file_nyc(sel_queries_file)

    # 2. read queries' real running times into memory
    labeled_queries = Util.load_labeled_queries_file_nyc(labeled_queries_file)
    # Build labeled_queries_map <id, labeled_query>
    labeled_queries_map = {}
    for query in labeled_queries:
        labeled_queries_map[query["id"]] = query

    # 3. new a Query Estimator
    query_estimator = Query_Estimator()

    # 4. train the Query Estimator for all plans
    print("start training query estimator ...")
    # plan = 1 ~ 7
    for plan in range(1, 8):
        xtr = []
        ytr = []
        sel_ids = Util.sel_ids_of_plan(plan)
        for query_sel in queries_sels:
            id = query_sel["id"]
            x = []
            for sel_id in sel_ids:
                x.append(query_sel["sel_" + str(sel_id)])
            xtr.append(x)
            labeled_query = labeled_queries_map[id]
            y = [labeled_query["time_" + str(plan)]]
            ytr.append(y)
        xtr = np.array(xtr)
        ytr = np.array(ytr)
        query_estimator.fit(plan, xtr, ytr)
        print("    plan [" + str(plan) + "] trained.")

    # 5. save Query Estimator models to files
    query_estimator.save(out_path)
    print("query estimator models saved.")

