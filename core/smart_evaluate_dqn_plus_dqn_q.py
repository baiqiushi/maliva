import argparse
from smart_util import Util


###########################################################
#  smart_evaluate_dqn_plus_dqn_q.py
#
# Purpose:
#   Given dqn evaluated_file and dqn_q evaluated file, output combined evaluated file
#     that uses the two-stage query generation approach (lossless first, then lossy).
#
#  -d   / --dimension               dimension: dimension of the queries. Default: 3
#  -nsr / --num_sample_ratios       number of sample ratios.
#  -nj  / --num_join                number of join methods. Default: 1
#  -ef  / --evaluated_file          input evaluated file that holds evaluated queries using dqn only
#  -eqf / --evaluated_quality_file  input evaluated (with) quality file that holds evaluated queries using dqn_q only
#  -tb  / --time_budget             time (second) for a query to be viable
#  -of  / --out_file                output file that holds the combined evaluated queries result
#
###########################################################


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Evaluate DQN.")
    parser.add_argument("-d", "--dimension",
                        help="dimension: dimension of the queries. Default: 3",
                        type=int, required=False, default=3)
    parser.add_argument("-nsr", "--num_sample_ratios", help="num_sample_ratios: number of sample ratios.", 
                        type=int, required=True)
    parser.add_argument("-nj", "--num_join", help="num_join: number of join methods. Default: 1", 
                        required=False, type=int, default=1)
    parser.add_argument("-ef", "--evaluated_file",
                        help="evaluated_file: input evaluated file that holds evaluated queries using dqn only",
                        type=str, required=True)
    parser.add_argument("-eqf", "--evaluated_quality_file",
                        help="evaluated_quality_file: input evaluated (with) quality file that holds evaluated queries using dqn_q only",
                        type=str, required=True)
    parser.add_argument("-tb", "--time_budget",
                        help="time_budget: time (second) for a query to be viable",
                        type=float, required=True)
    parser.add_argument("-of", "--out_file",
                        help="out_file: output file that holds the combined evaluated queries result",
                        type=str, required=True)
    parser.add_argument("-dbg", "--debug",
                        help="debug: debug query id. Default: -1",
                        type=int, required=False, default=-1)
    args = parser.parse_args()

    dimension = args.dimension
    num_of_sample_ratios = args.num_sample_ratios
    num_of_joins = args.num_join
    evaluated_queries_file = args.evaluated_file
    evaluated_quality_queries_file = args.evaluated_quality_file
    time_budget = args.time_budget
    output_file = args.out_file
    debug_qid = args.debug

    # load evaluated queries into memory
    evaluated_queries = Util.load_evaluated_queries_file(evaluated_queries_file)

    # load evaluated queries qualities into memory
    evaluated_quality_queries = Util.load_evaluated_queries_file(evaluated_quality_queries_file, has_quality=True)
    # Build map <id, query> for evaluated_quality_queries
    evaluated_quality_queries_map = {}
    for query in evaluated_quality_queries:
        evaluated_quality_queries_map[query["id"]] = query

    # loop evaluated queries, replace the evaluation results for those queries with reason = "not_possible"
    win_rate = 0.0
    for evaluated_query in evaluated_queries:
        id = evaluated_query["id"]
        evaluated_query["quality"] = 1.0
        if evaluated_query["win"] == 1:
            win_rate += 1
        if evaluated_query["reason"] == "not_possible":
            if id not in evaluated_quality_queries_map:
                print("[Error][smart_evaluate_dqn_plus_dqn_q.py] query [" + str(id) + "] in " + evaluated_queries_file + " is not in " + evaluated_quality_queries_file + "!")
                exit(0)
            evaluated_quality_query = evaluated_quality_queries_map[id]
            if evaluated_query["planning_time"] + evaluated_quality_query["planning_time"] + evaluated_quality_query["querying_time"] <= time_budget:
                evaluated_query["planning_time"] = evaluated_query["planning_time"] + evaluated_quality_query["planning_time"]
                evaluated_query["querying_time"] = evaluated_quality_query["querying_time"]
                evaluated_query["total_time"] = evaluated_query["planning_time"] + evaluated_query["querying_time"]
                evaluated_query["win"] = evaluated_quality_query["win"]
                evaluated_query["plans_tried"] = evaluated_query["plans_tried"] + "_X_" + evaluated_quality_query["plans_tried"]
                evaluated_query["reason"] = evaluated_quality_query["reason"]
                evaluated_query["quality"] = evaluated_quality_query["quality"]
                win_rate += 1
    win_rate = win_rate / len(evaluated_queries)

    # output evaluated queries to console
    print("======== Evaluation of DQN + DQN for Q ========")
    print(evaluated_queries_file)
    print("-----------------------------------")
    print("qid,    planning_time,    querying_time,    total_time,    win,    plans_tried,    reason,    quality")
    for query in evaluated_queries:
        print(str(query["id"]) + ",    " + str(query["planning_time"]) + ",    " +
                str(query["querying_time"]) + ",    " + str(query["total_time"]) + ",    " + str(query["win"]) +
                ",    " + query["plans_tried"] + ",    " + query["reason"] + ",    " + str(query["quality"]))

    print("-----------------------------------")
    print("win rate: " + str(win_rate))
    print("===================================")

    # output evaluated queries to file
    Util.dump_evaluated_queries_file(output_file, evaluated_queries, has_quality=True)
    print("evaluated queries saved to file [" + output_file + "].")

