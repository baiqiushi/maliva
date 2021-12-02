import argparse
from smart_util import Util
from smart_agent import Agent
from smart_dqn import DQN
from smart_environment_q import EnvironmentQ
import torch


###########################################################
#  smart_evaluate_dqn_q.py
#
#  -d   / --dimension               dimension: dimension of the queries. Default: 3
#  -nsr / --num_sample_ratios       number of sample ratios.
#  -nj  / --num_join                number of join methods. Default: 1
#  -mf  / --model_file              input file that holds trained dqn model
#  -lsf / --labeled_sample_file     input file that holds labeled sample queries for evaluation
#  -sqf / --sample_quality_file     input file that holds sample queries qualities for evaluation
#  -tb  / --time_budget             time (second) for a query to be viable
#  -bt  / --beta                    beta value for reward function. Default: 0.0
#  -ef  / --evaluated_file          output file that holds the evaluated queries result
#  -v   / --version                 version of DQN and environment to use
#  -dbg / --debug                   debug query id
#
# Dependencies:
#   pip install torch
#
###########################################################

# Evaluate DQN
#
# @param - dimension:                 int, dimension of the queries
# @param - num_of_sample_ratios:      int, number of sample ratios
# @param - dqn_model:                 DQN object, DQN model generated by smart_train_dqn.py
# @param - labeled_sample_queries:    [list of sample query objects], each sample query object being
#                                       {id, time_0, time_1, ..., time_(|d|*|s|-1)}, 
#                                       where |d| = dimension, |s| = num_of_sample_ratios
# @param - sample_queries_qualities:  [list of sample query objects], each sample query object being
#                                       {id, quality_0, quality_1, ..., quality_(|d|*|s|-1)}, 
#                                       where |d| = dimension, |s| = num_of_sample_ratios
# @param - time_budget:               float, time (second) for a query to be viable
# @param - beta:                      float, parameter to compute reward for a viable query.
# @param - num_of_joins:              int, number of join methods in hints set.
#
# @return - (list of evaluated query objects, total_reward), each query object being
#           {id, planning_time, querying_time, total_time, win(1/0), plans_tried(x_x_x_x), reason, quality}
def evaluate_dqn(dimension,
                 num_of_sample_ratios,
                 dqn_model,
                 labeled_sample_queries,
                 sample_queries_qualities,
                 time_budget,
                 beta,
                 version='1',
                 debug_qid=-1,
                 num_of_joins=1):

    # set DQN model as the policy_net for agent
    policy_net = dqn_model

    # init objects
    # version 0
    if version == '0':
        env = EnvironmentQ(dimension, 
                           num_of_sample_ratios, 
                           labeled_sample_queries, 
                           sample_queries_qualities, 
                           time_budget, 
                           beta, 
                           num_of_joins)
        agent = Agent(dimension, num_of_joins, num_of_sample_ratios, True)
    # default
    else:
        env = EnvironmentQ(dimension, 
                           num_of_sample_ratios, 
                           labeled_sample_queries, 
                           sample_queries_qualities, 
                           time_budget, 
                           beta, 
                           num_of_joins)
        agent = Agent(dimension, num_of_joins, num_of_sample_ratios, True)
        print("Invalid version " + str(version) + "!")
        exit(0)

    # evaluate labeled queries one by one
    evaluated_queries = []
    total_reward = 0.0
    for query in labeled_sample_queries:
        qid = query["id"]
        env.reset(qid)
        state = env.get_state()
        agent.reset()
        plans_tried = []

        # ++++++++++ DEBUG ++++++++++ #
        if 0 <= debug_qid < qid:
            break
        if qid == debug_qid:
            print("[DEBUG]    query [" + str(qid) + "]")
        # ---------- DEBUG ---------- #

        plan = 0
        while not env.done:
            action = agent.decide_action(state, policy_net)
            # action = agent.decide_action(state, policy_net, random_ai=True)
            plan = action  # sampling plan starts from 0
            plans_tried.append(plan)
            reward = env.take_action(plan)
            total_reward += reward
            state = env.get_state()

        planning_time = state.get_elapsed_time()
        querying_time = env.get_query_time()
        total_time = planning_time + querying_time
        if total_time <= time_budget:
            win = True
        else:
            win = False
        reason = env.get_done_reason()
        quality = env.get_query_quality()

        # ++++++++++ DEBUG ++++++++++ #
        if qid == debug_qid:
            print("[DEBUG]    planning_time: " + str(planning_time) +
                  ", querying_time: " + str(querying_time) +
                  ", total_time: " + str(total_time) +
                  ", win: " + str(win) +
                  ", reason: " + str(reason) + 
                  ", quality: " + str(quality))
        # ---------- DEBUG ---------- #

        evaluated_queries.append(
            {"id": qid,
             "planning_time": planning_time,
             "querying_time": querying_time,
             "total_time": total_time,
             "win": (1 if win else 0),
             "plans_tried": "_".join(str(x) for x in plans_tried),
             "reason": reason,
             "quality": quality
             }
        )

    return evaluated_queries, total_reward


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
    parser.add_argument("-mf", "--model_file",
                        help="model_file: input file that holds trained dqn model",
                        type=str, required=True)
    parser.add_argument("-lsf", "--labeled_sample_file",
                        help="labeled_sample_file: input file that holds labeled sample queries for evaluation",
                        type=str, required=True)
    parser.add_argument("-sqf", "--sample_quality_file",
                        help="sample_quality_file: input file that holds sample queries qualities for evaluation",
                        type=str, required=True)
    parser.add_argument("-tb", "--time_budget",
                        help="time_budget: time (second) for a query to be viable",
                        type=float, required=True)
    parser.add_argument("-bt", "--beta",
                        help="beta: beta value for reward function. Default: 0.0",
                        type=float, required=False, default=0.0)
    parser.add_argument("-ef", "--evaluated_file",
                        help="evaluated_file: output file that holds the evaluated queries result",
                        type=str, required=True)
    parser.add_argument("-v", "--version",
                        help="version: version of DQN and environment to use. Default: '0'",
                        type=str, required=False, default='0')
    parser.add_argument("-dbg", "--debug",
                        help="debug: debug query id. Default: -1",
                        type=int, required=False, default=-1)
    args = parser.parse_args()

    dimension = args.dimension
    num_of_sample_ratios = args.num_sample_ratios
    num_of_joins = args.num_join
    dqn_model_file = args.model_file
    labeled_sampe_queries_file = args.labeled_sample_file
    sample_queries_qualities_file = args.sample_quality_file
    time_budget = args.time_budget
    beta = args.beta
    evaluated_queries_file = args.evaluated_file
    version = args.version
    debug_qid = args.debug

    # load labeled sample queries into memory
    labeled_sample_queries = Util.load_labeled_sample_queries_file(dimension, num_of_sample_ratios, labeled_sampe_queries_file)

    # load sample queries qualities into memory
    sample_queries_qualities = Util.load_sample_queries_qualities_file(dimension, num_of_sample_ratios, sample_queries_qualities_file)

    # load DQN model
    # version 0
    if version == '0':
        dqn_model = DQN(dimension, num_of_joins, num_of_sample_ratios, True)
    # default
    else:
        dqn_model = DQN(dimension, num_of_joins, num_of_sample_ratios, True)
        print("Invalid version " + str(version) + "!")
        exit(0)
    dqn_model.load_state_dict(torch.load(dqn_model_file))
    dqn_model.eval()
    print("DQN model loaded into memory.")

    # evaluated DQN
    (evaluated_queries, total_reward) = evaluate_dqn(dimension,
                                                     num_of_sample_ratios,
                                                     dqn_model,
                                                     labeled_sample_queries,
                                                     sample_queries_qualities,
                                                     time_budget,
                                                     beta,
                                                     version=version,
                                                     debug_qid=debug_qid,
                                                     num_of_joins=num_of_joins)

    # in debug mode, do not output evaluated queries
    if debug_qid == -1:
        # output evaluated queries to console
        print("======== Evaluation of DQN ========")
        print(labeled_sampe_queries_file)
        print("-----------------------------------")
        print("qid,    planning_time,    querying_time,    total_time,    win,    plans_tried,    reason,    quality")
        for query in evaluated_queries:
            print(str(query["id"]) + ",    " + str(query["planning_time"]) + ",    " +
                  str(query["querying_time"]) + ",    " + str(query["total_time"]) + ",    " + str(query["win"]) +
                  ",    " + query["plans_tried"] + ",    " + query["reason"] + ",    " + str(query["quality"]))

        print("-----------------------------------")
        print("total reward: " + str(total_reward))
        print("===================================")

        # output evaluated queries to file
        Util.dump_evaluated_queries_file(evaluated_queries_file, evaluated_queries, has_quality=True)
        print("evaluated queries saved to file [" + evaluated_queries_file + "].")

