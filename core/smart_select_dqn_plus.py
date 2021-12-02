import argparse
import time
import torch
from smart_train_dqn_plus import train_dqn
from smart_evaluate_dqn_plus import evaluate_dqn
from smart_util import Util


###########################################################
#  smart_select_dqn_plus.py
#
# Purpose:
#   Try [number_of_tries] times training dqn models with the same given parameters on [train_labeled_sample_file],
#   and then use the [validate_labeled_sample_file] to select the best model out of them.
#
# Arguments:
#  -d   / --dimension                        dimension of the queries. Default: 3
#  -nsr / --num_sample_ratios                number of sample ratios.
#  -nj  / --num_join                         number of join methods. Default: 1
#  -tl  / --train_labeled_file               input file that holds labeled queries for training
#  -tls / --train_labeled_sample_file        input file that holds labeled sample queries for training
#  -tsq / --train_sample_quality_file        input file that holds sample queries qualities for training
#  -vl  / --validate_labeled_file            input file that holds labeled queries for validation
#  -vls / --validate_labeled_sample_file     input file that holds labeled sample queries for validation
#  -vsq / --validate_sample_quality_file     input file that holds sample queries qualities for validation
#  -uc  / --unit_cost                        time (second) to collect selectivity value for one condition
#  -tb  / --time_budget                      time (second) for a query to be viable
#  -bt  / --beta                             beta value for reward function. Default: 0.0
#  -nr  / --number_of_runs                   how many times to loop all queries for training. Default: 10
#  -bs  / --batch_size                       how many experiences used each time update the DQN weights. Default: 1024
#  -ed  / --eps_decay                        eps_decay rate for epsilon greedy strategy. Default: 0.001
#  -ms  / --memory_size                      how many experiences at most are stored in the replay memory. Default: 1000000
#  -mf  / --model_file                       output file that holds selected dqn model
#                                              (different tries models names will be suffixed with try id)
#  -v   / --version                          version of DQN and environment to use. Default: '0'
#  -tr  / --trace                            trace the evaluation result using training set, and output for each run. Default: False
#  -trf / --trace_file                       output file (no suffix) that holds the trace result. Default: None
#  -nes / --no_early_stop                    disable early_stop when model converges. Default: enabled
#  -nt  / --number_of_tries                  how many times of training to try. Default: 5
#
# Dependencies:
#   pip install pytorch
#
###########################################################


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Select the best DQN by training multiple tries.")
    parser.add_argument("-d", "--dimension",
                        help="dimension: dimension of the queries. Default: 3",
                        type=int, required=False, default=3)
    parser.add_argument("-nsr", "--num_sample_ratios", help="num_sample_ratios: number of sample ratios.", 
                        type=int, required=True)
    parser.add_argument("-nj", "--num_join", help="num_join: number of join methods. Default: 1", 
                        required=False, type=int, default=1)
    parser.add_argument("-tl", "--train_labeled_file",
                        help="train_labeled_file: input file that holds labeled queries for training",
                        type=str, required=True)
    parser.add_argument("-tls", "--train_labeled_sample_file",
                        help="train_labeled_sample_file: input file that holds labeled sample queries for training",
                        type=str, required=True)
    parser.add_argument("-tsq", "--train_sample_quality_file",
                        help="train_sample_quality_file: input file that holds sample queries qualities for training",
                        type=str, required=True)
    parser.add_argument("-vl", "--validate_labeled_file",
                        help="validate_labeled_file: input file that holds labeled queries for validation",
                        type=str, required=True)
    parser.add_argument("-vls", "--validate_labeled_sample_file",
                        help="validate_labeled_sample_file: input file that holds labeled sample queries for validation",
                        type=str, required=True)
    parser.add_argument("-vsq", "--validate_sample_quality_file",
                        help="validate_sample_quality_file: input file that holds sample queries qualities for validation",
                        type=str, required=True)
    parser.add_argument("-uc", "--unit_cost",
                        help="unit_cost: time (second) to collect selectivity value for one condition. Default: 0.05",
                        type=float, required=False, default=0.05)
    parser.add_argument("-tb", "--time_budget",
                        help="time_budget: time (second) for a query to be viable",
                        type=float, required=True)
    parser.add_argument("-bt", "--beta",
                        help="beta: beta value for reward function. Default: 0.0",
                        type=float, required=False, default=0.0)
    parser.add_argument("-nr", "--number_of_runs",
                        help="number_of_runs: how many times to loop all queries for training. Default: 10",
                        type=int, required=False, default=10)
    parser.add_argument("-bs", "--batch_size",
                        help="batch_size: how many experiences used each time update the DQN weights. Default: 1024",
                        type=int, required=False, default=1024)
    parser.add_argument("-ed", "--eps_decay",
                        help="eps_decay: eps_decay rate for epsilon greedy strategy. Default: 0.001",
                        type=float, required=False, default=0.001)
    parser.add_argument("-ms", "--memory_size",
                        help="memory_size: how many experiences at most are stored in the replay memory. "
                             "Default: 1000000",
                        type=int, required=False, default=1000000)
    parser.add_argument("-mf", "--model_file",
                        help="model_file: output file that holds selected dqn model",
                        type=str, required=True)
    parser.add_argument("-v", "--version",
                        help="version: version of DQN and environment to use. Default: '0'",
                        type=str, required=False, default='0')
    parser.add_argument("-tr", "--trace",
                        help="trace: trace the evaluation result using training set, "
                             "and output for each run. Default: False",
                        dest='trace', action='store_true')
    parser.set_defaults(trace=False)
    parser.add_argument("-trf", "--trace_file",
                        help="trace_file: output file (no suffix) that holds the trace result. Default: None",
                        type=str, required=False, default=None)
    parser.add_argument("-nes", "--no_early_stop",
                        help="no_early_stop: disable early_stop when model converges. Default: enabled",
                        dest='early_stop', action='store_false')
    parser.set_defaults(early_stop=True)
    parser.add_argument("-nt", "--number_of_tries",
                        help="number_of_tries: how many times to try the same hyper parameters. [optional] Default: 5",
                        type=int, required=False, default=5)
    args = parser.parse_args()

    dimension = args.dimension
    num_of_sample_ratios = args.num_sample_ratios
    num_of_joins = args.num_join
    train_labeled_queries_file = args.train_labeled_file
    train_labeled_sampe_queries_file = args.train_labeled_sample_file
    train_sample_queries_qualities_file = args.train_sample_quality_file
    validate_labeled_queries_file = args.validate_labeled_file
    validate_labeled_sampe_queries_file = args.validate_labeled_sample_file
    validate_sample_queries_qualities_file = args.validate_sample_quality_file
    unit_cost = args.unit_cost
    time_budget = args.time_budget
    beta = args.beta
    number_of_runs = args.number_of_runs
    batch_size = args.batch_size
    eps_decay = args.eps_decay
    memory_size = args.memory_size
    dqn_model_file = args.model_file
    version = args.version
    trace = args.trace
    trace_file = args.trace_file
    early_stop = args.early_stop
    number_of_tries = args.number_of_tries

    # load training/validation labeled queries into memory
    train_labeled_queries = Util.load_labeled_queries_file(dimension, train_labeled_queries_file, num_of_joins)
    validate_labeled_queries = Util.load_labeled_queries_file(dimension, validate_labeled_queries_file, num_of_joins)

    # load training/validation labeled sample queries into memory
    train_labeled_sample_queries = Util.load_labeled_sample_queries_file(dimension, num_of_sample_ratios, train_labeled_sampe_queries_file)
    validate_labeled_sample_queries = Util.load_labeled_sample_queries_file(dimension, num_of_sample_ratios, validate_labeled_sampe_queries_file)

    # load training/validation ample queries qualities into memory
    train_sample_queries_qualities = Util.load_sample_queries_qualities_file(dimension, num_of_sample_ratios, train_sample_queries_qualities_file)
    validate_sample_queries_qualities = Util.load_sample_queries_qualities_file(dimension, num_of_sample_ratios, validate_sample_queries_qualities_file)

    # select DQN model with highest total_reward
    print("===================================")
    print("    selecting DQN models starts")
    print("===================================")
    start = time.time()
    # tried trainings
    trials = []
    for i in range(1, number_of_tries + 1):
        print("----> try: " + str(i))

        # train DQN model
        start_train = time.time()
        if trace_file is None:
            trace_file_i = None
        else:
            trace_file_i = trace_file + "." + str(i)
        (trained_dqn, fit_total_reward) = train_dqn(dimension,
                                                    num_of_sample_ratios,
                                                    train_labeled_queries,
                                                    train_labeled_sample_queries,
                                                    train_sample_queries_qualities,
                                                    unit_cost,
                                                    time_budget,
                                                    beta,
                                                    number_of_runs,
                                                    batch_size=batch_size,
                                                    eps_decay=eps_decay,
                                                    memory_size=memory_size,
                                                    version=version,
                                                    trace=trace,
                                                    trace_file=trace_file_i,
                                                    early_stop=early_stop,
                                                    num_of_joins=num_of_joins)
        end_train = time.time()
        train_time = end_train - start_train

        # save this try's DQN model
        this_dqn_model_file = dqn_model_file + "." + str(i)
        torch.save(trained_dqn.state_dict(), this_dqn_model_file)

        # evaluated DQN on validation queries
        (evaluated_validate_queries, eval_total_reward) = evaluate_dqn(dimension,
                                                                       num_of_sample_ratios,
                                                                       trained_dqn,
                                                                       validate_labeled_queries,
                                                                       validate_labeled_sample_queries,
                                                                       validate_sample_queries_qualities,
                                                                       unit_cost,
                                                                       time_budget,
                                                                       beta,
                                                                       version=version,
                                                                       num_of_joins=num_of_joins)
        trials.append({"dqn": trained_dqn, "fit_total_reward": fit_total_reward,
                       "eval_total_reward": eval_total_reward, "train_time": train_time})
        print("----> try: " + str(i) + " is done, train_time: " + str(train_time) + " seconds, fit_total_reward: " + str(fit_total_reward) + ", eval_total_reward: " + str(eval_total_reward))
        print()
        print()
    end = time.time()
    print("===================================")
    print("    selecting DQN models ends")
    print("    total time: " + str(end - start) + " seconds.")
    print("===================================")
    print()

    # output win_count and total_reward for each trained model
    print("DQN Model ID,    train_time,    fit_total_reward,    eval_total_reward")
    total_train_time = 0.0
    highest_fit_total_reward = 0.0
    highest_eval_total_reward = 0.0
    selected_dqn = None
    selected_model_id = 0
    for i in range(number_of_tries):
        trial = trials[i]
        total_train_time += trial["train_time"]
        highest_fit_total_reward = max(highest_fit_total_reward, trial["fit_total_reward"])
        if trial["eval_total_reward"] > highest_eval_total_reward:
            highest_eval_total_reward = trial["eval_total_reward"]
            selected_dqn = trial["dqn"]
            selected_model_id = i + 1
        print(str(i + 1) + ",    " +
              str(trial["train_time"]) + ",    " +
              str(trial["fit_total_reward"]) + ",    " +
              str(trial["eval_total_reward"]))
    print("-----------------------------------")
    print(str(selected_model_id) + ",    " +
          str(total_train_time / number_of_tries) + ",    " +
          str(highest_fit_total_reward) + ",    " +
          str(highest_eval_total_reward))

    # save selected DQN model
    torch.save(selected_dqn.state_dict(), dqn_model_file)
    print("===================================")
    print("  model [" + str(selected_model_id) + "] is selected, and saved to file [" + dqn_model_file + "].")
    print("===================================")
    print()



