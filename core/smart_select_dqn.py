import argparse
import random
import time
import torch
from smart_train_dqn import train_dqn
from smart_evaluate_dqn import evaluate_dqn
from smart_query_estimator import Query_Estimator
from smart_util import Util


###########################################################
#  smart_select_dqn.py
#
# Purpose:
#   Try [number_of_tries] times training dqn models with the same given parameters,
#   but with a random sampled (given [sample_ratio]%) subset of queries in the given [train_file],
#   and then use the [validate_file] to select the best model out of them.
#
# Arguments:
#  -d  / --dimension       dimension: dimension of the queries. Default: 3
#  -nj / --num_join        number of join methods. Default: 1
#  -tf / --train_file      input file that holds labeled queries for training
#  -sr / --sample_ratio    percentage ratio of random sampling training queries. Default: 100
#  -vf / --validate_file   input file that holds labeled queries for validation
#  -uc / --unit_cost       time (second) to collect selectivity value for one condition
#  -tb / --time_budget     time (second) for a query to be viable
#  -nr / --number_of_runs  how many times to loop all queries for one training. Default: 10
#  -bs / --batch_size      how many experiences used each time update the DQN weights. Default: 1024
#  -ed / --eps_decay       eps_decay rate for epsilon greedy strategy. Default: 0.001
#  -ms / --memory_size     how many experiences at most are stored in the replay memory. Default: 1000000
#  -mf / --model_file      output file that holds selected dqn model
#                          (different tries models names will be suffixed with try id)
#  -v  / --version         version of DQN and environment to use
#  -tr / --trace           trace the evaluation result using training set, and output for each run. Default: False
#  -trf / --trace_file     output file (no suffix) that holds the trace result. Default: None
#  -nes / --no_early_stop  disable early_stop when model converges. Default: enabled
#  -nt / --number_of_tries how many times of training to try. Default: 5
#  ** Only required when version = 1/2:
#  -tllsf / --train_list_labeled_sel_file    list of labeled_sel_queries files for different sample sizes for training
#  -vllsf / --validate_list_labeled_sel_file list of labeled_sel_queries files for different sample sizes for validation
#  -tlsqf / --train_list_sel_query_file      list of sel_queries files for different sample sizes for training
#  -vlsqf / --validate_list_sel_query_file   list of sel_queries files for different sample sizes for validation
#  -scf  / --sel_costs_file                  input file that holds sel queries costs for different sample sizes
#  -qmp  / --qe_model_path                   input path to load the models used by Query Estimator
#  -sp   / --sample_pointer                  pointer to the sample size to use for the query_estimator. Default: 0
#
# Dependencies:
#   python3.7 & pip: https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb-cli3-install-linux.html
#   pip install pytorch
#
###########################################################


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Select the best DQN by training multiple tries.")
    parser.add_argument("-d", "--dimension",
                        help="dimension: dimension of the queries. Default: 3",
                        type=int, required=False, default=3)
    parser.add_argument("-nj", "--num_join", help="num_join: number of join methods. Default: 1", 
                        required=False, type=int, default=1)
    parser.add_argument("-tf", "--train_file",
                        help="input file that holds labeled queries for training",
                        type=str, required=True)
    parser.add_argument("-sr", "--sample_ratio",
                        help="sample_ratio: percentage ratio of random sampling training queries. Default: 100.0",
                        type=float, required=False, default=100.0)
    parser.add_argument("-vf", "--validate_file",
                        help="input file that holds labeled queries for validation",
                        type=str, required=True)
    parser.add_argument("-uc", "--unit_cost",
                        help="unit_cost: time (second) to collect selectivity value for one condition",
                        type=float, required=False, default=0.05)
    parser.add_argument("-tb", "--time_budget",
                        help="time_budget: time (second) for a query to be viable",
                        type=float, required=True)
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
                        help="version: version of DQN and environment to use. Default: '1'",
                        type=str, required=False, default='1')
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
    parser.add_argument("-tllsf", "--train_list_labeled_sel_file",
                        help="train_list_labeled_sel_file: "
                             "list of labeled_sel_queries files for different sample sizes for training",
                        action='append', required=False, default=[])
    parser.add_argument("-vllsf", "--validate_list_labeled_sel_file",
                        help="validate_list_labeled_sel_file: "
                             "list of labeled_sel_queries files for different sample sizes for validation",
                        action='append', required=False, default=[])
    parser.add_argument("-tlsqf", "--train_list_sel_query_file",
                        help="train_list_sel_query_file: "
                             "list of sel_queries files for different sample sizes for training",
                        action='append', required=False, default=[])
    parser.add_argument("-vlsqf", "--validate_list_sel_query_file",
                        help="validate_list_sel_query_file: "
                             "list of sel_queries files for different sample sizes for validation",
                        action='append', required=False, default=[])
    parser.add_argument("-scf", "--sel_costs_file",
                        help="sel_costs_file: input file that holds sel queries costs for different sample sizes",
                        type=str, required=False, default=None)
    parser.add_argument("-qmp", "--qe_model_path",
                        help="qe_model_path: input path to load the models used by Query Estimator",
                        type=str, required=False, default=None)
    parser.add_argument("-sp", "--sample_pointer",
                        help="sample_pointer: pointer to the sample size to use for the query_estimator. Default: 0",
                        type=int, required=False, default=0)
    args = parser.parse_args()

    dimension = args.dimension
    num_of_joins = args.num_join
    train_queries_file = args.train_file
    sample_ratio = args.sample_ratio
    validate_queries_file = args.validate_file
    unit_cost = args.unit_cost
    time_budget = args.time_budget
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
    sample_pointer = args.sample_pointer

    # load training queries into memory
    train_queries = Util.load_labeled_queries_file(dimension, train_queries_file, num_of_joins)

    # load validate queries into memory
    validate_queries = Util.load_labeled_queries_file(dimension, validate_queries_file, num_of_joins)

    if version == '1' or version == '2':
        train_list_labeled_sel_file = args.train_list_labeled_sel_file
        validate_list_labeled_sel_file = args.validate_list_labeled_sel_file
        train_list_sel_query_file = args.train_list_sel_query_file
        validate_list_sel_query_file = args.validate_list_sel_query_file
        sel_costs_file = args.sel_costs_file
        qe_model_path = args.qe_model_path

        # assert required parameters
        if len(train_list_labeled_sel_file) == 0:
            print("-tllsf / --train_list_labeled_sel_file is required when --version is 2!")
            exit(0)
        if len(validate_list_labeled_sel_file) == 0:
            print("-vllsf / --validate_list_labeled_sel_file is required when --version is 2!")
            exit(0)
        if len(train_list_sel_query_file) == 0:
            print("-tlsqf / --train_list_sel_query_file is required when --version is 2!")
            exit(0)
        if len(validate_list_sel_query_file) == 0:
            print("-vlsqf / --validate_list_sel_query_file is required when --version is 2!")
            exit(0)
        if sel_costs_file is None:
            print("-scf / --sel-costs-file is required when --version is 2!")
            exit(0)
        if qe_model_path is None:
            print("-qmp / --qe-model-path is required when --version is 2!")
            exit(0)
        if len(train_list_labeled_sel_file) != len(train_list_sel_query_file):
            print("lengths of train_list_labeled_sel_file & "
                  "train_list_sel_query_file must be the same when --version is 2!")
            exit(0)

        train_samples_labeled_sel_queries = Util.load_labeled_sel_queries_files(dimension, train_list_labeled_sel_file)
        validate_samples_labeled_sel_queries = Util.load_labeled_sel_queries_files(dimension, validate_list_labeled_sel_file)
        train_samples_query_sels = Util.load_queries_sels_files(dimension, train_list_sel_query_file)
        validate_samples_query_sels = Util.load_queries_sels_files(dimension, validate_list_sel_query_file)
        samples_sel_queries_costs = Util.load_sel_queries_costs_file(dimension, sel_costs_file)

        # new a Query Estimator
        query_estimator = Query_Estimator(dimension, num_of_joins)
        # load Query Estimator models from files
        query_estimator.load(qe_model_path)
    else:
        train_samples_labeled_sel_queries = []
        validate_samples_labeled_sel_queries = []
        train_samples_query_sels = []
        validate_samples_query_sels = []
        samples_sel_queries_costs = None
        samples_plan_estimate_errors = None
        query_estimator = None
        query_error_predictors = []

    # select DQN model with highest win_count
    print("===================================")
    print("    selecting DQN models starts")
    print("===================================")
    start = time.time()
    # tried trainings
    trials = []
    for i in range(1, number_of_tries + 1):
        print("----> try: " + str(i))

        # random sample a subset of queries for real training from the train_queries
        training_queries = random.sample(train_queries, int(len(train_queries) * sample_ratio / 100))

        # train DQN model
        start_train = time.time()
        if trace_file is None:
            trace_file_i = None
        else:
            trace_file_i = trace_file + "." + str(i)
        (trained_dqn, fit_rate) = train_dqn(dimension,
                                            training_queries,
                                            unit_cost,
                                            time_budget,
                                            number_of_runs,
                                            batch_size=batch_size,
                                            eps_decay=eps_decay,
                                            memory_size=memory_size,
                                            samples_labeled_sel_queries=train_samples_labeled_sel_queries,
                                            samples_query_sels=train_samples_query_sels,
                                            samples_sel_queries_costs=samples_sel_queries_costs,
                                            query_estimator=query_estimator,
                                            sample_pointer=sample_pointer,
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
        (evaluated_validate_queries, eval_rate) = evaluate_dqn(dimension,
                                                               trained_dqn,
                                                               validate_queries,
                                                               unit_cost,
                                                               time_budget,
                                                               samples_labeled_sel_queries=validate_samples_labeled_sel_queries,
                                                               samples_query_sels=validate_samples_query_sels,
                                                               samples_sel_queries_costs=samples_sel_queries_costs,
                                                               query_estimator=query_estimator,
                                                               sample_pointer=sample_pointer,
                                                               version=version,
                                                               num_of_joins=num_of_joins)
        trials.append({"dqn": trained_dqn, "fit_rate": fit_rate,
                       "eval_rate": eval_rate, "train_time": train_time})
        print("----> try: " + str(i) + " is done, train_time: " + str(train_time) + " seconds, fit_rate: " + str(fit_rate) + ", eval_rate: " + str(eval_rate))
        print()
        print()
    end = time.time()
    print("===================================")
    print("    selecting DQN models ends")
    print("    total time: " + str(end - start) + " seconds.")
    print("===================================")
    print()

    # output win_count and total_reward for each trained model
    print("DQN Model ID,    train_time,    fit_rate,    eval_rate")
    total_train_time = 0.0
    highest_fit_rate = 0.0
    highest_eval_rate = 0.0
    selected_dqn = None
    selected_model_id = 0
    for i in range(number_of_tries):
        trial = trials[i]
        total_train_time += trial["train_time"]
        highest_fit_rate = max(highest_fit_rate, trial["fit_rate"])
        if trial["eval_rate"] > highest_eval_rate:
            highest_eval_rate = trial["eval_rate"]
            selected_dqn = trial["dqn"]
            selected_model_id = i + 1
        print(str(i + 1) + ",    " +
              str(trial["train_time"]) + ",    " +
              str(trial["fit_rate"]) + ",    " +
              str(trial["eval_rate"]))
    print("-----------------------------------")
    print(str(selected_model_id) + ",    " +
          str(total_train_time / number_of_tries) + ",    " +
          str(highest_fit_rate) + ",    " +
          str(highest_eval_rate))

    # save selected DQN model
    torch.save(selected_dqn.state_dict(), dqn_model_file)
    print("===================================")
    print("  model [" + str(selected_model_id) + "] is selected, and saved to file [" + dqn_model_file + "].")
    print("===================================")
    print()



