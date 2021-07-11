import argparse
import copy
import random
import time
from smart_util import Util
from collections import namedtuple
from smart_agent_3d import EpsilonGreedyStrategy
from smart_agent_3d import Agent
from smart_dqn_3d import DQN
from smart_environment_3d import Environment
from smart_environment_v1_3d import Environment1
from smart_environment_v2_3d import Environment2
from smart_query_estimator_3d import Query_Estimator
import torch
import torch.optim as optim
import torch.nn.functional as F


###########################################################
#  smart_train_dqn_nyc.py
#
#  -lf / --labeled_file    input file that holds labeled queries for training
#  -uc / --unit_cost       time (second) to collect selectivity value for one condition
#  -tb / --time_budget     time (second) for a query to be viable
#  -nr / --number_of_runs  how many times to loop all queries for training. Default: 10
#  -bs / --batch_size      how many experiences used each time update the DQN weights. Default: 1024
#  -ed / --eps_decay       eps_decay rate for epsilon greedy strategy. Default: 0.001
#  -ms / --memory_size     how many experiences at most are stored in the replay memory. Default: 1000000
#  -mf / --model_file      output file that holds trained dqn model
#  -v  / --version         version of DQN and environment to use
#  -tr / --trace           trace the evaluation result using training set, and output for each run. Default: False
#  -trf / --trace_file     output file (no suffix) that holds the trace result. Default: None
#  -nes / --no_early_stop  disable early_stop when model converges. Default: enabled
#  ** Only required when version = 1/2:
#  -llsf / --list_labeled_sel_file  list of labeled_sel_queries files for different sample sizes
#  -lsqf / --list_sel_query_file    list of sel_queries files for different sample sizes
#  -scf  / --sel_costs_file         input file that holds sel queries costs for different sample sizes
#  -qmp  / --qe_model_path          input path to load the models used by Query Estimator
#  -sp   / --sample_pointer         pointer to the sample size to use for the query_estimator. Default: 0
#
# Dependencies:
#   python3.6+
#   pip install torch
#
###########################################################


Experience = namedtuple(
    'Experience',
    ('state', 'action', 'next_state', 'reward')
)


class ModelMemory:
    def __init__(self, capacity):
        self.capacity = capacity
        self.models = []
        self.win_rates = []
        self.push_count = 0

    def push(self, model, win_rate):
        if len(self.models) < self.capacity:
            self.models.append(copy.deepcopy(model))
            self.win_rates.append(win_rate)
        else:
            self.models[self.push_count % self.capacity] = copy.deepcopy(model)
            self.win_rates[self.push_count % self.capacity] = win_rate
        self.push_count += 1

    def converged(self, threshold):
        if len(self.models) < self.capacity:
            return False
        max_win_rate = max(self.win_rates)
        min_win_rate = min(self.win_rates)
        if max_win_rate == 0.0:
            max_win_rate = 1.0
        delta_ratio = (max_win_rate - min_win_rate) / abs(max_win_rate)
        if delta_ratio < threshold:
            return True
        else:
            return False

    def best_model(self):
        max_win_rate = max(self.win_rates)
        best_model_index = self.win_rates.index(max_win_rate)
        return self.models[best_model_index]

    def max_win_rate(self):
        return max(self.win_rates)


class ReplayMemory:
    def __init__(self, capacity):
        self.capacity = capacity
        self.memory = []
        self.push_count = 0

    def push(self, experience):
        if len(self.memory) < self.capacity:
            self.memory.append(experience)
        else:
            self.memory[self.push_count % self.capacity] = experience
        self.push_count += 1

    def sample(self, batch_size):
        return random.sample(self.memory, batch_size)

    def can_provide_sample(self, batch_size):
        return len(self.memory) >= batch_size


class QValues:

    @staticmethod
    def get_current(policy_net, states, actions):
        return policy_net(states).gather(dim=1, index=actions.unsqueeze(-1))

    @staticmethod
    def get_next(target_net, next_states):
        final_state_locations = next_states.flatten(start_dim=1) \
            .max(dim=1)[0].eq(0).type(torch.bool)
        non_final_state_locations = (final_state_locations == False)
        non_final_states = next_states[non_final_state_locations]
        batch_size = next_states.shape[0]
        values = torch.zeros(batch_size).to()
        values[non_final_state_locations] = target_net(non_final_states).max(dim=1)[0].detach()
        return values


def extract_tensors(experiences):
    # Convert batch of Experiences to Experience of batches
    batch = Experience(*zip(*experiences))

    t1 = torch.cat(batch.state)
    t2 = torch.cat(batch.action)
    t3 = torch.cat(batch.reward)
    t4 = torch.cat(batch.next_state)

    return t1, t2, t3, t4


# Train DQN
#
# @param - labeled_queries: [list of query objects], each query object being
#          {id, start_time, end_time, trip_distance_start, trip_distance_end, lng0, lat0, lng1, lat1,
#          time_0, time_1, ..., time_7}
# @param - unit_cost: float, time (second) to collect selectivity value for one condition
# @param - time_budget: float, time (second) for a query to be viable
# @param - number_of_runs: int, how many times to loop all queries for training
#
# * Only valid when version == 1/2:
# @param - samples_labeled_sel_queries: [list of [list of sel queries times]], each inside list being
#          [{id, start_time, end_time, trip_distance_start, trip_distance_end, lng0, lat0, lng1, lat1,
#          time_sel_1, time_sel_2, ..., time_sel_7}]
#          * the outside list is ordered by the sample sizes ascending (e.g., 5k, 50k, 500k)
# @param - samples_query_sels: [list of [list of query_sels]], each inside list being
#          [{id, start_time, end_time, trip_distance_start, trip_distance_end, lng0, lat0, lng1, lat1,
#          sel_1, sel_2, ..., sel_7}]
#          * the outside list is ordered by the sample sizes ascending (e.g., 5k, 50k, 500k)
# @param - samples_sel_queries_costs: [list of [list of sel query cost]], each inside list being
#          [cost(sel_1), cost(sel_2), ..., cost(sel_7)]
#          * the outside list is ordered by the sample sizes ascending (e.g., 5k, 50k, 500k)
# @param - query_estimator: object, Query_Estimator class instance
# @param - sample_pointer: int, [0~2], pointer to the sample size to use for the query_estimator. Default: 0
#
# @return - (DQN object of trained policy network, win_rate[=len(win_queries)/len(labeled_queries)])
def train_dqn(labeled_queries,
              unit_cost,
              time_budget,
              number_of_runs,
              batch_size=1024,
              gamma=0.999,
              eps_start=1,
              eps_end=0.001,
              eps_decay=0.001,
              target_update=10,
              memory_size=1000000,
              learning_rate=0.001,
              samples_labeled_sel_queries=[],
              samples_query_sels=[],
              samples_sel_queries_costs=None,
              query_estimator=None,
              sample_pointer=0,
              version='1',
              trace=False,
              trace_file=None,
              early_stop=True):

    # init objects
    strategy = EpsilonGreedyStrategy(eps_start, eps_end, eps_decay)

    # version 0
    if version == '0':
        env = Environment(labeled_queries, unit_cost, time_budget)
        policy_net = DQN()
        target_net = DQN()
        agent = Agent()
    # version 1
    elif version == '1':
        env = Environment1(labeled_queries,
                           samples_labeled_sel_queries,
                           samples_query_sels,
                           samples_sel_queries_costs,
                           query_estimator,
                           time_budget,
                           sample_pointer=sample_pointer)
        policy_net = DQN()
        target_net = DQN()
        agent = Agent()
    # version 2
    elif version == '2':
        env = Environment2(labeled_queries,
                           samples_labeled_sel_queries,
                           samples_query_sels,
                           samples_sel_queries_costs,
                           query_estimator,
                           time_budget,
                           sample_pointer=sample_pointer)
        policy_net = DQN()
        target_net = DQN()
        agent = Agent()
    # default
    else:
        env = Environment(labeled_queries, unit_cost, time_budget)
        policy_net = DQN()
        target_net = DQN()
        agent = Agent()
        print("Invalid version " + str(version) + "!")
        exit(0)
    memory = ReplayMemory(memory_size)
    target_net.load_state_dict(policy_net.state_dict())
    target_net.eval()
    optimizer = optim.Adam(params=policy_net.parameters(), lr=learning_rate)

    # keep a memory of recent 9 runs' models and total_rewards
    model_memory = ModelMemory(20)

    if trace:
        print("start training DQN ...")
        print("iteration, win_rate, avg_loss")
        traces = []

    start = time.time()
    # train DQN:
    #   for each run in total number_of_runs:
    #        (1) shuffle the order of queries
    #        (2) loop all queries once
    for run in range(number_of_runs):

        win_rate = 0.0
        avg_loss = 0.0

        # shuffle queries order
        random.shuffle(labeled_queries)

        for index, query in enumerate(labeled_queries):
            qid = query["id"]
            env.reset(qid)
            state = env.get_state()
            agent.reset()

            # print("train query " + str(qid))

            while not env.done:
                action = agent.select_action(strategy, state, policy_net)
                plan = action + 1
                # print("    try plan [" + str(plan) + "]")
                reward = env.take_action(plan)
                # print("        reward = " + str(reward))
                next_state = env.get_state()
                memory.push(Experience(state.get_tensor(),
                                       torch.tensor([action]),
                                       next_state.get_tensor(),
                                       torch.tensor([reward]))
                            )
                state = next_state

            if env.get_done_reason() == "win":
                win_rate += 1

            if memory.can_provide_sample(batch_size):
                # print("backward propagate ...")
                experiences = memory.sample(batch_size)
                states, actions, rewards, next_states = extract_tensors(experiences)
                # print("---- states ----")
                # print(states)
                # print("---- actions ----")
                # print(actions)
                # print("---- rewards ----")
                # print(rewards)
                # print("---- next states ----")
                # print(next_states)

                current_q_values = QValues.get_current(policy_net, states, actions)
                next_q_values = QValues.get_next(target_net, next_states)
                target_q_values = (next_q_values * gamma) + rewards

                loss = F.mse_loss(current_q_values, target_q_values.unsqueeze(1))
                avg_loss += loss.tolist()
                optimizer.zero_grad()
                loss.backward()
                # -- start --
                # suggested by https://stackoverflow.com/questions/47036246/dqn-q-loss-not-converging
                # for param in policy_net.parameters():
                #     param.grad.data.clamp_(-1, 1)
                # --- end ---
                optimizer.step()

        if run % target_update == (target_update - 1):
            target_net.load_state_dict(policy_net.state_dict())

        win_rate = win_rate / len(labeled_queries)
        avg_loss = avg_loss / len(labeled_queries)
        model_memory.push(policy_net, win_rate)

        if trace:
            print("%3d,    %1.2f,    %6.1f" % (run, win_rate, avg_loss))
            traces.append([run, win_rate])

        if early_stop and model_memory.converged(0.1):
            if trace:
                print("    ---->    Model converged.    <----")
            break

    env.close()
    end = time.time()
    policy_net = model_memory.best_model()
    max_win_rate = model_memory.max_win_rate()
    if trace:
        print("training DQN is done, takes " + str(end - start) + " seconds. max_win_rate = " + str(max_win_rate))
        if trace_file is not None:
            Util.dump_train_traces(traces, trace_file)
            print("traces wrote to file " + trace_file + ".csv")
        print()

    return policy_net, max_win_rate


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Train DQN.")
    parser.add_argument("-lf", "--labeled_file",
                        help="labeled_file: input file that holds labeled queries for training",
                        type=str, required=True)
    parser.add_argument("-uc", "--unit_cost",
                        help="unit_cost: time (second) to collect selectivity value for one condition. Default: 0.05",
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
                        help="model_file: output file that holds trained dqn model",
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
    parser.add_argument("-llsf", "--list_labeled_sel_file",
                        help="list_labeled_sel_file: list of labeled_sel_queries files for different sample sizes",
                        action='append', required=False, default=[])
    parser.add_argument("-lsqf", "--list_sel_query_file",
                        help="list_sel_query_file: list of sel_queries files for different sample sizes",
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

    labeled_queries_file = args.labeled_file
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
    sample_pointer = args.sample_pointer

    # load labeled queries into memory
    labeled_queries = Util.load_labeled_queries_file_nyc(labeled_queries_file)

    # For version = 1/2
    if version == '1' or version == '2':
        list_labeled_sel_file = args.list_labeled_sel_file
        list_sel_query_file = args.list_sel_query_file
        sel_costs_file = args.sel_costs_file
        qe_model_path = args.qe_model_path

        # assert required parameters
        if len(list_labeled_sel_file) == 0:
            print("-llsf / --list_labeled_sel_file is required when --version is 2!")
            exit(0)
        if len(list_sel_query_file) == 0:
            print("-lsqf / --list_sel_query_file is required when --version is 2!")
            exit(0)
        if sel_costs_file is None:
            print("-scf / --sel_costs_file is required when --version is 2!")
            exit(0)
        if qe_model_path is None:
            print("-qmp / --qe_model_path is required when --version is 2!")
            exit(0)
        if len(list_labeled_sel_file) != len(list_sel_query_file):
            print("lengths of list_labeled_sel_file & list_sel_query_file must be the same when --version is 2!")
            exit(0)

        samples_labeled_sel_queries = Util.load_labeled_sel_queries_files_nyc(list_labeled_sel_file)
        samples_query_sels = Util.load_sel_queries_files_nyc(list_sel_query_file)
        samples_sel_queries_costs = Util.load_sel_queries_costs_file_nyc(sel_costs_file)

        # new a Query Estimator
        query_estimator = Query_Estimator()
        # load Query Estimator models from files
        query_estimator.load(qe_model_path)
    else:
        samples_labeled_sel_queries = []
        samples_query_sels = []
        samples_sel_queries_costs = None
        query_estimator = None

    # train DQN model
    (trained_dqn, win_rate) = train_dqn(labeled_queries,
                                        unit_cost,
                                        time_budget,
                                        number_of_runs,
                                        batch_size=batch_size,
                                        eps_decay=eps_decay,
                                        memory_size=memory_size,
                                        samples_labeled_sel_queries=samples_labeled_sel_queries,
                                        samples_query_sels=samples_query_sels,
                                        samples_sel_queries_costs=samples_sel_queries_costs,
                                        query_estimator=query_estimator,
                                        sample_pointer=sample_pointer,
                                        version=version,
                                        trace=trace,
                                        trace_file=trace_file,
                                        early_stop=early_stop)

    # save DQN model
    torch.save(trained_dqn.state_dict(), dqn_model_file)
    print("DQN model saved to file [" + dqn_model_file + "].")

