import argparse
import copy
import random
import time
from smart_util import Util
from collections import namedtuple
from smart_agent import EpsilonGreedyStrategy
from smart_agent import Agent
from smart_dqn import DQN
from smart_environment_plus import EnvironmentPlus
import torch
import torch.optim as optim
import torch.nn.functional as F


###########################################################
#  smart_train_dqn_plus.py
#
#  -d   / --dimension               dimension of the queries. Default: 3
#  -nsr / --num_sample_ratios       number of sample ratios.
#  -nj  / --num_join                number of join methods. Default: 1
#  -lf  / --labeled_file            input file that holds labeled queries for training
#  -lsf / --labeled_sample_file     input file that holds labeled sample queries for training
#  -sqf / --sample_quality_file     input file that holds sample queries qualities for training
#  -uc  / --unit_cost               time (second) to collect selectivity value for one condition
#  -tb  / --time_budget             time (second) for a query to be viable
#  -bt  / --beta                    beta value for reward function. Default: 0.0
#  -nr  / --number_of_runs          how many times to loop all queries for training. Default: 10
#  -bs  / --batch_size              how many experiences used each time update the DQN weights. Default: 1024
#  -ed  / --eps_decay               eps_decay rate for epsilon greedy strategy. Default: 0.001
#  -ms  / --memory_size             how many experiences at most are stored in the replay memory. Default: 1000000
#  -mf  / --model_file              output file that holds trained dqn model
#  -v   / --version                 version of DQN and environment to use. Default: '0'
#  -tr  / --trace                   trace the evaluation result using training set, and output for each run. Default: False
#  -trf / --trace_file              output file (no suffix) that holds the trace result. Default: None
#  -nes / --no_early_stop           disable early_stop when model converges. Default: enabled
#
# Dependencies:
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
        self.total_reward = []
        self.push_count = 0

    def push(self, model, total_reward):
        if len(self.models) < self.capacity:
            self.models.append(copy.deepcopy(model))
            self.total_reward.append(total_reward)
        else:
            self.models[self.push_count % self.capacity] = copy.deepcopy(model)
            self.total_reward[self.push_count % self.capacity] = total_reward
        self.push_count += 1

    def converged(self, threshold):
        if len(self.models) < self.capacity:
            return False
        max_total_reward = max(self.total_reward)
        min_total_reward = min(self.total_reward)
        if max_total_reward == 0.0:
            max_total_reward = 1.0
        delta_ratio = (max_total_reward - min_total_reward) / abs(max_total_reward)
        if delta_ratio < threshold:
            return True
        else:
            return False

    def best_model(self):
        max_total_reward = max(self.total_reward)
        best_model_index = self.total_reward.index(max_total_reward)
        return self.models[best_model_index]

    def max_total_reward(self):
        return max(self.total_reward)


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
# @param - dimension:                int, dimension of the queries
# @param - num_of_sample_ratios:     int, number of sample ratios
# @param - labeled_queries:          [list of query objects], each query object being
#                                      {id, time_0, time_1, ..., time_(2**d-1)}
# @param - labeled_sample_queries:   [list of sample query objects], each sample query object being
#                                      {id, time_0, time_1, ..., time_(|d|*|s|-1)}, 
#                                      where |d| = dimension, |s| = num_of_sample_ratios
# @param - sample_queries_qualities: [list of sample query objects], each sample query object being
#                                      {id, quality_0, quality_1, ..., quality_(|d|*|s|-1)}, 
#                                      where |d| = dimension, |s| = num_of_sample_ratios
# @param - unit_cost:                float, time (second) to collect selectivity value for one condition
# @param - time_budget:              float, time (second) for a query to be viable
# @param - beta:                     float, parameter to compute reward for a viable query.
# @param - number_of_runs:           int, how many times to loop all queries for training
# @param - num_of_joins:             int, number of join methods in hints set.
#
# @return - (DQN object of trained policy network, total_reward)
def train_dqn(dimension,
              num_of_sample_ratios,
              labeled_queries,
              labeled_sample_queries,
              sample_queries_qualities,
              unit_cost,
              time_budget,
              beta,
              number_of_runs,
              batch_size=1024,
              gamma=0.999,
              eps_start=1,
              eps_end=0.001,
              eps_decay=0.001,
              target_update=10,
              memory_size=1000000,
              learning_rate=0.001,
              version='0',
              trace=False,
              trace_file=None,
              early_stop=True,
              num_of_joins=1):

    # init objects
    strategy = EpsilonGreedyStrategy(eps_start, eps_end, eps_decay)

    # version 0
    if version == '0':
        env = EnvironmentPlus(dimension, 
                              num_of_sample_ratios, 
                              labeled_queries,
                              labeled_sample_queries, 
                              sample_queries_qualities, 
                              unit_cost,
                              time_budget, 
                              beta, 
                              num_of_joins)
        policy_net = DQN(dimension, num_of_joins, num_of_sample_ratios)
        target_net = DQN(dimension, num_of_joins, num_of_sample_ratios)
        agent = Agent(dimension, num_of_joins, num_of_sample_ratios)
    # default
    else:
        env = EnvironmentPlus(dimension, 
                              num_of_sample_ratios, 
                              labeled_queries,
                              labeled_sample_queries, 
                              sample_queries_qualities, 
                              unit_cost,
                              time_budget, 
                              beta, 
                              num_of_joins)
        policy_net = DQN(dimension, num_of_joins, num_of_sample_ratios)
        target_net = DQN(dimension, num_of_joins, num_of_sample_ratios)
        agent = Agent(dimension, num_of_joins, num_of_sample_ratios)
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
        print("iteration, total_reward")
        traces = []

    start = time.time()
    # train DQN:
    #   for each run in total number_of_runs:
    #        (1) shuffle the order of queries
    #        (2) loop all queries once
    for run in range(number_of_runs):

        total_reward = 0.0

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
                total_reward += reward
                # print("        reward = " + str(reward))
                next_state = env.get_state()
                memory.push(Experience(state.get_tensor(),
                                       torch.tensor([action]),
                                       next_state.get_tensor(),
                                       torch.tensor([reward]))
                            )
                state = next_state

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
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

        if run % target_update == 1:
            target_net.load_state_dict(policy_net.state_dict())

        model_memory.push(policy_net, total_reward)

        if trace:
            print(str(run) + ", " + str(total_reward))
            traces.append([run, total_reward])

        if early_stop and model_memory.converged(0.1):
            if trace:
                print("    ---->    Model converged.    <----")
            break

    env.close()
    end = time.time()
    policy_net = model_memory.best_model()
    max_total_reward = model_memory.max_total_reward()
    if trace:
        print("training DQN is done, takes " + str(end - start) + " seconds. max_total_reward = " + str(max_total_reward))
        if trace_file is not None:
            Util.dump_train_traces(traces, trace_file)
            print("traces wrote to file " + trace_file + ".csv")
        print()

    return policy_net, max_total_reward


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Train DQN.")
    parser.add_argument("-d", "--dimension",
                        help="dimension: dimension of the queries. Default: 3",
                        type=int, required=False, default=3)
    parser.add_argument("-nsr", "--num_sample_ratios", help="num_sample_ratios: number of sample ratios.", 
                        type=int, required=True)
    parser.add_argument("-nj", "--num_join", help="num_join: number of join methods. Default: 1", 
                        type=int, required=False, default=1)
    parser.add_argument("-lf", "--labeled_file",
                        help="labeled_file: input file that holds labeled queries for training",
                        type=str, required=True)
    parser.add_argument("-lsf", "--labeled_sample_file",
                        help="labeled_sample_file: input file that holds labeled sample queries for training",
                        type=str, required=True)
    parser.add_argument("-sqf", "--sample_quality_file",
                        help="sample_quality_file: input file that holds sample queries qualities for training",
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
                        help="model_file: output file that holds trained dqn model",
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
    args = parser.parse_args()

    dimension = args.dimension
    num_of_sample_ratios = args.num_sample_ratios
    num_of_joins = args.num_join
    labeled_queries_file = args.labeled_file
    labeled_sampe_queries_file = args.labeled_sample_file
    sample_queries_qualities_file = args.sample_quality_file
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

    # load labeled queries into memory
    labeled_queries = Util.load_labeled_queries_file(dimension, labeled_queries_file, num_of_joins)

    # load labeled sample queries into memory
    labeled_sample_queries = Util.load_labeled_sample_queries_file(dimension, num_of_sample_ratios, labeled_sampe_queries_file)

    # load sample queries qualities into memory
    sample_queries_qualities = Util.load_sample_queries_qualities_file(dimension, num_of_sample_ratios, sample_queries_qualities_file)

    # train DQN model
    (trained_dqn, total_reward) = train_dqn(dimension,
                                            num_of_sample_ratios,
                                            labeled_queries,
                                            labeled_sample_queries,
                                            sample_queries_qualities,
                                            unit_cost,
                                            time_budget,
                                            number_of_runs,
                                            batch_size=batch_size,
                                            eps_decay=eps_decay,
                                            memory_size=memory_size,
                                            version=version,
                                            trace=trace,
                                            trace_file=trace_file,
                                            early_stop=early_stop,
                                            num_of_joins=num_of_joins)

    # save DQN model
    torch.save(trained_dqn.state_dict(), dqn_model_file)
    print("DQN model saved to file [" + dqn_model_file + "].")

