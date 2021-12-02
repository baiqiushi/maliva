import math
import random
import torch
from smart_util import Util


class EpsilonGreedyStrategy:
    def __init__(self, start, end, decay):
        self.start = start
        self.end = end
        self.decay = decay

    def get_exploration_rate(self, current_step):
        return self.end + (self.start - self.end) * \
            math.exp(-1. * current_step * self.decay)


class Agent:

    tried_actions = []

    def __init__(self, dimension, num_of_joins=1, num_of_sample_ratios=0, sampling_plan_only=False):
        self.dimension = dimension
        self.current_step = 0
        self.num_actions = Util.num_of_plans(dimension, num_of_joins, num_of_sample_ratios, sampling_plan_only)

    def reset(self):
        self.tried_actions = []

    def clear_memory(self):
        self.current_step = 0

    def select_action(self, strategy, state, policy_net):
        rate = strategy.get_exploration_rate(self.current_step)
        self.current_step += 1

        if rate > random.random():  # explore
            action = random.randrange(self.num_actions)
            while action in self.tried_actions:
                action = random.randrange(self.num_actions)
            self.tried_actions.append(action)
            # print("    ...random")
            return action
        else:  # exploit
            with torch.no_grad():
                # print("    ---decide")
                # predicted Q-Values from DQN
                q_values = policy_net(state.get_tensor()).tolist()[0]
                # sort tuple (Q-Value, action) in descending order
                q_value_actions = []
                for i in range(len(q_values)):
                    q_value_actions.append((q_values[i], i))
                q_value_actions.sort(reverse=True)
                # find the largest Q-Value action that has not been tried before
                for q_value_action in q_value_actions:
                    action = q_value_action[1]
                    if action not in self.tried_actions:
                        self.tried_actions.append(action)
                        return action

    def decide_action(self, state, policy_net):
        with torch.no_grad():
            # predicted Q-Values from DQN
            q_values = policy_net(state.get_tensor()).tolist()[0]
            # sort tuple (Q-Value, action) in descending order
            q_value_actions = []
            for i in range(len(q_values)):
                q_value_actions.append((q_values[i], i))
            q_value_actions.sort(reverse=True)
            # find the largest Q-Value action that has not been tried before
            for q_value_action in q_value_actions:
                action = q_value_action[1]
                if action not in self.tried_actions:
                    self.tried_actions.append(action)
                    return action
