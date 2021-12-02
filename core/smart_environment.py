import config
import torch
from smart_util import Util


class State:
    def __init__(self, dimension, num_of_joins=1):
        self.dimension = dimension
        self.num_of_plans = Util.num_of_plans(dimension, num_of_joins)
        # count of unknown selectivity values for each plan
        self.unknown_sels = [0.0 for plan in range(1, self.num_of_plans + 1)]
        # predicted running time for each plan
        self.predict_time = [0.0 for plan in range(1, self.num_of_plans + 1)]
        # elapsed time
        self.elapsed_time = 0.0

    def get_tensor(self):
        vector = []
        for plan in range(1, self.num_of_plans + 1):
            vector.append(self.unknown_sels[plan - 1])
        for plan in range(1, self.num_of_plans+1):
            vector.append(self.predict_time[plan - 1])
        vector.append(self.elapsed_time)
        return torch.tensor([vector])

    def set_unknown_sels(self, plan, value):
        self.unknown_sels[plan - 1] = value

    def set_predict_time(self, plan, value):
        self.predict_time[plan - 1] = value

    def set_elapsed_time(self, value):
        self.elapsed_time = value

    def get_unknown_sels(self):
        return self.unknown_sels

    def get_predict_time(self):
        return self.predict_time

    def get_elapsed_time(self):
        return self.elapsed_time


# TODO - currently use a perfect Query Estimator
#      - the real query time of different plans (1 ~ 2**d-1) will be used as estimated query time
class Environment:

    # @param - dimension: dimension of the queries
    # @param - labeled_queries: [list of query objects], each query object being
    #          {id, time_0, time_1, ..., time_(2**d-1)}
    # @param - unit_cost: float, time (second) to collect selectivity value for one condition
    # @param - time_budget: float, time (second) for a query to be viable
    # @param - num_of_joins: int, number of join methods in hints set.
    def __init__(self, dimension, labeled_queries, unit_cost, time_budget, num_of_joins=1):

        self.dimension = dimension
        self.num_of_joins = num_of_joins
        self.num_of_plans = Util.num_of_plans(dimension, num_of_joins)

        # parameters
        self.unit_cost = unit_cost
        self.time_budget = time_budget

        # store labeled_queries list into a hash map with query["id"] as the key
        self.queries = {}
        for query in labeled_queries:
            self.queries[query["id"]] = query

        # initialize the lookup dict of plan_id -> [list of sel ids]
        #   Example of plan_sels_table for dimension=3:
        #         self.plan_sels_table[1] = [1]  # 001 -> [001]
        #         self.plan_sels_table[2] = [2]  # 010 -> [010]
        #         self.plan_sels_table[3] = [1, 2, 3]  # 011 -> [001, 010, 011]
        #         self.plan_sels_table[4] = [4]  # 100 -> [100]
        #         self.plan_sels_table[5] = [4, 1, 5]  # 101 -> [100, 001, 101]
        #         self.plan_sels_table[6] = [4, 2, 6]  # 110 -> [100, 010, 110]
        #         self.plan_sels_table[7] = [4, 2, 1, 7]  # 111 -> [100, 010, 001, 111]
        self.plan_sels_table = {}
        for plan in range(1, self.num_of_plans + 1):
            self.plan_sels_table[plan] = Util.sel_ids_of_plan(plan, self.dimension, self.num_of_joins)

        # initialize member variables
        self.done = False
        self.done_reason = None
        self.query_time = 0.0
        self.qid = None
        self.state = None
        self.tried_plans = None
        self.tried_plans_time = None
        self.known_sels = None

        # reset environment
        self.reset()

    def reset(self, qid=0):
        self.done = False
        self.done_reason = None
        self.query_time = 0.0
        self.qid = qid
        self.state = State(self.dimension, self.num_of_joins)
        self.tried_plans = []
        self.tried_plans_time = []
        self.known_sels = []
        # initialize state
        for plan in range(1, self.num_of_plans + 1):
            self.state.set_unknown_sels(plan, len(self.plan_sels_table[plan]))
            self.state.set_predict_time(plan, 0.0)
        self.state.set_elapsed_time(0.0)
        return

    def close(self):
        return

    def num_actions_available(self):
        return self.num_of_plans - len(self.tried_plans)

    def take_action(self, plan):
        # 1. evaluate the predicted running time of given plan
        query = self.queries[self.qid]
        predict_time = query["time_" + str(plan)]
        self.tried_plans.append(plan)
        self.tried_plans_time.append(predict_time)

        # 2. compute the cost of evaluating given plan
        # sels needed to evaluate given plan
        needed_sels = self.plan_sels_table[plan]
        # subtracted sels that are already known
        needed_sels = [sel for sel in needed_sels if sel not in self.known_sels]
        # TODO - currently fix the unit cost to get any sel to be constant
        cost = self.unit_cost * len(needed_sels)

        # 3. update state
        self.known_sels = self.known_sels + needed_sels
        for plan in range(1, self.num_of_plans + 1):
            unknown_sels = self.plan_sels_table[plan]
            unknown_sels = [sel for sel in unknown_sels if sel not in self.known_sels]
            self.state.set_unknown_sels(plan, len(unknown_sels))
        self.state.set_predict_time(plan, predict_time)
        self.state.set_elapsed_time(self.state.get_elapsed_time() + cost)

        # 4. compute reward
        if self.state.get_elapsed_time() + predict_time <= self.time_budget:
            self.done = True
            self.done_reason = "win"
            self.query_time = predict_time
            reward = Util.reward(1.0, self.time_budget, self.state.get_elapsed_time() + self.query_time, 1.0)
        elif self.state.get_elapsed_time() >= self.time_budget:
            self.done = True
            self.done_reason = "planning_too_long"  # planning time is too long
            best_time = min(self.tried_plans_time)
            self.query_time = best_time
            reward = Util.reward(1.0, self.time_budget, self.state.get_elapsed_time() + self.query_time, 1.0)
        elif self.num_actions_available() == 0:
            best_time = min(self.tried_plans_time)
            cost = self.state.get_elapsed_time() + best_time - self.time_budget
            self.done = True
            self.done_reason = "not_possible"
            self.query_time = best_time
            reward = Util.reward(1.0, self.time_budget, self.state.get_elapsed_time() + self.query_time, 1.0)
        else:
            reward = 0.0

        return reward

    def get_state(self):
        return self.state

    def get_done_reason(self):
        return self.done_reason

    def get_query_time(self):
        return self.query_time

