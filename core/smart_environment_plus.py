import torch
from smart_util import Util


class StatePlus:
    def __init__(self, dimension, num_of_sample_ratios, num_of_joins=1):
        self.dimension = dimension
        self.num_of_sample_ratios = num_of_sample_ratios
        self.num_of_plans = Util.num_of_plans(dimension, num_of_joins, num_of_sample_ratios)
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


# v0 - use a perfect Query Estimator
#      consider both lossless plans and sampling plans
class EnvironmentPlus:

    # @param - dimension: dimension of the queries
    # @param - num_of_sample_ratios: int, number of sample ratios
    # @param - labeled_queries: [list of query objects], each query object being
    #            {id, time_0, time_1, ..., time_(2**d-1)}
    # @param - labeled_sample_queries: [list of sample query objects], each sample query object being
    #            {id, time_0, time_1, ..., time_(|d|*|s|-1)}, 
    #            where |d| = dimension, |s| = num_of_sample_ratios
    # @param - sample_queries_qualities: [list of sample query objects], each sample query object being
    #            {id, quality_0, quality_1, ..., quality_(|d|*|s|-1)}, 
    #            where |d| = dimension, |s| = num_of_sample_ratios
    # @param - unit_cost: float, time (second) to collect selectivity value for one condition
    # @param - time_budget: float, time (second) for a query to be viable
    # @param - beta: float, parameter to compute reward for a viable query, default: 0.0, because viable query is for sure viable.
    # @param - num_of_joins: int, number of join methods in hints set
    def __init__(self, dimension, num_of_sample_ratios, labeled_queries, labeled_sample_queries, sample_queries_qualities, unit_cost, time_budget, beta=0.0, num_of_joins=1):

        self.dimension = dimension
        self.num_of_sample_ratios = num_of_sample_ratios
        self.num_of_joins = num_of_joins
        # total number of plans
        self.num_of_plans = Util.num_of_plans(dimension, num_of_joins, num_of_sample_ratios)
        # number of lossless plans
        self.num_of_lossless_plans = Util.num_of_plans(dimension, num_of_joins)
        # number of sampling plans
        self.num_of_sampling_plans = Util.num_of_sampling_plans(dimension, num_of_sample_ratios)

        # parameters
        self.unit_cost = unit_cost
        self.time_budget = time_budget
        self.beta = beta

        # store labeled_queries list into a hash map with query["id"] as the key
        self.queries = {}
        for query in labeled_queries:
            self.queries[query["id"]] = query

        # populate labeled_sample_queries into the {queries} hash map
        #   Add an 'X' before each sampling plan id
        for labeled_sample_query in labeled_sample_queries:
            if labeled_sample_query["id"] in self.queries:
                for plan_id in range(0, self.num_of_sampling_plans):
                    self.queries[labeled_sample_query["id"]]["time_X" + str(plan_id)] = labeled_sample_query["time_" + str(plan_id)]
        
        # populate sample_queries_qualities into the {queries} hash map
        for sample_query_quality in sample_queries_qualities:
            if sample_query_quality["id"] in self.queries:
                for plan_id in range(1, self.num_of_lossless_plans + 1):
                    self.queries[sample_query_quality["id"]]["quality_" + str(plan_id)] = 1.0
                for plan_id in range(0, self.num_of_sampling_plans):
                    self.queries[sample_query_quality["id"]]["quality_X" + str(plan_id)] = sample_query_quality["quality_" + str(plan_id)]

        # initialize the lookup dict of plan_id -> [list of sel ids]
        #   Example of plan_sels_table for dimension=3:
        #         self.plan_sels_table[1] = [1]  # 001 -> [001]
        #         self.plan_sels_table[2] = [2]  # 010 -> [010]
        #         self.plan_sels_table[3] = [1, 2, 3]  # 011 -> [001, 010, 011]
        #         self.plan_sels_table[4] = [4]  # 100 -> [100]
        #         self.plan_sels_table[5] = [4, 1, 5]  # 101 -> [100, 001, 101]
        #         self.plan_sels_table[6] = [4, 2, 6]  # 110 -> [100, 010, 110]
        #         self.plan_sels_table[7] = [4, 2, 1, 7]  # 111 -> [100, 010, 001, 111]
        #         ---- Below are sampling plans ----
        #         self.plan_sels_table[X0] = [4]
        #         self.plan_sels_table[X1] = [4]
        #         ...
        #         self.plan_sels_table[X5] = [2]
        #         ...
        #         self.plan_sels_table[X10] = [1]
        #         ..
        #         self.plan_sels_table[X14] = [1]
        self.plan_sels_table = {}
        for plan in range(1, self.num_of_lossless_plans + 1):
            self.plan_sels_table[plan] = Util.sel_ids_of_plan(plan, self.dimension, self.num_of_joins)
        for plan in range(0, self.num_of_sampling_plans):
            self.plan_sels_table["X" + str(plan)] = Util.sel_ids_of_sampling_plan(plan, self.dimension, self.num_of_sample_ratios)

        # initialize member variables
        self.done = False
        self.done_reason = None
        self.query_time = 0.0
        self.query_quality = 0.0
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
        self.query_quality = 0.0
        self.qid = qid
        self.state = StatePlus(self.dimension, self.num_of_sample_ratios, self.num_of_joins)
        self.tried_plans = []
        self.tried_plans_time = []
        self.known_sels = []
        # initialize state
        for plan in range(1, self.num_of_plans + 1):
            if plan <= self.num_of_lossless_plans:
                plan_name = plan
            else:
                plan_name = "X" + str(plan - self.num_of_lossless_plans - 1)
            self.state.set_unknown_sels(plan, len(self.plan_sels_table[plan_name]))
            self.state.set_predict_time(plan, 0.0)
        self.state.set_elapsed_time(0.0)
        return

    def close(self):
        return

    def num_actions_available(self):
        return self.num_of_plans - len(self.tried_plans)

    def take_action(self, plan):
        # 0. get plan_name from plan (i.e., plan_id)
        if plan <= self.num_of_lossless_plans:
            plan_name = plan
        else:
            plan_name = "X" + str(plan - self.num_of_lossless_plans - 1)

        # 1. evaluate the predicted running time of given plan
        query = self.queries[self.qid]
        predict_time = query["time_" + str(plan_name)]
        self.tried_plans.append(plan_name)
        self.tried_plans_time.append(predict_time)

        # 2. compute the cost of evaluating given plan
        # sels needed to evaluate given plan
        needed_sels = self.plan_sels_table[plan_name]
        # subtracted sels that are already known
        needed_sels = [sel for sel in needed_sels if sel not in self.known_sels]
        # TODO - currently fix the unit cost to get any sel to be constant
        cost = self.unit_cost * len(needed_sels)

        # 3. update state
        self.known_sels = self.known_sels + needed_sels
        for iplan in range(1, self.num_of_plans + 1):
            if iplan <= self.num_of_lossless_plans:
                iplan_name = iplan
            else:
                iplan_name = "X" + str(iplan - self.num_of_lossless_plans - 1)
            unknown_sels = self.plan_sels_table[iplan_name]
            unknown_sels = [sel for sel in unknown_sels if sel not in self.known_sels]
            self.state.set_unknown_sels(iplan, len(unknown_sels))
        self.state.set_predict_time(plan, predict_time)
        self.state.set_elapsed_time(self.state.get_elapsed_time() + cost)

        # 4. compute reward
        # 4.1 find a viable plan
        if self.state.get_elapsed_time() + predict_time <= self.time_budget:
            self.done = True
            self.done_reason = "win"
            self.query_time = predict_time
            self.query_quality = query["quality_" + str(plan_name)]
            reward = Util.reward(self.beta, self.time_budget, self.query_time, self.query_quality)
        # 4.2 run out of time, use the fastest known plan
        elif self.state.get_elapsed_time() >= self.time_budget:
            self.done = True
            self.done_reason = "planning_too_long"  # planning time is too long
            best_plan_name = 0
            best_time = 100.0
            for idx in range(0, len(self.tried_plans)):
                tried_plan = self.tried_plans[idx]
                tried_plan_time = self.tried_plans_time[idx]
                if tried_plan_time < best_time:
                    best_time = tried_plan_time
                    best_plan_name = tried_plan
            self.query_time = best_time
            self.query_quality = query["quality_" + str(best_plan_name)]
            reward = Util.reward(self.beta, self.time_budget, self.query_time, self.query_quality)
        # 4.3 exhaust all plans but can't find a viable plan, use the fastest known plan
        elif self.num_actions_available() == 0:
            self.done = True
            self.done_reason = "not_possible"
            best_plan_name = 0
            best_time = 100.0
            for idx in range(0, len(self.tried_plans)):
                tried_plan = self.tried_plans[idx]
                tried_plan_time = self.tried_plans_time[idx]
                if tried_plan_time < best_time:
                    best_time = tried_plan_time
                    best_plan_name = tried_plan
            self.query_time = best_time
            self.query_quality = query["quality_" + str(best_plan_name)]
            reward = Util.reward(self.beta, self.time_budget, self.query_time, self.query_quality)
        else:
            reward = 0.0

        return reward

    def get_state(self):
        return self.state

    def get_done_reason(self):
        return self.done_reason

    def get_query_time(self):
        return self.query_time

    def get_query_quality(self):
        return self.query_quality

    def get_tried_plans(self):
        return self.tried_plans
