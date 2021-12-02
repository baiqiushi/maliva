import torch
from smart_util import Util


class StateQ:
    def __init__(self, dimension, num_of_sample_ratios, num_of_joins=1):
        self.dimension = dimension
        self.num_of_sample_ratios = num_of_sample_ratios
        self.num_of_plans = Util.num_of_plans(dimension, num_of_joins, num_of_sample_ratios, sampling_plan_only=True)
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
#      consider sampling plans only
#      no cost for estimation of the sampling plan (all selectivity values have been collected before running this Q agent)
class EnvironmentQ:

    # @param - dimension: dimension of the queries
    # @param - num_of_sample_ratios: int, number of sample ratios
    # @param - labeled_sample_queries: [list of sample query objects], each sample query object being
    #            {id, time_0, time_1, ..., time_(|d|*|s|-1)}, 
    #            where |d| = dimension, |s| = num_of_sample_ratios
    # @param - sample_queries_qualities: [list of sample query objects], each sample query object being
    #            {id, quality_0, quality_1, ..., quality_(|d|*|s|-1)}, 
    #            where |d| = dimension, |s| = num_of_sample_ratios
    # @param - time_budget: float, time (second) for a query to be viable
    # @param - beta: float, parameter to compute reward for a viable query, default: 0.0, because viable query is for sure viable.
    # @param - num_of_joins: int, number of join methods in hints set
    def __init__(self, dimension, num_of_sample_ratios, labeled_sample_queries, sample_queries_qualities, time_budget, beta=0.0, num_of_joins=1):

        self.dimension = dimension
        self.num_of_sample_ratios = num_of_sample_ratios
        self.num_of_joins = num_of_joins
        self.num_of_plans = Util.num_of_plans(dimension, num_of_joins, num_of_sample_ratios, sampling_plan_only=True)

        # parameters
        self.time_budget = time_budget
        self.beta = beta

        # store labeled_sample_queries list into a hash map with query["id"] as the key
        self.queries = {}
        for query in labeled_sample_queries:
            self.queries[query["id"]] = query
        
        # store sample_queries_qualities list into the {queries} hash map
        for query in sample_queries_qualities:
            if query["id"] not in self.queries:
                print("[Error][EnvironmentQ] query [" + str(query["id"]) + "] in sample_queries_qualities list but not in given labeled_sample_queries list.")
                exit(0)
            self.queries[query["id"]].update(query)

        # initialize member variables
        self.done = False
        self.done_reason = None
        self.query_time = 0.0
        self.query_quality = 0.0
        self.qid = None
        self.state = None
        self.tried_plans = None
        self.tried_plans_time = None

        # reset environment
        self.reset()

    def reset(self, qid=0):
        self.done = False
        self.done_reason = None
        self.query_time = 0.0
        self.query_quality = 0.0
        self.qid = qid
        self.state = StateQ(self.dimension, self.num_of_sample_ratios, self.num_of_joins)
        self.tried_plans = []
        self.tried_plans_time = []
        # initialize state
        for plan in range(1, self.num_of_plans + 1):
            self.state.set_unknown_sels(plan, 0)
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

        # 2. update state
        self.state.set_predict_time(plan, predict_time)

        # 3. compute reward
        # 3.1 find a viable sample plan
        if self.state.get_elapsed_time() + predict_time <= self.time_budget:
            self.done = True
            self.done_reason = "win"
            self.query_time = predict_time
            self.query_quality = query["quality_" + str(plan)]
            reward = Util.reward(self.beta, self.time_budget, self.query_time, self.query_quality)
        # 3.2 can't find a viable sample plan, use the fastest sample plan
        elif self.num_actions_available() == 0:
            best_plan = 0
            best_time = 100.0
            for idx in range(0, len(self.tried_plans)):
                tried_plan = self.tried_plans[idx]
                tried_plan_time = self.tried_plans_time[idx]
                if tried_plan_time < best_time:
                    best_time = tried_plan_time
                    best_plan = tried_plan
            self.done = True
            self.done_reason = "not_possible"
            self.query_time = best_time
            self.query_quality = query["quality_" + str(best_plan)]
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

