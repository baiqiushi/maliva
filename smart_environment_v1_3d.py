import config
import torch
from smart_util import Util


# Different from State,
#   State1 replaces the unknown_sels with estimate_costs
#   State1 replaces the predict_time with estimate_times
class State1:
    def __init__(self):
        # estimate cost for each plan (001 ~ 111)
        self.estimate_costs = [0.0 for plan in range(1, 8)]
        # estimate query time for each plan (001 ~ 111)
        self.estimate_times = [0.0 for plan in range(1, 8)]
        # elapsed time
        self.elapsed_time = 0.0

    def get_tensor(self):
        vector = []
        for plan in range(1, 8):
            vector.append(self.estimate_costs[plan - 1])
        for plan in range(1, 8):
            vector.append(self.estimate_times[plan - 1])
        vector.append(self.elapsed_time)
        return torch.tensor([vector])

    def set_estimate_costs(self, plan, value):
        self.estimate_costs[plan - 1] = value

    def set_estimate_times(self, plan, value):
        self.estimate_times[plan - 1] = value

    def set_elapsed_time(self, value):
        self.elapsed_time = value

    def get_estimate_costs(self):
        return self.estimate_costs

    def get_estimate_times(self):
        return self.estimate_times

    def get_elapsed_time(self):
        return self.elapsed_time


# Different from Environment,
#   Environment1 uses State1,
#   TODO - Environment1 is only for training dqn_v1, use Environment2 to evaluate dqn_v1
class Environment1:

    # @param - labeled_queries: [list of query objects], each query object being
    #          {id, ..., time_0, time_1, ..., time_7}
    # @param - samples_labeled_sel_queries: [list of [list of sel queries times]], each inside list being
    #          [{id, ..., time_sel_1, time_sel_2, ..., time_sel_7}]
    #          * the outside list is ordered by the sample sizes ascending (e.g., 5k, 50k, 500k)
    # @param - samples_query_sels: [list of [list of query_sels]], each inside list being
    #          [{id, ..., sel_1, sel_2, ..., sel_7}]
    #          * the outside list is ordered by the sample sizes ascending (e.g., 5k, 50k, 500k)
    # @param - samples_sel_queries_costs: [list of [list of sel query cost]], each inside list being
    #          [cost(sel_1), cost(sel_2), ..., cost(sel_7)]
    #          * the outside list is ordered by the sample sizes ascending (e.g., 5k, 50k, 500k)
    # @param - query_estimator: object, Query_Estimator class instance
    # @param - time_budget: float, time (second) for a query to be viable
    # @param - sample_pointer: int, [0 ~ 2],
    #                          pointer to the sample size to use for the query_estimator. Default: 0 (5K)
    def __init__(self,
                 labeled_queries,
                 samples_labeled_sel_queries,
                 samples_query_sels,
                 samples_sel_queries_costs,
                 query_estimator,
                 time_budget,
                 sample_pointer=0):

        # parameters
        self.query_estimator = query_estimator
        self.time_budget = time_budget
        self.sample_pointer = sample_pointer

        # initialize the lookup dict of plan_id -> [list of sel ids]
        self.plan_sels_table = {}
        self.plan_sels_table[1] = [1]  # 001 -> [001]
        self.plan_sels_table[2] = [2]  # 010 -> [010]
        self.plan_sels_table[3] = [1, 2, 3]  # 011 -> [001, 010, 011]
        self.plan_sels_table[4] = [4]  # 100 -> [100]
        self.plan_sels_table[5] = [4, 1, 5]  # 101 -> [100, 001, 101]
        self.plan_sels_table[6] = [4, 2, 6]  # 110 -> [100, 010, 110]
        self.plan_sels_table[7] = [4, 2, 1, 7]  # 111 -> [100, 010, 001, 111]

        # store labeled_queries list into a hash map with query["id"] as the key
        self.labeled_queries = {}
        for labeled_query in labeled_queries:
            self.labeled_queries[labeled_query["id"]] = labeled_query

        # store samples_labeled_sel_queries list into a list of hash maps with query["id"] as the key
        self.samples_labeled_sel_queries = []
        for sample_labeled_sel_queries in samples_labeled_sel_queries:
            self.sample_labeled_sel_queries = {}
            for labeled_sel_query in sample_labeled_sel_queries:
                self.sample_labeled_sel_queries[labeled_sel_query["id"]] = labeled_sel_query
            self.samples_labeled_sel_queries.append(self.sample_labeled_sel_queries)

        # store samples_query_sels list into a list of hash maps with query["id] as the key
        self.samples_query_sels = []
        for sample_query_sels in samples_query_sels:
            self.sample_query_sels = {}
            for query_sels in sample_query_sels:
                self.sample_query_sels[query_sels["id"]] = query_sels
            self.samples_query_sels.append(self.sample_query_sels)

        # store samples_sel_queries_costs as it is
        self.samples_sel_queries_costs = samples_sel_queries_costs

        # initialize member variables
        self.done = False
        self.done_reason = None
        self.query_time = 0.0
        self.qid = None
        self.state = None
        self.tried_plans = None
        self.tried_plans_time = None
        self.sample_known_sels = None

        # reset environment
        self.reset()

    def reset(self, qid=-1):
        self.done = False
        self.done_reason = None
        self.query_time = 0.0
        self.qid = qid
        self.state = State1()
        self.tried_plans = []
        self.tried_plans_time = []
        self.sample_known_sels = set()
        # initialize state
        sel_queries_costs = self.samples_sel_queries_costs[self.sample_pointer]
        for plan in range(1, 8):
            estimate_cost = 0.0
            plan_sels = self.plan_sels_table[plan]
            for sel in plan_sels:
                estimate_cost += sel_queries_costs[sel - 1]
            self.state.set_estimate_costs(plan, estimate_cost)
            self.state.set_estimate_times(plan, 0.0)
        self.state.set_elapsed_time(0.0)
        return

    def close(self):
        return

    def num_actions_available(self):
        return 7 - len(self.tried_plans)

    def estimate_query(self, plan):
        sample_query_sels = self.samples_query_sels[self.sample_pointer]
        query_sels = sample_query_sels[self.qid]

        sample_need_sels = set()
        # get the input vector for Query_Estimator
        xte = []
        sel_ids = Util.sel_ids_of_plan(plan)
        x = []
        for sel_id in sel_ids:
            x.append(query_sels["sel_" + str(sel_id)])
            sample_need_sels.add("sel_" + str(sel_id))
        xte.append(x)

        # estimate query time
        ypr = self.query_estimator.predict(plan, xte)
        estimate_time = ypr[0, 0]

        # get real cost
        real_cost = 0.0
        sample_new_sels = sample_need_sels.difference(self.sample_known_sels)
        sample_labeled_sel_queries = self.samples_labeled_sel_queries[self.sample_pointer]
        labled_sel_query = sample_labeled_sel_queries[self.qid]
        for sel in sample_new_sels:
            real_cost += labled_sel_query["time_" + sel]

        # put the newly got sels into sample known sels
        self.sample_known_sels.update(sample_new_sels)

        return estimate_time, real_cost

    def take_action(self, plan):
        # 1. estimate the query time of given plan -> estimate_time, real_cost
        estimate_time, real_cost = self.estimate_query(plan)
        self.tried_plans.append(plan)
        self.tried_plans_time.append(estimate_time)

        # 2. update state
        # 2.1 update estimate_costs
        for p in range(1, 8):
            sel_queries_costs = self.samples_sel_queries_costs[self.sample_pointer]
            plan_sels = self.plan_sels_table[p]
            estimate_cost = 0.0
            for sel in plan_sels:
                if "sel_" + str(sel) not in self.sample_known_sels:
                    estimate_cost += sel_queries_costs[sel - 1]
            self.state.set_estimate_costs(p, estimate_cost)
        # 2.2 update estimate_times
        self.state.set_estimate_times(plan, estimate_time)
        # 2.3 update elapsed_time
        self.state.set_elapsed_time(self.state.get_elapsed_time() + real_cost)

        # 3. compute reward
        # 3.1 based on the estimate_time, choose the plan
        if self.state.get_elapsed_time() + estimate_time <= self.time_budget:
            self.done = True
            reward = config.MDP.win_reward
            self.done_reason = "win"
        elif self.state.get_elapsed_time() > self.time_budget:
            reward = config.MDP.lose_reward
            self.done = True
            self.done_reason = "planning_too_long"  # planning time is too long
            self.query_time = self.get_best_plan_estimate_time()
        elif self.num_actions_available() == 0:
            self.query_time = self.get_best_plan_estimate_time()
            cost = self.state.get_elapsed_time() + self.query_time - self.time_budget
            reward = config.MDP.unit_cost_reward * cost
            self.done = True
            self.done_reason = "not_possible"
        else:
            reward = config.MDP.unit_cost_reward * real_cost

        return reward

    def get_state(self):
        return self.state

    def get_done_reason(self):
        return self.done_reason

    def get_query_time(self):
        return self.query_time

    def get_best_plan_estimate_time(self):
        estimate_times = self.state.get_estimate_times()
        # find best_estimate_time
        best_estimate_time = 100.0
        for action in range(0, 7):
            estimate_time = estimate_times[action]
            if 0 < estimate_time < best_estimate_time:
                best_estimate_time = estimate_time
        return float(best_estimate_time)

