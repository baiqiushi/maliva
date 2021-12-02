import csv
import math
import os.path


class Util:

    @staticmethod
    def num_of_plans(dimension, num_of_joins=1, num_of_sample_ratios=0, sampling_plan_only=False):
        num_of_plans = (2 ** dimension - 1) * num_of_joins  # plan 0 is the original plan (no hint)
        if num_of_sample_ratios > 0:
            num_of_sampling_plans = Util.num_of_sampling_plans(dimension, num_of_sample_ratios)
            if sampling_plan_only:
                return num_of_sampling_plans
            num_of_plans += num_of_sampling_plans
        return num_of_plans
    
    @staticmethod
    def use_index(hint_id, plan_id, dimension, num_of_joins=1):
        # translate the plan_id number into binary bits array
        # e.g., 6 -> 0,0,0,0,0,1,1,0
        plan_bits = [int(x) for x in '{:08b}'.format(plan_id)]
        # plan_bit_id that represents the hint_id given dimension
        plan_bit_id = len(plan_bits) - dimension + hint_id
        if plan_bits[plan_bit_id] == 1:
            return True
        return False
    
    @staticmethod
    def num_of_sampling_plans(dimension, num_of_sample_ratios):
        num_of_plans = dimension * num_of_sample_ratios
        return num_of_plans
    
    @staticmethod
    def hint_id_of_sampling_plan(num_of_sample_ratios, plan_id):
        return plan_id // num_of_sample_ratios
    
    @staticmethod
    def sample_ratio_id_of_sampling_plan(num_of_sample_ratios, plan_id):
        return plan_id % num_of_sample_ratios

    @staticmethod
    def reduce_join_method(plan, dimension):
        join_method = 1
        # reduce plan to be the index_selection plan within one join method
        while plan > (2 ** dimension - 1):
            plan = plan - (2 ** dimension - 1)
            join_method += 1
        return plan, join_method

    # This function decomposes an integer (≥1) into a sum of several powers of 2.
    #   Example 1: if n = 6 (0000,0110), then output [4, 2], because 6 = 4 (2^2) + 2 (2^1)
    #   Example 2: if n = 7 (0000,0111), then output [4, 2, 1], because 7 = 2^2 + 2^1 + 2^0
    @staticmethod
    def decompose_to_binary_numbers(n):
        powers = []
        i = 1
        while i <= n:
            if i & n:
                powers.append(i)
            i <<= 1  # i shifts bits to left by 1 (equivalently x2)
        return powers

    # return the selectivity value ids need to be collected to estimate query time of a given plan
    #   Example 1: if plan = 6 (0000,0110), then output [6, 4, 2],
    #                because two indexes (create_at, coordinate) are used for the plan,
    #                and we need the selectivities [4, 2] of create_at and coordinate filtering conditions separately,
    #                and also the & selectivity [6] on the (create_at & coordinate) filtering condition.
    #   Example 2: if plan = 2 (0000,0010), then output [2]
    @staticmethod
    def sel_ids_of_plan(plan, dimension=3, num_of_joins=1):
        if num_of_joins > 1:
            num_of_plans = Util.num_of_plans(dimension, num_of_joins)
            if plan < 1 or plan > num_of_plans:
                print("plan " + str(plan) + " is invalid given dimension "+ str(dimension) + ", number of joins " + str(num_of_joins))
                exit(0)
            plan, _ = Util.reduce_join_method(plan, dimension)
        # Note: when num_of_joins == 1, parameter dimension is not used at all
        # get the sel_ids of a reduced plan within 1 ~ 2**dimension-1
        sel_ids = Util.decompose_to_binary_numbers(plan)
        if plan not in sel_ids:
            sel_ids.append(plan)
        return sel_ids
    
    # return the selectivity value ids need to be collected to estimate query time of a given sampling plan
    @staticmethod
    def sel_ids_of_sampling_plan(plan, dimension, num_of_sample_ratios):
        sel_ids = []
        hint_id = Util.hint_id_of_sampling_plan(num_of_sample_ratios, plan)
        # hint_id = 0 --> sel_id = 4
        # hint_id = 1 --> sel_id = 2
        # hint_id = 2 --> sel_id = 1
        sel_ids.append(2**(dimension - 1 - hint_id))
        return sel_ids

    # return the number of selectivity values needed to estimate query time of a given plan
    @staticmethod
    def number_of_sels(plan):
        if plan < 1:
            return 0
        else:
            return len(Util.sel_ids_of_plan(plan))

    # dump labeled queries out to file
    @staticmethod
    def dump_labeled_queries_file(dimension, out_file, labeled_queries, num_of_joins=1):
        num_of_plans = Util.num_of_plans(dimension, num_of_joins)
        with open(out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query in labeled_queries:
                row = [query["id"]]
                for plan_id in range(0, num_of_plans + 1):
                    row.append(query["time_" + str(plan_id)])
                csv_writer.writerow(row)

    # dump labeled std queries out to file
    @staticmethod
    def dump_labeled_std_queries_file(dimension, out_file, labeled_queries, num_of_joins=1):
        num_of_plans = Util.num_of_plans(dimension, num_of_joins)
        with open(out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query in labeled_queries:
                row = [query["id"]]
                for plan_id in range(0, num_of_plans + 1):
                    row.append(query["time_" + str(plan_id) + "_std"])
                csv_writer.writerow(row)

    # load labeled queries into memory
    @staticmethod
    def load_labeled_queries_file(dimension, labeled_queries_file, num_of_joins=1):
        num_of_plans = Util.num_of_plans(dimension, num_of_joins)
        labeled_queries = []
        if os.path.isfile(labeled_queries_file):
            with open(labeled_queries_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    # number of columns of the CSV file should be num_of_plans + 2: [id, 0, 1, ..., num_of_plans]
                    # if len(row) != num_of_plans + 2:
                    #     print("[" + labeled_queries_file + "] has " + str(len(row)) + " columns, " \
                    #           "does NOT fit given dimension " + str(dimension) + ", number of joins " + str(num_of_joins))
                    #     exit(0)
                    query = {"id": int(row[0])}
                    for plan_id in range(0, num_of_plans + 1):
                        query["time_" + str(plan_id)] = float(row[1 + plan_id])
                    labeled_queries.append(query)
        else:
            print("[" + labeled_queries_file + "] does NOT exist! Exit!")
            exit(0)
        return labeled_queries

    # dump labeled sel queries out to file
    @staticmethod
    def dump_labeled_sel_queries_file(dimension, out_file, labeled_sel_queries):
        sel_combinations = 2 ** dimension - 1
        with open(out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query in labeled_sel_queries:
                row = [query["id"]]
                for plan_id in range(1, sel_combinations + 1):
                    row.append(query["time_" + str(plan_id)])
                csv_writer.writerow(row)

    # dump labeled sel std queries out to file
    @staticmethod
    def dump_labeled_sel_std_queries_file(dimension, out_file, labeled_sel_queries):
        sel_combinations = 2 ** dimension - 1
        with open(out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query in labeled_sel_queries:
                row = [query["id"]]
                for plan_id in range(1, sel_combinations + 1):
                    row.append(query["time_" + str(plan_id) + "_std"])
                csv_writer.writerow(row)

    # load labeled_sel_queries_file into memory
    @staticmethod
    def load_labeled_sel_queries_file(dimension, labeled_sel_queries_file):
        sel_combinations = 2 ** dimension - 1
        labeled_sel_queries = []
        if os.path.isfile(labeled_sel_queries_file):
            with open(labeled_sel_queries_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    # number of columns of the CSV file should be sel_combinations + 1: [id, 1, ..., sel_combinations]
                    if len(row) != sel_combinations + 1:
                        print("[" + labeled_sel_queries_file + "] has " + str(len(row)) + " columns, does NOT fit "
                                                                                          "given dimension " +
                              str(dimension))
                        exit(0)
                    labeled_sel_query = {"id": int(row[0])}
                    for sel in range(1, sel_combinations + 1):
                        labeled_sel_query["time_sel_" + str(sel)] = float(row[sel])
                    labeled_sel_queries.append(labeled_sel_query)
        else:
            print("[" + labeled_sel_queries_file + "] does NOT exist! Exit!")
            exit(0)
        return labeled_sel_queries

    # load list of labeled_sel_queries_files into memory
    @staticmethod
    def load_labeled_sel_queries_files(dimension, labeled_sel_queries_files):
        sel_combinations = 2 ** dimension - 1
        samples_labeled_sel_queries = []
        for labeled_sel_queries_file in labeled_sel_queries_files:
            labeled_sel_queries = []
            if os.path.isfile(labeled_sel_queries_file):
                with open(labeled_sel_queries_file, "r") as csv_in:
                    csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                    for row in csv_reader:
                        # number of columns of the CSV file should be sel_combinations + 1: [id, 1, ..., sel_combinations]
                        if len(row) != sel_combinations + 1:
                            print("[" + labeled_sel_queries_file + "] has " + str(len(row)) + " columns, does NOT fit "
                                                                                              "given dimension " +
                                  str(dimension))
                            exit(0)
                        labeled_sel_query = {"id": int(row[0])}
                        for sel in range(1, sel_combinations + 1):
                            labeled_sel_query["time_sel_" + str(sel)] = float(row[sel])
                        labeled_sel_queries.append(labeled_sel_query)
                print("[" + labeled_sel_queries_file + "] loaded into memory.")
            else:
                print("[" + labeled_sel_queries_file + "] does NOT exist! Exit!")
                exit(0)
            samples_labeled_sel_queries.append(labeled_sel_queries)
        return samples_labeled_sel_queries

    # dump queries selectivities out to file
    @staticmethod
    def dump_queries_sels_file(dimension, out_file, queries_sels):
        sel_combinations = 2 ** dimension - 1
        with open(out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query in queries_sels:
                row = [query["id"]]
                for plan_id in range(1, sel_combinations + 1):
                    row.append(query["sel_" + str(plan_id)])
                csv_writer.writerow(row)

    # load queries selectivities file into memory
    @staticmethod
    def load_queries_sels_file(dimension, queries_sels_file):
        sel_combinations = 2 ** dimension - 1
        queries_sels = []
        if os.path.isfile(queries_sels_file):
            with open(queries_sels_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    query_sels = {"id": int(row[0])}
                    # number of columns of the CSV file should be sel_combinations + 1: [id, 1, ..., sel_combinations]
                    if len(row) != sel_combinations + 1:
                        print("[" + queries_sels_file + "] has " + str(len(row)) + " columns, does NOT fit given "
                                                                                   "dimension " + str(dimension))
                        exit(0)
                    for sel in range(1, sel_combinations + 1):
                        query_sels["sel_" + str(sel)] = float(row[sel])
                    queries_sels.append(query_sels)
        else:
            print("[" + queries_sels_file + "] does NOT exist! Exit!")
            exit(0)
        return queries_sels

    # load list of sel_query_files into memory
    @staticmethod
    def load_queries_sels_files(dimension, queries_sels_files):
        sel_combinations = 2 ** dimension - 1
        samples_queries_sels = []
        for queries_sels_file in queries_sels_files:
            queries_sels = []
            if os.path.isfile(queries_sels_file):
                with open(queries_sels_file, "r") as csv_in:
                    csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                    for row in csv_reader:
                        query_sels = {"id": int(row[0])}
                        # number of columns of the CSV file should be sel_combinations + 1: [id, 1, ..., sel_combinations]
                        if len(row) != sel_combinations + 1:
                            print("[" + queries_sels_file + "] has " + str(len(row)) + " columns, does NOT fit given "
                                                                                      "dimension " + str(dimension))
                            exit(0)
                        for sel in range(1, sel_combinations + 1):
                            query_sels["sel_" + str(sel)] = float(row[sel])
                        queries_sels.append(query_sels)
                print("[" + queries_sels_file + "] loaded into memory.")
            else:
                print("[" + queries_sels_file + "] does NOT exist! Exit!")
                exit(0)
            samples_queries_sels.append(queries_sels)
        return samples_queries_sels

    # load samples_sel_queries_costs into memory
    @staticmethod
    def load_sel_queries_costs_file(dimension, sel_queries_costs_file):
        sel_combinations = 2 ** dimension - 1
        samples_sel_queries_costs = []
        if os.path.isfile(sel_queries_costs_file):
            with open(sel_queries_costs_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                next(csv_reader, None)  # skip header
                for row in csv_reader:
                    sel_queries_costs = []
                    # number of columns of the CSV file should be sel_combinations + 1: [sample_size, 1, ..., sel_combinations]
                    if len(row) != sel_combinations + 1:
                        print("[" + sel_queries_costs_file + "] has " + str(len(row)) + " columns, does NOT fit given "
                                                                                        "dimension " + str(dimension))
                        exit(0)
                    for sel in range(1, sel_combinations + 1):
                        sel_queries_costs.append(float(row[sel]))
                    samples_sel_queries_costs.append(sel_queries_costs)
            print("[" + sel_queries_costs_file + "] loaded into memory.")
        else:
            print("[" + sel_queries_costs_file + "] does NOT exist! Exit!")
            exit(0)
        return samples_sel_queries_costs

    @staticmethod
    def dump_train_traces(traces, trace_file):
        output_file = trace_file + ".csv"
        with open(output_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            # write header
            csv_writer.writerow(["iteration", "win_rate"])
            for trace in traces:
                row = [trace[0], trace[1]]
                csv_writer.writerow(row)

    # select queries that are possible to be viable
    @staticmethod
    def select_possible_queries(dimension, labeled_queries, time_budget, num_of_joins=1):
        num_of_plans = Util.num_of_plans(dimension, num_of_joins)
        # compute map of [id -> time_best]
        map_queries = []
        for query in labeled_queries:
            plan_times = []
            for plan in range(1, num_of_plans + 1):
                plan_times.append(query["time_" + str(plan)])
            time_best = min(plan_times)
            map_query = {"id": query["id"], "time_best": time_best}
            map_queries.append(map_query)

        # select possible viable queries
        selected_map_queries = [map_query for map_query in map_queries
                                if map_query["time_best"] <= time_budget]

        # join labeled_queries with selected_query_ids to filter the selected_queries
        selected_query_ids = set([map_query["id"] for map_query in selected_map_queries])
        selected_queries = [query for query in labeled_queries if query["id"] in selected_query_ids]

        return selected_queries

    # select queries based on how many good plans of the query meets time_budget
    @staticmethod
    def select_good_plans_queries(dimension, labeled_queries, time_budget, good_plans, num_of_joins=1):
        num_of_plans = Util.num_of_plans(dimension, num_of_joins)
        # compute map of [id -> # of good_plans]
        map_queries = []
        for query in labeled_queries:
            plan_times = []
            for plan in range(1, num_of_plans + 1):
                plan_times.append(query["time_" + str(plan)])
            number_of_good_plans = 0
            for plan_time in plan_times:
                if plan_time <= time_budget:
                    number_of_good_plans += 1
            map_query = {"id": query["id"], "number_of_good_plans": number_of_good_plans}
            map_queries.append(map_query)
        # select by hardness_ratio
        selected_map_queries = [map_query for map_query in map_queries
                                if map_query["number_of_good_plans"] == good_plans]

        # join labeled_queries with selected_query_ids to filter the selected_queries
        selected_query_ids = set([map_query["id"] for map_query in selected_map_queries])
        selected_queries = [query for query in labeled_queries if query["id"] in selected_query_ids]

        return selected_queries

    # count good queries with given time_budget for "basic approach"
    # @param - labeled_queries: [list of query objects], each query object being
    #          {id, time_0, time_1, ..., time_(2**d-1)}
    # @param - time_budget: float
    #
    # @return - count: int
    @staticmethod
    def count_good_queries_basic_approach(labeled_queries, time_budget, dimension, num_of_joins=1):
        num_of_plans = Util.num_of_plans(dimension, num_of_joins)
        count = 0
        for query in labeled_queries:
            if query["time_0"] <= time_budget:
                # handle the fluctuation error of time labeling
                #   time_0 can not be smaller than the best plan of all plans, 
                #     since original query plan must be one of the hinted plans
                all_plans_query_times = []
                for plan in range(1, num_of_plans + 1):
                    all_plans_query_times.append(query["time_" + str(plan)])
                best_plan_query_time = min(all_plans_query_times)
                # skip the case of original query faster than the best plan query due to labeling errors.
                if query["time_0"] < best_plan_query_time:
                    continue
                count += 1
        return count

    # count good queries with given time_budget in evaluated_queries join labeled_queries for "mdp approach"
    # @param - labeled_queries: [list of query objects], each query object being
    #          {id, time_0, time_1, ..., time_(2**d-1)}
    # @param - evaluated_queries: [list of query objects], each query object being
    #          {id, planning_time, querying_time, total_time, win, plans_tried, reason}
    # @param - time_budget: float
    #
    # @return - count: int
    @staticmethod
    def count_good_queries_mdp_approach(labeled_queries, evaluated_queries, time_budget):
        # Build map <id, query> for evaluated_queries
        evaluated_queries_map = {}
        for query in evaluated_queries:
            evaluated_queries_map[query["id"]] = query
        # loop labeled_queries, count how many of them are viable in evaluated_queries_map
        count = 0
        for query in labeled_queries:
            id = query["id"]
            evaluated_query = evaluated_queries_map[id]
            if evaluated_query["total_time"] <= time_budget:
                count += 1
        return count

    # count good queries with given time_budget and unit_cost in labeled_queries for "max possible"
    @staticmethod
    def count_good_queries_max_possible(dimension, labeled_queries, unit_cost, time_budget, num_of_joins=1):
        num_of_plans = Util.num_of_plans(dimension, num_of_joins)
        count = 0
        for query in labeled_queries:
            # traverse all plans, compute the best plan that has minimum total_time (planning_time + querying_time)
            time_best = 100.0
            for plan in range(1, num_of_plans + 1):
                number_of_sels = Util.number_of_sels(plan)
                planning_time = number_of_sels * unit_cost
                querying_time = query["time_" + str(plan)]
                total_time = planning_time + querying_time
                if total_time < time_best:
                    time_best = total_time
            if time_best <= time_budget:
                count += 1
        return count
    
    # compute qualities of queries with given time_budget in evaluated_queries join labeled_queries for "mdp approach"
    # @param - labeled_queries: [list of query objects], each query object being
    #          {id, time_0, time_1, ..., time_(2**d-1)}
    # @param - evaluated_quality_queries: [list of query objects], each query object being
    #          {id, planning_time, querying_time, total_time, win, plans_tried, reason, quality}
    # @param - time_budget: float
    #
    # @return - (avg_quality_total, std_quality_total, avg_quality_delta, std_quality_delta)
    @staticmethod
    def qualities_of_queries_mdp_approach(labeled_queries, evaluated_quality_queries, time_budget):
        # ---- DEBUG -----
        #print("\n----------\n")
        # ---- DEBUG -----
        # Build map <id, query> for evaluated_quality_queries
        evaluated_quality_queries_map = {}
        for query in evaluated_quality_queries:
            evaluated_quality_queries_map[query["id"]] = query
        # compute avg qualities
        sum_qualities_total_labeled_queries = 0.0
        cnt_qualities_total_labeled_queries = 0
        sum_qualities_delta_labeled_queries = 0.0
        cnt_qualities_delta_labeled_queries = 0
        # loop labeled_queries
        for query in labeled_queries:
            id = query["id"]
            evaluated_quality_query = evaluated_quality_queries_map[id]
            # only count viable queries
            if evaluated_quality_query["win"] == 1:
                sum_qualities_total_labeled_queries += evaluated_quality_query["quality"]
                cnt_qualities_total_labeled_queries += 1
                sum_qualities_delta_labeled_queries += evaluated_quality_query["quality"]
                cnt_qualities_delta_labeled_queries += 1
                # # lossless queries
                # if evaluated_quality_query["plans_tried"].find('X') == -1:
                #     sum_qualities_total_labeled_queries += evaluated_quality_query["quality"]
                #     cnt_qualities_total_labeled_queries += 1
                # # lossy queries
                # else:
                #     # ---- DEBUG -----
                #     #print(id, evaluated_quality_query["quality"], sep=", ")
                #     # ---- DEBUG -----
                #     sum_qualities_total_labeled_queries += evaluated_quality_query["quality"]
                #     cnt_qualities_total_labeled_queries += 1
                #     sum_qualities_delta_labeled_queries += evaluated_quality_query["quality"]
                #     cnt_qualities_delta_labeled_queries += 1
            else:
                sum_qualities_total_labeled_queries += evaluated_quality_query["quality"]
                cnt_qualities_total_labeled_queries += 1
        if cnt_qualities_total_labeled_queries == 0:
            cnt_qualities_total_labeled_queries = 1
        if cnt_qualities_delta_labeled_queries == 0:
            cnt_qualities_delta_labeled_queries = 1
        avg_qualities_total_labeled_queries = sum_qualities_total_labeled_queries / cnt_qualities_total_labeled_queries
        avg_qualities_delta_labeled_queries = sum_qualities_delta_labeled_queries / cnt_qualities_delta_labeled_queries
        sum_sqr_err_qualities_total_labeled_queries = 0.0
        sum_sqr_err_qualities_delta_labeled_queries = 0.0
        # loop labeled_queries again:
        for query in labeled_queries:
            id = query["id"]
            evaluated_quality_query = evaluated_quality_queries_map[id]
            # only count viable queries
            if evaluated_quality_query["win"] == 1:
                sum_sqr_err_qualities_total_labeled_queries += (evaluated_quality_query["quality"] - avg_qualities_total_labeled_queries) ** 2
                sum_sqr_err_qualities_delta_labeled_queries += (evaluated_quality_query["quality"] - avg_qualities_delta_labeled_queries) ** 2
                # # lossless queries
                # if evaluated_quality_query["plans_tried"].find('X') == -1:
                #     sum_sqr_err_qualities_total_labeled_queries += (evaluated_quality_query["quality"] - avg_qualities_total_labeled_queries) ** 2
                # # lossy queries
                # else:
                #     sum_sqr_err_qualities_total_labeled_queries += (evaluated_quality_query["quality"] - avg_qualities_total_labeled_queries) ** 2
                #     sum_sqr_err_qualities_delta_labeled_queries += (evaluated_quality_query["quality"] - avg_qualities_delta_labeled_queries) ** 2
            else:
                sum_sqr_err_qualities_total_labeled_queries += (evaluated_quality_query["quality"] - avg_qualities_total_labeled_queries) ** 2
        std_qualities_total_labeled_queries = math.sqrt(sum_sqr_err_qualities_total_labeled_queries / cnt_qualities_total_labeled_queries)
        std_qualities_delta_labeled_queries = math.sqrt(sum_sqr_err_qualities_delta_labeled_queries / cnt_qualities_delta_labeled_queries)
        return (avg_qualities_total_labeled_queries, 
               std_qualities_total_labeled_queries,
               avg_qualities_delta_labeled_queries,
               std_qualities_delta_labeled_queries)
    
    # compute qualities result of queries with given time_budget in evaluated_queries join labeled_queries for "mdp approach"
    # @param - labeled_queries: [list of query objects], each query object being
    #          {id, time_0, time_1, ..., time_(2**d-1)}
    # @param - evaluated_quality_queries: [list of query objects], each query object being
    #          {id, planning_time, querying_time, total_time, win, plans_tried, reason, quality}
    # @param - time_budget: float
    #
    # @return - [a list of [query_id,  quality,  sampling_plan_id]]
    @staticmethod
    def qualities_result_of_queries_mdp_approach(labeled_queries, evaluated_quality_queries, time_budget):
        # Build map <id, query> for evaluated_quality_queries
        evaluated_quality_queries_map = {}
        for query in evaluated_quality_queries:
            evaluated_quality_queries_map[query["id"]] = query
        qualities_result = []
        # loop labeled_queries
        for query in labeled_queries:
            id = query["id"]
            evaluated_quality_query = evaluated_quality_queries_map[id]
            # only count viable queries
            if evaluated_quality_query["win"] == 1:
                # only count sampling queries
                if evaluated_quality_query["plans_tried"].find('X') != -1:
                    # get the sampling_plan_id
                    sampling_plan_id = int(evaluated_quality_query["plans_tried"].split('_')[-1])
                    qualities_result.append([id, evaluated_quality_query["quality"], sampling_plan_id])
        return qualities_result
    
    # dump evaluated queries out to file
    @staticmethod
    def dump_evaluated_queries_file(out_file, evaluated_queries, has_quality=False):
        with open(out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query in evaluated_queries:
                if has_quality:
                    csv_writer.writerow([
                        query["id"],
                        query["planning_time"],
                        query["querying_time"],
                        query["total_time"],
                        query["win"],
                        query["plans_tried"],
                        query["reason"],
                        query["quality"]
                    ])
                else:
                    csv_writer.writerow([
                        query["id"],
                        query["planning_time"],
                        query["querying_time"],
                        query["total_time"],
                        query["win"],
                        query["plans_tried"],
                        query["reason"]
                    ])

    # load evaluated_file into memory
    @staticmethod
    def load_evaluated_queries_file(evaluated_queries_file, has_quality=False):
        evaluated_queries = []
        if os.path.isfile(evaluated_queries_file):
            with open(evaluated_queries_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    if has_quality:
                        evaluated_query = {
                            "id": int(row[0]),
                            "planning_time": float(row[1]),
                            "querying_time": float(row[2]),
                            "total_time": float(row[3]),
                            "win": int(row[4]),
                            "plans_tried": row[5],
                            "reason": row[6],
                            "quality": float(row[7])
                        }
                    else:
                        evaluated_query = {
                            "id": int(row[0]),
                            "planning_time": float(row[1]),
                            "querying_time": float(row[2]),
                            "total_time": float(row[3]),
                            "win": int(row[4]),
                            "plans_tried": row[5],
                            "reason": row[6]
                        }
                    evaluated_queries.append(evaluated_query)
        else:
            print("[" + evaluated_queries_file + "] does NOT exist! Exit!")
            exit(0)
        return evaluated_queries

    # load list of evaluated_files into memory
    @staticmethod
    def load_evaluated_queries_files(list_evaluated_queries_file, has_quality=False):
        samples_evaluated_queries = []
        for evaluated_queries_file in list_evaluated_queries_file:
            evaluated_queries = []
            if os.path.isfile(evaluated_queries_file):
                with open(evaluated_queries_file, "r") as csv_in:
                    csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                    for row in csv_reader:
                        if has_quality:
                            evaluated_query = {
                                "id": int(row[0]),
                                "planning_time": float(row[1]),
                                "querying_time": float(row[2]),
                                "total_time": float(row[3]),
                                "win": int(row[4]),
                                "plans_tried": row[5],
                                "reason": row[6],
                                "quality": float(row[7])
                            }
                        else:
                            evaluated_query = {
                                "id": int(row[0]),
                                "planning_time": float(row[1]),
                                "querying_time": float(row[2]),
                                "total_time": float(row[3]),
                                "win": int(row[4]),
                                "plans_tried": row[5],
                                "reason": row[6]
                            }
                        evaluated_queries.append(evaluated_query)
            else:
                print("[" + evaluated_queries_file + "] does NOT exist! Exit!")
                exit(0)
            samples_evaluated_queries.append(evaluated_queries)
        return samples_evaluated_queries

    # dump features_queries into file
    @staticmethod
    def dump_queries_features(dimension, out_file, queries_features):
        with open(out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query in queries_features:
                row = [query["id"]]
                for d in range(1, dimension + 1):
                    row.append(query["feature_" + str(d)])
                csv_writer.writerow(row)

    # load features_queries_file into memory
    @staticmethod
    def load_queries_features_file(dimension, queries_features_file):
        queries_features = []
        if os.path.isfile(queries_features_file):
            with open(queries_features_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    query_features = {
                        "id": int(row[0])
                    }
                    for d in range(1, dimension + 1):
                        query_features["feature_" + str(d)] = float(row[d])
                    queries_features.append(query_features)
        else:
            print("[" + queries_features_file + "] does NOT exist! Exit!")
            exit(0)
        return queries_features
    
    # dump queries errors out to file
    @staticmethod
    def dump_queries_errors_file(dimension, out_file, queries_errors, num_of_joins=1):
        num_of_plans = Util.num_of_plans(dimension, num_of_joins)
        with open(out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query_errors in queries_errors:
                row = [query_errors["id"]]
                for plan_id in range(1, num_of_plans + 1):
                    row.append(query_errors["error_" + str(plan_id)])
                csv_writer.writerow(row)
    
    # check if result file exists
    @staticmethod
    def exist_query_result(_result_path, _query_id, _hint_id, _sample_ratio_id):
        result_file = _result_path
        if _result_path.endswith("/"):
            result_file = result_file + "result_" +  str(_query_id)
        else:
            result_file = result_file + "/result_" + str(_query_id)
        
        if _hint_id >= 0:
            result_file = result_file + "_h" + str(_hint_id)
        
        if _sample_ratio_id >= 0:
            result_file = result_file + "_s" + str(_sample_ratio_id)
        
        result_file = result_file + ".csv"
        if os.path.isfile(result_file): 
            return True
        return False

    # load query result from file under given path
    @staticmethod
    def load_query_result(_result_path, _query_id, _hint_id, _sample_ratio_id):
        result_file = _result_path
        if _result_path.endswith("/"):
            result_file = result_file + "result_" +  str(_query_id)
        else:
            result_file = result_file + "/result_" + str(_query_id)
        
        if _hint_id >= 0:
            result_file = result_file + "_h" + str(_hint_id)
        
        if _sample_ratio_id >= 0:
            result_file = result_file + "_s" + str(_sample_ratio_id)
        
        result_file = result_file + ".csv"
        result = []
        if os.path.isfile(result_file): 
            # if result is timeout, return []
            with open(result_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                first_row = next(csv_reader)
                if first_row[0] == "timeout":
                    return result
            # otherwise, parse the record in each row
            with open(result_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    record = {
                        "id": row[0],
                        "coordinate[0]": float(row[1]),
                        "coordinate[1]": float(row[2])
                    }
                    result.append(record)
        else:
            print("[" + result_file + "] does NOT exist! Exit!")
            exit(0)
        return result

    # dump query result out to file under given path
    @staticmethod
    def dump_query_result(_result_path, _query_id, _hint_id, _sample_ratio_id, _result):
        result_file = _result_path
        if _result_path.endswith("/"):
            result_file = result_file + "result_" +  str(_query_id)
        else:
            result_file = result_file + "/result_" + str(_query_id)
        
        if _hint_id >= 0:
            result_file = result_file + "_h" + str(_hint_id)
        
        if _sample_ratio_id >= 0:
            result_file = result_file + "_s" + str(_sample_ratio_id)
        
        result_file = result_file + ".csv"
        with open(result_file, "w") as csv_out:
                csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                csv_writer.writerows(_result)
    
    # load query result from given file
    @staticmethod
    def load_query_result_file(result_file):
        result = []
        if os.path.isfile(result_file): 
            # if result is timeout, return []
            with open(result_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                first_row = next(csv_reader)
                if first_row[0] == "timeout":
                    return result
            # otherwise, parse the record in each row
            with open(result_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    record = {
                        "id": row[0],
                        "coordinate[0]": float(row[1]),
                        "coordinate[1]": float(row[2])
                    }
                    result.append(record)
        else:
            print("[" + result_file + "] does NOT exist! Exit!")
            exit(0)
        return result

    # dump labeled sample queries out to file
    @staticmethod
    def dump_labeled_sample_queries_file(dimension, num_of_sample_ratios, out_file, labeled_sample_queries):
        num_of_sampling_plans = Util.num_of_sampling_plans(dimension, num_of_sample_ratios)
        with open(out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query in labeled_sample_queries:
                row = [query["id"]]
                for plan_id in range(0, num_of_sampling_plans):
                    row.append(query["time_" + str(plan_id)])
                csv_writer.writerow(row)
    
    # load labeled sample queries into memory
    @staticmethod
    def load_labeled_sample_queries_file(dimension, num_of_sample_ratios, labeled_sample_queries_file):
        num_of_sampling_plans = Util.num_of_sampling_plans(dimension, num_of_sample_ratios)
        labeled_sample_queries = []
        if os.path.isfile(labeled_sample_queries_file):
            with open(labeled_sample_queries_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    query = {"id": int(row[0])}
                    for plan_id in range(0, num_of_sampling_plans):
                        query["time_" + str(plan_id)] = float(row[1 + plan_id])
                    labeled_sample_queries.append(query)
        else:
            print("[" + labeled_sample_queries_file + "] does NOT exist! Exit!")
            exit(0)
        return labeled_sample_queries
    
    # dump labeled std sample queries out to file
    @staticmethod
    def dump_labeled_std_sample_queries_file(dimension, num_of_sample_ratios, out_file, labeled_sample_queries):
        num_of_sampling_plans = Util.num_of_sampling_plans(dimension, num_of_sample_ratios)
        with open(out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query in labeled_sample_queries:
                row = [query["id"]]
                for plan_id in range(0, num_of_sampling_plans):
                    row.append(query["time_" + str(plan_id) + "_std"])
                csv_writer.writerow(row)
    
    # compute Jaccard similarity value between original_query_result and query_result
    @staticmethod
    def jaccard_similarity(original_query_result, query_result):
        subset_distinct_coordinates = set()
        for record in query_result:
            x = record["coordinate[0]"]
            y = record["coordinate[1]"]
            subset_distinct_coordinates.add((x, y))
        superset_distinct_coordinates = set()
        for record in original_query_result:
            x = record["coordinate[0]"]
            y = record["coordinate[1]"]
            superset_distinct_coordinates.add((x, y))
        return len(subset_distinct_coordinates) / len(superset_distinct_coordinates)
    
    # load sample queries qualities into memory
    @staticmethod
    def load_sample_queries_qualities_file(dimension, num_of_sample_ratios, sample_queries_qualities_file):
        num_of_sampling_plans = Util.num_of_sampling_plans(dimension, num_of_sample_ratios)
        sample_queries_qualities = []
        if os.path.isfile(sample_queries_qualities_file):
            with open(sample_queries_qualities_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    query = {"id": int(row[0])}
                    for plan_id in range(0, num_of_sampling_plans):
                        query["quality_" + str(plan_id)] = float(row[1 + plan_id])
                    sample_queries_qualities.append(query)
        else:
            print("[" + sample_queries_qualities_file + "] does NOT exist! Exit!")
            exit(0)
        return sample_queries_qualities

    # dump sample queries qualities out to file
    @staticmethod
    def dump_sample_queries_qualities_file(dimension, num_of_sample_ratios, out_file, sample_queries_qualities):
        num_of_sampling_plans = Util.num_of_sampling_plans(dimension, num_of_sample_ratios)
        with open(out_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query in sample_queries_qualities:
                row = [query["id"]]
                for plan_id in range(0, num_of_sampling_plans):
                    row.append(query["quality_" + str(plan_id)])
                csv_writer.writerow(row)
    
    def reward(beta, time_budget, total_time, query_quality):
        return beta * (time_budget - total_time) / time_budget + (1.0 - beta) * query_quality 
