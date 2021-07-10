import csv
import os.path
import matplotlib.pyplot as plt
import numpy as np


class Util:

    # This function decomposes a integer (≥1) into a sum of several powers of 2.
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
    def sel_ids_of_plan(plan):
        sel_ids = Util.decompose_to_binary_numbers(plan)
        if plan not in sel_ids:
            sel_ids.append(plan)
        return sel_ids

    # return the number of selectivity values need to be collected to estimate query time of a given plan
    @staticmethod
    def number_of_sels(plan):
        if plan == 1 or plan == 2 or plan == 4:
            return 1
        elif plan == 3 or plan == 5 or plan == 6:
            return 3
        else:
            return 0

    # load queries (nyc) into memory
    @staticmethod
    def load_queries_file_nyc(queries_file):
        queries = []
        if os.path.isfile(queries_file):
            with open(queries_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    query = {"id": int(row[0]),
                             "start_time": row[1],
                             "end_time": row[2],
                             "trip_distance_start": row[3],
                             "trip_distance_end": row[4],
                             "lng0": row[5],
                             "lat0": row[6],
                             "lng1": row[7],
                             "lat1": row[8]
                             }
                    queries.append(query)
        else:
            print("[" + queries_file + "] does NOT exist! Exit!")
            exit(0)
        return queries

    # load labeled queries (nyc) into memory
    @staticmethod
    def load_labeled_queries_file_nyc(labeled_queries_file):
        labeled_queries = []
        if os.path.isfile(labeled_queries_file):
            with open(labeled_queries_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    query = {"id": int(row[0]),
                             "start_time": row[1],
                             "end_time": row[2],
                             "trip_distance_start": row[3],
                             "trip_distance_end": row[4],
                             "lng0": float(row[5]),
                             "lat0": float(row[6]),
                             "lng1": float(row[7]),
                             "lat1": float(row[8]),
                             "time_0": float(row[9]),
                             "time_1": float(row[10]),
                             "time_2": float(row[11]),
                             "time_3": float(row[12]),
                             "time_4": float(row[13]),
                             "time_5": float(row[14]),
                             "time_6": float(row[15]),
                             "time_7": float(row[16])
                             }
                    labeled_queries.append(query)
        else:
            print("[" + labeled_queries_file + "] does NOT exist! Exit!")
            exit(0)
        return labeled_queries

    # dump labeled queries (nyc) into file
    @staticmethod
    def dump_labeled_queries_file_nyc(labeled_queries, labeled_queries_file):
        with open(labeled_queries_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for query in labeled_queries:
                row = [query["id"],
                       query["start_time"],
                       query["end_time"],
                       query["trip_distance_start"],
                       query["trip_distance_end"],
                       query["lng0"],
                       query["lat0"],
                       query["lng1"],
                       query["lat1"]
                       ]
                for plan in range(0, 8):
                    row.append(query["time_" + str(plan)])
                csv_writer.writerow(row)

    @staticmethod
    def load_labeled_sel_queries_file_nyc(labeled_sel_queries_file):
        labeled_sel_queries = []
        if os.path.isfile(labeled_sel_queries_file):
            with open(labeled_sel_queries_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    labeled_sel_query = {
                        "id": int(row[0]),
                        "start_time": row[1],
                        "end_time": row[2],
                        "trip_distance_start": row[3],
                        "trip_distance_end": row[4],
                        "lng0": float(row[5]),
                        "lat0": float(row[6]),
                        "lng1": float(row[7]),
                        "lat1": float(row[8])
                    }
                    for fc in range(1, 8):
                        labeled_sel_query["time_sel_" + str(fc)] = float(row[8 + fc])
                    labeled_sel_queries.append(labeled_sel_query)
        else:
            print("[" + labeled_sel_queries_file + "] does NOT exist! Exit!")
            exit(0)
        return labeled_sel_queries

    # load list of labeled_sel_queries_files (nyc) into memory
    @staticmethod
    def load_labeled_sel_queries_files_nyc(labeled_sel_queries_files):
        samples_labeled_sel_queries = []
        for labeled_sel_queries_file in labeled_sel_queries_files:
            labeled_sel_queries = []
            if os.path.isfile(labeled_sel_queries_file):
                with open(labeled_sel_queries_file, "r") as csv_in:
                    csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                    for row in csv_reader:
                        labeled_sel_query = {
                            "id": int(row[0]),
                            "start_time": row[1],
                            "end_time": row[2],
                            "trip_distance_start": row[3],
                            "trip_distance_end": row[4],
                            "lng0": float(row[5]),
                            "lat0": float(row[6]),
                            "lng1": float(row[7]),
                            "lat1": float(row[8])
                        }
                        for fc in range(1, 8):
                            labeled_sel_query["time_sel_" + str(fc)] = float(row[8 + fc])
                        labeled_sel_queries.append(labeled_sel_query)
                print("[" + labeled_sel_queries_file + "] loaded into memory.")
            else:
                print("[" + labeled_sel_queries_file + "] does NOT exist! Exit!")
                exit(0)
            samples_labeled_sel_queries.append(labeled_sel_queries)
        return samples_labeled_sel_queries

    # load sel_queries_file (nyc) into memory
    @staticmethod
    def load_sel_queries_file_nyc(sel_queries_file):
        queries_sels = []
        if os.path.isfile(sel_queries_file):
            with open(sel_queries_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    query_sels = {
                        "id": int(row[0]),
                        "start_time": row[1],
                        "end_time": row[2],
                        "trip_distance_start": row[3],
                        "trip_distance_end": row[4],
                        "lng0": float(row[5]),
                        "lat0": float(row[6]),
                        "lng1": float(row[7]),
                        "lat1": float(row[8]),
                        "sel_1": float(row[9]),
                        "sel_2": float(row[10]),
                        "sel_3": float(row[11]),
                        "sel_4": float(row[12]),
                        "sel_5": float(row[13]),
                        "sel_6": float(row[14]),
                        "sel_7": float(row[15])
                    }
                    queries_sels.append(query_sels)
            print("[" + sel_queries_file + "] loaded into memory.")
        else:
            print("[" + sel_queries_file + "] does NOT exist! Exit!")
            exit(0)
        return queries_sels

    # load list of sel_queries_files (nyc) into memory
    @staticmethod
    def load_sel_queries_files_nyc(sel_queries_files):
        samples_queries_sels = []
        for sel_queries_file in sel_queries_files:
            queries_sels = []
            if os.path.isfile(sel_queries_file):
                with open(sel_queries_file, "r") as csv_in:
                    csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                    for row in csv_reader:
                        query_sels = {
                            "id": int(row[0]),
                            "start_time": row[1],
                            "end_time": row[2],
                            "trip_distance_start": row[3],
                            "trip_distance_end": row[4],
                            "lng0": float(row[5]),
                            "lat0": float(row[6]),
                            "lng1": float(row[7]),
                            "lat1": float(row[8]),
                            "sel_1": float(row[9]),
                            "sel_2": float(row[10]),
                            "sel_3": float(row[11]),
                            "sel_4": float(row[12]),
                            "sel_5": float(row[13]),
                            "sel_6": float(row[14]),
                            "sel_7": float(row[15])
                        }
                        queries_sels.append(query_sels)
                print("[" + sel_queries_file + "] loaded into memory.")
            else:
                print("[" + sel_queries_file + "] does NOT exist! Exit!")
                exit(0)
            samples_queries_sels.append(queries_sels)
        return samples_queries_sels

    # load samples_sel_queries_costs (nyc) into memory
    @staticmethod
    def load_sel_queries_costs_file_nyc(sel_queries_costs_file):
        samples_sel_queries_costs = []
        if os.path.isfile(sel_queries_costs_file):
            with open(sel_queries_costs_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                next(csv_reader, None)  # skip header
                for row in csv_reader:
                    sel_queries_costs = [
                        # row[0] is the sample size
                        float(row[1]),
                        float(row[2]),
                        float(row[3]),
                        float(row[4]),
                        float(row[5]),
                        float(row[6]),
                        float(row[7])
                    ]
                    samples_sel_queries_costs.append(sel_queries_costs)
            print("[" + sel_queries_costs_file + "] loaded into memory.")
        else:
            print("[" + sel_queries_costs_file + "] does NOT exist! Exit!")
            exit(0)
        return samples_sel_queries_costs

    # load evaluated_file into memory
    @staticmethod
    def load_evaluated_queries_file(evaluated_queries_file):
        evaluated_queries = []
        if os.path.isfile(evaluated_queries_file):
            with open(evaluated_queries_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    evaluated_query = {
                        "id": int(row[0]),
                        "keyword": row[1],
                        "planning_time": float(row[2]),
                        "querying_time": float(row[3]),
                        "total_time": float(row[4]),
                        "win": int(row[5]),
                        "plan_tried": row[6],
                        "reason": row[7]
                    }
                    evaluated_queries.append(evaluated_query)
        else:
            print("[" + evaluated_queries_file + "] does NOT exist! Exit!")
            exit(0)
        return evaluated_queries

    # load list of evaluated_files into memory
    @staticmethod
    def load_evaluated_queries_files(list_evaluated_queries_file):
        samples_evaluated_queries = []
        for evaluated_queries_file in list_evaluated_queries_file:
            evaluated_queries = []
            if os.path.isfile(evaluated_queries_file):
                with open(evaluated_queries_file, "r") as csv_in:
                    csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                    for row in csv_reader:
                        evaluated_query = {
                            "id": int(row[0]),
                            "keyword": row[1],
                            "planning_time": float(row[2]),
                            "querying_time": float(row[3]),
                            "total_time": float(row[4]),
                            "win": int(row[5]),
                            "plan_tried": row[6],
                            "reason": row[7]
                        }
                        evaluated_queries.append(evaluated_query)
            else:
                print("[" + evaluated_queries_file + "] does NOT exist! Exit!")
                exit(0)
            samples_evaluated_queries.append(evaluated_queries)
        return samples_evaluated_queries

    # load features_queries_file into memory
    @staticmethod
    def load_features_queries_file(features_queries_file):
        queries_features = []
        if os.path.isfile(features_queries_file):
            with open(features_queries_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                for row in csv_reader:
                    query_features = {
                        "id": int(row[0]),
                        "feature_1": float(row[1]),
                        "feature_2": float(row[2]),
                        "feature_3": float(row[3])
                    }
                    queries_features.append(query_features)
        else:
            print("[" + features_queries_file + "] does NOT exist! Exit!")
            exit(0)
        return queries_features

    # select queries (nyc) that are possible to be viable
    # @param - labeled_queries: [list of query objects], each query object being
    #          {id, ..., time_0, time_1, ..., time_7}
    # @param - time_budget: float
    #
    # @return - selected_queries: the same format as labeled_queries
    @staticmethod
    def select_possible_queries_3d(labeled_queries, time_budget):
        # compute map of [id -> time_best]
        map_queries = []
        for query in labeled_queries:
            plan_times = [
                query["time_1"],
                query["time_2"],
                query["time_3"],
                query["time_4"],
                query["time_5"],
                query["time_6"],
                query["time_7"]
            ]
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
    # @param - labeled_queries: [list of query objects], each query object being
    #          {id, keyword, start_time, end_time, lng0, lat0, lng1, lat1, time_0, time_1, ..., time_7}
    # @param - time_budget: float
    # @param - good_plans: select the queries that have # of good plans ≤ good_plans
    #
    # @return - selected_queries: the same format as labeled_queries
    @staticmethod
    def select_good_plans_queries_3d(labeled_queries, time_budget, good_plans):
        # compute map of [id -> # of good_plans]
        map_queries = []
        for query in labeled_queries:
            plan_times = [
                query["time_1"],
                query["time_2"],
                query["time_3"],
                query["time_4"],
                query["time_5"],
                query["time_6"],
                query["time_7"]
            ]
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
    #          {id, keyword, start_time, end_time, lng0, lat0, lng1, lat1, time_0, time_1, ..., time_7}
    # @param - time_budget: float
    #
    # @return - count: int
    @staticmethod
    def count_good_queries_basic_approach(labeled_queries, time_budget):
        count = 0
        for query in labeled_queries:
            if query["time_0"] <= time_budget:
                count += 1
        return count

    # count good queries with given time_budget in evaluated_queries join labeled_queries for "mdp approach"
    # @param - labeled_queries: [list of query objects], each query object being
    #          {id, keyword, start_time, end_time, lng0, lat0, lng1, lat1, time_0, time_1, ..., time_7}
    # @param - evaluated_queries: [list of query objects], each query object being
    #          {id, keyword, planning_time, querying_time, total_time, win, plan_tried}
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
    def count_good_queries_max_possible_3d(labeled_queries, unit_cost, time_budget):
        count = 0
        for query in labeled_queries:
            # TODO - currently for PostgreSQL, we have 6 possible plans
            # traverse all plans, compute the best plan that has minimum total_time (planning_time + querying_time)
            time_best = 100.0
            for plan in range(1, 8):
                number_of_sels = Util.number_of_sels(plan)
                planning_time = number_of_sels * unit_cost
                querying_time = query["time_" + str(plan)]
                total_time = planning_time + querying_time
                if total_time < time_best:
                    time_best = total_time
            if time_best <= time_budget:
                count += 1
        return count

    # check if the query is possible to be viable with given time_budget and unit_cost
    @staticmethod
    def viable_3d(query, time_budget):
        # traverse all plans, compute the best plan that has minimum total_time (planning_time + querying_time)
        time_best = 100.0
        for plan in range(1, 8):
            querying_time = query["time_" + str(plan)]
            total_time = querying_time
            if total_time < time_best:
                time_best = total_time
        if time_best <= time_budget:
            return True
        return False

    @staticmethod
    def load_learning_curves(input_file):
        train_sizes = []
        train_scores_mean = []
        train_scores_std = []
        train_scores_median = []
        train_scores_min = []
        train_scores_max = []
        test_scores_mean = []
        test_scores_std = []
        test_scores_median = []
        test_scores_min = []
        test_scores_max = []
        fit_times_mean = []
        fit_times_std = []
        fit_times_median = []
        fit_times_min = []
        fit_times_max = []
        if os.path.isfile(input_file):
            with open(input_file, "r") as csv_in:
                csv_reader = csv.reader(csv_in, delimiter=',', quotechar='"')
                next(csv_reader, None)  # skip header
                for row in csv_reader:
                    train_sizes.append(float(row[0]))
                    train_scores_mean.append(float(row[1]))
                    train_scores_std.append(float(row[2]))
                    train_scores_median.append(float(row[3]))
                    train_scores_min.append(float(row[4]))
                    train_scores_max.append(float(row[5]))
                    test_scores_mean.append(float(row[6]))
                    test_scores_std.append(float(row[7]))
                    test_scores_median.append(float(row[8]))
                    test_scores_min.append(float(row[9]))
                    test_scores_max.append(float(row[10]))
                    fit_times_mean.append(float(row[11]))
                    fit_times_std.append(float(row[12]))
                    fit_times_median.append(float(row[13]))
                    fit_times_min.append(float(row[14]))
                    fit_times_max.append(float(row[15]))
        else:
            print("[" + input_file + "] does NOT exist! Exit!")
            exit(0)
        return train_sizes, \
               train_scores_mean, \
               train_scores_std, \
               train_scores_median, \
               train_scores_min, \
               train_scores_max, \
               test_scores_mean, \
               test_scores_std, \
               test_scores_median, \
               test_scores_min, \
               test_scores_max, \
               fit_times_mean, \
               fit_times_std, \
               fit_times_median, \
               fit_times_min, \
               fit_times_max

    @staticmethod
    def dump_learning_curves(train_sizes,
                             train_scores_mean,
                             train_scores_std,
                             train_scores_median,
                             train_scores_min,
                             train_scores_max,
                             test_scores_mean,
                             test_scores_std,
                             test_scores_median,
                             test_scores_min,
                             test_scores_max,
                             fit_times_mean,
                             fit_times_std,
                             fit_times_median,
                             fit_times_min,
                             fit_times_max,
                             output_file):
        with open(output_file, "w") as csv_out:
            csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            # write header
            csv_writer.writerow(["train_sizes",
                                 "train_scores_mean",
                                 "train_scores_std",
                                 "train_scores_median",
                                 "train_scores_min",
                                 "train_scores_max",
                                 "test_scores_mean",
                                 "test_scores_std",
                                 "test_scores_median",
                                 "test_scores_min",
                                 "test_scores_max",
                                 "fit_times_mean",
                                 "fit_times_std",
                                 "fit_times_median",
                                 "fit_times_min",
                                 "fit_times_max"
                                 ])
            for i in range(len(train_sizes)):
                row = [train_sizes[i],
                       train_scores_mean[i],
                       train_scores_std[i],
                       train_scores_median[i],
                       train_scores_min[i],
                       train_scores_max[i],
                       test_scores_mean[i],
                       test_scores_std[i],
                       test_scores_median[i],
                       test_scores_min[i],
                       test_scores_max[i],
                       fit_times_mean[i],
                       fit_times_std[i],
                       fit_times_median[i],
                       fit_times_min[i],
                       fit_times_max[i]
                       ]
                csv_writer.writerow(row)
        print("wrote to csv file ", output_file, ".")

    @staticmethod
    def draw_learning_curves(train_sizes,
                             train_scores_mean,
                             train_scores_std,
                             test_scores_mean,
                             test_scores_std,
                             fit_times_mean,
                             fit_times_std,
                             save_file,
                             show_figure=False):
        # draw learning curves
        train_sizes = np.array(train_sizes)
        train_scores_mean = np.array(train_scores_mean)
        train_scores_std = np.array(train_scores_std)
        test_scores_mean = np.array(test_scores_mean)
        test_scores_std = np.array(test_scores_std)

        fig, ax = plt.subplots()

        # plot learning curve
        ax.set_title("Learning Curves")
        ax.set_xlabel("Training size")
        ax.set_ylabel("Score")
        ax.set_ylim(ymin=0)
        ax.grid()
        ax.fill_between(train_sizes, train_scores_mean - train_scores_std,
                             train_scores_mean + train_scores_std, alpha=0.1,
                             color="r")
        ax.fill_between(train_sizes, test_scores_mean - test_scores_std,
                             test_scores_mean + test_scores_std, alpha=0.1,
                             color="g")
        ax.plot(train_sizes, train_scores_mean, 'o-', color="r",
                     label="Training score")
        ax.plot(train_sizes, test_scores_mean, 'o-', color="g",
                     label="Validation score")
        ax.legend(loc="best")

        plt.savefig(save_file)
        if show_figure:
            plt.show()
