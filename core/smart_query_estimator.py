from config import QueryTimeEstimatorConfig as conf
from smart_util import Util
import numpy as np
import pickle
from sklearn.linear_model import LinearRegression


###########################################################
#  Query_Estimator
#
# Description:
#   Estimate query time on different query plans:
#     using/not using index on [d1, d2, ...] dimensions
#     in combination order of x bits decimal: (1 - using / 0 - not using),
#       00000, 00001, 00010, 00011, ..., 11111
#     example:
#       10100 - using d1 and d3 indexes, not using d2, d4 or d5 index
# Implementation:
#   We train one dedicated linear regression model for each of the plans [1 ~ 2**d-1].
#   The features for each plan is different.
#   Example 1: the features for plan 00100 are the only selectivity on the d3 dimension, i.e. sel(d3).
#   Example 2: the features for plan 11000 are the 3 features: sel(d1), sel(d2) and sel(d1 & d2).
#   Example 3: the features for plan 11010 are the 4 features:
#                sel(d1), sel(d2), sel(d4) and sel(d1 & d2 & d4).
#
###########################################################
class Query_Estimator:
    def __init__(self, dimension, num_of_joins=1):
        self.dimension = dimension
        self.num_of_plans = Util.num_of_plans(dimension, num_of_joins)
        self.models = {}
        for plan in range(1, self.num_of_plans + 1):
            model = LinearRegression()
            self.models[plan] = model

    # fit model for plan
    # @prame _plan - int, the plan id of the model
    # @param _x - numpy 2d array, each row is a vector of features for one query,
    #             different plan ids could have different number of columns.
    # @param _y - numpy 2d array, each row has only the target value for the corresponding query
    def fit(self, _plan, _x, _y):
        model = self.models[_plan]
        # remove those data points that are no less than the timeout cut
        filter_index = _y[:, 0] < conf.timeout
        if len(_x[filter_index]) < 1:
            print(_x.shape)
            print(_y.shape)
            model.fit(_x, _y)
            return
        _x = _x[filter_index]
        _y = _y[filter_index]
        print(_x.shape)
        print(_y.shape)
        model.fit(_x, _y)

    # save all models to files under the given path
    def save(self, path):
        # trim the last '/'
        if path.endswith('/'):
            path = path[:-1]
        for plan in range(1, self.num_of_plans + 1):
            model = self.models[plan]
            filename = "query_estimator_plan_" + str(plan) + ".model"
            model_file = path + "/" + filename
            pickle.dump(model, open(model_file, "wb"))

    # load all models from files under the given path
    def load(self, path):
        # trim the last '/'
        if path.endswith('/'):
            path = path[:-1]
        for plan in range(1, self.num_of_plans + 1):
            filename = "query_estimator_plan_" + str(plan) + ".model"
            model_file = path + "/" + filename
            self.models[plan] = pickle.load(open(model_file, "rb"))

    # predict time for plan
    # @param _plan - int, the plan id of the model
    # @param _x - numpy 2d array, each row is a vector of features for one query,
    #             different plan ids could have different number of columns.
    # @return y - numpy 2d array, each row has only the predicted value for the corresponding query
    def predict(self, _plan, _x, mode="application"):
        model = self.models[_plan]
        y = model.predict(_x)
        # cap predicted time to be the timeout cut when in analyze mode
        if mode == "analyze":
            y = np.clip(y, a_min=0.0, a_max=conf.timeout)
        return y.astype(np.float32)

