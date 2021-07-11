from config import QueryTimeEstimatorConf as conf
import numpy as np
import pickle
from sklearn.linear_model import LinearRegression


###########################################################
#  Query_Estimator
#
# Description:
#   Estimate query time on different query plans:
#     using/not using index on [A, B, C] dimensions
#     in combination order of 3 bits decimal: (1 - using / 0 - not using),
#       000, 001, 010, ..., 111
#     example:
#       101 - using A and C indexes, not using B index
# Implementation:
#   We train one dedicated linear regression model for each of the plans (1 ~ 7).
#   The features for each plan is different.
#   Example 1: the features for plan 001 are the only selectivity on the C dimension, i.e. sel(C).
#   Example 2: the features for plan 011 are the 3 features: sel(B), sel(C) and sel(B & C).
#   Example 3: the features for plan 111 are the 4 features:
#                sel(A), sel(B), sel(C) and sel(A & B & C).
#
###########################################################
class Query_Estimator:
    def __init__(self):
        self.models = {}
        for plan in range(1, 8):
            model = LinearRegression()
            self.models[plan] = model

    # fit model for plan
    # @prame _plan - int, the plan id of the model
    # @param _x - numpy 2d array, each row is a vector of features for one query,
    #             different plan ids could have different number of columns.
    # @param _y - numpy 2d array, each row has only the target value for the corresponding query
    def fit(self, _plan, _x, _y):
        # find data points that are under the timeout cut
        filter_index = _y[:, 0] < conf.timeout
        _x1 = _x[filter_index]
        _y1 = _y[filter_index]
        print(_x1.shape)
        print(_y1.shape)
        model = self.models[_plan]
        # not enough data points under timeout cut
        if _x1.shape[0] < 2:
            # use all data points for training
            model.fit(_x, _y)
        else:
            # otherwise, use data points under timeout cut
            model.fit(_x1, _y1)

    # save all models to files under the given path
    def save(self, path):
        # trim the last '/'
        if path.endswith('/'):
            path = path[:-1]
        for plan in range(1, 8):
            model = self.models[plan]
            filename = "query_estimator_plan_" + str(plan) + ".model"
            model_file = path + "/" + filename
            pickle.dump(model, open(model_file, "wb"))

    # load all models from files under the given path
    def load(self, path):
        # trim the last '/'
        if path.endswith('/'):
            path = path[:-1]
        for plan in range(1, 8):
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

