####################
#    Config
####################
from twitter import Twitter
from twitter_join import TwitterJoin
from twitter_join2 import TwitterJoin2
from smart_featurizer_twitter import TwitterFeaturizer
from nyc import NYC
from tpch import TPCH
from smart_featurizer_nyc import NYCFeaturizer


class PostgreSQLConfig:
    hostname = "localhost"
    username = "postgres"
    password = "maliva"
    timeout = 4000  # 4 seconds


class MDP:
    unit_cost_reward = -1
    win_reward = 100.0
    lose_reward = -100.0
    beta = 0.5


class QueryTimeEstimatorConfig:
    timeout = 4  # 4 seconds, should be the same as timeout in DB conf


database_configs = {"postgresql": PostgreSQLConfig}
datasets = {"twitter": Twitter, "nyc": NYC, "tpch": TPCH, "twitter_join": TwitterJoin, "twitter_join2": TwitterJoin2}
featurizers = {"twitter": TwitterFeaturizer, "nyc": NYCFeaturizer}
