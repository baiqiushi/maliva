####################
#    Config
####################


class PostgresConf:
    hostname = "localhost"
    username = "postgres"
    password = "postgres"
    timeout = 4000  # 4 seconds


class NYC:
    database = "nyc"
    table = "nyc_15m"
    table_size = 14863778
    index_on_pickup_datetime = "idx_nyc_15m_pickup_datetime"
    index_on_trip_distance = "idx_nyc_15m_trip_distance"
    index_on_pickup_coordinates = "idx_nyc_15m_pickup_coordinates"
    min_pickup_datetime = "2010-01-01 00:00:00"
    max_pickup_datetime = "2010-01-31 23:59:59"
    max_pickup_datetime_zoom = 14  # (2010-01-31 - 2010-01-01) = 31 days = 8928 (x5 mins), log2(8928) = 13.12
    min_trip_distance = 0
    max_trip_distance = 50
    max_trip_distance_zoom = 9  # (50 - 0) * 10 = 500 (x0.1 mi), log2(500) = 8.96
    min_pickup_coordinates_lng = -74.35
    min_pickup_coordinates_lat = 40.45
    max_pickup_coordinates_lng = -73.65
    max_pickup_coordinates_lat = 40.95
    max_pickup_coordinates_zoom = 11


class MDP:
    unit_cost_reward = -1
    win_reward = 100.0
    lose_reward = -100.0


class QueryTimeEstimatorConf:
    timeout = 4  # 4 seconds, should be the same as timeout in DB conf

