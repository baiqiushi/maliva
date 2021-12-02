import psycopg2
import socket
import struct
import time
import json
from smart_util import Util


BAO_HOST = "localhost"
BAO_PORT = 9381
START_QUERY_MESSAGE = "{\"type\": \"query\"}\n"
START_FEEDBACK_MESSAGE = "{\"type\": \"reward\"}\n";
START_PREDICTION_MESSAGE = "{\"type\": \"predict\"}\n";
TERMINAL_MESSAGE = "{\"final\": true}\n"


class Bao:

    def __init__(self, _database_config, _dataset, dimension=3, num_of_joins=1):
        self.config = {
            "host": _database_config.hostname,
            "user": _database_config.username,
            "password": _database_config.password,
            "database": _dataset.database,
        }
        # init PG connection
        self.conn = psycopg2.connect(**self.config)
        self.cursor = self.conn.cursor()
        self.dataset = _dataset
        self.dimension = dimension
        self.num_of_joins = num_of_joins
        self.num_of_arms = Util.num_of_plans(dimension, num_of_joins) + 1  # +1 for the no hint at all arm

        self.buffer_json = {self.dataset.table: 1}
        for index in self.dataset.indexes:
            self.buffer_json[index] = 1

    def plan_query(self, sql):

        # Create a socket (SOCK_STREAM means a TCP socket) to bao_server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to server and send data
            sock.connect((BAO_HOST, BAO_PORT))
            sock.sendall(bytes(START_QUERY_MESSAGE, "utf-8"))

            # traverse arms
            for arm in range(0, self.num_of_arms):
                ## for each arm:
                # rewrite the sql with hint indicated by arm
                sql = self.arm_to_hint(sql, arm)
                
                # explain the plan from PostgreSQL
                plan_json = self.explain_query_plan(sql)
                
                # transform plan json to Bao's plan json
                bao_plan_json = self.transform_plan_json(plan_json)
                
                # send each arm's plan to bao_server
                sock.sendall(bytes(json.dumps(bao_plan_json) + "\n", "utf-8"))
            
            # send the buffer info to bao_server
            # TODO - EXPLAIN does not support get buffer info without running the query
            sock.sendall(bytes(json.dumps(self.buffer_json) + "\n", "utf-8"))
        
            sock.sendall(bytes(TERMINAL_MESSAGE, "utf-8"))
            sock.shutdown(socket.SHUT_WR)

            # Receive data from the server and shut down
            selected_arm = struct.unpack("I", sock.recv(4))[0]
            
        return selected_arm
    
    def run_query(self, sql, bao_reward=False, bao_select=False):
        planning_time = 0
        querying_time = 0
        
        # if Bao SELECT enabled, select plan using Bao
        if bao_select:
            start = time.time()
            selected_arm = self.plan_query(sql)
            sql = self.arm_to_hint(sql, selected_arm)
            end = time.time()
            planning_time = end - start
        
        # run query against PostgreSQL
        start = time.time()
        try:
            self.cursor.execute(sql)
            self.cursor.fetchall()
        except Exception as error:
            print(error)
        end = time.time()
        querying_time = end - start

        # if Bao REWARD enabled, report query plan and running time to bao_server
        if bao_reward:
            self.report_reward(sql, querying_time)
        
        return (planning_time, querying_time)
    
    def report_reward(self, sql, querying_time):
        # Create a socket (SOCK_STREAM means a TCP socket) to bao_server
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                # Connect to server and send data
                sock.connect((BAO_HOST, BAO_PORT))
                sock.sendall(bytes(START_FEEDBACK_MESSAGE, "utf-8"))

                # explain the plan from PostgreSQL
                plan_json = self.explain_query_plan(sql)
                
                # transform plan json to Bao's plan json
                bao_plan_json = self.transform_plan_json(plan_json)
                
                # send the plan to bao_server
                sock.sendall(bytes(json.dumps(bao_plan_json) + "\n", "utf-8"))

                # send the buffer info to bao_server
                sock.sendall(bytes(json.dumps(self.buffer_json) + "\n", "utf-8"))

                # send reward json to bao_server
                reward_json = self.reward_json(querying_time)
                sock.sendall(bytes(json.dumps(reward_json) + "\n", "utf-8"))

                sock.sendall(bytes(TERMINAL_MESSAGE, "utf-8"))
                sock.shutdown(socket.SHUT_RDWR)
    
    def predict_query(self, sql, arm):
        # Create a socket (SOCK_STREAM means a TCP socket) to bao_server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to server and send data
            sock.connect((BAO_HOST, BAO_PORT))
            sock.sendall(bytes(START_PREDICTION_MESSAGE, "utf-8"))

            # rewrite the sql with hint indicated by arm
            sql = self.arm_to_hint(sql, arm)

            # explain the plan from PostgreSQL
            plan_json = self.explain_query_plan(sql)
            
            # transform plan json to Bao's plan json
            bao_plan_json = self.transform_plan_json(plan_json)
            
            # send the plan to bao_server
            sock.sendall(bytes(json.dumps(bao_plan_json) + "\n", "utf-8"))

            # send the buffer info to bao_server
            sock.sendall(bytes(json.dumps(self.buffer_json) + "\n", "utf-8"))

            sock.sendall(bytes(TERMINAL_MESSAGE, "utf-8"))
            sock.shutdown(socket.SHUT_WR)

            # Receive data from the server and shut down
            predicted_reward = struct.unpack("d", sock.recv(8))[0]
        
        return predicted_reward / 1000.0
    
    def explain_query_plan(self, sql):
        
        # explain sql to PostgreSQL for plan json
        sql = "EXPLAIN (FORMAT JSON) " + sql
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()[0][0]  # [(real_plan,)]
        except Exception as error:
            print(error)
            return None

    def arm_to_hint(self, sql, arm):
        # generate hint if arm is 1 ~ self.num_of_arms-1 (index starting from 0, arm[0] is no hint at all)
        if 1 <= arm < self.num_of_arms:
            hint = self.dataset.construct_hint_str(self.dimension, arm)
            sql = hint + sql
        return sql
    
    # input plan_json example:
    # [
        {
            "Plan":{
                "Node Type":"Bitmap Heap Scan",
                "Parallel Aware":false,
                "Relation Name":"tweets_100m",
                "Alias":"t",
                "Startup Cost":926.69,
                "Total Cost":1138.14,
                "Plan Rows":50,
                "Plan Width":24,
                "Recheck Cond":"((coordinate <@ '(-71.06233984375,41.89250859375),(-71.96966015625,41.50749140625)'::box) AND (to_tsvector('english'::regconfig, text) @@ '''shower'''::tsquery))",
                "Plans":[
                    {
                    "Node Type":"BitmapAnd",
                    "Parent Relationship":"Outer",
                    "Parallel Aware":false,
                    "Startup Cost":926.69,
                    "Total Cost":926.69,
                    "Plan Rows":50,
                    "Plan Width":0,
                    "Plans":[
                        {
                            "Node Type":"Bitmap Index Scan",
                            "Parent Relationship":"Member",
                            "Parallel Aware":false,
                            "Index Name":"idx_tweets_100m_coordinate",
                            "Startup Cost":0.0,
                            "Total Cost":451.42,
                            "Plan Rows":10000,
                            "Plan Width":0,
                            "Index Cond":"(coordinate <@ '(-71.06233984375,41.89250859375),(-71.96966015625,41.50749140625)'::box)"
                        },
                        {
                            "Node Type":"Bitmap Index Scan",
                            "Parent Relationship":"Member",
                            "Parallel Aware":false,
                            "Index Name":"idx_tweets_100m_text",
                            "Startup Cost":0.0,
                            "Total Cost":475.0,
                            "Plan Rows":50000,
                            "Plan Width":0,
                            "Index Cond":"(to_tsvector('english'::regconfig, text) @@ '''shower'''::tsquery)"
                        }
                    ]
                    }
                ]
            }
        }
    # ]
    def transform_plan_json(self, plan_json):
        root_node = plan_json[0]["Plan"]
        return {"Plan": self.transform_node(root_node)}
    
    def transform_node(self, node):
        new_node = {}
        new_node["Node Type"] = node["Node Type"]
        if "Relation Name" in node:
            new_node["Relation Name"] = node["Relation Name"]
        if "Index Name" in node:
            new_node["Index Name"] = node["Index Name"]
        new_node["Total Cost"] = node["Total Cost"]
        new_node["Plan Rows"] = node["Plan Rows"]
        if "Plans" in node:
            new_node["Plans"] = []
            for child in node["Plans"]:
                new_node["Plans"].append(self.transform_node(child))
        return new_node
    
    def reward_json(self, querying_time):
        return {"reward": querying_time * 1000.0, "pid": 0}


# Test cases for Bao
if __name__ == "__main__":
    test_sql = "SELECT id,        coordinate   FROM tweets_100m t  WHERE to_tsvector('english', t.text)@@to_tsquery('english', 'holder')   AND t.create_at between '2016-04-09 17:28:51' and '2016-04-10 17:28:51'   AND t.coordinate <@ box '((-87.97819295000001,32.38895135),(-80.71963045,35.469088850000006))'"
    bao = Bao()
    (pt, qt) = bao.run_query(test_sql, bao_reward=True, bao_select=False)
    print("Planning time; ", str(pt), "Querying time: ", str(qt))
    (pt, qt) = bao.run_query(test_sql, bao_reward=True, bao_select=True)
    print("Planning time; ", str(pt), "Querying time: ", str(qt))
    
    for arm in range(1, 8):
        qt = bao.predict_query(test_sql, arm)
        print("Arm-" + str(arm) + " Prediction: ", str(qt))
