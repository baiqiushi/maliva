import torch.nn as nn
import torch.nn.functional as F
from smart_util import Util


class DQN(nn.Module):
    def __init__(self, dimension, num_of_joins=1, num_of_sample_ratios=0, sampling_plan_only=False):
        super().__init__()

        num_of_plans = Util.num_of_plans(dimension, num_of_joins, num_of_sample_ratios, sampling_plan_only)
        self.fc1 = nn.Linear(in_features=num_of_plans*2+1, out_features=num_of_plans)
        self.fc2 = nn.Linear(in_features=num_of_plans, out_features=num_of_plans*2)
        self.out = nn.Linear(in_features=num_of_plans*2, out_features=num_of_plans)

    def forward(self, t):
        t = F.relu(self.fc1(t))
        t = F.relu(self.fc2(t))
        t = self.out(t)
        return t

