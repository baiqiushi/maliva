import torch.nn as nn
import torch.nn.functional as F


class DQN(nn.Module):
    def __init__(self):
        super().__init__()

        # 7 plans (001 ~ 111),
        self.fc1 = nn.Linear(in_features=7*2+1, out_features=8)
        self.fc2 = nn.Linear(in_features=8, out_features=14)
        self.out = nn.Linear(in_features=14, out_features=7)

    def forward(self, t):
        t = F.relu(self.fc1(t))
        t = F.relu(self.fc2(t))
        t = self.out(t)
        return t

