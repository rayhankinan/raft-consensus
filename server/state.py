from enum import Enum


class State(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2
