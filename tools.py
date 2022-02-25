import requests
import os
import json
import logging
import pandas as pd


def format_rules(self,usernames): # ! Use Usernames over User IDs because they are capped at 15
    rules = []
    for i in range(0,len(usernames),22): # ! if this can find average, can be more efficient
        # ! Add check for abnormal lengths
        rule = ''
        for user in usernames[i:i+22]:
            rule+=f"from:{user} OR "
        rules.append(rule[:-4:])
    return rules