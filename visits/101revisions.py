import pandas as pd
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import json
import seaborn as sns
import networkx as nx

pd.options.display.max_rows = 50
pd.options.display.max_columns = 15
pd.options.display.width = 500

data = pd.read_csv("data/101users.csv").head(400)

# data = pd.concat([data,  axis=1)
data = data.apply(lambda x: pd.Series({**json.loads(x['properties']), **x}), axis=1)

data['page'] = data['page'].apply(lambda x: x.replace("/wiki/", "/")[1:])
data = data[data['name'] == '$view']
data = data[data['page'] != 'search']
data = data[data['page'] != 'resource']
data = data.drop('properties', axis=1)

print(data)