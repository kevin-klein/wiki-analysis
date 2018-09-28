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

data = pd.read_csv("data/101users.csv")#.head(400)

# data = pd.concat([data,  axis=1)
data = data.apply(lambda x: pd.Series({**json.loads(x['properties']), **x}), axis=1)

data['page'] = data['page'].apply(lambda x: x.replace("/wiki/", "/")[1:])
data = data[data['name'] == '$view']
data = data[data['page'] != 'search']
data = data[data['page'] != 'resource']

data = data.drop('properties', axis=1)

data = data.groupby('visit_id').apply(
    lambda d: pd.DataFrame([{'source': x, 'target': y} for y in d['page'].unique() for x in d['page'].unique()]))

data = data.groupby(['source', 'target']).size().to_frame('count').reset_index()

# data = data.pivot_table(values='count', index='source', columns='target', aggfunc=np.sum).fillna(0.0)

print(data)

G = nx.Graph()

for i, x in data.iterrows():
    if x['source'] < x['target'] and x['count'] > 2:  # and x['count'] > 2
        G.add_edge(x['source'], x['target'], weight=x['count'])

degree = nx.degree(G)

nx.draw(G, nodelist=degree.keys(),pos=nx.spring_layout(G), node_size=[np.log(v) * 20 for v in degree.values()])
#nx.draw(G, node_list=degree.keys(), pos=nx.spring_layout(G), node_size=degree)
plt.show()

# sns.heatmap(data)
# plt.show()
