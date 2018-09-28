import pandas as pd
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import json

pd.options.display.max_rows = 50
pd.options.display.max_columns = 15
pd.options.display.width = 500

data = pd.read_csv("101users.csv")

# data = pd.concat([data,  axis=1)
data = data.apply(lambda x: pd.Series({**json.loads(x['properties']), **x}), axis=1)

data['page'] = data['page'].apply(lambda x: x.replace("/wiki/", "/")[1:])
data = data[data['name'] == '$view']
data = data[data['page'] != 'search']
data = data[data['page'] != 'resource']

data = data.drop('properties', axis=1)

grouped = data.groupby(['visit_id', 'page']).size().sort_values()
grouped = grouped.to_frame('count').reset_index()
grouped['once'] = grouped['count'] == 1

pt = grouped.pivot_table('visit_id', index=['page'], columns=['once'], aggfunc=len)
pt = pt.fillna(0.0)

pt['rating'] = pt[False] - pt[True]
pt = pt.sort_values('rating')

pt.rename(columns={True: 'once', False: 'more than once'}, inplace=True)

pt.to_csv('data/page_ratings.csv')

page_grouped = data.groupby(['page']).size().sort_values()

page_grouped.to_csv('data/page_views.csv')

visit_history = data

visit_history['time'] = visit_history['time'].astype("datetime64")

visit_history.groupby([visit_history["time"].dt.year, visit_history["time"].dt.month]).size().plot(kind="line")

plt.savefig("data/views_over_time.png")

data.groupby(['visit_id']).size().sort_values().to_frame('count').plot(kind='box')
plt.show()

print(data.groupby(['visit_id']).size().sort_values().to_frame('count').describe())




# grouped_by_count = grouped_by_visit_id.groupby(['count']).mean().reset_index()
# grouped_by_count.columns = ['count', 'average number of page views']
#
# grouped_by_count.to_csv('views_per_visit.csv')



#
# print(len(grouped))
#
#
#
#
# for (x, data) in grouped:
#     print(x)
#     print(data)
