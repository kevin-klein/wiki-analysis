import sqlite3
import csv
import networkx as nx
import re
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
from networkx.readwrite import json_graph
import json
import networkx.algorithms.centrality
import networkx.algorithms.community as cmt

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class Page(object):

    def __init__(self, d):
        # print(d)
        self.title = d['title']
        self.namespace = d['namespace']
        self.raw_content = d['raw_content']
        # self.created_at = d['created_at']
        # self.updated_at = d['updated_at']

    def full_title(self):
        return '{}:{}'.format(self.namespace, self.title)

    def links(self):
        return re.findall(r'\[\[([^]]+)\]\]', str(self.raw_content))

    def __repr__(self):
        return self.full_title() #+ " " + self.updated_at

class PageChange(Page):

    def __init__(self, d):
        Page.__init__(self, d)
        self.page_id = d['page_id']

from multiprocessing import Pool

def get_pages():
    conn = sqlite3.connect('db.sqlite3')
    conn.row_factory = dict_factory
    cursor = conn.cursor()

    pages = []
    for page in cursor.execute('select * from pages where pages.id not in(select page_id from page_changes)').fetchall():
        pages.append(Page(page))

    conn.close()

    return pages

def get_changes():
    conn = sqlite3.connect('db.sqlite3')
    conn.row_factory = dict_factory
    cursor = conn.cursor()

    changes = []

    for change in cursor.execute('select * from page_changes where page_changes.raw_content <> \'\' order by created_at').fetchall():
        changes.append(PageChange(change))

    conn.close()
    return changes

def create_graph(page_list):
    # t, scope = item
    # page_list = []
    #
    # for _, page in scope.items():
    #     page_list.append(page)

    G = nx.Graph()
    for page in page_list:
        G.add_node(page.full_title())

    for page in page_list:
        links = page.links()

        for link in links:
            if 'http:' in link or 'https:' in link:
                continue

            if '::' in link:
                predicate, object = link.split('::', 1)
                if G.has_node(object):
                    G.add_edge(page.full_title(), object, name=predicate)
            else:
                if G.has_node(link):
                    G.add_edge(page.full_title(), link, name='linksTo')

    # d = json_graph.node_link_data(G)
    # with open('graphs/graph_' + t.replace(':', '').replace('.', '').replace(' ', '') + '.json', 'w') as f:
    #     json.dump(d, f)

    return G

    # pos = nx.spring_layout(G, k=0.15,iterations=20)
    # nx.draw(G, pos=pos, with_labels=False)
    # print("path_" + t[:9] + ".png")

    # options = {
    #     'node_color': 'black',
    #     'node_size': 50,
    #     'line_color': 'grey',
    #     'linewidths': 0,
    #     'width': 0.1,
    # }
    # nx.draw(G, **options)
    # plt.show()
    #
    # plt.savefig("path_" + t[:9] + ".png")

    # documents = []
    # for page in page_list:
    #     documents.append(page.raw_content)
    #
    # if documents:
    #     tfidf = TfidfVectorizer().fit_transform(documents)
    #     # similarities = tfidf * tfidf.T
    #     # print(type(similarities))
    #     similarities = cosine_similarity(tfidf)
    #
    #     # similarities = similarities.A
    #
    #     # m = similarities.shape[0]
    #     # strided = np.lib.stride_tricks.as_strided
    #     # s0,s1 = similarities.strides
    #     # similarities = strided(similarities.ravel()[1:], shape=(m-1,m), strides=(s0+s1,s1)).reshape(m,-1)
    #
    #     print(similarities)
    #     if len(documents) == 5:
    #         raise

def read_json_file(filename):
    with open(filename) as f:
        js_graph = json.load(f)
    return json_graph.node_link_graph(js_graph)

if __name__ == '__main__':
    # G = read_json_file('graphs/graph_2017-08-22183008786189.json')
    #

    data = pd.read_csv('pages.csv')
    pages = list(data.apply(lambda x: Page(x.to_dict()), axis=1))

    # print(pages)
    G = create_graph(pages)

    bc = networkx.algorithms.betweenness_centrality(G)

    # with open('betweenness_centrality.json', 'w') as f:
    #     json.dump(bc, f, indent=4)

    G.remove_edges_from(nx.selfloop_edges(G))
    kcores = networkx.algorithms.core_number(G)

    lines = {}

    for page in pages:
        lines[page.full_title()] = len(str(page.raw_content).split('\n'))


    metrics = pd.DataFrame({'bc': bc,'kcores': kcores,'lines': lines})#.transpose()
    # print(metrics)
    # metrics = metrics.rename({0: 'betweenness_centrality', 1: 'k-core'}, axis='columns')

    tags = pd.read_csv('tags.csv')
    tags['idx'] = tags.apply(lambda x: '{}:{}'.format(x['namespace'], x['title']),axis=1)
    tags = tags.set_index('idx')

    data = pd.concat([tags,metrics], axis=1, join='inner')
    print(data.corr())





    # with open('core_numbers.json', 'w') as f:
    #     json.dump(networkx.algorithms.core_number(G), f, indent=4)

    # data = pd.read_csv('pages.csv')
    # data['link'] = data.apply(lambda x: 'http://101wiki.softlang.org/{}:{}'.format(x['namespace'], x['title']).replace(' ', '_'), axis=1)
    # data['tag'] = np.NaN
    #
    # sample = data.sample(50)
    #
    # sample.to_csv('sample.csv', index=False)

    # changes = get_changes()
    # pages = get_pages()
    #
    # wikis_at_times = {}
    #
    # for idx, change in enumerate(changes):
    #     specific_wiki = {}
    #     for page in changes[0:idx]:
    #         specific_wiki[page.full_title()] = page
    #
    #     for page in pages:
    #         specific_wiki[page.full_title()] = page
    #
    #     wikis_at_times[change.created_at] = specific_wiki
    #
    # p = Pool(8)
    # p.map(process_wiki_time, wikis_at_times.items())
