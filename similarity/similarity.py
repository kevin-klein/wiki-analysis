import sqlite3
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import csv

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class Page(object):

    def __init__(self, d):
        self.title = d['title']
        self.namespace = d['namespace']
        self.page_id = d['page_id']
        self.raw_content = d['raw_content']
        self.created_at = d['created_at']

    def full_title(self):
        return '{}:{}'.format(self.namespace, self.title)

conn = sqlite3.connect('db.sqlite3')
conn.row_factory = dict_factory

cursor = conn.cursor()

changes = []

for change in cursor.execute('select * from page_changes where page_changes.raw_content <> \'\' order by created_at').fetchall():
    changes.append(Page(change))

wikis_at_times = {}

for idx, change in enumerate(changes):
    specific_wiki = {}
    for page in changes[0:idx]:
        specific_wiki[page.full_title()] = page
    wikis_at_times[change.created_at] = specific_wiki

for t, scope in wikis_at_times.items():
    page_list = []

    for t, page in scope.items():
        page_list.append(page)

    documents = []
    for page in page_list:
        documents.append(page.raw_content)

    if documents:
        tfidf = TfidfVectorizer().fit_transform(documents)
        # similarities = tfidf * tfidf.T
        # print(type(similarities))
        similarities = cosine_similarity(tfidf)

        # similarities = similarities.A

        # m = similarities.shape[0]
        # strided = np.lib.stride_tricks.as_strided
        # s0,s1 = similarities.strides
        # similarities = strided(similarities.ravel()[1:], shape=(m-1,m), strides=(s0+s1,s1)).reshape(m,-1)

        print(similarities)
        if len(documents) == 5:
            raise
