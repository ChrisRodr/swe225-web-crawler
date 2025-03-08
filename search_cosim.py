# milestone 2 

import os
import pandas as pd
import time
from collections import defaultdict
import numpy as np
import json

from create_index import read_json, tokenize_data
from utils.file_handler import get_file_name_hash_value

    

# target_tokens = ['iftekhar', 'ahmed', 'machine', 'learning', 'acm', 'master', 'software', 'engineering']
# target_tokens += ['of']
index_dir = 'output_tfidf/tfidf_index'

# query = input("enter query:\n").lower()
query = 'iftekhar'
query = query.split(' ')

candidates = {} # key - doc name, value - magnitude of document vector
for token in query: 
    prefix = token[0]
    if prefix.isdigit(): prefix='numbers'
    hash_val = get_file_name_hash_value(token)
    path = os.path.join(index_dir, prefix, f'{hash_val}.csv')
    df = pd.read_csv(path)
    print(f'number of docs for token ({token}): {len(df)}')

    # get tfidf of tokens in a document
    for idx, row in df.iterrows(): 

        start_time = time.time()

        magnitude = 0
        dot_prod = 0

        candidate_path = row['doc_name']
        
        # get list of tokens used in a doc
        doc_content = tokenize_data(read_json(candidate_path))
        unique_tokens = list(set(doc_content))

        # look up tfidf of each token
        for token2 in unique_tokens: 
            prefix2 = token2[0]
            if prefix2.isdigit(): prefix2='numbers'
            hash_val2 = get_file_name_hash_value(token2)
            path2 = os.path.join(index_dir, prefix2, f'{hash_val2}.csv')
            if not os.path.isfile(path2): continue # temp fix
            df2 = pd.read_csv(path2)
            tfidf = df2[df2['doc_name']==candidate_path]['tfidf'].item()
            magnitude += tfidf**2

            if token2 in query: dot_prod += tfidf

        print(f'[{idx+1} / {len(df)}] time for constructing doc vector: {(time.time()-start_time)/60:.2f} mins.')

        magnitude = np.sqrt(magnitude)
        query_magnitude = np.sqrt(len(query))

        cos_sim = dot_prod /  (magnitude * query_magnitude)
        # print(cos_sim)
        candidates[candidate_path] = cos_sim

res = {k: v for k, v in sorted(candidates.items(), key=lambda item: item[1])}
for doc in res: 
    with open(doc, 'r') as f: 
        url = json.load(f).get("url")
        print(url)