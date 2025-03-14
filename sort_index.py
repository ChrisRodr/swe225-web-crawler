import os 
import pandas as pd 
from time import time


in_dir = '/home/hsuc14/80_repo/swe225-web-crawler/output'
out_dir = './output_sorted'

prefixs = os.listdir(in_dir)
for prefix in prefixs: 

    start_time = time()
    
    prefix_dir = os.path.join(in_dir, prefix)
    file_names = os.listdir(prefix_dir)

    for file_name in file_names: 
        # sort
        content = pd.read_csv(os.path.join(prefix_dir, file_name), names=['doc_id','tfidf'])
        content.sort_values(by=['tfidf'], ascending=False, inplace=True)

        # create output dir
        out_subdir = os.path.join(out_dir, prefix)
        if not os.path.isdir(out_subdir): os.makedirs(out_subdir)

        # save to new dir
        out_path = os.path.join(out_subdir, file_name)
        content.to_csv(out_path, index=None, header=None)

    print(f'directory {prefix} took {(time()-start_time)/60:.2f} mins.')