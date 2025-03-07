import pandas as pd
import os
import numpy as np
import time

debug = False

# dirs = ['output_1_new', 'output_2_new', 'output_3_new'] # 1 output dir for each chunk
dirs = ['output_1_new', 'output_2_new', 'output_3_new']
out_dir = 'output_tfidf'


def collect_token_lists(): 

    subdirs = [d for d in os.listdir(dirs[0]) if os.path.isdir(os.path.join(dirs[0], d))]
    subdirs = sorted(subdirs)

    n_tokens = 0

    # save all unique tokens
    for subdir in subdirs: 
        names = []
        for d in dirs: 
            file_names = sorted(os.listdir(os.path.join(d, subdir)))
            names += file_names
        names = list(set(names))

        save_path = os.path.join(out_dir, 'token_lists', f'{subdir}.csv')
        if not os.path.isdir(os.path.dirname(save_path)): 
            os.makedirs(os.path.dirname(save_path))
        names_df = pd.DataFrame(names)
        # print(f'subdir {subdir}: {len(names_df)}')
        n_tokens += len(names_df)
        names_df.to_csv(save_path, index=None, header=['token_name'])
        
        if debug: break

    # print(f'total: {n_tokens}')

    # simple concat for unique word count files
    unique_wc = pd.DataFrame()
    for d in dirs: 
        path = os.path.join(d, 'unique_word_count.csv')
        df = pd.read_csv(path, header=None)
        unique_wc = pd.concat([unique_wc, df], ignore_index=True)
    unique_wc.to_csv(os.path.join(out_dir, 'unique_word_count.csv'), header=['doc_name', 'count'], index=None)


def calculate_tfidf(data_all, unique_word_count): 

    # TF = frequency of token T in document D / total number of tokens N in document D
    tf1 = data_all['freq'].to_numpy()

    # tf2 = unique_word_count[unique_word_count['doc_name']==data_all['doc_name']]['count']
    tf2 = unique_word_count[unique_word_count['doc_name'].isin(data_all['doc_name'])]['count'].to_numpy()

    tf = tf1 * tf2

    idf_num = len(unique_word_count)
    idf_denom = len(data_all)
    idf = np.log10(idf_num / idf_denom)

    # print(tf1)
    # print(tf2)
    # print(idf_num)
    # print(idf_denom)
    # print(tf*idf)
    # exit()

    return tf * idf
    

def process_tokens(): 

    save_dir = os.path.join(out_dir, 'tfidf_index')
    if not os.path.isdir(save_dir): os.mkdir(save_dir)
    
    token_list_dir = os.path.join(out_dir, 'token_lists')
    token_list_paths = [os.path.join(token_list_dir, f) for f in os.listdir(token_list_dir)]

    unique_word_count = pd.read_csv(os.path.join(out_dir, 'unique_word_count.csv'))
    unique_word_count.sort_values(by=['doc_name'], inplace=True)
    unique_word_count = unique_word_count.reset_index(drop=True)

    # all tokens with same prefix
    for token_list_path in token_list_paths: 

        prefix = os.path.basename(token_list_path).split('.')[0]
        
        file_names = pd.read_csv(token_list_path)
        file_names = file_names['token_name'].tolist()
        file_names = sorted(file_names)

        start_time = time.time()

        # all files with the same token
        for file_name in file_names: 
            # print('processing token: ', file_name)
            data_all = pd.DataFrame(columns=['doc_name', 'freq'])
            for d in dirs: 
                data_path = os.path.join(d, prefix, file_name)
                if os.path.isfile(data_path): 
                    data = pd.read_csv(data_path, names=['doc_name', 'freq'])
                    data_all = pd.concat([data_all, data], ignore_index=True)
            
            
            data_all.sort_values(by=['doc_name'], inplace=True)
            data_all = data_all.reset_index(drop=True)

            tfidf_score = calculate_tfidf(data_all, unique_word_count)
            col1 = data_all['doc_name']
            col2 = pd.Series(tfidf_score, name='tfidf')
            df = pd.concat([col1, col2], axis=1, ignore_index=True)
            df.columns = ['doc_name', 'tfidf']
            df.sort_values(by=['tfidf'], ascending=False, inplace=True)

            save_path = os.path.join(save_dir, prefix, file_name)
            if not os.path.isdir(os.path.dirname(save_path)): os.mkdir(os.path.dirname(save_path))
            df.to_csv(save_path, index=None, header=['doc_name', 'tfidf'])

        print(f'time elapsed for prefix ({prefix}): {(time.time() - start_time)/60:.2f} mins.')


if __name__=="__main__":
    
    # collect_token_lists()
    # print('checkpoint')
    process_tokens()