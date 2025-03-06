# alternative script to create index -- in progress


import os, sys
from collections import defaultdict
import json
import nltk
from bs4 import BeautifulSoup
import csv
import time

from utils.file_handler import create_folders_for_alphabet, get_file_name_hash_value, remove_current_index
from utils.tokenizer import tokenize

nltk.download('punkt_tab')


def get_next_batch(chunk_idx, batch_size, in_dir): 
    ''' gets the next n file paths each iteration. returns as list. '''

    tot_num_files = 55_393
    if debug: tot_num_files = 1
    
    if chunk_idx != 0:
        chunk_size = tot_num_files // 3
        start_idx = (chunk_idx-1)*chunk_size    
        if chunk_idx==3:
            end_idx = tot_num_files
        else: 
            end_idx = start_idx + chunk_size
    else: 
        chunk_size = tot_num_files
        start_idx, end_idx = 0, tot_num_files
    
    global chunk_num_files
    chunk_num_files = end_idx - start_idx
        
    batch = []
    file_idx = 0
    for sub_dir in sorted(os.listdir(in_dir)):
        file_names = sorted(os.listdir(os.path.join(in_dir, sub_dir)))
        for file_name in file_names: 
            
            if file_idx in range(start_idx, end_idx):
                batch.append(os.path.join(in_dir, sub_dir, file_name))
                
                if len(batch) == batch_size: 
                    yield batch
                    batch = [] # reset batch after call
            
            file_idx += 1 # increment
                    
    if batch: yield batch # last batch


def read_json(file_name):
    # Read JSON content from a file
    with open(file_name, 'r', encoding='utf-8') as file:
        json_content = json.load(file)
    return json_content['content']


def tokenize_data(html_content):
    ''' parse data for alphanumeric characters, add weights, apply porter stemmer'''

    header_weights = {
        'title': 50,
        'h1'   : 21,
        'h2'   : 18,
        'h3'   : 15,
        'h4'   : 12,
        'h5'   : 9,
        'h6'   : 6,
        'b'    : 3,
    }
    
    # Parse the HTML content
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text()
    tokens = tokenize(text)
    alpha_numeric_tokens = [token for token in tokens if token.isalnum()]
    
    # Find and weight words in headers and bold tags separately
    weighted_tokens = []
    for tag, weight in header_weights.items():
        for header in soup.find_all(tag):
            header_tokens = tokenize(header.get_text())
            weighted_tokens.extend(header_tokens * weight)
    
    # Combine regular tokens
    weighted_tokens.extend(alpha_numeric_tokens)

    # Stem the tokens
    stemmer = nltk.stem.PorterStemmer()
    stemmed_tokens = [stemmer.stem(token) for token in weighted_tokens]

    return stemmed_tokens


def get_word_freqs(tokens, token_freqs, doc_name): 

    freqs = defaultdict(int)
    for token in tokens: 
        freqs[token] += 1

    for token, freq in freqs.items(): 
        token_freqs[token].append([doc_name, freq])

    return token_freqs


def get_unique_word_count(tokens, unique_counts, doc_name): 

    count = len(set(tokens))
    unique_counts.append([doc_name, count])

    return unique_counts


def write_to_file(content, file_name): 

    # Write the content to the CSV file
    try:
        with open(file_name, mode='a+', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(content) 
    except Exception as e:
        print(type(content))
        print(content)
        print(e)
        print(f"there was a problem with writing to the file: {file_name}.")


def save_batch(token_freqs, unique_counts, out_dir): 
    
    # save each token as a file
    for key, value in token_freqs.items():

        # Determine the folder prefix from the first letter of the key (e.g., 'c' for 'cat')
        folder_prefix = key[0].lower()
        if folder_prefix.isdigit(): folder_prefix = "numbers"

        folder_path = os.path.join(out_dir, folder_prefix)

        if not os.path.isdir(folder_path): 
            folder_path = os.path.join(out_dir, "others")
        
        # Prepare the filename and the content for the CSV file
        # content = [ posting.values() for posting in value ]
        file_name = os.path.join(folder_path, f"{get_file_name_hash_value(key)}.csv")
        write_to_file(value, file_name)

    # save unique word count
    file_name = os.path.join(out_dir, 'unique_word_count.csv')
    write_to_file(unique_counts, file_name)


def construct_index(chunk_idx, batch_size, in_dir, out_dir): 
    ''' each batch saves current info to disk. '''

    batches = get_next_batch(chunk_idx, batch_size, in_dir)

    for batch_idx, batch in enumerate(batches): 

        start_time = time.time()
    
        token_freqs = defaultdict(list) # key - token, value - [document name, frequency]
        unique_counts = [] # [document name, total number of unique tokens]
    
        for file_path in batch: 
            html_content = read_json(file_path)
            tokens = tokenize_data(html_content)

            # update
            token_freqs = get_word_freqs(tokens, token_freqs, file_path)
            unique_counts = get_unique_word_count(tokens, unique_counts, file_path)

        save_batch(token_freqs, unique_counts, out_dir)

        print(f'batch #{batch_idx+1} done. time elapsed: {time.time() - start_time/60:.2f} mins.')


def main(): 

    global debug
    debug = False

    batch_size = 500
    purge_output = True
    in_dir = './DEV'
    out_dir = 'output_3_new'
    chunk_idx = 3

    if purge_output: remove_current_index(out_dir)

    create_folders_for_alphabet(out_dir)

    construct_index(chunk_idx, batch_size, in_dir, out_dir)

if __name__=="__main__": 

    main()