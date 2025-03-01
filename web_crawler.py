import os, shutil
import json
import nltk
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from bs4 import BeautifulSoup
from utils.file_handler import create_folders_for_alphabet, set_token_to_file, sort_csv_files
from utils.tokenizer import tokenize
import re
import time 

# Global token-tfidf map
token_tfidf_map = {}

# File name in corpus counter
file_name_counter = 0

def read_json(file_name):
    # Read JSON content from a file
    with open(file_name, 'r', encoding='utf-8') as file:
        json_content = json.load(file)
    return json_content

def tokenizer(html_content):
    # TODO: Adjust header weights
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
    # Extract text content
    text = soup.get_text()
    tokens = tokenize(text)

    alpha_numeric_tokens = tokens
    
    # Find and weight words in headers and bold tags separately
    weighted_tokens = []
    for tag, weight in header_weights.items():
        for header in soup.find_all(tag):
            header_tokens = tokenize(header.get_text())
            weighted_tokens.extend(header_tokens * weight)
    
    # Combine regular tokens
    weighted_tokens.extend(tokens)
    return weighted_tokens

def get_next_batch(n, in_dir = './DEV/'): 
    ''' gets the next n file paths each iteration. returns as list. '''

    batch = []
    for dir, _, file_names in os.walk(in_dir): 

        if file_names: # if list isn't empty

            for file_name in file_names: 
                batch.append(os.path.join(dir, file_name))
                # print(file_name)

                if len(batch) == n: 
                    yield batch
                    batch = [] # reset batch after call


    if batch: yield batch # if num of files doesn't divide nicely with n

def construct_index():
    # Ensure you have downloaded the necessary NLTK data
    nltk.download('punkt_tab')
    
    # Create a stemmer
    stemmer = PorterStemmer()

    print("Writing partial indices...")

    # initialize generator
    N = 1 # batch size (don't make it too small)
    get_file_names_list = get_next_batch(N)

    create_folders_for_alphabet()

    global file_name_counter
    batch_idx = 0
    timer_start = time.time()

    for idx, file_names in enumerate(get_file_names_list):

        batch_idx += 1

        # Get and clear the map before starting
        global token_tfidf_map
        token_tfidf_map.clear()

        # Create a list to store the tokenized and stemmed content of each file
        documents = []
    
        # Iterate through each file name
        for file_name in file_names:
            json_content = read_json(file_name)
            html_content = json_content["content"]
            tokens = tokenizer(html_content)
            stemmed_tokens = [stemmer.stem(token) for token in tokens]
            documents.append(" ".join(stemmed_tokens))
            file_name_counter = file_name_counter + 1

            if file_name_counter%1000==0: 
                print(f"\ntime for 1000 files: {time.time() - timer_start:.4f} seconds\n")
                timer_start = time.time()
            else:
                print(f"\rFiles read: {file_name_counter} / 55,393 - batch #{batch_idx}", end='') # tot num files = 55,393
    
        # Calculate TF-IDF scores
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(documents)
    
        # Extract the unique words and their TF-IDF scores
        feature_names = vectorizer.get_feature_names_out()
        tfidf_scores = tfidf_matrix.toarray()
    
        # Populate the dictionary with tokens and their scores
        for doc_index, doc_scores in enumerate(tfidf_scores):
            for word_index, score in enumerate(doc_scores):
                token = feature_names[word_index]
                score = score.item()
                if token not in token_tfidf_map:
                    token_tfidf_map[token] = []
                token_tfidf_map[token].append([file_names[doc_index], score])

        # Write the map to the files.
        set_token_to_file(token_tfidf_map)

        # DEBUG Line
        return

    # Sort the postings in the files.
    print("Sorting postings of the files...")
    sort_csv_files()

    return "Index constructed!"

def main():
    # Call the construct_index function
    result = construct_index()
    print(result)
    print(f"Number of file read from corpus: {file_name_counter}")
    

if __name__ == "__main__":
    
    purge_output = False
    if purge_output: 
        ans = input("warning!!! are you sure you want to purge the output directory and all its contents? (y/n)\n")
        
        if ans=='y': 
            print('removing directory')
            shutil.rmtree('./output')
            print('directory removed.')
        else: 
            print('preserving the directory.')
    
    main()
