import math
from collections import defaultdict
import os, shutil
import json
import nltk
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from bs4 import BeautifulSoup
from utils.file_handler import create_folders_for_alphabet, set_token_to_file_2, sort_csv_files, update_posting_duplicates_and_sort
from utils.tokenizer import tokenize
import re
import time 

# Global document count and document frequencies
# These are to keep track of all the documents and the frequency of each 
# token in all the documents. Needed for tf_idf scoring.
document_count = 0
document_frequencies = defaultdict(int)

# This is the inverted index that will be updated along the way
# of processing all the batches.
inverted_index = defaultdict(list)

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
    stemmer = PorterStemmer()
    stemmed_tokens = [stemmer.stem(token) for token in weighted_tokens]

    return stemmed_tokens

def process_document(doc_text):
    """
		Doc id is the path to the document.

		Get the term frquency for a document text.
		This will also update the document frequencies global variable
		and the document count.
		It will update the inverted index for
    """
    global inverted_index
    global document_count
    global document_frequencies
    document_count += 1

    # Tokenize the document (simple split; consider using NLTK or spaCy for better tokenization)
    tokens = tokenizer(doc_text)
    
    # Term frequencies for the current document
    term_frequencies = defaultdict(int)
    for token in tokens:
        term_frequencies[token] += 1

    # Update document frequencies
    unique_terms = set(tokens)
    for term in unique_terms:
        document_frequencies[term] += 1

    return term_frequencies

def compute_tfidf(term_frequencies, doc_id):
    """
		This will compute the tfidf for the term frequencies of a document.
		This takes into account the document count.
    """
    global document_frequencies
    global document_count
    global inverted_index

    tfidf_scores = {}
    for term, tf in term_frequencies.items():
        # Term Frequency (can be raw count, normalized, or log-scaled)
        term_freq = tf  # Or tf / max_tf in document

        # Inverse Document Frequency
        df = document_frequencies[term]
        idf = math.log((document_count) / df)

        # TF-IDF Score
        tfidf_scores[term] = term_freq * idf

        # Update the inverted index
        inverted_index[term].append({
            'doc_id': doc_id,
            'tf': tf,
            'tfidf': tfidf_scores[term]
        })

def update_tfidf():
    global document_frequencies
    global document_count
    global inverted_index

    for term, postings in inverted_index.items():
        inverted_index[term] = []

        for posting in postings:
            df = document_frequencies[term]
            idf = math.log((document_count) / df)
            tfidf = posting['tf'] * idf

            inverted_index[term].append({
                'doc_id': posting['doc_id'],
                'tf': posting['tf'],
                'ifidf': tfidf
                })


def read_json(file_name):
    # Read JSON content from a file
    with open(file_name, 'r', encoding='utf-8') as file:
        json_content = json.load(file)
    return json_content

def get_next_batch(n, in_dir = './DEV/'): 
    ''' gets the next n file paths each iteration. returns as list. '''

    batch = []
    for dir, _, file_names in os.walk(in_dir): 

        if file_names: # if list isn't empty

            for file_name in file_names: 
                batch.append(os.path.join(dir, file_name))
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
    N = 5_000 # batch size (don't make it too small)
    get_file_names_list = get_next_batch(N)

    create_folders_for_alphabet()

    batch_idx = 0
    timer_start = time.time()

    for idx, file_names in enumerate(get_file_names_list):

        batch_idx += 1

        global inverted_index

        for file_name in file_names:
            json_content = read_json(file_name)
            html_content = json_content["content"]
            term_freq = process_document(html_content)
            compute_tfidf(term_freq, file_name)

        # Update the tfidf scores in inverted index
        update_tfidf()

        # Write the map to the files.
        set_token_to_file_2(inverted_index)

        if batch_idx == 2:
            break

    # Update the postings to only have one instanse of a file
    # And sort the postings.
    update_posting_duplicates_and_sort()

    print("Index Constructed")


def main():
    # Call the construct_index function
    construct_index()
    print(f"Number of file read from corpus: {document_count}")
    

if __name__ == "__main__":
    
    purge_output = True
    if purge_output: 
        ans = input("warning!!! are you sure you want to purge the output directory and all its contents? (y/n)\n")
        
        if ans=='y': 
            print('removing directory')
            shutil.rmtree('./output')
            print('directory removed.')
        else: 
            print('preserving the directory.')
    
    main()