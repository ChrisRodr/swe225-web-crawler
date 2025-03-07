import math
from collections import defaultdict
import os, shutil
import json
import nltk
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from bs4 import BeautifulSoup
from utils.file_handler import create_folders_for_alphabet, set_token_to_file_2, \
            sort_csv_files, update_posting_duplicates_and_sort, postings_from_file, \
            remove_current_index
from utils.tokenizer import tokenize
import re
import time
import sys

###############################################################

# Index Construction Functions

###############################################################

# Global document count and document frequencies
# These are to keep track of all the documents and the frequency of each 
# token in all the documents. Needed for tf_idf scoring.
document_count = 55_393
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

def load_mapping_table(mapping_file: str, reverse = False):
    mapping_dict = {}
    
    # Open and read the mapping table file
    with open(mapping_file, 'r') as file:
        for line in file:
            parts = line.strip().split(" ", 1)
            
            if len(parts) == 2:
                id = int(parts[0])  
                file_path = parts[1] 
                file_hash = hash(file_path)
                
                # Store in dictionary: file_hash -> id
                if reverse:
                    mapping_dict[id] = file_path
                else:
                    mapping_dict[file_path] = id
    
    return mapping_dict


def process_document(doc_text):
    """
		Doc id is the path to the document.

		Get the term frquency for a document text.
		This will also update the document frequencies global variable
		and the document count.
		It will update the inverted index for
    """
    global document_frequencies

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
            'tfidf': round(tfidf_scores[term], 2)
        })

def read_json(file_name):
    # Read JSON content from a file
    with open(file_name, 'r', encoding='utf-8') as file:
        json_content = json.load(file)
    return json_content

def get_next_batch(chunk_idx, batch_size, in_dir = './DEV/'): 
    ''' gets the next n file paths each iteration. returns as list. '''

    tot_num_files = 55_393
    
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

def construct_index(chunk_idx, batch_size=5_000):

    global inverted_index

    # Ensure you have downloaded the necessary NLTK data
    nltk.download('punkt_tab')

    create_folders_for_alphabet(output_dir)


    # load mapping table
    mapping_file = 'mapping_table.txt'
    target_mapping = load_mapping_table(mapping_file)

    # initialize generator
    print("Writing partial indices...")
    local_document_count = 0
    batch_number = 0
    get_file_names_list = get_next_batch(chunk_idx, batch_size)
    for idx, file_names in enumerate(get_file_names_list):

        inverted_index.clear()

        batch_number += 1
        # Process each file
        for file_name in file_names:
            json_content = read_json(file_name)
            html_content = json_content["content"]
            term_freq = process_document(html_content)
            local_document_count += 1
            compute_tfidf(term_freq, target_mapping[file_name])
            print(f"\rDocuments processed: {local_document_count} / {chunk_num_files} - batch_number: {batch_number} / {math.ceil(chunk_num_files/batch_size)}", end='')
            sys.stdout.flush()

        # Write the map to the files.
        print("\nWriting the inverted index for this batch")
        set_token_to_file_2(inverted_index, output_dir)

    # Update the postings to only have one instanse of a file
    # And sort the postings.
    print("Updating and sorting postings")
    update_posting_duplicates_and_sort(output_dir)

    print("Index Constructed")


###############################################################

# User Query Functions

###############################################################

def tokenizer_query(query_string):
    """
        Tokenizes the user query and stems the words.
        Only alpha numeric values.
    """
    stemmer = PorterStemmer()

    tokens = tokenize(query_string)
    alpha_numeric_tokens = [token for token in tokens if token.isalnum()]
    stemmed_tokens = [stemmer.stem(token) for token in alpha_numeric_tokens]

    return stemmed_tokens

def promt_user():
    """
        Promts the user for a query and tokenizes the input
    """
    print("Search MiniGoogle")
    user_input = input("="*50 + '\n')
    print("="*50)
    return tokenizer_query(user_input)
    print(user_input)

def grab_postings(token_array):
    posting_map = {}

    for token in token_array:
        posting_map[token] = postings_from_file(token, output_dir)

    # Debug to print out postings from file
    # for key, value in posting_map.items():
    #     print(f"Key: {key}")
    #     for item in value:
    #         print(f"  Document ID: {item['doc_id']}")
    #         print(f"  Term Frequency (TF): {item['tf']}")
    #         print(f"  TF-IDF: {item['tfidf']}")
    #         print("")

    return posting_map

def list_ranked_documents(posting_map):
    """
        This function will look at the posting map.
        For each document it will sum up the tfidf scores.
        Then sort them from greatest to smallest.
    """
    document_map = defaultdict(float)
    for token, postings in posting_map.items():
        for posting in postings:
            document_map[posting['doc_id']] += float(posting['tfidf'])

    return sorted(document_map, key=lambda k: document_map[k], reverse=True)

def query_data():

    tokened_query = promt_user()

    print("Processing query...")
    total_start_time = time.time()

    print("Grabing postings from files...")
    start_time = time.time()
    posting_map = grab_postings(tokened_query)
    end_time = time.time()
    grab_postings_time = (end_time - start_time) * 1000 # In miliseconds
    print(f"Grabbing postings took: {grab_postings_time:.2f} ms")

    print("Ranking and sorting the documents.")
    start_time = time.time()
    document_list = list_ranked_documents(posting_map)
    end_time = time.time()
    document_list_time = (end_time - start_time) * 1000 # In miliseconds
    print(f"Ranking and sorting the retreived documents took: {document_list_time:.2f} ms")

    print("Printing the documents")
    start_time = time.time()

    # load mapping table
    mapping_file = 'mapping_table.txt'
    target_mapping = load_mapping_table(mapping_file, True)


    for doc in document_list:
        #print(target_mapping[int(doc)])
        file_path = target_mapping[int(doc)]
        import json
        # Open and parse the JSON file
        with open(file_path, 'r') as file:
            data = json.load(file)

        # Extract the "url" value
        url_value = data.get("url")

        print(url_value)
        
    end_time = time.time()
    print_doc_time = (end_time - start_time) * 1000 # In miliseconds
    print(f"Printing the ducuments took: {print_doc_time:.2f} ms")

    total_end_time = time.time()
    total_time = (total_end_time - total_start_time) * 1000  # Convert to milliseconds
    print(f"Total execution time: {total_time:.2f} ms")

###############################################################

# MAIN

###############################################################

def main():

    ### OPTIONS ###
    
    # Selection between creating the index and querying the data
    construct_index_flag = True
    batch_size = 500
    purge_output = True

    ###############

    global output_dir
    output_dir = "output"

    if construct_index_flag:
        # first argument - data chunk / second argument - output dir
        if len(sys.argv) != 3: 
            exit('select data chunk to process as an argument (0 (all), 1, 2, or 3) and set output dir.')
        chunk_idx = int(sys.argv[1])
        assert chunk_idx in range(4), 'chunk_idx should be 0, 1, 2, or 3. '
        output_dir = sys.argv[2]
        
        #if purge_output: remove_current_index(output_dir)

        # Call the construct_index function
        construct_index(chunk_idx, batch_size)
        print(f"Number of file read from corpus: {document_count}")

    else:
        query_data()
    

if __name__ == "__main__":
    main()
