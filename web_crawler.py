import os
import json
import nltk
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from bs4 import BeautifulSoup
from utils.file_handler import create_folders_for_alphabet, set_token_to_file

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
    header_weights = {'title': 50, 'h1': 21, 'h2': 18, 'h3': 15, 'h4': 12, 'h5': 9, 'h6': 6, 'b': 3}
    
    # Parse the HTML content
    soup = BeautifulSoup(html_content, "html.parser")
    # Extract text content
    text = soup.get_text()
    tokens = nltk.word_tokenize(text)
    
    # Find and weight words in headers and bold tags separately
    weighted_tokens = []
    for tag, weight in header_weights.items():
        for header in soup.find_all(tag):
            header_tokens = nltk.word_tokenize(header.get_text())
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
    N = 5_000 # batch size
    get_file_names_list = get_next_batch(N)

    global file_name_counter
    for file_names in get_file_names_list:
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
            print(f"\rFiles read: {file_name_counter}", end='')
    
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
                if token not in token_tfidf_map:
                    token_tfidf_map[token] = []
                token_tfidf_map[token].append((file_names[doc_index], score))

        # TODO: Write the map to the files.
        

    # Testing {"cat": "documentid, score?"}
    # use it in the begining
    create_folders_for_alphabet()

    # test write
    test_data_token = {'cat': "documentid,99"}
    set_token_to_file(test_data_token)
    test_data_token = {'cat': "documentid_2,199"}
    set_token_to_file(test_data_token)
    test_data_token = {'dog': "documentid_2,199"}
    set_token_to_file(test_data_token)

    # TODO: Sort the postings in the files.
    # print("Sorting postings of the files...")

    return "Index constructed!"

def main():
    # Call the construct_index function
    result = construct_index()
    print(result)
    print(f"Number of file read from corpus: {file_name_counter}")
    

if __name__ == "__main__":
    main()
