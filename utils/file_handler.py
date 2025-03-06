import os, shutil
import csv
import configparser
import hashlib


def get_file_name_hash_value(file_name):
    # Create a SHA-256 hash object
    hash_object = hashlib.sha256()
    
    # Update the hash object with the token name (converted to bytes)
    hash_object.update(file_name.encode())
    
    # Get the hexadecimal representation of the hash value
    hash_value = hash_object.hexdigest()
    
    return hash_value

# def get_output_dir_from_config(config_file='config.ini'):
#     """
#     Reads the output_dir from the config file.
    
#     :param config_file: Path to the configuration file (default is 'config.ini').
#     :return: The output directory as specified in the config file.
#     """
#     config = configparser.ConfigParser()
#     config.read(config_file)
#     return config.get('settings', 'output_dir', fallback='output') 


def create_folders_for_alphabet(output_dir):
    """
    Creates folders for each letter from A to Z under the specified output directory.
    
    :param output_dir: The base directory under which the folders will be created (default is 'output').
    """
    # Check if the output directory exists, if not, create it
    # output_dir = get_output_dir_from_config()
    
    # Loop through A to Z and create each folder
    for letter in range(97, 123):  
        folder_name = chr(letter)
        folder_path = os.path.join(output_dir, folder_name)
        
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        print(f"Created folder for each letter.")

    number_dir = os.path.join(output_dir, "numbers")
    if not os.path.isdir(number_dir): 
        os.makedirs(number_dir)
        print(f'created folder for numbers.')

    others_dir = os.path.join(output_dir, "others")
    if not os.path.isdir(others_dir): 
        os.makedirs(others_dir)
        print(f'created folder for others.')


def set_token_to_file(test_data_token, output_dir):
    """
    Creates a CSV file based on the provided token and stores it in the correct subfolder
    under 'output/<prefix>/' where <prefix> is the first letter of the key in the dictionary.
    
    :param test_data_token: Dictionary containing the data for the file. Example: {'cat': "documentid,99"}
    """
    for key, value in test_data_token.items():

        # Determine the folder prefix from the first letter of the key (e.g., 'c' for 'cat')
        folder_prefix = key[0].lower()
        if folder_prefix.isdigit(): folder_prefix = "numbers"

        # folder_path = os.path.join(get_output_dir_from_config(), folder_prefix)
        folder_path = os.path.join(output_dir, folder_prefix)

        if not os.path.isdir(folder_path): 
            print(f"\nwarning: the following token does not start with a valid character: {key}. skipping token.\n")
            continue
        
        # Prepare the filename and the content for the CSV file
        filename = os.path.join(folder_path, f"{key}.csv")
   
        # Write the content to the CSV file
        try:
            with open(filename, mode='a+', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(value) 
        except:
            print(f"there was a problem with writing to the file: {filename}.")

def set_token_to_file_2(test_data_token, output_dir):
    """
    Creates a CSV file based on the provided token and stores it in the correct subfolder
    under 'output/<prefix>/' where <prefix> is the first letter of the key in the dictionary.
    
    :param test_data_token: Dictionary containing the data for the file. Example: {'cat': "documentid,99"}
    """
    for key, value in test_data_token.items():

        # Determine the folder prefix from the first letter of the key (e.g., 'c' for 'cat')
        folder_prefix = key[0].lower()
        if folder_prefix.isdigit(): folder_prefix = "numbers"

        # folder_path = os.path.join(get_output_dir_from_config(), folder_prefix)
        folder_path = os.path.join(output_dir, folder_prefix)

        if not os.path.isdir(folder_path): 
            # folder_path = os.path.join(get_output_dir_from_config(), "others")
            folder_path = os.path.join(output_dir, "others")
        
        # Prepare the filename and the content for the CSV file
        filename = os.path.join(folder_path, f"{get_file_name_hash_value(key)}.csv")
   
        # Write the content to the CSV file
        try:
            with open(filename, mode='a+', newline='') as file:
                writer = csv.writer(file)
                writer.writerows([ posting.values() for posting in value ]) 
        except Exception as e:
            print(type(value))
            print(value)
            print(e)
            print(f"there was a problem with writing to the file: {filename}.")

def sort_csv_files(output_dir):

    # Loop through each folder under the 'output' directory
    # output_dir = get_output_dir_from_config()
    for folder in os.listdir(output_dir):
        folder_path = os.path.join(output_dir, folder)
        
        if os.path.isdir(folder_path):
            # Loop through each file in the folder
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)
                
                if file_name.endswith('.csv') and os.path.isfile(file_path):
                    # Read and sort the CSV file by the second column
                    sorted_rows = []
                    with open(file_path, mode='r', newline='', encoding='utf-8') as f:
                        csv_reader = csv.reader(f)
                        header = next(csv_reader)  # Assuming first row is the header
                        sorted_rows = sorted(csv_reader, key=lambda row: int(row[1]))

                    # Create a new file to write the sorted data
                    output_file_path = os.path.join(folder_path, f"sorted_{file_name}")
                    with open(output_file_path, mode='w', newline='', encoding='utf-8') as f:
                        csv_writer = csv.writer(f)
                        # Write the header followed by the sorted rows
                        csv_writer.writerow(header)
                        csv_writer.writerows(sorted_rows)

                    print(f"Sorted file created: {output_file_path}")
                    

def update_posting_duplicates_and_sort(output_dir):

    postings = {}

    # Loop through each folder under the 'output' directory
    # output_dir = get_output_dir_from_config()
    for folder in os.listdir(output_dir):
        folder_path = os.path.join(output_dir, folder)
        
        if os.path.isdir(folder_path):
            # Loop through each file in the folder
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)

                if file_name.endswith('.csv') and os.path.isfile(file_path):

                    with open(file_path, mode='r', newline='', encoding='utf-8') as f:
                        csv_reader = csv.reader(f)
                        for row in csv_reader:
                            postings[row[0]] = (row[1], row[2])



                    with open(file_path, mode='w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)

                        rows = []
                        for doc_id, values in postings.items():
                            rows.append((doc_id, values[0], values[1]))

                        sorted_rows = sorted(rows, key=lambda x: float(x[1]), reverse=True)

                        writer.writerows(sorted_rows)

def postings_from_file(token, output_dir):
    # Determine the folder prefix from the first letter of the token (e.g., 'c' for 'cat')
    folder_prefix = token[0].lower()
    if folder_prefix.isdigit(): folder_prefix = "numbers"

    folder_path = os.path.join(output_dir, folder_prefix)

    if not os.path.isdir(folder_path): 
        folder_path = os.path.join(output_dir, "others")
    
    # Prepare the filename and the content for the CSV file
    filename = os.path.join(folder_path, f"{get_file_name_hash_value(token)}.csv")

    postings = []
    if os.path.exists(filename):
        with open(filename, mode='r', newline='', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            # TODO: For now it reads all of them. 
            # If the query time is too slow, then consider retreiving less...
            docid = row[0].replace("./DEV/", "", 1)
            for row in csv_reader:
                postings.append({
                    'doc_id': docid,
                    'tfidf': round(row[2], 2),
                    })

    return postings


def remove_current_index(output_dir): 
    ''' remake index saved to file '''
    
    ans = input("warning!!! are you sure you want to purge the output directory and all its contents? (y/n)\n")
    
    if ans=='y': 
        if os.path.isdir(output_dir): 
            print('removing directory')
            shutil.rmtree(output_dir)
            print('directory removed.')
    else: 
        print('preserving the directory.')
