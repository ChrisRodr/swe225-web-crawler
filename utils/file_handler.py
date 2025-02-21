import os
import csv
import configparser

def get_output_dir_from_config(config_file='config.ini'):
    """
    Reads the output_dir from the config file.
    
    :param config_file: Path to the configuration file (default is 'config.ini').
    :return: The output directory as specified in the config file.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    return config.get('settings', 'output_dir', fallback='output') 


def create_folders_for_alphabet():
    """
    Creates folders for each letter from A to Z under the specified output directory.
    
    :param output_dir: The base directory under which the folders will be created (default is 'output').
    """
    # Check if the output directory exists, if not, create it
    output_dir = get_output_dir_from_config()

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created base directory: {output_dir}")
    
    # Loop through A to Z and create each folder
    for letter in range(97, 123):  
        folder_name = chr(letter)
        folder_path = os.path.join(output_dir, folder_name)
        
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f"Created folder: {folder_path}")
        else:
            print(f"Folder already exists: {folder_path}")



def set_token_to_file(test_data_token):
    """
    Creates a CSV file based on the provided token and stores it in the correct subfolder
    under 'output/<prefix>/' where <prefix> is the first letter of the key in the dictionary.
    
    :param test_data_token: Dictionary containing the data for the file. Example: {'cat': "documentid,99"}
    """
    for key, value in test_data_token.items():
        # Determine the folder prefix from the first letter of the key (e.g., 'c' for 'cat')
        folder_prefix = key[0].lower()
        folder_path = os.path.join(get_output_dir_from_config(), folder_prefix)
        
        # I think we dont need it 
        # if not os.path.exists(folder_path):
        #     os.makedirs(folder_path)
        
        # Prepare the filename and the content for the CSV file
        filename = os.path.join(folder_path, f"{key}.csv")
        content = value.split(",") 

        # Write the content to the CSV file
        with open(filename, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(content) 
        
        print(f"File created: {filename}")


def sort_csv_files():

    # Loop through each folder under the 'output' directory
    output_dir = get_output_dir_from_config()
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
                    
