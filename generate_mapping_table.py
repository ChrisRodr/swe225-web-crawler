import os
import json

def generate_mapping_table(directory: str, output_file: str):
    # Prepare the mapping table (index to file path)
    mapping_table = {}

    # Walk through the directory and its subdirectories
    index = 1
    for root, dirs, files in os.walk(directory):
        # Filter only .json files
        json_files = [f for f in files if f.endswith('.json')]
        
        # Add each file to the mapping table
        for file in json_files:
            file_path = os.path.join(root, file)
            mapping_table[index] = file_path
            index += 1

    # Write the mapping table to the output file
    with open(output_file, 'w') as outfile:
        for index, file_path in mapping_table.items():
            outfile.write(f"{index} {file_path}\n")

    print(f"Mapping table has been written to {output_file}")

###############################################################

# MAIN

###############################################################
def load_mapping_table(mapping_file: str):
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
                mapping_dict[file_path] = id
    
    return mapping_dict

def main():
    directory = './DEV'  # Root directory where to start searching
    output_file = 'mapping_table.txt'  # Output file to store the mapping
    generate_mapping_table(directory, output_file)
    #mapping_file = 'mapping_table.txt'
    #target = load_mapping_table(mapping_file)
    #print(target[1])
    #print(target['./DEV/xtune_ics_uci_edu/da5aff1b5ca2bad6609f97f11c91fef3a503ded6d9d0f14592793c9391b92fd9.json'])

if __name__ == "__main__":
    main()
