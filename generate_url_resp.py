# check URL responses from pages listed in mapping_table.txt

import os
import requests
import pandas as pd
import json
from tqdm import tqdm
import time

from utils.file_handler import load_mapping_table

politeness_delay = 0.5

mapping_table = load_mapping_table('mapping_table.txt')
resp = []

unsecure = 0
for i, path in enumerate(tqdm(mapping_table)):

    with open(path, 'r') as file:
        data = json.load(file)
    url = data.get('url')

    # check response
    try: 
        status_code = requests.get(url).status_code
    except:
        status_code = -1
        unsecure += 1

    with open('url_responses_40.txt', 'a+') as f:
        f.write(f'{i+1} {status_code}\n')

    time.sleep(politeness_delay)


print('number of unsecure links:', unsecure)