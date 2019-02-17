
# coding: utf-8

# Download the data from the page and load to Pandas DataFrame

# In[1]:

import csv
import pandas as pd
import os
import requests

from tqdm.auto import tqdm


# In[15]:


data_folder = './imdb'

if not os.path.exists(data_folder):
    os.mkdir(data_folder)

def fetch_or_resume(url, filename):
    block_size = 1024
    wrote = 0

    # Connecto to server
    headers = {}
    response = requests.get(url, headers=headers, stream=True)
    total_size = int(response.headers.get('content-length'))

    if os.path.exists(filename):
        print("File {} already exists".format(filename))

        # Check file size
        filename_size = os.path.getsize(filename)
        print(filename_size, total_size)
        if filename_size == total_size:
            print("Warning, No downloading, the file {} has the required size.".format(filename))
            return

    # Download file
    with open(filename, 'wb') as file:
        for data in tqdm(iterable = response.iter_content(chunk_size = block_size),
                                  total = total_size//block_size,
                                  desc = os.path.basename(url),
                                  unit = 'KB'):
            wrote = wrote  + len(data)
            file.write(data)

        if total_size != 0 and wrote != total_size:
            print("ERROR, something went wrong")

def get_imdb_dataframe(url, download=False, dtypes=None):
    base = os.path.basename(url)
    filename = os.path.join(data_folder, base)
    if download:
        fetch_or_resume(url, filename)

    return pd.read_csv(filename, sep='\t',  dtype=dtypes,
                        na_values={'\\N'}, quoting=csv.QUOTE_NONE)

def name_basics_df():
    return get_imdb_dataframe('https://datasets.imdbws.com/name.basics.tsv.gz')

def title_episode_df():
    return get_imdb_dataframe('https://datasets.imdbws.com/title.episode.tsv.gz')

def title_principals_df():
    url = 'https://datasets.imdbws.com/title.principals.tsv.gz'
    dtypes = {
        'ordering': 'uint8',
        'category': 'category',
        'job': 'category',
    }
    return get_imdb_dataframe(url=url, dtypes=dtypes)

def title_ratings_df():
    return get_imdb_dataframe('https://datasets.imdbws.com/title.ratings.tsv.gz')

def title_akas_df():
    return get_imdb_dataframe('https://datasets.imdbws.com/title.akas.tsv.gz')

def title_basics_df():
    return get_imdb_dataframe('https://datasets.imdbws.com/title.basics.tsv.gz')

def title_crew_df():
    return get_imdb_dataframe('https://datasets.imdbws.com/title.crew.tsv.gz')
