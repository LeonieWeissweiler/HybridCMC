# %%
import pandas as pd
import spacy

import re
from tqdm import tqdm
import os
from itertools import chain
import sys
import pandas as pd
import lzma
import bz2
import os
import zstandard as zstd
import io
import csv
import json


CHUNK_SIZE = 100

def open_zst(file_path):
    dctx = zstd.ZstdDecompressor()
    with open(file_path, 'rb') as f:
        decompressor = dctx.stream_reader(f)
        stream_reader = io.BufferedReader(decompressor)
        while True:
            line = stream_reader.readline()
            if not line:
                break
            yield line.decode('utf-8')

def json_chunks_iterator_custom(file, chunk_size):
    chunk = []
    for line in file:
        chunk.append(json.loads(line))
        if len(chunk) >= chunk_size:
            for line in chunk:
                yield line
            chunk = []
    if chunk:  # handle the last chunk which can be less than chunk_size
        for line in chunk:
            yield line



# %%
import re
def remove_markdown(text: str) -> str:
    # Remove headings
    text = re.sub(r"^#.*$", "", text, flags=re.MULTILINE)
    
    # Remove bold and italic formatting
    text = re.sub(r"(\*\*|__|_|\*|\~\~)", "", text)
    
    # Remove inline code
    text = re.sub(r"`[^`]*`", "", text)
    
    # Remove blockquotes
    text = re.sub(r"^>.*$", "", text, flags=re.MULTILINE)
    
    # Remove code blocks
    text = re.sub(r"```[^`]*```", "", text, flags=re.MULTILINE)
    
    # Remove links and images
    text = re.sub(r"!\[.*\]\(.*\)|\[(.*)\]\(.*\)", r"\1", text)
    
    # Remove horizontal rules
    text = re.sub(r"^([-*_]\s*){3,}$", "", text, flags=re.MULTILINE)
    
    # Remove HTML tags
    text = re.sub(r"<[^>]*>", "", text)

    # Remove extra whitespace and newlines
    text = re.sub(r"\s+", " ", text).strip()

    #remove urls
    text = re.sub(r'(https?:\/\/)?(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,4}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)', "", text)

    #remove [deleted] and [removed]
    text = re.sub(r'\[deleted\]|\[removed\]', "", text)

    return text

# create an empty dataframe with columns for the information you want to store
reddit_path = None

out_path = None
year = sys.argv[1]
month = sys.argv[2]
COMMENTS_FILE_PATH = reddit_path + year + '/RC_' + year + '-' + month
OUTPUT_FILE_PATH = out_path + year + '_' + month + '_parsed.txt'
os.makedirs(out_path, exist_ok=True)


bz2_file_path = COMMENTS_FILE_PATH + '.bz2'
xz_file_path = COMMENTS_FILE_PATH + '.xz'
zst_file_path = COMMENTS_FILE_PATH + '.zst'

if os.path.exists(bz2_file_path):
  file = bz2.open(bz2_file_path, 'rt')    
  json_chunks_iterator = pd.read_json(file, lines=True, dtype=False, chunksize=100)    
  json_chunks_texts_iterator = (c["body"] for c in json_chunks_iterator)
  json_texts_iterator = chain.from_iterable(json_chunks_texts_iterator)
elif os.path.exists(xz_file_path):
  file = lzma.open(xz_file_path, 'r')
  json_chunks_iterator = pd.read_json(file, lines=True, dtype=False, chunksize=100)
  json_chunks_texts_iterator = (c["body"] for c in json_chunks_iterator)
  json_texts_iterator = chain.from_iterable(json_chunks_texts_iterator)
elif os.path.exists(zst_file_path):
  file = open_zst(zst_file_path)
  json_chunks_iterator = json_chunks_iterator_custom(file, CHUNK_SIZE)
  json_chunks_texts_iterator = (c["body"] for c in json_chunks_iterator)
  json_texts_iterator = json_chunks_texts_iterator
else:
  raise FileNotFoundError("File not found: {}".format(COMMENTS_FILE_PATH))


json_texts_clean_iterator = (remove_markdown(txt) for txt in json_texts_iterator)

# Load Spacy model
nlp = spacy.load("en_core_web_sm")

# Create a generator for sentence segmentation and analysis
def parsing_iterator(text_iterator):
    for text in text_iterator:
        doc = nlp(text)
        text_length_so_far = 0
        for sent in doc.sents:
          dependencies = [(token.text, token.lemma_, token.pos_, token.dep_, token.head.text, token.head.i-text_length_so_far) for token in sent]
          text_length_so_far += len(sent)
          # throw out if only one word or two words but one is PUNCT
          if len(dependencies) > 2 or (len(dependencies) == 2 and dependencies[0][2] != 'PUNCT' and dependencies[1][2] != 'PUNCT'):
            yield (dependencies)



sentence_dependencies = parsing_iterator(json_texts_clean_iterator)

# 4. Do whatever you want with the data
# Everything is already processed, now you can iterate, and save stuff for example to disk
# This will ONLY iterate over things it identified as caused motion
# create an empty dataframe with columns for the information you want to store
with open(OUTPUT_FILE_PATH, mode='w') as f:

  for sentence in sentence_dependencies:
    for word in sentence:
      f.write('\t'.join([str(info) for info in word]) + '\n')
    f.write('\n')
