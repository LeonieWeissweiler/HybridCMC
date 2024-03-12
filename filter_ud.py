# %%
# %%
import pandas as pd
import spacy
import re
import os
import csv
import sys
from tqdm import tqdm


adpositions = ['across', 'at', 'down', 'from', 'in', 'inside', 'into', 'near', 'off', 'off of', 'on', 'onto', 'out of', 'out to', 'outside', 'outside of', 'over', 'through', 'to', 'towards', 'under', 'up', 'within']

def check_dobj(i, dependencies, verb):
    if i >= len(dependencies):
        return None
    text, lemma, pos, dep, head, head_i = dependencies[i]

    if pos in ['VERB', 'AUX']:
        return None
    
    if dep == 'ROOT':
        return None

    if dep == 'dobj' and head == verb:
        return i

    return check_dobj(head_i, dependencies, verb) 


def check_adp(start_i, dependencies, adpositions): #note that this does not include dependencies because spacy can't be trusted on those
    if start_i + 1 < len(dependencies):
        longer_adp = dependencies[start_i][0] + " " + dependencies[start_i + 1][0]
        if longer_adp in adpositions:
            return ([start_i, start_i + 1], longer_adp)
    
    if start_i < len(dependencies) and dependencies[start_i][0] in adpositions:
        return ([start_i], dependencies[start_i][1])

    return (None, None)

def check_pobj(dependencies, adp_index):
    pobj_token_list = [(lemma, pobj_i) for pobj_i, (text, lemma, pos, dep, head, head_i) in enumerate(dependencies) if head_i in adp_index and dep == 'pobj' and pos not in ['NUM', 'PUNCT']]
    if len(pobj_token_list) > 1:
        raise Exception
    if len(pobj_token_list) == 1:
        return pobj_token_list[0]
    return (None, None)

# Why     why     SCONJ   advmod  is      1
def is_cm(dependencies, nlp, sentence):
    for i, (token_text, lemma, pos, dep, head, head_i) in enumerate(dependencies):
        if pos == 'VERB':
            # Look for a direct object (dobj) immediately following the verb
            dobj_i = check_dobj(i + 1, dependencies, token_text)
            if dobj_i:
                dobj_lemma = dependencies[dobj_i][1]
                # Check if the following token is an adposition (ADP) from the list
                (adp_index, adp_lemma) = check_adp(dobj_i + 1, dependencies, adpositions)
                if adp_index:
                    # do another spacy dependency parse
                    doc = nlp(sentence)
                    dependencies = [(token.text, token.lemma_, token.pos_, token.dep_, token.head.text, token.head.i) for token in doc]
                    pobj_lemma, pobj_i = check_pobj(dependencies, adp_index)
                    if pobj_lemma:
                        result_dict = {'verb': lemma, 'verb_i' : i, 'direct_object': dobj_lemma, 'direct_object_i': dobj_i, 'preposition': adp_lemma, 'preposition_i': adp_index, 'prepositional_object': pobj_lemma, 'prepositional_object_i': pobj_i}
                        return result_dict
                    # Look for a prepositional object (pobj) child of the adp



    return None

# %%
COLUMNS = ['sentence', 'verb', 'direct_object', 'preposition', 'prepositional_object' , 'verb_i', 'direct_object_i', 'preposition_i', 'prepositional_object_i']
IN_FOLDER = None
OUT_FOLDER = None

# %%
def sentence_reader(file_path):
    # read in the file line by line and yield sentences
    with open(IN_FOLDER+ file_path, 'r') as f:
        current_sentence = []
        for line in f:
            if line.strip():  # If the line is not empty
                parts = line.strip().split('\t')
                if line[0] == " ":
                    parts = [" ", " "] + parts
                parts[-1] = int(parts[-1])
                word_info = tuple(parts)  # Create a tuple excluding the last column (index information)
                current_sentence.append(word_info)
            else:
                if current_sentence:
                    yield current_sentence
                    current_sentence = []  # Reset for the next sentence
                    
def recover_sentence(dependencies):
    try:
        sentence = " ".join([text for text, lemma, pos, dep, head, head_i in dependencies])
    except:
        print(dependencies)
        raise Exception
    return sentence



# %%
import time
import os
def candidate_sentences(sentence_dependencies_iterator, nlp):
    for dependencies in sentence_dependencies_iterator:
        sentence = recover_sentence(dependencies)
        try:
            construction_dict = is_cm(dependencies, nlp, sentence)
            if construction_dict:
                construction_dict['sentence'] = sentence
                yield construction_dict
        except Exception as e:
            pass
        
def process_file(file_name):
    nlp = spacy.load("en_core_web_sm")
    
    # Check if the output file already exists
    if os.path.exists(OUT_FOLDER + file_name):
        print(file_name, "already exists. Quitting...")
        return
    
    text_iterator = sentence_reader(file_name)
    candidates = candidate_sentences(text_iterator, nlp)
    
    with open(OUT_FOLDER + file_name, mode='w') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(COLUMNS)

        for candidate in tqdm(candidates):
            values = [candidate[k] for k in COLUMNS]  # correct order
            writer.writerow(values)


# %%
import os
def files_not_modified_within_hour(folder_path):
    one_hour_ago = time.time() - 3600  # 3600 seconds = 1 hour
    txt_files_in_folder = [f for f in os.listdir(folder_path) if f.endswith('.txt')]
    not_modified_files = [f for f in txt_files_in_folder if os.path.getmtime(os.path.join(folder_path, f)) <= one_hour_ago]
    return not_modified_files

# Usage
folder_to_check = None
not_modified_files_list = files_not_modified_within_hour(folder_to_check)

# iterate in parallel over the files
from multiprocessing import Pool
print(os.cpu_count())
with Pool(os.cpu_count()) as p:
    p.map(process_file, not_modified_files_list)



