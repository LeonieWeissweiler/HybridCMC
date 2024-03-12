import pandas as pd
import os
from tqdm import tqdm
import sys

last_year = int(sys.argv[1])
in_folder = None
out_folder = None
#create out_folder if necessary
if not os.path.exists(out_folder):
    os.makedirs(out_folder)
# files are csv in format sentence,verb,direct_object,preposition,prepositional_object,direct_object_i,preposition_i,object_i
# read in all files into one dataframe
# make sure to check that they are files and not folders

df = pd.DataFrame()
for file in tqdm(os.listdir(in_folder)):
    if os.path.isfile(os.path.join(in_folder,file)):
        path_year = int(file[:4])
        if path_year > last_year:
            continue
        # custom read in function
        # the headers are in the first line
        # the separator is a comma
        try:
            new_df = pd.read_csv(os.path.join(in_folder,file), header=0, sep="\t", keep_default_na=False)
        except Exception as e:
            print(file, e)
            continue
        df = pd.concat([df, new_df])


# %%
# drop duplicates 
df.drop_duplicates(inplace=True)

# drop verbs with non-alphabetical characters
df = df[df.verb.str.isalpha()]



# %%
# group dataframe by verb
grouped = df.groupby("verb")

# create out_folder if it doesn't exist already
if not os.path.exists(out_folder):
    os.makedirs(out_folder)

# %%
# create a dataframe for each verb
for verb, group in tqdm(grouped):
    # skip if file already exists
    if os.path.exists(os.path.join(out_folder,verb+".tsv")):
        continue
    try:
        group.to_csv(os.path.join(out_folder,verb+".tsv"), index=False, sep="\t")
    except:
        print(verb)
        continue
    # grouped_combination = group.groupby(["direct_object", "preposition"])

