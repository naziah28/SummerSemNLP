import pandas as pd 
import json
import gzip
import shutil
import glob 
from langdetect import detect 

def save_authors_df(dataframe, outdir):
    df = dataframe
    authors = []
    for i in df.authors:
        authors.extend(i)
        
    author_df = pd.DataFrame.from_dict(authors)
    
    author_df = author_df.dropna(subset=['ids'])
    author_df.ids = author_df.ids.str[0]
    
    author_df = author_df.dropna() # probably unnecessary 
    author_df.ids.iloc[:] = author_df.ids.astype(int)
    
    author_df.to_csv(outdir+'authors.csv')

def get_train_df(dir_path, limit=1, lang='en'):
	train_files = sorted(glob.glob(dir_path+"s2-corpus-*.gz"))
	print("Found {} files. Reading {}.".format(len(train_files), limit))

	lines = []
	# Load dataframe for all papers 
	for filepath in train_files[:limit]:
	    print("Reading {}".format(filepath))
	    with gzip.open(filepath, 'rb') as f_in:
	        print(f_in)

	        # unzip, but not necessary 
	        # with open(filepath.strip('.gz'), 'wb') as f_out:
	        #     shutil.copyfileobj(f_in, f_out)

	        for cnt, line in enumerate(f_in):
	        	try: 
		            lines.append(json.loads(line))
		        except: # any line errors 
		        	pass 

	print('read in {}. entities'.format(len(lines)))

	# Create dataframe 
	train_df = pd.DataFrame.from_dict(lines)

	# remove any entities without abstracts  
	train_df = train_df[train_df.paperAbstract != '']

	# remove any that aren't of language lang: 
	train_df = train_df[[detect(i) =='en' for i in train_df.title]]

	print(train_df.head())

	return train_df

df = get_train_df('data/papers/')
save_authors_df(df, 'data/papers/')

