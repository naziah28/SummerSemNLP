import pandas as pd 
import json
import gzip
import shutil
import glob 
import os 
from langdetect import detect 

def uncompress_and_delete(dir_path, limit): 
	train_files = sorted(glob.glob(dir_path+"s2-corpus-*.gz"))
	print("Found {} files. Reading {}.".format(len(train_files), limit))

	lines = []
	# Load dataframe for all papers
	if limit == -1: 
		limit = len(train_files)-1  
	for filepath in train_files[:limit]:
		print("Reading {}".format(filepath))
		with gzip.open(filepath, 'rb') as f_in:
			with open(filepath.strip('.gz'), 'wb') as f_out:
				shutil.copyfileobj(f_in, f_out) 
		print("removing {}".format(filepath))

		os.remove(filepath)


def save_and_get_authors_df(dataframe, outdir):
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
    return author_df

def save_df(df, outfile): 
    df.to_csv(outfile + ".csv")
    df.to_parquet(outfile + ".parquet")


def get_train_df(dir_path, limit=-1, lang='en'):
	train_files = sorted(glob.glob(dir_path+"s2-corpus-*"))
	print("Found {} files. Reading {}.".format(len(train_files), limit))

	lines = []
	# Load dataframe for all papers
	if limit == -1: 
		limit = len(train_files)-1  
	for filepath in train_files[:limit]:
	    print("Reading {}".format(filepath))
	    with open(filepath, 'rb') as f_in:
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
	print('Creating training DataFrame')
	train_df = pd.DataFrame.from_dict(lines)

	# remove any entities without abstracts
	print('Removing null abstracts')  
	train_df = train_df[train_df.paperAbstract != '']

	# remove any that aren't of language lang:
	print('Only keeping {} language titles'.format(lang)) 
	train_df = train_df.head(20)
	train_df = train_df[[detect(i) =='en' for i in train_df.title]]

	print('Complete!')
	print(train_df.head())
	print(train_df.shape)

	save_df(dir_path + "train_df")

	return train_df


# uncompress_and_delete('/media/bigdata/s4431520/data/', limit=5)
df = get_train_df('/media/bigdata/s4431520/data/', limit=1)
# save_authors_df(df, 'data/papers/')

