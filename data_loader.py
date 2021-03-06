import pandas as pd
import dask.dataframe as dd 
import numpy as np
import json
import gzip
import shutil
import glob 
import os 
import math 
from langdetect import detect 

JOURNALS = ["Advanced Materials",
"IEEE/CVF Conference on Computer Vision and Pattern Recognition",
"Energy & Environmental Science",
"ACS Nano",
"Nano Letters",
"Nature Materials",
"Renewable and Sustainable Energy Reviews",
"Neural Information Processing Systems (NIPS)",
"Journal of Materials Chemistry. A",
"Nature Nanotechnology",
"Advanced Functional Materials",
"Advanced Energy Materials",
"International Conference on Learning Representations",
"Nature Photonics",
"ACS Applied Materials & Interfaces",
"Chemistry of Materials",
"Nanoscale",
"European Conference on Computer Vision",
"International Conference on Machine Learning (ICML)",
"Journal of Cleaner Production"]


COLUMNS = ['entities', 'year', 'outCitations', 
'id', 'authors', 'journalName', 'paperAbstract', 
'inCitations', 'title', 'doi', 'venue']

def uncompress_source_data(dir_path, delete=False, limit=-1): 
    train_files = sorted(glob.glob(dir_path+"s2-corpus-*.gz"))
    print("Found {} files. Reading {}.".format(len(train_files), limit))

    lines = []
    # Load dataframe for all papers
    if limit == -1: 
        limit = len(train_files)  
    for filepath in train_files[:limit]:
        print("Reading {}".format(filepath))
        with gzip.open(filepath, 'rb') as f_in:
            with open(filepath.strip('.gz'), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out) 
        print("removing {}".format(filepath))
        
        if delete: 
            os.remove(filepath)


def save_and_get_authors_df(df, outdir):
    authors = []
    for i in df.authors: 
        string_dict = i.replace("'", '"')
        try: 
            author_dict = json.loads(string_dict)
            authors.extend(author_dict)
        except: # since i replace ' with ", this impaces names with ' in them
            pass

    author_df = pd.DataFrame.from_dict(authors)
    author_df = author_df.dropna() # probably unnecessary 
    
    print(author_df.head())
    
    # Convert id list to string and remove duplicates 
    author_df["ids"] = author_df.ids.astype(str)    
    author_df = author_df[~author_df.duplicated()]
    df = df.reset_index(drop=True)
    
    author_df.to_csv(outdir+'authors.csv')
    return author_df


def load_and_save_to_df(dir_path, limit=10, reps=-1):
	train_files = sorted(glob.glob(dir_path+"s2-corpus-*"))
	print("Found {} files. Reading {}.".format(len(train_files), limit))

	if reps == -1: 
		reps = math.ceil(len(train_files)/limit)

	train_files_indexed = np.array(train_files)
	num_splits = np.ceil(train_files_indexed.shape[0]/limit)
	train_files_indexed = np.array_split(train_files_indexed, num_splits)

	train_files_indexed = train_files_indexed[:reps]

	print(train_files_indexed)
	print(reps)

	for rep in range(reps): 
		lines = []
		for idx, filepath in enumerate(train_files_indexed[rep][:limit]):
			
			try: # Sometimes errors out on the filepath for some reason 
				with open(filepath, 'rb') as f_in:
					print("Reading {}".format(filepath))

					for cnt, line in enumerate(f_in):
						try: 
							lines.append(json.loads(line))
						except: # any line errors 
							pass 
			except: 
				pass 

		print('read in {}. entities'.format(len(lines)))
		csvpath = "{}raw_df_rep{}.csv".format(dir_path, rep)


		# Create dataframe 
		print('Creating training DataFrame')
		df = pd.DataFrame.from_dict(lines)


		df = preprocess_df(df)
		saveto = "{}preprocessed_df_rep{}.csv".format(dir_path, rep)
		df.to_csv(saveto, compression='gzip')


def combine_preprocessed_df(dir_path):
    preprocessed_files = sorted(glob.glob(dir_path+"preprocessed_df_rep*.csv"))
    print("Found {} files.".format(len(preprocessed_files)))

    df_p = []

    for file in preprocessed_files: 
        print("Reading {}".format(file))
        df_p.append(dd.read_csv(file, compression='gzip', engine='python', blocksize=None, dtype={'Unnamed: 0': 'object',
       'year': 'object'}))

    df = dd.concat(df_p) 
    return df 

def index_important_columns(df, cols=COLUMNS): 
    df = df[cols]
    return df 

def filter_language(df, lang='en'): 
    # remove any that aren't of language lang:
    # dont need since assume DBLP is english  (??) 
    # print('Only keeping {} language titles'.format(lang)) 
    df = df[[detect(i) =='en' for i in df.title]]
    return df 


def preprocess_df(df):

    # remove any entities without abstracts
    print('Removing null abstracts')  
    df = df[df.paperAbstract != '']

    # remove all non comp sci papers
    # DBLP (compsci bibliography): https://dblp.uni-trier.de/

    # this line below is not reliable, assumes that sources is a 
    # list of only one element
    df["sources_parsed"] = [i[0] if len(i)>0 else "" for i in df.sources]
    df = df[df.sources_parsed == 'DBLP']
    
    print('Completed preprocessing')
    print(df.head())
    return df




# uncompress_source_data('/media/bigdata/s4431520/data/zipped/', delete=True, limit=-1)
# df = load_and_save_to_df('/media/bigdata/s4431520/data/', limit=10, reps=-1)
# df = preprocess_df(df)
# df = combine_preprocessed_df('/media/bigdata/s4431520/data/')
# df = index_important_columns(df, COLUMNS)





def main(datadir, outdir, unzip, deletezip):
    if unzip: 
        uncompress_and_delete(datadir, deletezip)

    df = load_to_df(datadir, limit=2, reps=1)
    df = preprocess_df
    save_df(df)



# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description="Semantic Scholar Corpus Data preprocessing")
#     parser.add_argument("-d", "--datadir", dest="rawdir",
#                         help="Directory path for input data (unzipped or zipped)",
#                         metavar="FILE", default='example/dahlia.png')
#     parser.add_argument("-o", "--outdir", dest="outdir",
#                         help="Directory path for output dataframe",
#                         metavar="FILE", default="example/dahlia_out.png")
#     parser.add_argument("-u", "--unzip", dest="unzip",
#                         help="if files in datadir need to be unzipped",
#                         metavar="FLOAT", default=2e-2)
#     parser.add_argument("-r", "--deletezip", dest="deletezip",
#                         help="if zipped files in datadir are to be deleted \
#                         (due to storage constraints)",
#                         metavar="FLOAT", default=2.0)


#     args = parser.parse_args()

#     main(args.datadir, 
#         args.outdir, 
#         float(args.unzip), 
#         float(args.deletezip))

