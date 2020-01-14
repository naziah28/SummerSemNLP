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
    df.to_csv(outfile + ".csv", compression='gzip')

    df.to_parquet(outfile + ".parquet", compression='gzip')


def get_train_df(dir_path, limit=-1, lang='en'):
	train_files = sorted(glob.glob(dir_path+"s2-corpus-*"))
	print("Found {} files. Reading {}.".format(len(train_files), limit))

	lines = []
	# Load dataframe for all papers
	if limit == -1: 
		limit = len(train_files)  
	for filepath in train_files[:limit]:
	    print("Reading {}".format(filepath))
	    with open(filepath, 'rb') as f_in:
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
        train_df = train_df[[detect(i) =='en' for i in train_df.title]]

	print('Complete!')
	print(train_df.head())

	save_df(train_df, dir_path + "preprocessed_df")

	return train_df


# uncompress_and_delete('/media/bigdata/s4431520/data/', limit=5)
for i in range(18):
    df = get_train_df('/media/bigdata/s4431520/data/s{}/'.format(i), limit=-1)
# df = get_train_df('data/papers/', limit=1)

# save_authors_df(df, 'data/papers/')




# def main(imdir, outdir, _lambda, kappa, beta_max):

#     # load image into array 
#     tf_img = tf.keras.preprocessing.image.load_img(imdir)
#     img_arr = np.array(tf_img)

#     # pass image and calculate and output gradient smoothing 
#     out_img = l0_calc(img_arr, _lambda, kappa, beta_max)
    
#     # save image from output array 
#     im = Image.fromarray(out_img.astype(np.uint8))
#     im.save(outdir)


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description="Semantic Scholar Corpus Data preprocessing")
#     parser.add_argument("-p", "--raw_gzip", dest="rawdir",
#                         help="Directory path for input gzip raw data",
#                         metavar="FILE", default='example/dahlia.png')
#     parser.add_argument("-d", "--unzipped_data", dest="outdir",
#                         help="Directory path for output image",
#                         metavar="FILE", default="example/dahlia_out.png")
#     parser.add_argument("-l", "--lamdaval", dest="lamdaval",
#                         help="lambda parameter",
#                         metavar="FLOAT", default=2e-2)
#     parser.add_argument("-k", "--kappa", dest="kappa",
#                         help="kappa parameter",
#                         metavar="FLOAT", default=2.0)
#     parser.add_argument("-b", "--beta_max", dest="beta_max",
#                         help="beta max parameter",
#                         metavar="FLOAT", default=1e5)


#     args = parser.parse_args()

#     main(args.imgdir, 
#         args.outdir, 
#         float(args.lamdaval), 
#         float(args.kappa), 
#         float(args.beta_max))

