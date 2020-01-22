# Summer Semester Project: - BERT for Scientific Literature Mining

Project Link: https://student.eait.uq.edu.au/projects/?act=project_detail&id=4128
Project Report: link to view only google doc? or overleaf document? 


# Repository Structure: 

# Data: 
To Download the data, follow the instructions here: http://api.semanticscholar.org/corpus/download/. tl;dr: The preferred method is through AWS CLI (link): 

`aws s3 cp --no-sign-request --recursive s3://ai2-s2-research-public/open-corpus/2020-01-13/ destinationPath`

## Unzipping: 
Data downloaded from AWS is gzipped which may be unzipped as below: 

`python3 unzip_files --args`

Run with `-d` if you wish to delete the files after they have been unzipped (for device storage constraints)

## Preprocess Data: 
Corpus data is first preprocessed to convert the json formats into pandas dataframes. Due to the volume of the corpus of the dataset. The complete dataset is preprocessed in batches of a number of your choosing.

To preprocess in batches, run: 

`python3 data_loader.py --- args`

Each batch is saved as a separate dataframe. A reference file for the batches preprocessed is saved as a dictionary too as a JSON file. 

### Indexing data 
During preprocessing, the data maybe be indexed for a certain source papers have been added from. This is currently an option between either `Medline` or `DBLP`, medicine and computer and engineering papers respectively. To index with a source option, run: 

`python3 data_loader.py --- args`

Default value of `--sources` is set to None which does not index the dataset. 

## Creating Authors Dataframe
A dataframe of the contributing authors and their repectivive author IDs and paper IDs is compiled and saved in the provided output directory too.   

# Visualisation + Experimentation
During data exploration, I found it useful to run my experiments in a notebook. To connect `jupyter-notebook` locally to your chosen cluster, run: 

`ssh command` 

When logging into the cluster. Then when running Jupyter Notebook on the cluster, run: 

`jupyter-notebook --port`

Finally load up `localhost:\\{localport}`
