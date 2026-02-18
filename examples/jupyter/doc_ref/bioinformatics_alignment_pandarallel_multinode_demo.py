# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # Multi-Node Bioinformatics Alignment `pandarallel` Examples with Controlled Number of Progress Bars
#
# In the bioinformatics community, the `pandas` `DataFrame` is a popular tool for holding, manipulating, and performing computations on genomic sequence data and their associated properties.  For larger datasets, the need to parallelize operations on DataFrames motivates the need for tools such as `pandarallel`.  Because `pandarallel` is implemented against the standard Python `multiprocessing` library, this represents an opportunity for `dragon` to accelerate and enable greater scalability to users' code without necessarily requiring them to modify their code or patterns of thinking around their code.
#
# The example proposed uses a variant of NPSR1 linked to moderate/severe (stage III/IV) endometriosis, asthma, and sleep-related disorders. This variant is queried against a small dataset of nucleotide and protein sequences for the closest match. The closeness of the match is determined by the pairwise alignment, the E-value, and the percentage of match coverage. 
#
# In a multi-node `dragon` execution configuration, some nodes may be slower/faster than others and it may be helpful to see the relative progress/speed of one cluster's nodes versus others -- this motivates showing more than just a single progress bar representing all workers.
#
# The use case illustrates how parallel_apply from pandarallel is used for feature engineering for a k-means clustering use case. The features are commonly utilized in bioinformatics. 

# !pip install pandarallel
# !pip install biopython
# !pip install pyalign
# !pip install scikit-learn
# !pip install matplotlib
# !pip install seaborn

# +
import dragon
import multiprocessing

import cloudpickle

import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'

import numpy as np
import pandas as pd

import Bio
from Bio import SeqIO, Entrez
import pyalign
import time
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import seaborn as sns
import os.path
import pandarallel; pandarallel.__version__
# -

# The NPSR1 variant, rs142885915, is linked to stage III/IV endometriosis. The GenBank record is linked here: https://www.ncbi.nlm.nih.gov/protein/AKI72104.1

Entrez.email = raise Exception("need to set your email address here")
handle = Entrez.efetch(db="protein", id="AKI72104.1", rettype="gb", retmode="text")
read = SeqIO.read(handle, "genbank")
handle.close()
endo_name, endo_transl, endo_descr = str(read.name), str(read.seq), str(read.description)

# A subset of the protein database is created that is specific to NPSR1. The database is written to a csv file. If the database exists, it is read from the file. 

if os.path.isfile("NSPR1_proteins_database.csv"):
    aa_df = pd.read_csv("NSPR1_proteins_database.csv", on_bad_lines='skip')
else:
    handle = Entrez.esearch(db="protein", term="npsr1[gene] AND mammals[ORGN]", retmax='100')
    record = Entrez.read(handle)
    aa_identifiers = list(record["IdList"])
    handle.close()
    aa_id_names, aa_sequences, aa_descriptions = [endo_name], [endo_transl], [endo_descr]
    for idx, seq_id in enumerate(aa_identifiers):
        try:
            handle = Entrez.efetch(db="protein", id=seq_id,  retmode='text', rettype='gb')
            read = SeqIO.read(handle, "genbank")
            aa_id_names.append(str(read.name))
            aa_sequences.append(str(read.seq))
            aa_descriptions.append(str(read.description))
            handle.close()
        except:
            pass
    aa_df = pd.DataFrame(list(zip(aa_id_names, aa_sequences, aa_descriptions)), columns=['ID Name','Sequence', 'Description'])
    aa_df.to_csv("NSPR1_proteins_database.csv", index=False)

aa_df

# The use of a global variable inside a lambda function demonstrates key functionality from `cloudpickle` that is not otherwise available through `dill`. There is one progress bar for each worker. 

multiprocessing.set_start_method("dragon")
pandarallel.core.dill = cloudpickle
ctx = multiprocessing.get_context("dragon")
ctx.Manager = type("PMgr", (), {"Queue": ctx.Queue})
pandarallel.core.CONTEXT = ctx
pandarallel.pandarallel.initialize(progress_bar=True)


# The pairwise alignment algorithm from PyAlign can be used for either nucleotide or amino acid sequences to find similar regions in two sequences. The pairwise alignment score can point to similar functions, evolutionary origins, and structural elements in the two sequences. The higher the score, the better the alignment.

def alignment_algorithm(sequence_1, sequence_2, gap):
    alignment = pyalign.global_alignment(sequence_1, sequence_2, gap_cost=gap, eq=1, ne=-1)
    return alignment.score


# The E value is used to determine the number of hits one can expect to see when searching the database. As the score increases, the E value decreases. This means there is a reduction in noise. The smaller the E-value, the better the match. The E value is calculated with using the Jaccard distance.

def jaccard_similarity(list1, list2):
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(set(list1)) + len(set(list2))) - intersection
    return 1.0 - float(intersection) / union


# The new column of values in our pandas.DataFrame that shows the pairwise alignment from PyAlign.

start = time.monotonic()
aa_df['PyAlign Alignment Score'] = aa_df['Sequence'].parallel_apply(lambda seq2: alignment_algorithm(endo_transl, seq2, gap=0))
stop = time.monotonic()
aa_functions, aa_bar_num, aa_tot_time = ['PyAlign Alignment Score'],[10],[stop-start]
aa_df.sort_values(by=['PyAlign Alignment Score'],  inplace = True, ascending=False)
aa_df = aa_df[['ID Name','Sequence', 'PyAlign Alignment Score', 'Description']]
aa_df.head()

# The new column of values in our pandas.DataFrame that shows the E value for the sequences.

start = time.monotonic()
aa_df['E Value'] = aa_df['Sequence'].parallel_apply(lambda seq2: jaccard_similarity(list(endo_transl), list(seq2)))
stop = time.monotonic()
aa_functions.append('E Value')
aa_bar_num.append(10)
aa_tot_time.append(stop-start)
aa_df.sort_values(by=['E Value'],  inplace = True, ascending=True)
aa_df = aa_df[['ID Name','Sequence', 'PyAlign Alignment Score','E Value', 'Description']]
aa_df.head()

# For this new column in the pandas dataframe created from parallel_apply, we will use the sequencing coverage percentage which provides the percentage of coverage of the aligned sequence reads. The final protein dataframe output shows the alignment, E value, and percentage coverage ordered by percentage coverage and E value. The best matches line up with the query sequence.

start = time.monotonic()
aa_df['Percentage Coverage'] = aa_df['PyAlign Alignment Score'].parallel_apply(lambda match: 100*(float(match/len(endo_transl))))
stop = time.monotonic()
aa_functions.append('Percentage Coverage')
aa_bar_num.append(10)
aa_tot_time.append(stop-start)
aa_df.sort_values(by=['Percentage Coverage'],  inplace = True, ascending=False)
aa_df = aa_df[['ID Name','Sequence', 'PyAlign Alignment Score','E Value', 'Percentage Coverage', 'Description']]
aa_df

# The time for the pandarallel parallel_apply for the respective applications is displayed in the pandas dataframe below.  

std_time_df = pd.DataFrame(list(zip(aa_functions, aa_bar_num, aa_tot_time)), columns=['Pandarallel Function','Number of Bars', 'Time'])
std_time_sum = std_time_df['Time'].sum()
std_time_df.loc[len(std_time_df.index)] = ['Total Time for All Dragon Multiprocessing Pandarallel Processes (Amino Acids)', "N/A", std_time_sum]
std_time_df

# The correlation for the variables in the nucleotide pandas dataframe are plotted, and the variables for k-means clustering are identified.

sns.PairGrid(aa_df).map(sns.scatterplot);

# The x-axis is the PyAlign Alignment Score, and the y-axis is percentage coverage. The scatterplot function from the seaborn library is used for k-means clustering using the variables identified.

sns.scatterplot(data = aa_df[['PyAlign Alignment Score', 'E Value', 'Percentage Coverage']], x = 'PyAlign Alignment Score', y = 'Percentage Coverage', hue = 'E Value')

# The cluster number is determined from the elbow method and the default arguments for the k-means algorithm.

# +
X = np.array(aa_df.loc[:,['PyAlign Alignment Score', 'Percentage Coverage']])

euclidean = []
for i in range(1, 10):
    model = KMeans(n_clusters = i)
    model.fit(X)                              
    euclidean.append(model.inertia_)

plt.plot(range(1, 10), euclidean)
plt.xlabel('Cluster number')
plt.ylabel('Euclidean Sum of Squares')
plt.show()
# -

# The k-means algorithm is plotted, and the default arguments for the k-means algorithm is used. 

model = KMeans(n_clusters=3).fit(X)
plt.scatter(X[:,0], X[:,1], c=model.labels_.astype(float))
