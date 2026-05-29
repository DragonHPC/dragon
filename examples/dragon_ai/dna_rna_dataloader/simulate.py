#!/usr/bin/env python
"""Creates a simulated data set of random letters to test our custom dataset and dataloader classes

"""

import pandas as pd
import numpy as np


def gen_sim_data(
    letters=[["A", "C", "G", "T"], ["P", "Q", "R", "S"]],
    n_seq=[5000, 5000],
    seq_lens=[100, 100],
    file_name="data/01_raw/sim_1.txt",
):
    """Constructs  simulated datasets of sequences
    with each sequence made up of one of two independent character sets

    Parameters
    ----------
    letters : [[chr]]
        List of lists of characters to simulate the sequences
    n_seq: [int]
        List of number of sequences to simulate for each group
    kmer_size: [int]
        List of lengths of sequences to simulate for each group
    context_size: int
        The number of kmers or words to consider on either side of the target kmer
    num_noise_words: int
        The number of noise words to use for negative sampling
    transform: [torchvision.Transform]
        List of mames of the transformer function(s) used to transform the data
    """

    all_strings = []
    for g in range(len(n_seq)):
        for s in range(n_seq[g]):
            all_strings.append("".join(np.random.choice(letters[g]) for i in range(seq_lens[g])))

    print(len(all_strings))
    sel_sequence_dict = pd.DataFrame(all_strings)
    sel_sequence_dict.rename(columns={list(sel_sequence_dict)[0]: "sequence"}, inplace=True)
    sel_sequence_dict["doc_id"] = ["design_id_" + str(i) for i in range(len(all_strings))]
    sel_sequence_dict.to_csv(file_name, index=False, header=False)
