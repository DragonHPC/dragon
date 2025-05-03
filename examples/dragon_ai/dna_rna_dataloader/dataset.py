#!/usr/bin/env python
"""Creates a Torch Dataset and Dataloader objects with custom tokenizer.

Create a simple data set and loader to work with PyTorch Dataset and Dataloader class. Wanted to use these instead of
the torch.text alternatives to keep things simple. In addition, using Dataloader in combination with DDP will hopefully
be more efficient with parallel processing and GPUs (yet to test)

"""
import dragon.ai.torch
import collections
from itertools import product
import logging

import numpy as np
from torch.utils.data import DataLoader
from dragon.ai.torch.dictdataset import DragonDataset
import torch
import torch.multiprocessing as torch_mp
from dragon.data.ddict import DDict
import time


class DragonDictArgs(object):
    """Class for managing dragon distributed dictionary arguments."""

    def __init__(self, managers_per_node: int, n_nodes: int, total_mem: int):
        self.managers_per_node = managers_per_node
        self.n_nodes = n_nodes
        self.total_mem = total_mem


# Helper Functions using DragonDataset
class SeqDataset(torch.utils.data.Dataset):
    """
    A class to represent a dataset to store sequence data.

    ...

    Attributes
    ----------
    file_name: str
        Name of the file that contains one sequence per line
    data: [str]
        List of seqeunces read as lines from the file
    transform: str
        Name of the function(s) used to transform the data
    context_size: int
        The number of kmers or words to consider on either side of the target kmer

    Methods
    -------
    __len__():
        Prints the number of lines or seqeunces in the data
    __getitem__():
        Gets the next seqeunce in the dataset and applies transformtaions on the data
    """

    def __init__(
        self, file_name, dragon_dict_args, kmer_size=4, context_size=3, num_noise_words=5, transform=None
    ):
        """Constructs the dataset of sequences
        that should be passed to the negative sampling loss.

        Parameters
        ----------
        file_name: str
                Name of the file that contains one sequence per line
        kmer_size: int
            The size of the kmers to extract from the sequence
        context_size: int
            The number of kmers or words to consider on either side of the target kmer
        num_noise_words: int
            The number of noise words to use for negative sampling
        transform: [torchvision.Transform]
            List of mames of the transformer function(s) used to transform the data
        """

        self.file_name = file_name

        # The ddict timer calculates how long it takes to fill the ddict for the dataset of interest
        timer = time.perf_counter()
        # create DDict using arguments
        dict_data = DDict(
            managers_per_node=dragon_dict_args.managers_per_node,
            n_nodes=dragon_dict_args.n_nodes,
            total_mem=dragon_dict_args.total_mem,
            timeout=None,
        )
        # write information from file into the ddict
        with open(file_name, "r") as file:
            for line_number, line in enumerate(file, start=0):
                dict_data[line_number] = line.strip()
                last_line_num = line_number
        list_of_line_numbers = range(1, last_line_num)
        self.data = DragonDataset(dict_data, dataset_keys=list_of_line_numbers)
        end_timer = time.perf_counter()
        # Output timer
        print("DDict_Timer", end_timer - timer, flush=True)
        print(dict_data.stats)
        self.kmer_size = kmer_size
        self.transform = transform
        self.context_size = context_size
        self.num_noise_words = num_noise_words
        self.word_probs = None  # Initialize this to None to make sure _build_vocab() does not error out
        self.doc_to_ix, self.word_to_ix, self.ix_to_word, self.word_probs = self._build_vocab()
        self.sample_noise = lambda: np.random.choice(
            self.word_probs.shape[0], self.num_noise_words, p=self.word_probs
        ).tolist()

    def __len__(self):
        """Gets the length or number of sequences in the dataset.

        Returns
        ----------
            len(int): The number of lines in the input file
        """
        return len(self.data)

    def __getitem__(self, idx):
        """Gets the next sequence in the file and extracts kmers by applying the specified transformations
        In addition, also outputs the context for each target kmer.

        Parameters
        ----------
        idx: int
            The index of the next sequence in the file
        Returns
        ----------
        [[str], [str], [[str]]]: List of lists of sequence (document) ids, kmers, and kmer contexts
        """

        try:
            sample = self.data[idx]
        except:
            sample = self.data[idx - 10]
        seq = sample.split(",")[0]
        doc_id = sample.split(",")[1].rstrip()

        if self.transform:
            # Add padding to the sequence so there is enough for all kmers at the beginning and end to have enough context kmers
            seq = "X" * self.kmer_size * self.context_size + seq + "Y" * self.kmer_size * self.context_size
            # Kmerize
            seqs = self.transform(seq)  # Kmer tokenize the sequence and get a list of sequence(s) back

            batch = []
            for seq in seqs:  # For each sequence
                if self.context_size == 0:
                    batch.append([doc_id, seq, []])

                # Add context kmers
                full_context = []  # The context kmers for each target kmer in this sequence
                for in_doc_pos in range(
                    self.context_size, (len(seq) - self.context_size)
                ):  # For each kmer find the context
                    context_indices = (
                        in_doc_pos + diff
                        for diff in range(-self.context_size, self.context_size + 1)
                        if diff != 0
                    )

                    current_context = []
                    for i in context_indices:
                        context_kmer = seq[i]
                        current_context.append(context_kmer)
                    full_context.append(current_context)

                # Add noise targets
                if self.word_probs is not None:
                    target_noise_ids = []
                    for in_doc_pos in range(self.context_size, (len(seq) - self.context_size)):
                        current_noise = [self.ix_to_word[no] for no in self.sample_noise()]
                        current_noise.insert(0, seq[in_doc_pos])
                        target_noise_ids.append(current_noise)
                else:
                    target_noise_ids = []

                seq = seq[self.context_size : (len(seq) - self.context_size)]
                batch.append([[doc_id] * len(seq), seq, full_context, target_noise_ids])

        if self.word_probs is None:
            return batch
        else:
            if len(batch) > 1:
                batch_flat = [item for sublist in batch for item in sublist]
            doc_ids = torch.tensor(
                [
                    self.doc_to_ix[item]
                    for i, sublist in enumerate(batch_flat)
                    for item in sublist
                    if i % 4 == 0
                ]
            )
            target_ids = torch.tensor(
                [
                    self.word_to_ix[item]
                    for i, sublist in enumerate(batch_flat)
                    for item in sublist
                    if i % 4 == 1
                ]
            )
            context_ids = torch.tensor(
                [
                    [self.word_to_ix[nested_item] for nested_item in item]
                    for i, sublist in enumerate(batch_flat)
                    for item in sublist
                    if i % 4 == 2
                ]
            )
            target_noise_ids = torch.tensor(
                [
                    [self.word_to_ix[nested_item] for nested_item in item]
                    for i, sublist in enumerate(batch_flat)
                    for item in sublist
                    if i % 4 == 3
                ]
            )

            return (doc_ids, target_ids, context_ids, target_noise_ids)

    def _build_vocab(self):
        """Gets the next sequence in the file and extracts kmers by applying the specified transformations
        In addition, also outputs the context for each target kmer.

        Parameters
        ----------

        Returns
        ----------
        ([str], [str], [[str]]]: List of lists of sequence (document) ids, kmers, and kmer contexts
        """

        torch_mp.set_start_method("dragon", force=True)
        loader = DataLoader(self, batch_size=4, shuffle=False, collate_fn=my_collate, num_workers=8)

        # Create vocabulary
        count = 0
        docs = set()
        vocab = set()
        char_set = set()
        vocab_freq = collections.Counter(vocab)
        print("Building Vocabulary")

        for i, batch in enumerate(loader):
            # the layers...
            batch_flat = [item for sublist in batch for item in sublist]
            batch_kmers = [item for sublist in batch_flat for item in sublist[1]]
            docs.update(set([item for sublist in batch_flat for item in sublist[0]]))
            vocab.update(set(batch_kmers))
            char_set.update(set([char for kmer in batch_kmers for char in kmer]))
            vocab_freq.update(collections.Counter(batch_kmers))
            count += 1

        # Add begin and end kmers
        vocab.add("XXXX")
        vocab.add("YYYY")
        vocab_freq["XXXX"] = 0
        vocab_freq["YYYY"] = 0
        # Add all combinations of X and Y as well
        print("".join(char_set))
        for nx in range(1, self.kmer_size):
            all_comb = ["".join(i) for i in list(product("".join(char_set), repeat=self.kmer_size - nx))]
            for comb in all_comb:
                vocab.add("X" * nx + comb)
                vocab_freq["X" * nx + comb] = 0
                vocab.add(comb + "Y" * nx)
                vocab_freq[comb + "Y" * nx] = 0

        # Create final word to ix dictionary
        doc_to_ix = {doc: i for i, doc in enumerate(docs)}
        word_to_ix = {word: i for i, word in enumerate(vocab)}
        ix_to_word = {i: word for i, word in enumerate(vocab)}
        probs = noise_distribution(vocab_freq, word_to_ix)

        return doc_to_ix, word_to_ix, ix_to_word, probs


class KmerTokenize(object):
    """A custom tokenizer class to parse the sequences into kmers
    Inspired From: https://github.com/fhalab/embeddings_reproduction/
    ...

    Attributes
    ----------
    k: int
        The size of kmers
    overlap: boolean
        Should kmers be calculated with or without overlap between them
    merge: boolean
        Should the different kmer stretches from the same sequence be merged.

    Methods
    -------
    __call__():
        Function to kmerize the sequence and return them
    """

    def __init__(self, kmer_hypers):
        """Constructs the kmer tokenizer to break a sequence into batch of kmers

        Parameters
        ----------
        kmer_hypers: (dict of {str: int, str: bool, str: bool})
                Name of the file that contains one sequence per line
        """
        self.k = kmer_hypers["k"]
        self.overlap = kmer_hypers["overlap"]
        self.merge = kmer_hypers["merge"]

    def __call__(self, seq):
        """Generates kmers from a sequence and returns a list of kmers

        Parameters
        ----------
        seq: str
            The seqeunce to be broken down into kmers


        Returns
        -------
        [[str]]: List of lists of kmers

        """
        N = len(seq)

        if self.overlap:
            seq = seq.rstrip()
            kmers = [[seq[i : i + self.k] for i in range(N - self.k + 1)]]
        else:
            kmers = [[seq[i : i + self.k] for i in range(j, N - self.k + 1, self.k)] for j in range(self.k)]

        if self.merge:
            return kmers
        else:
            print("In untested else")
            kms = []
            for km in kmers:
                kms.append(km)
            return kms


# a simple custom collate function
def my_collate(batch):
    """A custom collation function to process batch items
    The default collate function uses zip to process each sequence at the kmer level.
    The current implementation just separates each list

    Parameters
    ----------
    batch: [[[[str], [str], [[str]]]]]
        Nested list of strings

    """
    data = [item for item in batch]

    return data


def noise_distribution(vocab_freq, word_to_ix):
    """We use a unigram distribution raised to the 3/4rd power,
    as proposed by T. Mikolov et al. in Distributed Representations
    of Words and Phrases and their Compositionality
    Inspired From: https://github.com/fhalab/embeddings_reproduction/

    """

    probs = np.zeros(len(vocab_freq))

    for word, freq in vocab_freq.items():
        probs[word_to_ix[word]] = freq

    probs = np.power(probs, 0.75)
    probs /= np.sum(probs)

    return probs
