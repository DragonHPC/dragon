#!/usr/bin/env python
"""Creates a Doc2Vec DM model: https://medium.com/@amarbudhiraja/understanding-document-embeddings-of-doc2vec-bfe7237a26da


"""

import time
import re
import collections

import pandas as pd
import torch
from torch.utils.data import DataLoader
from torch.optim import Adam
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA

from models import DM
from models import NegativeSampling


# Time counter
class AverageMeter(object):
    """Computes and stores the average and current value"""

    def __init__(self, name, fmt=":f"):
        self.name = name
        self.fmt = fmt
        self.reset()

    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val, n=1):
        self.val = val
        self.sum += val * n
        self.count += n
        self.avg = self.sum / self.count

    def __str__(self):
        fmtstr = "{name} {val" + self.fmt + "} ({avg" + self.fmt + "})"
        return fmtstr.format(**self.__dict__)


def run_DM_model(
    SeqData,
    num_epochs=1,
    batch_size=4,
    shuffle=False,
    pin_memory=False,
    non_blocking=True,
    num_workers=0,
    use_gpu=False,
    do_pca=False,
):
    print(use_gpu)
    # Create DataLoader
    loader = DataLoader(
        SeqData, batch_size=batch_size, shuffle=shuffle, pin_memory=pin_memory, num_workers=num_workers
    )

    print(torch.cuda.is_available())
    if torch.cuda.is_available() and use_gpu == True:
        print("here")
        use_gpu = True
        cuda0 = torch.device("cuda:0")

    else:
        use_gpu = False
    print(use_gpu)

    # Embedding parameters
    vec_dim = 24
    num_docs = len(SeqData.doc_to_ix)
    num_words = len(SeqData.word_to_ix)
    print(num_docs)

    # Training parameters
    train_batch_size = batch_size  # How many target words to use for training in each batch
    n_epochs = num_epochs

    # Model initialization
    model = DM(vec_dim, num_docs=num_docs, num_words=num_words)
    cost_func = NegativeSampling()
    optimizer = Adam(params=model.parameters(), lr=0.1)
    if use_gpu:
        print("model to cuda")
        model.to("cuda")

    # Training

    # Keep track of time
    torch.cuda.synchronize()
    data_load_time = AverageMeter("Data", ":6.3f")
    batch_run_time = AverageMeter("Data", ":6.3f")
    data_load_end = time.time()

    print(n_epochs)
    for epoch_i in range(n_epochs):
        print(epoch_i)
        epoch_start_time = time.time()
        loss = []
        n_batches = 0

        for i, batch in enumerate(loader):
            data_load_time.update(time.time() - data_load_end)
            batch_run_start = time.time()

            doc_ids = batch[0]
            target_ids = batch[1]
            context_ids = batch[2]
            target_noise_ids = batch[3]

            # CUDA check
            if use_gpu:
                doc_ids.to("cuda", non_blocking=non_blocking)
                target_ids.to("cuda", non_blocking=non_blocking)
                context_ids.to("cuda", non_blocking=non_blocking)
                target_noise_ids.to("cuda", non_blocking=non_blocking)

            # Do the forward and backward low level batches
            n_train_batches = 0
            train_batch_size = 1
            for j in range(0, doc_ids.shape[0], train_batch_size):
                train_doc_ids = doc_ids[j : j + train_batch_size, :].squeeze()
                train_target_ids = target_ids[j : j + train_batch_size, :].squeeze()
                train_context_ids = context_ids[j : j + train_batch_size, :].squeeze()
                train_target_noise_ids = target_noise_ids[j : j + train_batch_size, :].squeeze()

                # Zero-grad is important
                optimizer.zero_grad()

                # Model forward pass
                x = model.forward(train_context_ids, train_doc_ids, train_target_noise_ids)

                # Negative sampling cost function
                x_cost = cost_func.forward(x)
                x_cost.backward()
                optimizer.step()

                n_train_batches += 1
            n_batches += 1
            data_load_end = time.time()
            batch_run_time.update(time.time() - batch_run_start)

        # end of epoch
        epoch_total_time = round(time.time() - epoch_start_time)
        print("Time taken for epoch:")
        print(epoch_total_time)

        if use_gpu == 1:
            # Release GPU cache
            torch.cuda.empty_cache()

    if do_pca == True:
        # Kmeans clustering
        vocab = pd.DataFrame.from_dict(SeqData.word_to_ix, orient="index")
        vocab["kmer"] = vocab.index
        vocab = vocab.set_index(0)

        kmer_ACTG = [len(re.findall("[PQRSXY]", i)) == 0 for i in vocab["kmer"]]
        kmer_PQRS = [len(re.findall("[ACGTXY]", i)) == 0 for i in vocab["kmer"]]
        kmer_kmeans = KMeans(n_clusters=2, random_state=0).fit(model._W.cpu().detach().numpy())
        doc_kmeans = KMeans(n_clusters=2, random_state=0).fit(model._D.cpu().detach().numpy())
        ACGT_counter = collections.Counter(kmer_kmeans.labels_[kmer_ACTG])
        PQRS_counter = collections.Counter(kmer_kmeans.labels_[kmer_PQRS])
        print(ACGT_counter)
        print(PQRS_counter)

        # PCA kmers
        pca_fit = PCA(n_components=2).fit(model._W.cpu().detach().numpy())
        print(pca_fit.explained_variance_ratio_)
        reduced_data_kmers = PCA(n_components=2).fit_transform(model._W.cpu().detach().numpy())

        # PCA Docs
        pca_fit = PCA(n_components=2).fit(model._D.cpu().detach().numpy())
        print(pca_fit.explained_variance_ratio_)
        reduced_data_docs = PCA(n_components=2).fit_transform(model._D.cpu().detach().numpy())

        return (model, kmer_kmeans, doc_kmeans, reduced_data_kmers, reduced_data_docs)
    else:
        return (data_load_time, batch_run_time)
