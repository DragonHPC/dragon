# Dragon-PyTorch Test Cases

## Reductions Test

The following test checks that the Dragon PyTorch patch works. If the patch of init_reductions was not made then this will fail with with an error that Dragon does not support duplicate file descriptors. If that error was not raised, then we would see that the tensor has incorrect values. This test is inspired by PyTorch's `TestMultiprocessing._test_sharing` test located in `test/test_multiprocessing.py`.

```
dragon test_pytorch_patches.py PyTorchPatches.test_reductions_patch -f -v
```

## DataLoader Placement Test

 The following test checks that the processes spawned by the Dataloader are on the same node as the spawning process. The example involves CUDA, normalizing the MNIST dataset, loading the patched Dragon dataloader with the MNIST dataset, and running the training loop over the dataset hosted in the dataloader.

For the following test, there should be a GPU allocation with 2 nodes. PyTorch needs to be installed for the type of GPU on the nodes. To confirm the test works, ssh into the node that is printed and confirm there are 5 `python3` processes started. Alternatively, ssh into the other nodes in the allocation other than the one printed and confirm there are no `python3` processes started. 

We run as follows to test when CUDA is available or not:
```
dragon test_pytorch_patches.py PyTorchPatches.test_placement_use_cuda -f -v
dragon test_pytorch_patches.py PyTorchPatches.test_placement_use_cpu -f -v
```