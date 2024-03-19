from __future__ import print_function
import unittest
import dragon
import multiprocessing as mp
import dragon.ai.torch
import torch
from torchvision import datasets, transforms
import os
import socket
import inspect
from dragon.ai.torch.monkeypatching import dragon_fp_register 
from multiprocessing.reduction import ForkingPickler


def simple_fill(queue, event):
    data = queue.get()
    data[0][:] = 4
    event.set()

class PyTorchPatches(unittest.TestCase):
    """The test is designed to run on GPU systems."""
    
    def test_placement(self):
        mp.set_start_method("dragon")
        host_name = socket.gethostname()
        print(f"Dataloader python processes should be on {host_name}", flush=True)
        # Training settings
        device = torch.device("cuda", 0)
        train_kwargs = {"batch_size": 32}
        cuda_kwargs = {
            "num_workers": 4,
            "pin_memory": True,
            "shuffle": True,
            "multiprocessing_context": "dragon",
            "persistent_workers": True,
        }
        train_kwargs.update(cuda_kwargs)

        # normalize and scale the MNIST dataset
        transform = transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))])

        dataset1 = datasets.MNIST("./data", train=True, download=True, transform=transform)

        # create the dataloader for the MNIST dataset
        train_loader = torch.utils.data.DataLoader(dataset1, **train_kwargs)

        # training loop
        for batch_idx, (data, target) in enumerate(train_loader):
            data, target = data.to(device), target.to(device)
            self.assertIsInstance(data, torch.Tensor)
            self.assertIsInstance(target, torch.Tensor)

    def test_reductions_patch(self):
        mp.set_start_method("dragon")
        os_patch = False
        if "DRAGON_PATCH_TORCH" in os.environ:
            os_patch = True
        # check that the OS patch is Dragon Patch Torch
        self.assertTrue(os_patch)
        # check that the reductions and the file with reductions exist
        self.assertTrue(ForkingPickler.register != classmethod(dragon_fp_register))
        self.assertTrue(torch.multiprocessing.reductions.init_reductions)
        self.assertTrue(inspect.getfile(torch.multiprocessing.reductions.init_reductions))
        
        x = torch.zeros(5, 5).to('cpu', torch.float)
        q = mp.Queue()
        e = mp.Event()

        data = [x, x[:, 1]]
        q.put(data)

        p = mp.Process(target=simple_fill, args=(q, e))
        p.daemon = True
        p.start()

        self.assertTrue(e.wait(10))
        #this is the opposite behavior of what torch tests
        #torch_multiprocessing expects these to be in share 
        #memory and thus that the value is changed by the 
        #spawned process to a value of 4. 
        self.assertTrue(data[0].eq(0).all())
        self.assertTrue(data[1].eq(0).all())

        p.join(100)
        self.assertFalse(p.is_alive())        



if __name__ == "__main__":
    unittest.main()