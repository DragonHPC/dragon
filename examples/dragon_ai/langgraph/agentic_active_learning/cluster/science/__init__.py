"""What the science is, and where to swap in your own.

This layer is deliberately thin. `solver` builds the miniAMR command line and
records the memory layout shared with its C hook. `surrogate` is a small Torch
ensemble. `sampling` decides what to run next and when to stop. None of it
knows Dragon exists, so replacing the solver or the model touches only this
directory.
"""
