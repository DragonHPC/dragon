"""
The surrogate: the small trained model the campaign exists to produce.

It learns to predict `quantity` from `dials` -- given the speed and size of the
object, how many blocks did the mesh end up with? The training data is whatever
real runs have finished so far.

It is a small ensemble rather than one network, because their disagreement is a
usable signal on its own: where the members disagree, the campaign has not
measured, so that is where the next simulations go.

The arithmetic is deliberately plain. Both dials and the block count are scaled
onto roughly 0..1 by fixed constants from solver.py, never by statistics
learned from the data, so there is nothing to carry around or undo. Nothing here
knows about Dragon.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import torch
import torch.nn as nn

from science import solver

ENSEMBLE_SIZE = 3        # networks per fit; their disagreement drives `propose`
HIDDEN = 16
EPOCHS = 400
LEARNING_RATE = 0.01
VALIDATE_FRACTION = 0.25

MODEL_FILE = "surrogate.pt"
CARD_FILE = "model_card.txt"

_LOW = torch.tensor([solver.SPEED_RANGE[0], solver.SIZE_RANGE[0]])
_SPAN = torch.tensor([solver.SPEED_RANGE[1] - solver.SPEED_RANGE[0],
                      solver.SIZE_RANGE[1] - solver.SIZE_RANGE[0]])


class Net(nn.Module):
    """Two dials in, one scaled block count out."""

    def __init__(self, hidden: int = HIDDEN) -> None:
        super().__init__()
        # tanh rather than relu: the quantity being learned is smooth.
        self.stack = nn.Sequential(
            nn.Linear(2, hidden), nn.Tanh(),
            nn.Linear(hidden, 1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.stack(x)


def scale_dials(many_dials: list[list[float]]) -> torch.Tensor:
    """Both dials onto 0..1 using their known ranges, not the data."""
    # Fixed constants, so nothing has to be carried with the model or undone.
    return (torch.tensor(many_dials, dtype=torch.float32) - _LOW) / _SPAN


def split_counts(n: int) -> tuple[int, int]:
    n_validate = max(1, int(n * VALIDATE_FRACTION))
    return n - n_validate, n_validate


def _tensors(training_pairs: list[dict[str, Any]]) -> tuple[torch.Tensor, torch.Tensor]:
    dials = scale_dials([pair["dials"] for pair in training_pairs])
    blocks = torch.tensor([[pair["quantity"]] for pair in training_pairs],
                          dtype=torch.float32) / solver.MAX_BLOCKS
    if (dials.ndim != 2 or dials.shape[1] != 2 or
            not torch.isfinite(dials).all() or not torch.isfinite(blocks).all()):
        raise ValueError("training pairs must contain two finite dials and a finite quantity")
    return dials, blocks


def train_member(training_pairs: list[dict[str, Any]], seed: int,
                 member: int) -> dict[str, Any]:
    """Fit ONE ensemble member. This is what runs as a Dragon Batch task.

    Members differ only in where they start. Nothing is shared while they run.
    The last quarter of the data is held back for scoring in `assemble`.
    """
    # One thread per member (each is its own process); the matmuls are tiny.
    torch.set_num_threads(1)
    if len(training_pairs) < 3:
        raise ValueError("at least three data points required")
    dials, blocks = _tensors(training_pairs)
    n_train, _ = split_counts(len(training_pairs))
    torch.manual_seed(seed + member)
    net = Net()
    optimiser = torch.optim.Adam(net.parameters(), lr=LEARNING_RATE)
    loss_fn = nn.MSELoss()

    net.train()
    for _ in range(EPOCHS):
        optimiser.zero_grad()
        loss = loss_fn(net(dials[:n_train]), blocks[:n_train])
        loss.backward()
        optimiser.step()

    return {
        "member": member,
        "weights": {k: v.clone() for k, v in net.state_dict().items()},
    }


def assemble(training_pairs: list[dict[str, Any]],
             members: list[dict[str, Any]]) -> dict[str, Any]:
    """Combine finished members, then score them on the held-out tail."""
    ordered = sorted(members, key=lambda m: m["member"])
    if [m["member"] for m in ordered] != list(range(ENSEMBLE_SIZE)):
        raise ValueError("ensemble members missing or duplicated")
    n_train, n_validate = split_counts(len(training_pairs))
    model = {
        "member_weights": [m["weights"] for m in ordered],
        "dataset_ids": [p["run"] for p in training_pairs],
        "n_train": n_train, "n_validate": n_validate,
        "n_total": len(training_pairs),
    }
    held_out = training_pairs[n_train:]
    predicted, _ = predict(model, [p["dials"] for p in held_out])
    # Error as a fraction of the block cap, not of the true count: a run whose
    # object never entered the mesh has ~8 blocks, and a relative error on that
    # would swamp the mean no matter how good the fit is elsewhere.
    errors = [abs(guess - p["quantity"]) / solver.MAX_BLOCKS
              for guess, p in zip(predicted, held_out)]
    model["validate_error"] = sum(errors) / len(errors)
    return model


def _restore(model: dict[str, Any]) -> list[Net]:
    members = []
    for weights in model["member_weights"]:
        net = Net()
        net.load_state_dict(weights)
        net.eval()
        members.append(net)
    return members


def predict(model: dict[str, Any],
            many_dials: list[list[float]]) -> tuple[list[float], list[float]]:
    """Answer for each point, and how much the ensemble disagreed about it.

    Both come back as block counts: the members answer on the scaled 0..1 axis,
    so the mean and the spread are multiplied back up by the same constant.
    """
    members = _restore(model)
    x = scale_dials(many_dials)

    with torch.no_grad():
        # (members, points), so mean and std below reduce over the members.
        member_answers = torch.stack([net(x).squeeze(-1) for net in members])
    predicted = member_answers.mean(0) * solver.MAX_BLOCKS
    disagreement = member_answers.std(0) * solver.MAX_BLOCKS  # this is what `propose` samples on
    if not torch.isfinite(predicted).all() or not torch.isfinite(disagreement).all():
        raise ValueError("nonfinite ensemble prediction")
    return predicted.tolist(), disagreement.tolist()


def save_surrogate(directory: str | Path, model: dict[str, Any],
                   card: str) -> tuple[Path, Path]:
    """Write the model and its card side by side."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    model_path = directory / MODEL_FILE
    card_path = directory / CARD_FILE
    torch.save(model, model_path)
    card_path.write_text(card)
    return model_path, card_path
