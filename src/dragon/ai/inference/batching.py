"""
Dynamic batching module for Dragon Inference Pipeline.

This module provides independent, well-defined batching functionality
that collects prompts over a time window and forwards batched inputs
for processing.
"""

import time
import multiprocessing as mp
from queue import Empty
from typing import List, Tuple, Any, Optional
import logging

log = logging.getLogger(__name__)


class BatchItem:
    """A single item to be batched."""

    def __init__(
        self,
        user_prompt: str,
        formatted_prompt: str,
        response_queue: mp.Queue,
        latency_metrics: Tuple[float, float, float],
    ):
        """Initialize a BatchItem instance.

        :param user_prompt: Raw user input.
        :type user_prompt: str
        :param formatted_prompt: Formatted prompt with system instructions.
        :type formatted_prompt: str
        :param response_queue: Queue used to send the response.
        :type response_queue: mp.Queue
        :param latency_metrics: Tuple of (entry_time, cpu_latency, guard_latency).
        :type latency_metrics: tuple[float, float, float]
        """
        self.user_prompt = user_prompt
        self.formatted_prompt = formatted_prompt
        self.response_queue = response_queue
        self.latency_metrics = latency_metrics

    def __repr__(self):
        return f"BatchItem(user_prompt={self.user_prompt!r}, formatted_prompt={self.formatted_prompt!r}, response_queue={self.response_queue!r}, latency_metrics={self.latency_metrics!r})"

    def __eq__(self, other):
        if not isinstance(other, BatchItem):
            return NotImplemented
        return (
            self.user_prompt == other.user_prompt
            and self.formatted_prompt == other.formatted_prompt
            and self.response_queue == other.response_queue
            and self.latency_metrics == other.latency_metrics
        )

    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, state):
        self.__dict__.update(state)


class Batch:
    """A collection of items to be processed together."""

    def __init__(self, items: List[BatchItem], batch_id: int, created_at: float):
        """Initialize a Batch instance.

        :param items: List of BatchItem instances.
        :type items: list[BatchItem]
        :param batch_id: Unique identifier for the batch.
        :type batch_id: int
        :param created_at: Timestamp when the batch was created.
        :type created_at: float
        """
        self.items = items
        self.batch_id = batch_id
        self.created_at = created_at

    def __repr__(self):
        return f"Batch(items={self.items!r}, batch_id={self.batch_id!r}, created_at={self.created_at!r})"

    def __eq__(self, other):
        if not isinstance(other, Batch):
            return NotImplemented
        return (
            self.items == other.items
            and self.batch_id == other.batch_id
            and self.created_at == other.created_at
        )

    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, state):
        self.__dict__.update(state)

    @property
    def size(self) -> int:
        """Get the batch size.

        :returns: Number of items in the batch.
        :rtype: int
        """
        return len(self.items)

    @property
    def user_prompts(self) -> List[str]:
        """Extract user prompts from batch items.

        :returns: List of user prompts.
        :rtype: list[str]
        """
        return [item.user_prompt for item in self.items]

    @property
    def formatted_prompts(self) -> List[str]:
        """Extract formatted prompts from batch items.

        :returns: List of formatted prompts.
        :rtype: list[str]
        """
        return [item.formatted_prompt for item in self.items]

    @property
    def response_queues(self) -> List[mp.Queue]:
        """Extract response queues from batch items.

        :returns: List of response queues.
        :rtype: list[mp.Queue]
        """
        return [item.response_queue for item in self.items]

    @property
    def latency_metrics(self) -> List[Tuple[float, float, float]]:
        """Extract latency metrics from batch items.

        :returns: List of latency metrics tuples.
        :rtype: list[tuple[float, float, float]]
        """
        return [item.latency_metrics for item in self.items]


class DynamicBatcher:
    """
    Dynamic batching component that collects prompts over a time window
    and forwards batched inputs for processing.
    """

    def __init__(
        self,
        batch_wait_seconds: float,
        max_batch_size: int,
        enabled: bool = True,
    ):
        """Initialize the dynamic batcher.

        :param batch_wait_seconds: Time to wait before flushing a batch.
        :type batch_wait_seconds: float
        :param max_batch_size: Maximum number of items in a batch.
        :type max_batch_size: int
        :param enabled: Whether batching is enabled. If ``False``, items are
            processed individually.
        :type enabled: bool
        """
        self.batch_wait_seconds = batch_wait_seconds
        self.max_batch_size = max_batch_size
        self.enabled = enabled

        self._current_batch: List[BatchItem] = []
        # Start time is set when the first item of a batch arrives
        self._batch_start_time: float = 0.0
        self._batch_counter: int = 0

    def add_item(
        self,
        user_prompt: str,
        formatted_prompt: str,
        response_queue: mp.Queue,
        latency_metrics: Tuple[float, float, float],
    ) -> Optional[Batch]:
        """Add an item to the current batch.

        A :class:`Batch` is returned if the batch is ready to be processed
        (either the time window has expired or the maximum batch size has
        been reached); otherwise ``None`` is returned.

        :param user_prompt: Raw user input.
        :type user_prompt: str
        :param formatted_prompt: Formatted prompt with system instructions.
        :type formatted_prompt: str
        :param response_queue: Queue used to send the response.
        :type response_queue: mp.Queue
        :param latency_metrics: Tuple of
            ``(entry_time, cpu_latency, guard_latency)``.
        :type latency_metrics: tuple[float, float, float]
        :returns: A :class:`Batch` if ready to process, otherwise ``None``.
        :rtype: Optional[Batch]
        """
        item = BatchItem(
            user_prompt=user_prompt,
            formatted_prompt=formatted_prompt,
            response_queue=response_queue,
            latency_metrics=latency_metrics,
        )

        self._current_batch.append(item)

        # If this is the first item in a new batch, start the timer
        if len(self._current_batch) == 1:
            self._batch_start_time = time.time()

        # If batching is disabled, return batch immediately
        if not self.enabled:
            return self._create_and_reset_batch()

        # Check if we should flush the batch
        batch_size = len(self._current_batch)

        if self.should_check_batch() or batch_size >= self.max_batch_size:
            return self._create_and_reset_batch()

        return None

    def flush_batch(self) -> Optional[Batch]:
        """Force flush the current batch and return it for processing.

        This is useful when shutting down to process any remaining items.

        :returns: A :class:`Batch` if there are items to process, otherwise
            ``None``.
        :rtype: Optional[Batch]
        """
        if self._current_batch:
            return self._create_and_reset_batch()
        return None

    def should_check_batch(self) -> bool:
        """Return ``True`` if enough time has passed to check the batch.

        This can be called in a polling loop to determine whether to flush
        the batch (to avoid checking too frequently).

        :returns: ``True`` if the batch should be checked for flushing,
            otherwise ``False``.
        :rtype: bool
        """
        if not self._current_batch:
            return False

        elapsed_time = time.time() - self._batch_start_time
        return elapsed_time >= self.batch_wait_seconds

    def _create_and_reset_batch(self) -> Batch:
        """Create a Batch from current items and reset the internal state.

        :returns: The created :class:`Batch`.
        :rtype: Batch
        """
        batch = Batch(
            items=self._current_batch.copy(),
            batch_id=self._batch_counter,
            created_at=self._batch_start_time,
        )

        self._batch_counter += 1
        self._current_batch = []
        self._batch_start_time = time.time()

        return batch

    @property
    def current_batch_size(self) -> int:
        """Get the current number of items in the batch.
        :returns: Number of items in the current batch.
        :rtype: int"""
        return len(self._current_batch)

    @property
    def current_batch_age(self) -> float:
        """Get the age of the current batch in seconds.
        :returns: Age of the current batch in seconds.
        :rtype: float
        """
        return time.time() - self._batch_start_time
