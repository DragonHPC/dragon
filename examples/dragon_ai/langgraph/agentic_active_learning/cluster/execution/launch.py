"""Own creator handles only; this helper is not passed into graph nodes."""

import os
from dataclasses import dataclass
from typing import Any


@dataclass
class Resources:
    state_ddict: Any = None
    mesh_ddict: Any = None
    batch: Any = None
    inference: Any = None
    queue: Any = None
    safe: bool = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def start_inference(self, args):
        """Start the configured LLM outside any Policy scope."""
        from dragon.ai.inference.config import (
            BatchingConfig,
            HardwareConfig,
            InferenceConfig,
            ModelConfig,
        )
        from dragon.ai.inference.inference_utils import Inference
        from dragon.native.queue import Queue

        config = InferenceConfig(
            model=ModelConfig(
                model_name=args.llm,
                hf_token=os.environ.get("HF_TOKEN", ""),
                tp_size=1,
                max_tokens=args.max_tokens,
                max_model_len=args.max_tokens * 8,
                temperature=args.temperature,
            ),
            hardware=HardwareConfig(
                num_nodes=1,
                num_gpus=max(1, args.gpus),
                num_inf_workers_per_cpu=1,
            ),
            batching=BatchingConfig(
                enabled=True,
                batch_type="dynamic",
                batch_wait_seconds=0.05,
                max_batch_size=16,
            ),
        )
        # Keep cleanup blocked until the caller confirms all tasks have stopped.
        self.safe = False
        # Dragon: the LLM runs as Dragon processes inside this allocation; nodes reach it through one Queue.
        self.queue = Queue(maxsize=256)
        self.inference = Inference(config=config, input_queue=self.queue)
        self.inference.initialize()
        return self.queue

    def close(self) -> None:
        if self.safe:
            try:
                if self.batch is not None:
                    import settings

                    # Dragon: destroy() is the launcher's job; workers only join. Order matters: tasks, then stores.
                    self.batch.join(timeout=settings.CANCEL_TIMEOUT)
                    self.batch.destroy(timeout=settings.CANCEL_TIMEOUT)
                    self.batch = None
                if self.inference is not None:
                    self.inference.destroy()
                    self.inference = None
                if self.queue is not None:
                    self.queue.destroy()
                    self.queue = None
                if self.mesh_ddict is not None:
                    self.mesh_ddict.destroy()
                    self.mesh_ddict = None
                if self.state_ddict is not None:
                    self.state_ddict.destroy()
                    self.state_ddict = None
            except Exception as exc:
                self.safe = False
                print(f"[summary] shutdown unconfirmed: {exc}", flush=True)
        if not self.safe:
            print("[summary] ABORT: resources not explicitly destroyed; task termination may be unknown. "
                  "Cluster operator cleanup required; launcher lifetime still applies.", flush=True)