.. _inference-cookbook:

Inference Service Examples
++++++++++++++++++++++++++

These examples show common ways to use the Dragon inference service from Python
applications. For the full user guide, see :ref:`inference_tutorial`. For the
API reference, see :ref:`InferenceAPI`.

Run examples that create Dragon native objects under Dragon, for example
``dragon my_inference_app.py``.


Example 1 - Single Prompt Service
=================================

This is the smallest useful service: one backend, one request queue, and one
plain text request submitted through
:py:meth:`dragon.ai.inference.Inference.query`.

.. code-block:: python
   :linenos:

   import os

   from dragon.ai.inference import (
       BatchingConfig,
       DynamicWorkerConfig,
       GuardrailsConfig,
       HardwareConfig,
       Inference,
       InferenceConfig,
       ModelConfig,
   )
   from dragon.native.queue import Queue


   def main() -> None:
       inference_queue = Queue()
       response_queue = Queue()

       config = InferenceConfig(
           model=ModelConfig(
               model_name="meta-llama/Llama-3.1-8B-Instruct",
               hf_token=os.environ["HF_TOKEN"],
               tp_size=1,
               max_tokens=256,
           ),
           hardware=HardwareConfig(num_nodes=1, num_gpus=1),
           batching=BatchingConfig(enabled=True, batch_type="dynamic"),
           guardrails=GuardrailsConfig(enabled=False),
           dynamic_worker=DynamicWorkerConfig(enabled=False),
       )

       service = Inference(config, inference_queue)

       try:
           service.initialize()
           service.query(("Give me a one-sentence definition of RDMA.", response_queue))
           result = response_queue.get()
           print(result["assistant"])
       finally:
           service.destroy()
           response_queue.close()


   if __name__ == "__main__":
       main()


Example 2 - Shared Chat Proxy
=============================

Use :py:class:`~dragon.ai.inference.DragonQueueLLMProxy` when agent
or application code already speaks in chat messages. The proxy is lightweight:
create one in every client or agent process and point all of them at the shared
service queue.

.. code-block:: python
   :linenos:

   import asyncio

    from dragon.ai.inference import DragonQueueLLMProxy


   async def answer_from_agent(inference_queue, question: str) -> str:
       proxy = DragonQueueLLMProxy(inference_queue, max_concurrent_requests=8)
       try:
           return await proxy.chat(
               [
                   {"role": "system", "content": "You answer briefly."},
                   {"role": "user", "content": question},
               ]
           )
       finally:
           await proxy.shutdown()


   response = asyncio.run(
       answer_from_agent(inference_queue, "Why use one shared inference backend?")
   )
   print(response)

The service response dictionary includes metrics, but the proxy returns only the
assistant text. If the backend sends an exception object, the proxy re-raises it
in the caller.


Example 3 - Structured Output
=============================

Pass ``json_schema`` to request structured generation. The backend builds
per-request vLLM sampling parameters so a dynamically batched request can mix
structured and free-form outputs.

.. code-block:: python
   :linenos:

   schema = {
       "type": "object",
       "properties": {
           "title": {"type": "string"},
           "next_steps": {
               "type": "array",
               "items": {"type": "string"},
               "minItems": 1,
               "maxItems": 4,
           },
       },
       "required": ["title", "next_steps"],
   }

   response = await proxy.chat(
       [
           {"role": "system", "content": "Return only JSON."},
           {"role": "user", "content": "Plan an inference benchmark."},
       ],
       json_schema=schema,
   )

   print(response)


Example 4 - Pre-Batched Prompt List
===================================

Dynamic batching is easiest for concurrent online requests. Pre-batching is
useful when an offline driver already has a list of prompts and wants to submit
groups sized for the backend.

.. code-block:: python
   :linenos:

   from dragon.native.queue import Queue

   prompts = [
       "Summarize the first log file.",
       "Summarize the second log file.",
       "Summarize the third log file.",
   ]

   prebatch_config = InferenceConfig(
       model=ModelConfig(
           model_name="meta-llama/Llama-3.1-8B-Instruct",
           hf_token=os.environ["HF_TOKEN"],
           tp_size=1,
           max_tokens=256,
       ),
       hardware=HardwareConfig(num_nodes=1, num_gpus=1),
       batching=BatchingConfig(
           enabled=True,
           batch_type="pre-batch",
           max_batch_size=len(prompts),
       ),
       guardrails=GuardrailsConfig(enabled=False),
       dynamic_worker=DynamicWorkerConfig(enabled=False),
   )

   inference_queue = Queue()
   response_queue = Queue()
   service = Inference(prebatch_config, inference_queue)

   try:
       service.initialize()
       service.query((prompts, response_queue))

       for _ in prompts:
           result = response_queue.get()
           print(result["assistant"])
   finally:
       service.destroy()
       response_queue.close()

In pre-batch mode the caller submits a list of prompts and a shared response queue, and each
generated response is placed on that queue. ``max_batch_size`` maps to the vLLM
``max_num_seqs`` limit, so set it greater than or equal to the number of prompts
to run the whole batch in a single concurrent generation pass.


Example 5 - Two Services in One Allocation
==========================================

Use ``node_offset`` when a workflow needs two independent inference services,
such as a large reasoning model and a smaller summarizer model.

.. code-block:: python
   :linenos:

   reasoning_config = InferenceConfig(
       model=ModelConfig(
           model_name="meta-llama/Llama-3.1-70B-Instruct",
           hf_token=os.environ["HF_TOKEN"],
           tp_size=4,
       ),
       hardware=HardwareConfig(num_nodes=1, num_gpus=4, node_offset=0),
       batching=BatchingConfig(enabled=True, max_batch_size=16),
       guardrails=GuardrailsConfig(enabled=False),
       dynamic_worker=DynamicWorkerConfig(enabled=False),
   )

   summarizer_config = InferenceConfig(
       model=ModelConfig(
           model_name="meta-llama/Llama-3.1-8B-Instruct",
           hf_token=os.environ["HF_TOKEN"],
           tp_size=1,
       ),
       hardware=HardwareConfig(num_nodes=1, num_gpus=1, node_offset=1),
       batching=BatchingConfig(enabled=True, max_batch_size=32),
       guardrails=GuardrailsConfig(enabled=False),
       dynamic_worker=DynamicWorkerConfig(enabled=False),
   )

The first service uses the first selected node. The second service starts from
the next node in the allocation.


Example 6 - Guardrails Enabled
==============================

Enable guardrails when user prompts should be classified before they reach the
LLM. PromptGuard runs in the preprocessing path and rejects prompts whose
jailbreak score is greater than or equal to the sensitivity threshold.

.. code-block:: python
   :linenos:

   config.guardrails = GuardrailsConfig(
       enabled=True,
       prompt_guard_model="meta-llama/Prompt-Guard-86M",
       prompt_guard_sensitivity=0.5,
   )

Rejected prompts receive a normal response dictionary with ``assistant`` set to
``Your input has been categorized as malicious. Please try again.``. Model
latency and token throughput metrics are zero for rejected requests because the
LLM was not called.
