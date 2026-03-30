"""
Guardrails processing module for Dragon Inference Pipeline.

This module handles prompt safety checking and jailbreak detection,
separated from the main LLM inference logic.
"""

import logging
from typing import List, Tuple, Optional
from .prompt_guard_utils import PromptGuard
from .config import GuardrailsConfig

log = logging.getLogger(__name__)


class GuardrailsProcessor:
    """
    Handles prompt safety checking using PromptGuard model.

    This class is responsible ONLY for guardrails/safety checking,
    completely separated from batching and LLM inference logic.
    """

    def __init__(self, config: GuardrailsConfig, hf_token: str):
        """Initialize the guardrails processor.

        :param config: Guardrails configuration.
        :type config: GuardrailsConfig
        :param hf_token: HuggingFace token for model access.
        :type hf_token: str
        """
        self.config = config
        self.enabled = config.enabled
        self.sensitivity = config.prompt_guard_sensitivity

        # Initialize PromptGuard model only if enabled
        self.prompt_guard: Optional[PromptGuard] = None
        if self.enabled:
            self.prompt_guard = PromptGuard(config.prompt_guard_model, hf_token)
            log.info(f"GuardrailsProcessor initialized with model: {config.prompt_guard_model}")
        else:
            log.info("GuardrailsProcessor disabled")

    def check_prompts(self, prompts: List[str]) -> Tuple[List[bool], List[float], float]:
        """Check a list of prompts for jailbreak attempts.

        :param prompts: List of user prompts to check.
        :type prompts: list[str]
        :returns: Tuple ``(is_safe, jailbreak_scores, processing_time)``
            where ``is_safe`` is a list of booleans (``True`` if safe,
            ``False`` if malicious), ``jailbreak_scores`` are the scores per
            prompt, and ``processing_time`` is the total processing time in
            seconds.
        :rtype: tuple[list[bool], list[float], float]
        """
        if not self.enabled:
            # If disabled, all prompts are considered safe
            return [True] * len(prompts), [0.0] * len(prompts), 0.0

        # Get jailbreak scores
        jailbreak_scores, processing_time = self.prompt_guard.get_jailbreak_scores_for_texts(prompts)

        # Determine which prompts are safe (below sensitivity threshold)
        is_safe = [score < self.sensitivity for score in jailbreak_scores]

        log.debug(f"Checked {len(prompts)} prompts: " f"{sum(is_safe)} safe, {len(prompts) - sum(is_safe)} malicious")

        return is_safe, jailbreak_scores, processing_time

    def filter_batch(
        self,
        prompts: List[str],
        formatted_prompts: List[str],
        response_queues: List,
        latency_metrics: List[Tuple[float, float, float]],
    ) -> Tuple[List[str], List[str], List, List[Tuple[float, float, float]], List[int], float]:
        """Filter a batch of prompts, separating safe from malicious ones.

        :param prompts: List of user prompts.
        :type prompts: list[str]
        :param formatted_prompts: List of formatted prompts.
        :type formatted_prompts: list[str]
        :param response_queues: List of response queues.
        :type response_queues: list
        :param latency_metrics: List of latency metric tuples.
        :type latency_metrics: list[tuple[float, float, float]]
        :returns: Tuple ``(safe_prompts, safe_formatted, safe_queues,
            safe_metrics, malicious_indices, processing_time)`` where the
            ``safe_*`` lists contain only safe entries, ``malicious_indices``
            are the indices of malicious prompts and ``processing_time`` is
            the guardrails processing time in seconds.
        :rtype: tuple
        """
        if not self.enabled:
            # If disabled, return everything as-is
            return prompts, formatted_prompts, response_queues, latency_metrics, [], 0.0

        is_safe, jailbreak_scores, processing_time = self.check_prompts(prompts)

        safe_prompts = []
        safe_formatted = []
        safe_queues = []
        safe_metrics = []
        malicious_indices = []

        for idx, (safe, prompt, formatted, queue, metrics) in enumerate(
            zip(is_safe, prompts, formatted_prompts, response_queues, latency_metrics)
        ):
            if safe:
                safe_prompts.append(prompt)
                safe_formatted.append(formatted)
                safe_queues.append(queue)
                safe_metrics.append(metrics)
            else:
                malicious_indices.append(idx)

        return (
            safe_prompts,
            safe_formatted,
            safe_queues,
            safe_metrics,
            malicious_indices,
            processing_time,
        )

    def get_malicious_response(self) -> str:
        """Get the standard response for malicious prompts.

        :returns: Standard response string for malicious prompts.
        :rtype: str
        """
        return "Your input has been categorized as malicious. Please try again."
