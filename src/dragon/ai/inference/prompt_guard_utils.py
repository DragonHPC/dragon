import torch
from torch.nn.functional import softmax
import time
from typing import List
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
)


class PromptGuard:
    """Utilities for loading the PromptGuard model and evaluating text for jailbreaks and indirect injections.

    Note that the underlying model has a maximum recommended input size of 512 tokens as a DeBERTa model.
    The final two functions in this file implement efficient parallel batched evaluation of the model on a list
    of input strings of arbitrary length, with the final score for each input being the maximum score across all
    chunks of the input string.
    """

    def __init__(self, model: str, hf_token: str) -> None:
        """Initialize the PromptGuard model wrapper.

        :param model: Name or path of the HuggingFace PromptGuard model.
        :type model: str
        :param hf_token: HuggingFace token.
        :type hf_token: str
        """
        self.model, self.tokenizer = self.load_model_and_tokenizer(model, hf_token)

    def load_model_and_tokenizer(self, model_name: str, hf_token: str):
        """Load the PromptGuard model from Hugging Face or a local model.

        :param model_name: Name or path of the HuggingFace PromptGuard model.
        :type model_name: str
        :param hf_token: HuggingFace token.
        :type hf_token: str
        :returns: Tuple ``(model, tokenizer)``.
        :rtype: tuple
        """
        model = AutoModelForSequenceClassification.from_pretrained(
            pretrained_model_name_or_path=model_name, token=hf_token
        )
        tokenizer = AutoTokenizer.from_pretrained(pretrained_model_name_or_path=model_name, token=hf_token)
        return model, tokenizer

    def preprocess_text_for_promptguard(self, text: str) -> str:
        """Preprocess the text by removing spaces that break apart larger tokens.
        This hotfixes a workaround to PromptGuard, where spaces can be inserted into a string
        to allow the string to be classified as benign.

        :param text: Input text to preprocess.
        :type text: str
        :returns: Preprocessed text.
        :rtype: str
        """
        try:
            cleaned_text = ""
            index_map = []
            for i, char in enumerate(text):
                if not char.isspace():
                    cleaned_text += char
                    index_map.append(i)
            tokens = self.tokenizer.tokenize(cleaned_text)
            result = []
            last_end = 0
            for token in tokens:
                token_str = self.tokenizer.convert_tokens_to_string([token])
                start = cleaned_text.index(token_str, last_end)
                end = start + len(token_str)
                original_start = index_map[start]
                if original_start > 0 and text[original_start - 1].isspace():
                    result.append(" ")
                result.append(token_str)
                last_end = end
            return "".join(result)
        except Exception:
            return text

    def get_class_probabilities(
        self,
        text: str,
        temperature: float = 1.0,
        device: str = "cpu",
        preprocess: bool = True,
    ):
        """Evaluate the model on the given text with temperature-adjusted softmax.

        Note that, as this is a DeBERTa model, the input text should have a
        maximum length of 512 tokens.

        :param text: Input text to classify.
        :type text: str
        :param temperature: Temperature for the softmax function.
        :type temperature: float
        :param device: Device on which to evaluate the model.
        :type device: str
        :param preprocess: Whether to run input-length preprocessing.
        :type preprocess: bool
        :returns: Probability of each class adjusted by the temperature.
        :rtype: torch.Tensor
        """
        if preprocess:
            text = self.preprocess_text_for_promptguard(text)
        # Encode the text
        inputs = self.tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
        inputs = {key: value.to(device) for key, value in inputs.items()}

        # Get logits from the model
        with torch.no_grad():
            logits = self.model(**inputs).logits

        # Apply temperature scaling
        scaled_logits = logits / temperature
        # Apply softmax to get probabilities
        probabilities = softmax(scaled_logits, dim=-1)
        return probabilities

    def get_jailbreak_score(
        self,
        text: str,
        temperature: float = 1.0,
        device: str = "cpu",
        preprocess: bool = True,
    ):
        """Evaluate the probability that a string contains a jailbreak.

        This is suitable for filtering dialogue between a user and an LLM.

        :param text: Input text to evaluate.
        :type text: str
        :param temperature: Temperature for the softmax function.
        :type temperature: float
        :param device: Device on which to evaluate the model.
        :type device: str
        :param preprocess: Whether to run input-length preprocessing.
        :type preprocess: bool
        :returns: Tuple ``(score, elapsed_time)`` where ``score`` is the
            probability of malicious content and ``elapsed_time`` is the time
            taken to compute it.
        :rtype: tuple[float, float]
        """
        start_time = time.time()
        probabilities = self.get_class_probabilities(text, temperature, device, preprocess)
        return probabilities[0, 2].item(), round(time.time() - start_time, 2)

    def get_indirect_injection_score(
        self,
        text: str,
        temperature: float = 1.0,
        device: str = "cpu",
        preprocess: bool = True,
    ):
        """Evaluate the probability that text contains embedded instructions.

        This includes both malicious and benign instructions and is intended
        for filtering third-party inputs (for example, web searches or tool
        outputs) into an LLM.

        :param text: Input text to evaluate.
        :type text: str
        :param temperature: Temperature for the softmax function.
        :type temperature: float
        :param device: Device on which to evaluate the model.
        :type device: str
        :param preprocess: Whether to run input-length preprocessing.
        :type preprocess: bool
        :returns: Tuple ``(score, elapsed_time)`` where ``score`` is the
            combined probability of embedded instructions and ``elapsed_time``
            is the time taken to compute it.
        :rtype: tuple[float, float]
        """
        start_time = time.time()
        probabilities = self.get_class_probabilities(text, temperature, device, preprocess)
        return (probabilities[0, 1] + probabilities[0, 2]).item(), round(time.time() - start_time, 2)

    def process_text_batch(
        self,
        texts: List[str],
        temperature: float = 1.0,
        device: str = "cpu",
        preprocess: bool = True,
    ):
        """Process a batch of texts and return their class probabilities.

        :param texts: List of texts to process.
        :type texts: list[str]
        :param temperature: Temperature for the softmax function.
        :type temperature: float
        :param device: Device on which to evaluate the model.
        :type device: str
        :param preprocess: Whether to run input-length preprocessing.
        :type preprocess: bool
        :returns: Tensor containing the class probabilities for each text in
            the batch.
        :rtype: torch.Tensor
        """
        if preprocess:
            texts = [self.preprocess_text_for_promptguard(text) for text in texts]
        inputs = self.tokenizer(texts, return_tensors="pt", padding=True, truncation=True, max_length=512)
        inputs = {key: value.to(device) for key, value in inputs.items()}

        with torch.no_grad():
            logits = self.model(**inputs).logits
        scaled_logits = logits / temperature
        probabilities = softmax(scaled_logits, dim=-1)
        return probabilities

    def get_scores_for_texts(
        self,
        texts: List[str],
        score_indices: List[int],
        temperature: float = 1.0,
        device: str = "cpu",
        max_batch_size: int = 16,
        preprocess: bool = True,
    ):
        """Compute scores for a list of texts.

        Texts of arbitrary length are broken into chunks and processed in
        parallel, with the final score for each text being the maximum across
        chunks.

        :param texts: List of texts to evaluate.
        :type texts: list[str]
        :param score_indices: Indices of classes whose scores are summed for
            the final score calculation.
        :type score_indices: list[int]
        :param temperature: Temperature for the softmax function.
        :type temperature: float
        :param device: Device on which to evaluate the model.
        :type device: str
        :param max_batch_size: Maximum number of text chunks to process in a
            single batch.
        :type max_batch_size: int
        :param preprocess: Whether to run input-length preprocessing.
        :type preprocess: bool
        :returns: List of scores for each text.
        :rtype: list[float]
        """
        all_chunks = []
        text_indices = []
        for index, text in enumerate(texts):
            chunks = [text[i : i + 512] for i in range(0, len(text), 512)]
            all_chunks.extend(chunks)
            text_indices.extend([index] * len(chunks))
        all_scores = [0] * len(texts)
        for i in range(0, len(all_chunks), max_batch_size):
            batch_chunks = all_chunks[i : i + max_batch_size]
            batch_indices = text_indices[i : i + max_batch_size]
            probabilities = self.process_text_batch(batch_chunks, temperature, device, preprocess)
            scores = probabilities[:, score_indices].sum(dim=1).tolist()

            for idx, score in zip(batch_indices, scores):
                all_scores[idx] = max(all_scores[idx], score)
        return all_scores

    def get_jailbreak_scores_for_texts(
        self,
        texts: List[str],
        temperature: float = 1.0,
        device: str = "cpu",
        max_batch_size: int = 16,
        preprocess: bool = True,
    ):
        """Compute jailbreak scores for a list of texts.

        :param texts: List of texts to evaluate.
        :type texts: list[str]
        :param temperature: Temperature for the softmax function.
        :type temperature: float
        :param device: Device on which to evaluate the model.
        :type device: str
        :param max_batch_size: Maximum number of text chunks to process in a
            single batch.
        :type max_batch_size: int
        :param preprocess: Whether to run input-length preprocessing.
        :type preprocess: bool
        :returns: Tuple ``(scores, elapsed_time)`` where ``scores`` is the
            list of jailbreak scores and ``elapsed_time`` is the total time
            taken.
        :rtype: tuple[list[float], float]
        """
        start_time = time.time()
        response = self.get_scores_for_texts(texts, [2], temperature, device, max_batch_size, preprocess)
        return response, round(time.time() - start_time, 2)

    def get_indirect_injection_scores_for_texts(
        self,
        texts: List[str],
        temperature: float = 1.0,
        device: str = "cpu",
        max_batch_size: int = 16,
        preprocess: bool = True,
    ):
        """Compute indirect injection scores for a list of texts.

        :param texts: List of texts to evaluate.
        :type texts: list[str]
        :param temperature: Temperature for the softmax function.
        :type temperature: float
        :param device: Device on which to evaluate the model.
        :type device: str
        :param max_batch_size: Maximum number of text chunks to process in a
            single batch.
        :type max_batch_size: int
        :param preprocess: Whether to run input-length preprocessing.
        :type preprocess: bool
        :returns: Tuple ``(scores, elapsed_time)`` where ``scores`` is the
            list of indirect injection scores and ``elapsed_time`` is the
            total time taken.
        :rtype: tuple[list[float], float]
        """
        start_time = time.time()
        response = self.get_scores_for_texts(texts, [1, 2], temperature, device, max_batch_size, preprocess)
        return response, round(time.time() - start_time, 2)
