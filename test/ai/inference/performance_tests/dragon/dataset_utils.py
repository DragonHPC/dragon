import random
from transformers import AutoTokenizer


class SonnetDataset:
    """
    Simplified implementation of the Sonnet dataset.  Loads poem lines from a
    text file and generates sample requests.  Default values here copied from
    `benchmark_serving.py` for the sonnet dataset.
    """

    def __init__(
        self,
        dataset_path,
        random_seed,
        model_name_or_path,
        num_requests,
        input_len_tokens,
        prefix_len_tokens,
        return_prompt_formatted,
    ) -> None:
        """Initialize :class:`SonnetDataset`.

        :param dataset_path: Path to text file with poem lines.
        :type dataset_path: str
        :param random_seed: Random seed for sampling.
        :type random_seed: int
        :param model_name_or_path: HuggingFace model name or path for the
            tokenizer.
        :type model_name_or_path: str
        :param num_requests: Number of sample requests to generate.
        :type num_requests: int
        :param input_len_tokens: Desired input length in tokens.
        :type input_len_tokens: int
        :param prefix_len_tokens: Desired prefix length in tokens.
        :type prefix_len_tokens: int
        :param return_prompt_formatted: Whether to return the prompt
            formatted for chat models.
        :type return_prompt_formatted: bool
        """
        self.dataset_path = dataset_path
        self.random_seed = random_seed
        self.num_requests = num_requests
        self.input_len = input_len_tokens
        self.prefix_len = prefix_len_tokens
        self.return_prompt_formatted = return_prompt_formatted

        # Set random seed
        random.seed(self.random_seed)
        # Initialize tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(
            model_name_or_path, trust_remote_code=True
        )
        # Load Dataset
        self.load_data()

    def load_data(self) -> None:
        """Load poem lines from the dataset file.

        :raises ValueError: If ``dataset_path`` is not provided.
        """
        if not self.dataset_path:
            raise ValueError("dataset_path must be provided.")
        with open(self.dataset_path, encoding="utf-8") as f:
            self.data = f.readlines()

    def sample(self) -> list:
        """Generate sample requests from the poem lines.

        :raises ValueError: If ``input_len`` is less than the base prompt
            length.
        :returns: List of sample prompts.
        :rtype: list[str]
        """
        # Calculate average token length for a poem line.
        tokenized_lines = [self.tokenizer(line).input_ids for line in self.data]

        avg_len = sum(len(tokens) for tokens in tokenized_lines) / len(tokenized_lines)

        has_chat_template = getattr(self.tokenizer, "chat_template", None) is not None

        # Build the base prompt.
        base_prompt = "Pick as many lines as you can from these poem lines:\n"
        if has_chat_template:
            base_msg = [{"role": "user", "content": base_prompt}]
            base_fmt = self.tokenizer.apply_chat_template(
                base_msg, add_generation_prompt=True, tokenize=False
            )
            base_offset = len(self.tokenizer(base_fmt).input_ids)
        else:
            base_offset = len(self.tokenizer(base_prompt).input_ids)

        if self.input_len <= base_offset:
            raise ValueError(
                f"'input_len' must be higher than the base prompt length "
                f"({base_offset})."
            )

        # Determine how many poem lines to use.
        num_input_lines = round((self.input_len - base_offset) / avg_len)
        num_prefix_lines = round((self.prefix_len - base_offset) / avg_len)

        samples = []
        for _ in range(self.num_requests):
            # Completely randomize each sample by selecting random lines for both prefix and extra
            # Then shuffle the combined lines to ensure different ordering each time
            all_selected_lines = random.choices(self.data, k=num_input_lines)
            random.shuffle(all_selected_lines)  # Randomize the order of selected lines

            prompt = f"{base_prompt}{''.join(all_selected_lines)}"
            msg = [{"role": "user", "content": prompt}]

            if has_chat_template:
                prompt_formatted = self.tokenizer.apply_chat_template(
                    msg, add_generation_prompt=True, tokenize=False
                )
            else:
                prompt_formatted = prompt
            prompt_len = len(self.tokenizer(prompt_formatted).input_ids)
            samples.append(prompt_formatted if self.return_prompt_formatted else prompt)
        return samples
