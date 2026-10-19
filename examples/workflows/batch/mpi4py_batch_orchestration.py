

"""Launch several mpi4py jobs through the Batch workflow API."""

from pathlib import Path
import sys

from dragon import Batch, ProcessTemplate
from dragon.infrastructure.facts import PMIBackend


def main() -> None:
    example_path = Path(__file__).resolve().with_name("mpi4py_example.py")
    template = ProcessTemplate(target=sys.executable, args=(str(example_path),))
    sizes = [3, 2, 2, 1]

    with Batch() as batch:
        jobs = [
            batch.options(pmi=PMIBackend.CRAY, name=f"mpi-job-{index}").job(
                process_templates=[(size, template)]
            )
            for index, size in enumerate(sizes)
        ]

        for job in jobs:
            try:
                print(f"Got exit codes {job.get()}", flush=True)
            except Exception as exc:
                print(f"Job got exception: {exc}", flush=True)


if __name__ == "__main__":
    main()