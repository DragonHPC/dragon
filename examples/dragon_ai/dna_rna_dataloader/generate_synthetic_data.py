import random
import argparse


def generate_sequence(length, bases):
    """Generate a random sequence of given length from provided bases."""
    return "".join(random.choices(bases, k=length))


def write_file_with_design_id(file_path, file_size_gb):
    """
    Write a file with half DNA and half RNA sequences,
    each prefixed with a unique design ID.

    Parameters:
        file_path (str): The path of the file to write.
        file_size_gb (float): The target size of the file in GB.
    """
    # Convert GB to bytes
    target_size_bytes = int(file_size_gb * 1024**3)
    dna_bases = ["A", "T", "C", "G"]
    rna_bases = ["A", "U", "C", "G"]

    # Allocate half the target size for DNA and half for RNA
    dna_size = target_size_bytes // 2
    rna_size = target_size_bytes // 2

    with open(file_path, "w") as file:
        line_number = 1

        # Write DNA sequences with design ID
        while dna_size > 0:
            chunk_size = min(1024, dna_size)  # Write in chunks of 1 KB
            dna_sequence = generate_sequence(chunk_size, dna_bases)
            file.write(str(dna_sequence) + ",design_id_" + str(line_number) + "\n")
            dna_size -= len(dna_sequence) + 1
            line_number += 1

        # Write RNA sequences with design ID
        while rna_size > 0:
            chunk_size = min(1024, rna_size)  # Write in chunks of 1 KB
            rna_sequence = generate_sequence(chunk_size, rna_bases)
            file.write(str(rna_sequence) + ",design_id_" + str(line_number) + "\n")
            rna_size -= len(rna_sequence) + 1
            line_number += 1


# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Capture user input about size of synthetic DNA and RNA files in GB."
    )
    parser.add_argument("--size", type=float, help="Specify the size of a single file in GB")
    args = parser.parse_args()
    if args.size:
        # if size provided, generate file of given sizes
        file_size_gb = args.size
        file_path = "seq" + str(file_size_gb) + "gb.txt"
        write_file_with_design_id(file_path, file_size_gb)
    else:
        # create files from 1 GB to 32 GB
        for i in range(6):
            file_size_gb = 2**i
            file_path = "seq" + str(file_size_gb) + "gb.txt"
            write_file_with_design_id(file_path, file_size_gb)
