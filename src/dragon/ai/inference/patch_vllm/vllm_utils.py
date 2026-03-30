import vllm
import os
from packaging.specifiers import SpecifierSet

def get_vllm_version():
    """Return the installed vLLM version string.

    :returns: The vLLM version string.
    :rtype: str
    """
    return vllm.__version__

def get_vllm_utils_file():
    """Finds the vllm package in your python venv. 
    Furthermore, finds a specific vllm file called '__init__.py' in the 
    utils package folder.
    This is for vLLM versions ``> 0.8.5``.

    :returns: File path of ``utils/__init__.py`` in the vLLM base package
        directory.
    :rtype: str
    """
    package_path = os.path.dirname(vllm.__file__)
    print(f'VLLM base package path: {package_path=}', flush=True)
    vllm_utils_file_path = None
    for root, dirs, files in os.walk(package_path):
        if root == os.path.join(package_path, 'utils'):
            for file in files:
                if file == '__init__.py':
                    vllm_utils_file_path = os.path.join(root, file)
            dirs.clear()  # Prevent descending into subdirectories
            break  # Remove this line if you want to traverse the entire tree
    print(f'VLLM utils.py script path: {vllm_utils_file_path=}', flush=True)
    return vllm_utils_file_path

def get_vllm_utils_file_0_8_5():
    """Finds the vllm package in your python venv. 
    Furthermore, finds a specific vllm file called 'utils.py' in the 
    base package folder.
    This is for vLLM versions ``<= 0.8.5`` where the line to replace is
    located in ``utils.py``.

    :returns: File path of ``utils.py`` in the vLLM base package directory.
    :rtype: str
    """
    package_path = os.path.dirname(vllm.__file__)
    print(f'VLLM base package path: {package_path=}', flush=True)
    vllm_utils_file_path = None
    for root, dirs, files in os.walk(package_path):
        for file in files:
            if file == 'utils.py':
                vllm_utils_file_path = os.path.join(root, file)
        dirs.clear()
        break
    print(f'VLLM utils.py script path: {vllm_utils_file_path=}', flush=True)
    return vllm_utils_file_path

def replace_line(file_path, original_phrase, replace_phrase):
    """Replace a specific line in a file.

    :param file_path: Path to the file.
    :type file_path: str
    :param original_phrase: Original line/string to look for.
    :type original_phrase: str
    :param replace_phrase: Updated line/string to write in its place.
    :type replace_phrase: str
    """
    # Get all lines.
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.", flush=True)
        return

    # Search and replace line
    for i, line in enumerate(lines):
        if line.strip() == original_phrase.strip():
            print('\nSuccessfully found and replaced patch line!\n', flush=True)
            indentation = line[:len(line) - len(line.lstrip())]
            lines[i] = indentation + replace_phrase + '\n'
            break

    # Write back updated lines to original file.
    with open(file_path, 'w') as f:
        f.writelines(lines)

def get_vllm_system_utils_file():
    """Finds the vllm package in your python venv. 
    Furthermore, finds a specific vllm file called 'system_utils.py' in the 
    utils package folder.
    This is for vLLM versions ``>= 0.12.0`` where ``get_mp_context`` moved
    to ``system_utils.py``.

    :returns: File path of ``utils/system_utils.py`` in the vLLM base package
        directory.
    :rtype: str
    """
    package_path = os.path.dirname(vllm.__file__)
    print(f'VLLM base package path: {package_path=}', flush=True)
    vllm_system_utils_file_path = None
    for root, dirs, files in os.walk(package_path):
        if root == os.path.join(package_path, 'utils'):
            for file in files:
                if file == 'system_utils.py':
                    vllm_system_utils_file_path = os.path.join(root, file)
            dirs.clear()  # Prevent descending into subdirectories
            break
    print(f'VLLM system_utils.py script path: {vllm_system_utils_file_path=}', flush=True)
    return vllm_system_utils_file_path


if __name__ == "__main__":

    vllm_version = get_vllm_version()
    print(f'Installed vLLM version: {vllm_version}', flush=True)
    
    vllm_spec_old = SpecifierSet("<=0.8.5")
    vllm_spec_0_12 = SpecifierSet(">=0.12.0")
    
    if vllm_version in vllm_spec_old:
        print('vLLM version <=0.8.5', flush=True)
        vllm_utils_file_path = get_vllm_utils_file_0_8_5()
    elif vllm_version in vllm_spec_0_12:
        print('vLLM version >=0.12.0 - using system_utils.py', flush=True)
        vllm_utils_file_path = get_vllm_system_utils_file()
    else:
        # vLLM 0.8.6 - 0.11.x
        print('vLLM version >0.8.5 and <0.12.0', flush=True)
        vllm_utils_file_path = get_vllm_utils_file()

    if vllm_utils_file_path == None:
        raise ValueError('Unable to find vllm package. Make sure you are in your python virtual environment and have installed VLLM package.')

    # Replace VLLM's spawn context with existing dragon context.
    original_phrase = "return multiprocessing.get_context(mp_method)"
    replace_phrase = "return multiprocessing.get_context()"
    replace_line(vllm_utils_file_path, original_phrase, replace_phrase)