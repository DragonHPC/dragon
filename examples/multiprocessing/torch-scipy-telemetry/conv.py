"""This contains all of the functions used for the SciPy convolution jobs.
"""
import scipy.signal
import numpy as np


# Generate the scipy data
def init_data(args):
    """Generates SciPy Image Data

    :param args: input args from command line
    :type args: ArgumentParser object
    :return: returns image and filter pairing
    :rtype: tuple
    """
    image = np.zeros((args.size, args.size))
    nimages = int(float(args.mem) / float(image.size))
    print(f"Number of images: {nimages}", flush=True)
    images = []
    for j in range(nimages):
        images.append(np.zeros((args.size, args.size)))
    filters = [np.random.normal(size=(4, 4)) for _ in range(nimages)]
    return zip(images, filters)


def f(args):
    """Use scipy to convolve an image with a filter

    :param args: tuple containing image and filter
    :type args: tuple
    :return: convolved image
    :rtype: np.array
    """
    image, random_filter = args
    # Do some image processing.
    return scipy.signal.convolve2d(image, random_filter)[::5, ::5]
