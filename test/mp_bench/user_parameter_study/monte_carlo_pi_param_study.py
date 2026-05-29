import user_executable
import random
import argparse
from user_parameter_study import ParameterStudy as ParameterStudy


def pi_work_items(ps, num_points):
    for i in range(num_points):
        x = (2 * random.random()) - 1
        y = (2 * random.random()) - 1
        ps.add_work_item([x, y])


def in_circle(x, y):
    if ((x**2) + (y**2)) < 1:
        return True
    else:
        return False


def process_args():
    parser = argparse.ArgumentParser(description="Get Monte Carlo integration options.")
    parser.add_argument("--num_points", type=int, default=10000, help="number of points to generate")
    parser.add_argument("--num_workers", type=int, default=10, help="number of workers to spawn at a time")
    parser.add_argument("--c_exec", action="store_true", help="use C executable instead of Python function")
    parser.add_argument("--python_exec", action="store_true", help="use Python executable with subprocess")
    parser.add_argument("--func_out_file", action="store_true", help="use Python function from a separate file")
    parser.add_argument("--func_in_file", action="store_true", help="user Python function within file. Default.")
    args = parser.parse_args()
    check_sum = args.c_exec + args.python_exec + args.func_out_file + args.func_in_file
    if check_sum > 1:
        raise Exception("Please choose only one type of executable/function")
    return args


if __name__ == "__main__":
    args = process_args()
    if args.c_exec:
        circ = ParameterStudy(num_dimensions=2, executable="./a.out", num_workers=args.num_workers)
    elif args.python_exec:
        circ = ParameterStudy(num_dimensions=2, executable="python user_executable.py", num_workers=args.num_workers)
    elif args.func_out_file:
        circ = ParameterStudy(num_dimensions=2, func=user_executable.in_circle, num_workers=args.num_workers)
    else:
        circ = ParameterStudy(num_dimensions=2, func=in_circle, num_workers=args.num_workers)
    num_points = args.num_points
    pi_work_items(circ, num_points)
    circ.go()
    inside = 0
    for item in circ.results:
        if item == True:
            inside += 1
        elif item == False:
            pass
        elif item.strip() == "True":
            inside += 1
    print("Estimated value of pi: ", (inside / num_points) * 4, "\n")
    print(circ.performance_info(output="str"))
