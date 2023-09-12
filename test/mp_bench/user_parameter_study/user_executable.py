#!/usr/bin/env python
import sys

def in_circle(x,y):
    if ((x**2)+(y**2)) < 1:
        return True
    else:
        return False

if __name__ == '__main__':
    print(in_circle(float(sys.argv[1]), float(sys.argv[2])))
