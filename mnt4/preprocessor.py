#!/usr/bin/env python

from __future__ import print_function
import sys

def main():

    for line in sys.stdin:
	line = list(line)
	if line [0] == '>':
		line = '\n'
	else:
		line[-1] = ''
	line = ''.join(line)
	print(line, end = "")



if __name__ == "__main__":
    main()


