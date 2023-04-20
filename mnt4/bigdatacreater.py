#!/usr/bin/env python

from __future__ import print_function
from itertools import izip
import sys

def main():
    raw_positive = sys.argv[3]
    positive_set = set(open(raw_positive).read().splitlines())
    
    filenames = sys.argv[1:3]
    files = [open(i, "r") for i in filenames]
    for rows in izip(*files):
	rows = tuple(x.strip() for x in rows)
        if rows[1] in positive_set:
	    print(rows[0] + ',' + rows[1] + ',' + '1')
	else:
	    print(rows[0] + ',' + rows[1] + ',' + '0')


if __name__ == "__main__":
    main()


