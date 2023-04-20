#!/usr/bin/env python

import sys

def main():
    for line in sys.stdin:
        line = line.strip()
        words = line.split()
        for word in words:
            print '%s%s%d' % (word, '\t', 1)

if __name__ == "__main__":
    main()

