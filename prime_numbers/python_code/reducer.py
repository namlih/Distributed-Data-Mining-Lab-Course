#!/usr/bin/env python
import sys

def main():
    count = 0
    for data in sys.stdin:
	count += 1
    print(count)



if __name__ == "__main__":
    main()
