#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

import sys
import re

def read_input(file):
    for line in file:
        # split the line into words
        yield line.split()

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    for words in data:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        for word in words:

	    if re.match('^[A-Za-z]+$', word):
		word = word.strip();
		word = word.upper();
          	print '%s%s%d' % (word, separator, 1)

if __name__ == "__main__":
    main()
