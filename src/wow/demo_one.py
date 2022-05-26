#!/usr/bin/env python
# encoding: utf-8

"""
In the bash shell the presense of the line
#!/usr/bin/env python
Allows this script to be run using:
./demo_one.py
Otherwise use:
python demo_one.py
This script will take the first "word" after demo_one.py as the message for the print_something 
function
"""

import sys


def print_something(something):
    print(something, flush=True)
    return something


if __name__ == "__main__":
    message = "hello"
    if len(sys.argv) > 1:
        message = sys.argv[1]

    print_something(message)
