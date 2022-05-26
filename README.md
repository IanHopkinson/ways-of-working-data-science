# ways-of-working-data-science

This repository contains a template for Python projects in data science

Written to accompany this blog post:
https://ianhopkinson.org.uk/2022/05/a-way-of-working-data-science/


2022-05-25 Ian Hopkinson

## Installation

Create a virtual environment (Python 3.9), using Anaconda:

`conda create -n wow python=3.9`

and activate it:

`conda activate wow`

and then install using:

`pip install -e .`

## Scripts

In the src/wow directory:
- **db_utils.py** - contains database utilities 
- **utils.py** - contains utilities for initialising a logger and writing a list of dictionaries to a file
- **demo_one.py** - is a simple demo of a script which takes an optional commandline argument using `sys.argv`
- **cli.py** - uses an entry_point in setup.cfg and the click library to implement a simple command line application

In the tests/ directory:
- **test_db_utils.py** - tests the db_utils.py functions
- **test_demo_one.py** - tests the demo_one.py functions
