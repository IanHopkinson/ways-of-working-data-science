#!/usr/bin/env python
# encoding: utf-8

"""
setup.cfg defines an entry_point for the command cli-demo of the cli_group function
cli_group acts a placeholder to route commands of the form:

cli-demo [command] argument

This reproduces the default action of the demo_one.py script:
cli-demo action

And this passes in an argument:
cli-demo action goodbye

"""

import click
from wow.demo_one import print_something


@click.group()
def cli_group() -> None:
    pass


@cli_group.command()
@click.argument("message", default="hello")
def action(**kwargs):
    print_something(kwargs["message"])
