#!/usr/bin/env python
# encoding: utf-8

import csv
import logging
import os

from typing import Any, Dict, List, Optional, Union


def write_dictionary(
    filename: Union[str, bytes, os.PathLike],
    data: List[Dict[str, Any]],
    append: Optional[bool] = True,
    delimiter: Optional[str] = ",",
) -> None:
    """
    Writes a list of dictionaries to a CSV file

    :param filename: file path to the output file
    :param data: A list of ordered dictionaries to write
    :param append: if True then data is appended to an existing file, if False and the file exists
                   then the file is deleted
    :param delimiter: Delimiter character as per dictwriter interface
    :return: returns None
    """
    if len(data) == 0:
        logging.info("No data supplied to write_dictionary for filename: {}".format(filename))
        return

    keys = list(data[0].keys())

    newfile = not os.path.isfile(filename)

    if not append and not newfile:
        logging.warning(
            "Append is False, and {} exists therefore file is being deleted".format(filename)
        )
        os.remove(filename)
        newfile = True
    elif not newfile and append:
        logging.info(
            "Append is True, and {} exists therefore data is being appended".format(filename)
        )
    else:
        logging.info("New file {} is being created".format(filename))

    with open(filename, "a+", encoding="utf-8") as output_file:
        dict_writer = csv.DictWriter(output_file, keys, lineterminator="\n", delimiter=delimiter)
        if newfile:
            dict_writer.writeheader()
        dict_writer.writerows(data)


# Logging to file and console simultaneously
# https://aykutakin.wordpress.com/2013/08/06/logging-to-console-and-file-in-python/
def initialise_logger(output_file, mode="both", force=False, handler_mode="w", verbose=False):
    #   This code is copied across from the ninetales-data-visualisation repo
    #   It sets up logging to file and screen, possible it should go in the ihutilities repo
    if verbose:
        formatter = logging.Formatter("%(asctime)s|%(module)s|%(funcName)s|%(lineno)d|%(message)s")
    else:
        formatter = logging.Formatter("%(message)s")
    logger = logging.getLogger()

    logger.setLevel(logging.INFO)
    # This removes previously defined handlers before adding out own
    if force:
        logger.handlers.clear()

    if mode == "both":
        # create console handler and set level to info
        # We infer that if there are any log handlers then there must be a StreamHandler
        # which we don't want to duplicate
        if len(logger.handlers) == 0:
            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

    if mode == "both" or mode == "file only":
        # create error file handler and set level to info
        handler = logging.FileHandler(output_file, handler_mode, encoding=None, delay="true")
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
