# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 10:50:59 2020

@author: disbr007
Logging module helper functions
"""

from datetime import datetime

import logging
import os
import platform
import sys

WINDOWS = 'Windows'
LINUX = 'Linux'

# TODO: Update this default log location
if platform.system() == WINDOWS:
    DEFAULT_LOGDIR = r'V:\pgc\data\scratch\jeff\projects\planet\logs'
elif platform.system() == LINUX:
    DEFAULT_LOGDIR = r'/mnt/pgc/data/scratch/jeff/projects/planet/logs'


def logging_level_int(logging_level):
    """
    Return integer representing logging level in logging module
    Parameters
    ----------
    logging_level: str
        One of the logging levels: 'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'

    Returns
    ---------
    INT
        The int corresponding to the logging level
    """
    if logging_level in logging._levelToName.values():
        for key, value in logging._levelToName.items():
            if value == logging_level:
                level_int = key
    else:
        level_int = 0

    return level_int


def create_logger(logger_name, handler_type,
                  handler_level=None,
                  filename=None,
                  duplicate=False):
    """
    Check if handler of specified type already exists on the logger name
    passed. If it does, and duplicate == False, no new handler is created.
    If it does not, the new handler is created.

    Parameters
    ----------
    logger_name : STR
        The name of the logger, can be existing or not.
    handler_type : STR
        Handler type, either 'sh' for stream handler or 'fh' for file handler.
    handler_level : STR
        One of the logging levels: 'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'
    filename : STR
        Name of logging file, existing or new.
    duplicate : BOOLEAN

    Returns
    -------
    logger : logging.Logger
        Either the pre-existing or newly created logger.
    """
    # Create logger
    logger = logging.getLogger(logger_name)

    # Check if handlers already exists for logger
    handlers = logger.handlers
    # print('handlers: {}'.format(handlers))
    handler = None
    # Parse input for requested handler type and level
    if handler_type == 'sh':
        ht = logging.StreamHandler()
    elif handler_type == 'fh':
        if not filename:
            print("""Error: Must provide a path to write log file to when using handler_type='fh'""")
        # if os.path.exists(filename):
        #     os.remove(filename)
        ht = logging.FileHandler(filename)
    else:
        print('Unrecognized handler_type argument: {}'.format(handler_type))
    desired_level = logging_level_int(handler_level)

    for h in handlers:
        # Check if existing handlers are of the right type (console or file)
        if isinstance(h, type(ht)):
            # Check if existing handler is of right level
            existing_level = h.level
            if existing_level == desired_level:
                # print('handler exists, not adding')
                handler = h
                break
            elif existing_level > desired_level:
                logger.removeHandler(h)

    # If no handler of specified type and level was found, create it
    if handler is None:
        # print('new handler')
        handler = ht
        logger.setLevel(desired_level)
        # Create console handler with a higher log level
        handler.setLevel(desired_level)
        # Create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        # Add the handler to the logger
        logger.addHandler(handler)
        # print('created handler', logger_name)
        # print(datetime.datetime.now())

    # Do not propogate messages from children up to parent
    logger.propagate = False

    return logger


def create_logfile_path(name, logdir=None):
    now = datetime.now().strftime('%Y%b%d_%H%m%S').lower()
    logname = '{}_{}.log'.format(name, now)
    if not logdir:
        logdir = DEFAULT_LOGDIR
    logfile = os.path.join(logdir, logname)

    return logfile
