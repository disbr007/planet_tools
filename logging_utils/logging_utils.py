# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 10:50:59 2020

@author: disbr007
Logging module helper functions
"""

from datetime import datetime

import logging
import os
from pathlib import Path
import platform
import sys

from get_config import get_config


class CustomError(Exception):
    """pass"""

    pass


def LOGGING_CONFIG(level):
    """Config dict for logging module"""
    CONFIG = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': { 
            'standard': { 
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
        },
        'handlers': {
            'default': {
                'level': level,
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout',  # Default is stderr
            },
            'module': {
                'level': logging.DEBUG,
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout',  # Default is stderr
            },
        },
        'loggers': {
            '': {  # root logger
                'handlers': ['default'],
                'level': 'WARNING',
                'propagate': True
            },
            '__main__': {  # if __name__ == '__main__'
                'handlers': ['default'],
                'level': level,
                'propagate': False
            },
            'module': {
                'handlers': ['default'],
                'level': logging.DEBUG,
                'propagate': True
            },
        }
    }
    return CONFIG


def logging_level_int(logging_level):
    """
    Return integer representing logging level in logging module
    Parameters
    ----------
    logging_level : STR
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
    handler = None
    # Parse input for requested handler type and level
    if handler_type == 'sh':
        ht = logging.StreamHandler()
    elif handler_type == 'fh':
        if not filename:
            print("""Error: Must provide a path to write log file to when using handler_type='fh'""")
        if os.path.exists(filename):
            os.remove(filename)
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
                handler = h
                # print('handler exists, not adding')
                break

    # If no handler of specified type and level was found, create it
    if handler is None:
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


def project_path():
    if platform.system() == 'Windows':
        prj_path = r'C:\code\pgc-code-all'
    elif platform.system() == 'Linux':
        prj_path = r'/mnt/pgc/data/scratch/jeff/code/pgc-code-all'

    return prj_path


def project_modules():
    modules = [f.split('.')[0] for root, dirs, files in os.walk(project_path()) for f in files if f.endswith('.py')]

    return modules


def imported_modules():
    # TODO: modules is missing submodule name, i.e. submodule.fxn is just fxn?
    modules = project_modules()
    imported = [m for m in modules if m in sys.modules.keys()]
    
    return imported


def create_module_loggers(handler_type, handler_level,
                          filename=None, duplicate=False):

    imported = imported_modules()
    loggers = []
    for i in imported:
        logger = create_logger(logger_name=i,
                               handler_type=handler_type,
                               handler_level=handler_level,
                               filename=filename,
                               duplicate=duplicate)
        loggers.append(logger)
    
    return loggers


def create_logfile_path(name, logdir=None):
    now = datetime.now().strftime('%Y%b%d_%H%m%S')
    logname = '{}_{}.log'.format(name, now)
    if not logdir:
        logdir = get_config("logdir")
    logfile = os.path.join(logdir, logname)

    return logfile
