'''
Created on Aug 1, 2014

@author: jrosner, jtaghiyar

##TODO:
- add machine info to the log
'''

import logging, warnings, functools, sys
from ruffus.proxy_logger import make_shared_logger_and_proxy, setup_std_shared_logger
from datetime import datetime
import getpass
import socket
import traceback

class PipelineLogger(object):
    '''
    takes care of logging the events
    '''

    def __init__(self, verbose=False):

        # verbose prints to stdout in addition to the log
        self.verbose = verbose


    def get_logger(self, logger_name, log_file):
        '''
        Returns a shared logger and proxy
        '''
        # the log file should be this format:
        # '/<project_path>/<pipeline_name>_<run_id>.log'

        logger_args                = {}
        logger_args["file_name"]   = log_file
        logger_args["level"]       = logging.DEBUG
        logger_args["rotating"]    = True
        logger_args["maxBytes"]    = 10000000
        logger_args["backupCount"] = 10
        logger_args["formatter"]   = "[%(asctime)s] [%(name)s] [%(levelname)s]:\t%(message)s"

        logger_proxy, logger_mutex = make_shared_logger_and_proxy (setup_std_shared_logger,
                                                                   logger_name,
                                                                   logger_args)

        return [logger_proxy, logger_mutex]


    def log_info(self, log, message):
        '''
        Log info messege to the specified log
        '''

        logger_proxy, logger_mutex = log

        with logger_mutex:
            logger_proxy.info(message)

        if self.verbose:
            print message


    def log_warning(self, log, message):
        '''
        Log warning messege to the specified log
        '''

        logger_proxy, logger_mutex = log

        with logger_mutex:
            logger_proxy.warning(message)

        if self.verbose:
            print message


    def log_error(self, log, message):
        '''
        Log error messege to the specified log
        '''

        logger_proxy, logger_mutex = log

        with logger_mutex:
            logger_proxy.error(message)

        if self.verbose:
            print message


    def log_pipeline_header(self, log, args, pipeline_name, run_id, kronos_version):
        '''
        Logs general run information
        '''

        hdr_break = '-' * 30

        # log pipeline header
        self.log_info(log, hdr_break)
        self.log_info(log, ' PIPELINE RUN INFO')
        self.log_info(log, hdr_break)
        self.log_info(log, 'Kronos version: ' + kronos_version)
        self.log_info(log, 'pipeline name: ' + pipeline_name)
        self.log_info(log, 'run id: ' + run_id)
        self.log_info(log, 'launch date: ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.log_info(log, 'user: ' + getpass.getuser())
        self.log_info(log, 'hostname: ' + socket.getfqdn())
        self.log_info(log, 'command line arguments:')

        # convert args namespace to dictionary
        args_dict = vars(args)

        # log command line args
        for k, v in args_dict.iteritems():
            line = k + '\t\t= '+ str(v)
            self.log_info(log, '\t'+line)

        # log footer
        self.log_info(log, hdr_break)

        # log header for run information
        self.log_info(log, ' PIPELINE BEGINS')
        self.log_info(log, hdr_break)


    def log_pipeline_footer(self, log):
        '''
        log end date/time
        '''

        self.log_info(log, '-' * 30)
        self.log_info(log, ' PIPELINE ENDS')
        self.log_info(log, '-' * 30)
        self.log_info(log, 'finish date: ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


class LogInfo(PipelineLogger):
    
    """
    log info messages.
    """
    
    def __init__(self, logger, verbose=False):
        super(LogInfo, self).__init__(verbose)
        self.l = logger

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                stdoback   = sys.stdout
                sys.stdout = self
                res = func(*args, **kwargs)
                return res

            except:
                print >> sys.stderr, 'unexpected error in "{}"'.format(func.__name__)
                traceback.print_exc()
                raise 

            finally:
                sys.stdout = stdoback

        return wrapper

    def write(self, message):
        self.log_info(self.l, message.strip())


class LogWarnErr(PipelineLogger):
    
    """
    log warnings and errors.
    """
    
    def __init__(self, logger, verbose=False):
        super(LogWarnErr, self).__init__(verbose)
        self.l = logger
        self.flag = True

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                stdeback   = sys.stderr
                sys.stderr = self
                res = func(*args, **kwargs)
                return res

            except:
                print >> sys.stderr, 'unexpected error in "{}"'.format(func.__name__)
                traceback.print_exc()
                raise 

            finally:
                sys.stderr = stdeback

        return wrapper

    def write(self, message):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            w = filter(lambda i: issubclass(i.category, UserWarning), w)
            if len(w):
                self.log_warning(self.l, message.strip())

            else:
                self.log_error(self.l, message.strip())
