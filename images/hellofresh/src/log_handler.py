import logging
from logging.handlers import TimedRotatingFileHandler
import os


def list_files(startpath):
    '''for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        print(subindent)
        for f in files:
            print('Files{}{}'.format(subindent, f))'''

def initialize_logger():
    logging.basicConfig(format ='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
                        filename='.'
                                 './logs/etl_pipeline.log',
                        level=logging.INFO)
