import os.path, sys
import logging
DAG_PATH = os.path.dirname(os.path.abspath(__file__))

if DAG_PATH not in sys.path:
    sys.path.insert(0, DAG_PATH)
    sys.path.insert(0, DAG_PATH + '/plugins')

logging.debug('*** Python System Path: ' + str(sys.path))
