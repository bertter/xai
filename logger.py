import os
import logging
import pprint
from functools import partial

LOG_LEVEL = 'INFO'

SCRIPT_PATH = os.path.abspath(os.path.dirname(__file__))

logging.basicConfig(
	level=getattr(logging, LOG_LEVEL),
	filename='{}/tracker.log'.format(SCRIPT_PATH),
	format='[%(asctime)s][%(name)s][%(levelname)-5s][%(filename)s(%(lineno)s)] %(message)s',
	datefmt='%Y-%m-%d %H:%M:%S'
)

def ActiveConsole():
	console = logging.StreamHandler()
	logging.getLogger('').addHandler(console)

log = logging.getLogger('Tracker')

#log_error = partial(log_print, level = 'error')
#log_info = partial(log_print, level = 'info')