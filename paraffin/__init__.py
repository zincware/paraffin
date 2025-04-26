import importlib.metadata
import logging

log = logging.getLogger(__name__)

log.setLevel(logging.DEBUG)
# attach a console handler that pritns the time
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s %(message)s")
ch.setFormatter(formatter)
log.addHandler(ch)

__version__ = importlib.metadata.version("paraffin")
