from configs.settings import settings
import logging
import os
from datetime import datetime

timestamp = datetime.now().strftime("%d_%b_%Y_%H%M%S")
PATH = f"logger/logfile_{timestamp}.log"

def get_logfile():
    return PATH

def check_logfile():
    if os.path.isfile(PATH):
        os.remove(PATH)
    else:
        print(f"Log file does not exist. Creating {PATH}...")

def initialize_logger(name): 
    # Create and configure logger
    check_logfile()
    logging.basicConfig(level=logging.DEBUG,
                        filename=PATH,
                        format='%(name)s %(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%d-%m-%Y %H:%M:%S',
                        filemode='w')
    
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger().addHandler(console)
    
    # Creating an object
    settings.logger = logging.getLogger(name)