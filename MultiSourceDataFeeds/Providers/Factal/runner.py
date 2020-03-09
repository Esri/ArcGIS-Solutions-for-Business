import factal

from configparser import ConfigParser
import datetime
import logging
import time
import os


# def get_logger(start_time, log_dir, run_name):

#     the_logger = logging.getLogger(run_name)
#     the_logger.setLevel(logging.DEBUG)

#     # Base Date & Time
#     time = datetime.datetime.fromtimestamp(start_time).strftime('%H_%M_%S')

#     # Ensure Directories Exist
#     if not os.path.exists(log_dir):
#         os.makedirs(log_dir)

#     # Set Console Handler
#     ch = logging.StreamHandler()
#     ch.setLevel(logging.DEBUG)

#     # Set File Handler
#     fh = logging.FileHandler(os.path.join(log_dir, f'{run_name}_{time}.log'), 'w')
#     fh.setLevel(logging.INFO)

#     formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

#     ch.setFormatter(formatter)
#     fh.setFormatter(formatter)

#     the_logger.addHandler(ch)
#     the_logger.addHandler(fh)

#     the_logger.info('Logger Initialized')

#     return the_logger


if __name__ == "__main__":

    # Get Start Timie
    start_time = time.time()

    # Get Current Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]

    # # Get Logger
    # logger = get_logger(start_time, os.path.join(this_dir, 'logs'), 'FACTAL')

    # Read Configuration File
    config = ConfigParser()
    config.read(os.path.join(this_dir, 'config.ini'))

    # Factal Parameters
    factal_token = config.get('Factal', 'token')

    # AGOL Parameters
    agol_url = config.get('AGOL', 'agol_url')
    username = config.get('AGOL', 'user_name')
    password = config.get('AGOL', 'password')
    incident_item_id = config.get('AGOL', 'incident_item_id')
    topic_layer_id = config.get('AGOL', 'topic_layer_id')

    # Geodatabase Parameters
    local_db = config.get('GDB', 'local_db')
    fc_name  = config.get('GDB', 'fc_name')

    # Create Factal Extractor Instance
    e = factal.Extractor(factal_token)

    # Connect to GIS & Populate with Current Factal API Feed
    e.connect(agol_url, username, password)

    # Run Baseline Solution Logic
    responses = e.run_solution(incident_item_id, topic_layer_id)

    # # Write Responses to Logger
    # for response in responses:
    #     logger.info(response)

    # # Log Run Time
    # logger.info(f'Program Run Time: {round(((time.time() - start_time) / 60), 2)} Minute(s)')

    # # Ensure Logger Handlers Are Cleaned Up
    # logging.shutdown()
