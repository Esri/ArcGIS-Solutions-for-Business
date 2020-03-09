from extractor import Extractor

from configparser import ConfigParser
import os


if __name__ == "__main__":

    # Get Current Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]

    # Read Configuration File
    config = ConfigParser()
    config.read(os.path.join(this_dir, 'config.ini'))
    
    # AGOL Parameters
    agol_url = config.get('AGOL', 'agol_url')
    username = config.get('AGOL', 'username')
    password = config.get('AGOL', 'password')
    v2_hfl   = config.get('AGOL', 'v2_hfl')
    v2_hft   = config.get('AGOL', 'v2_hft')
    v1_hft   = config.get('AGOL', 'v1_hft')
    v1_gdb   = config.get('AGOL', 'v1_gdb')

    e = Extractor()

    e.connect(agol_url, username, password)

    # e.build_v1('GDELT Solutions')

    e.run_v1(v1_hft, v1_gdb)
