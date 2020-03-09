from extractor import Extractor

from configparser import ConfigParser
import json
import os


def update_wm_time_widget(v2_hfl, v2_map, gis):

    # Unpack Item Targets
    map_itm = gis.content.get(v2_map)
    hfl_itm = gis.content.get(v2_hfl)
    hfl_lyr = hfl_itm.layers[0]

    # Collect Earliest and Most Recent Dates in Hosted Feature Layer
    hfl_sdf = hfl_lyr.query(out_fields='extracted_date', return_geometry=False).sdf
    unique_dates = sorted(hfl_sdf['extracted_date'].unique())
    start, end = unique_dates[0], unique_dates[-1]

    # Update Time Widget in Web Map
    map_data = map_itm.get_data()
    map_data['widgets']['timeSlider']['properties']['startTime'] = start.astype('uint64') / 1e6
    map_data['widgets']['timeSlider']['properties']['endTime'] = end.astype('uint64') / 1e6
    map_itm.update(data=json.dumps(map_data))


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
    v2_map   = config.get('AGOL', 'v2_map')
    v1_hft   = config.get('AGOL', 'v1_hft')

    e = Extractor()

    e.connect(agol_url, username, password)

    # Update AGOL Features
    e.run_v2(v2_hfl)

    # update_wm_time_widget(v2_hfl, v2_map, e.gis)


