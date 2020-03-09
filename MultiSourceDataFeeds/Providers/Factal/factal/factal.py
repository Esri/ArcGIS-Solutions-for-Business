import factal.schema as schema
from arcgis.features import GeoAccessor
from arcgis.gis import GIS
from datetime import datetime, timedelta
import pandas as pd
import requests
import time

class Extractor(object):

    def __init__(self, token):

        self.token = token
        self.urls  = self.get_urls()
        self.gis   = None

    @staticmethod
    def get_urls():

        """ Return Dictionary of Target API Endpoints """

        return {
            'item': 'https://www.factal.com/api/v2/item',
            'topic': 'https://www.factal.com/api/v2/topic'
        }


    def add(self, lyr, sdf, id_field):

        incoming_ids = sdf[id_field].tolist()
        existing_ids = [f.attributes[id_field] for f in lyr.query().features]
        new_item_ids = list(set(incoming_ids).difference(set(existing_ids)))

        add_features = sdf[sdf[id_field].isin(new_item_ids)]

        if len(add_features) > 0:
            res = lyr.edit_features(adds=add_features.spatial.to_featureset())['addResults']
            return len([i for i in res if i['success']])
        else:
            return 0


    def build_incident_hfl(self):

        incidents, arcs = self.parse_items(self.fetch_items())

        incident_df = self.get_df(incidents)

        incident_df.spatial.to_featurelayer(f'Factal_{round(time.time())}', gis=self.gis, tags='Factal')


    def connect(self, agol_url, username, password):

        self.gis = GIS(agol_url, username, password)


    def convert_item_to_df(self, item_data):

        """ Return Data Frame from a List of Dictionaries Representing Item/Topic Locations """
        df = pd.DataFrame(item_data)
        df = df.spatial.from_xy(df, 'longitude', 'latitude')

        # Convert Time Series to UTC
        for field in schema.item_times:
            df[field] = pd.to_datetime(df[field], utc=True)

        return df


    def convert_topic_to_df(self, topic_data):

        """ Return Data Frame from a List of Dictionaries Representing Topics """

        df = pd.DataFrame(topic_data)

        # Convert Time Series to UTC
        for field in schema.topic_times:
            df[field] = pd.to_datetime(df[field], utc=True)

        return df


    def delete_features(self, lyr, id_field, id_list):
        '''
        '''
        
        # Get Existing AGOL Features from topics layer
        exist_df = lyr.query().features

        # Return if the existing layer dataframe has no records
        if len(exist_df) > 0:

            # Selection OIDs from the topics layer for deletion.
            # if item id from layer is in list, capture OID and store in list
            the_oid = lyr.properties.objectIdField
            del_ids = [str(f.attributes[the_oid])
                       for f in exist_df if f.attributes[id_field] in id_list]

            if len(del_ids) > 0:
                res = lyr.delete_features(deletes=','.join(del_ids))['deleteResults']
                print('Deleted {} rows'.format(len([i for i in res if i['success']])))


    def fetch_items(self, endpoint='item', limit=250, **kwargs):

        ''' 
        Fetch Items & Return List of Result Dictionaries.

        Additional API key's can be passed in and added to the payload 
        using keyword arguments.
        
        '''
        headers = {'Authorization': f'Token {self.token}'}
        payload = {
            'order_by': 'last_item_date',
            'active': 'True',
            'limit': limit,
        }

        for key, val in kwargs.items():
            payload.update({key:val})

        response = requests.get(self.urls[endpoint], params=payload, headers=headers)
        if response.status_code == 200:
            return response.json()['results']

        else:
            raise Exception(f'Fetching Factal Items Returned Status: {response.status_code}')


    def get_gis_item(self, item_id):

        results = self.gis.content.search(f'id: {item_id}')

        if len(results) != 1:
            raise Exception(f'Empty of Ambiguous Result for Item ID: {item_id}')

        return results[0]


    def get_location(self, location_list):

        for loc in location_list:
            loc.update({'sort': schema.loc_pref.get(loc['category'], 0)})

        sorted_locs = sorted(location_list, key=lambda k: k['sort'])

        return sorted_locs[0]['latitude'], sorted_locs[0]['longitude'], sorted_locs[0]['category'], sorted_locs[0]['name']

    def parse_items(self, item_list):

        """ Iterates Through Item Dictionaries & Returns Item Features & Related Arcs """
        
        all_items = []
        all_topics = []

        for item in item_list:

            i_inc = {}  # One Item Dictionary per Item
            locs = [] # List to store location information for an item

            #Dictionary holder to store values for different kinds of topics.
            topic_dict = dict()
            for kind in schema.topic_kinds:
                topic_dict.setdefault(kind, list())

            # Push Basic Item Level Values
            i_inc.update({k: v for k, v in item.items() if k in schema.item_fields})
            i_inc.update({'id': str(item['id'])})

            # Add severity hex color codes to item dictionary
            if str(i_inc['severity']) in schema.severity_hex_color_codes.keys():
                i_inc['severity_hex_color'] = schema.severity_hex_color_codes[str(i_inc['severity'])]
            else:
                # Default value if value doesn't exist in hex color dictionary
                i_inc['severity_hex_color'] = '#FFFFFF'

            # Check for blank URLs if the main source is from twitter
            if not i_inc['url']:
                domain = item['url_domain']
                tweet_id = item['tweet_id']
                i_inc['url'] = f'https://{domain}/status/{tweet_id}'


            # Sort Topic Kinds for Specific Handlers
            for topic in item['topics']:

                t_inc = {} # One topic dictionary per topic. Each item can have 1 or more topics

                # Pull out topic key
                topic_key = topic['topic']

                # Push topic level values
                t_inc.update({k: v for k, v in topic_key.items() if k in schema.topic_fields})
                
                # Push keys/values outside the topic key
                t_inc.update({k: v for k, v in topic.items() if k in schema.topic_fields_other})

                # Add item ID to topic
                t_inc.update({'item_id': str(item['id'])})

                # Add time stamp of item to topic
                t_inc.update({'latest_item_date': item['updated_date']})

                # Only keep descriptions that are relevant
                if topic_key['kind'] != 'arc':
                    t_inc['description'] = ' '
                else:
                    t_inc['description'] = topic_key['description']

                # Extract all topics with the kind = location.  This stores the lat/long for the item
                if topic_key['kind'] == 'location':
                    locs.append(topic['topic'])

                # Store values for each topic kind.  The list of values will be converted to a semicolon delimited string
                # and added to the associated kind field.
                if topic['topic']['kind'] in schema.topic_kinds:
                    topic_dict[topic['topic']['kind']].append(topic['topic']['name'])

                else:
                    print(f"Need Handler for Kind: {topic['topic']['kind']}")

                # Append topic content to all topics list
                all_topics.append(t_inc)

            # Ignore Anything without a Location Topic
            if not locs: continue

            # Push most accurate Location to Incident Feature
            lat, lon, cat, loc_name = self.get_location(locs)
            i_inc.update({'latitude': lat, 'longitude': lon, 'resolution': cat, 'location_name': loc_name})

            # Add resolution hex color codes to item dictionary
            if i_inc['resolution'] in schema.resolution_hex_color_codes.keys():
                i_inc['resolution_hex_color'] = schema.resolution_hex_color_codes[i_inc['resolution']]
            else:
                # Default value if value doesn't exist in hex color dictionary
                i_inc['resolution_hex_color'] =  '#D4D4D4'
            
            # Push all kind values to semicolon delimited string and set
            # Push delimited string to item dictionary
            for k, val in topic_dict.items():
                list_to_string = ';'.join(val)
                i_inc.update({k: list_to_string})

            all_items.append(i_inc)

        return all_items, all_topics


    def update_items(self, lyr, sdf, id_field, time_field):

        # Get Existing AGOL Features
        exist_df = lyr.query().sdf

        # Return if the Existing Dataframe Has No Records
        if len(exist_df) < 1: return 0, None

        # Define UTC for Comparison Against Current API Query Data Frame
        exist_df[time_field] = pd.to_datetime(exist_df[time_field], utc=True)
        
        # https://stackoverflow.com/a/57980631: This call correctly modifies the precision so both DFs are both in seconds.
        exist_df[time_field] = exist_df[time_field].dt.ceil(freq='s')
        sdf[time_field] = sdf[time_field].dt.ceil(freq='s')
        #exist_df[time_field] = exist_df[time_field].map(lambda x: x.replace(microsecond=0))
        #sdf[time_field] = sdf[time_field].map(lambda x: x.replace(microsecond=0))

        # Convert datetimes to epoch seconds
        # https://stackoverflow.com/a/35630179: This approach is significantly faster for conversion
        #exist_df[time_field] = exist_df[time_field].astype('int64')//1e9
        #sdf[time_field] = sdf[time_field].astype('int64')//1e9

        # Join on ID & Compare Dates
        merge_df = exist_df.merge(sdf, on=id_field, suffixes=('_e', '_i'))

        # If the timestamps coming in are newer than existing timestamps, overwrite existing with newer
        # timestamps and add the suffix _i
        merge_df = merge_df[merge_df[f'{time_field}_i'] > merge_df[f'{time_field}_e']]

        # We Just Want to Keep the OBJECTID Field from the Existing Features
        # Any timestamps with the suffix _i are kept for applying updates.
        update_df = merge_df[[c for c in merge_df.columns if not c.endswith('_e')]]

        # Remove the suffix _i.  They are no longer needed to identify what's been updated.
        update_df.columns = update_df.columns.str.replace('_i', '')

        # Get list of updated item ids. This list will be returned to records for the related topics
        updated_item_ids = update_df[id_field].values

        if len(update_df) > 0:
            res = lyr.edit_features(updates=update_df.spatial.to_featureset())['updateResults']
            results = len([i for i in res if i['success']])
            return results, updated_item_ids
        else:
            return '0', None

    def update_topics(self, lyr, sdf, id_field, item_ids):
        '''
        '''

        if item_ids is None: return 0

        self.delete_features(lyr, id_field, item_ids)
        sdf_selection = sdf[sdf[id_field].isin(item_ids)]

        if len(sdf_selection) > 0:
            res = lyr.edit_features(adds=sdf_selection.spatial.to_featureset())[
                'addResults']
            return len([i for i in res if i['success']])
        else:
            return 0


    def run_solution(self, content_itemID, content_topicID):

        # Build SpatialDataFrames From Factal API Items
        itms, topics = self.parse_items(self.fetch_items())
        itm_df = self.convert_item_to_df(itms)
        topics_df = self.convert_topic_to_df(topics)

        # Fetch Items from GIS
        curr_itm = self.get_gis_item(content_itemID)
        topics_itm = self.get_gis_item(content_topicID)

        # Unpack Layers & Tables
        curr_lyr = curr_itm.layers[0]
        topics_tbl = topics_itm.tables[0]

        # Run Updates
        curr_itm_upd_res, item_IDs = self.update_items(curr_lyr, itm_df, schema.itm_id, schema.itm_time_check)
        topic_upd_res= self.update_topics(topics_tbl, topics_df, schema.topic_id, item_IDs)

        # Run Adds
        curr_itm_add_res = self.add(curr_lyr, itm_df, schema.itm_id)
        topic_add_res = self.add(topics_tbl, topics_df, schema.topic_id)

        # Add Log Strings
        curr_add_str = f'{content_itemID}: Items Added {curr_itm_add_res}'
        topic_add_str = f'{content_topicID}: Topics Added {topic_add_res}'

        # Update Log Strings
        curr_upd_str = f'{content_itemID}: Items Updated {curr_itm_upd_res}'
        topics_upd_str = f'{content_topicID}: Topics Updated {topic_upd_res}'

        return [curr_add_str, curr_upd_str, topic_add_str, topics_upd_str]





