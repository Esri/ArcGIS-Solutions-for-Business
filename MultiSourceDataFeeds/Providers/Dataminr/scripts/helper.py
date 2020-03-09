import arcpy
import configparser
import json
import requests
import os
import string
import sys
import time
from datetime import datetime
arcpy.env.overwriteOutput = True

def call_dataminr(token, client_id, client_secret, total_minutes_back='', listID='', listName='', query_term=''):
    '''
    This is the main function that initiates calling the Dataminr API and processing requests.
    
    The following three are required: token, client_id, client_secret.
    
    total_minutes_back is an optional time based parameter.  If no time is specified, a maximum of 100 alerts can be process.  This the max threshold for alert requests.

    The listID/listName and query_term parameters are based on the the type of query the user selects in the GP tool.

    Other dependant functions called throughout are:
    - get_dataminr_token (called when the max threshold for Requests have been made; 180)
    - make_dataminr_request (Make a request against the Dataminr API)
    - prep_dataminr_time_request (Prep the start/end time thresholds before making request. See function for more details)
    - process_alerts_obj (Loop through each item from the response)

    The return object is a dictionary.  The outer key is based on the alert ID and the inner key are the alert attributes.  The attributes are controlled 
    by the default_dataminr_attributes function.  The values assigned to each attribute are defined by the dataminr API schema path found in the 
    extract_dataminr_content function.
    '''     
    # Counter for API Calls
    query_counter = 1

    #Collection dictionary to store alerts for the current list
    alerts_dict = dict()

    #Output message to user based on query type.
    if query_term:
        arcpy.AddMessage("Working on the following query keyword:.......... {}".format(query_term))
    else:
        arcpy.AddMessage("Working on the following list:.......... {} ({})".format(listName, listID))

    # Created an additional Logic check If user decides to pull data based on a time frame.
    if total_minutes_back:

        # Lookback interval logic check based on the user specified time frame.
        if total_minutes_back > 1440:
            lookback_interval = 1440 # Default lookback interval if more than a day. Currently set to 1 day or 1440 minutes
        else:
            lookback_interval = total_minutes_back # Default lookback interval if less than a day

        end_time = int(time.mktime(datetime.now().timetuple()) * 1000) #Set default end time parameter to current time in epoch time milliseconds.  This represents alerts triggered before this time
        start_time = (end_time - lookback_interval * 60 * 1000) # Set default start time parameter in epoch time. This represents alerts triggered after this time
        end_time_frame = (end_time - total_minutes_back * 60 * 1000) # Set end time frame based on the number of minutes the user specified to look back to.
        arcpy.AddMessage('Start time: {}  |  End Time: {}'.format(start_time, end_time))

        while end_time > end_time_frame:
            # Check if we reached maximum query calls with out token
            if query_counter > 180:
                token = get_dataminr_token(client_id, client_secret)
                arcpy.AddMessage('{}Acquiring new token since we reached out call limit'.format('.....'*5))
                query_counter = 0 # Reset counter
            
            # Increase counter
            query_counter += 1

            # Poll alerts for list.
            payload = {'Authorization': 'dmauth {}'.format(token)}
            response_object, lookback_interval, start_time, end_time = prep_dataminr_time_request(payload, start_time, end_time, lookback_interval, end_time_frame, listID, query_term)

            #Check status of response object. Make sure it's ok before moving on.
            if response_object.status_code == 200:
                pass
            elif response_object.status_code == 400:
                # a return of error code 400 means that we have reached the end time frame and no alerts were captured in the last frame query.
                break
            elif response_object.status_code == 429:
                arcpy.AddMessage('Exceeded rate limit, getting a new token')
                token = get_dataminr_token(client_id, client_secret)
                payload = {'Authorization': 'dmauth {}'.format(token)}
                response_object, lookback_interval, start_time, end_time = prep_dataminr_time_request(payload, start_time, end_time, lookback_interval, end_time_frame, listID, query_term)
            else:
                arcpy.AddMessage('Internal Server Error: The Dataminr server experienced an error. | Code: {}.'.format(response_object.status_code))
                break

            # Convert response object to json so we process alerts.
            EventsList = json.loads(response_object.text)

            # Message to end user on the number of alerts that will be processed.
            arcpy.AddMessage("{} alerts were captured between {} and {}.".format(
                len(EventsList),
                datetime.fromtimestamp(int(start_time/1000)).strftime('%Y-%m-%d %I:%M:%S %p'),
                datetime.fromtimestamp(int(end_time/1000)).strftime('%Y-%m-%d %I:%M:%S %p')
                ))

            alerts_dict.update(extract_dataminr_content(EventsList, listID, listName))

            # Update time parameters for next request
            end_time = start_time
            start_time -= lookback_interval * 60 * 1000

            # Make sure start time is not less than end time frame.
            if start_time < end_time_frame:
                diff = end_time_frame - start_time
                start_time += diff
    else:
        # Request alerts
        payload = {'Authorization': 'dmauth {}'.format(token)}
        response_object = make_dataminr_request(payload, '', '', listID, query_term)

        # Check status of response object. Make sure it's ok before moving on.
        if response_object.status_code == 200:
            pass
        elif response_object.status_code == 400:
            # a return of error code 400 means that we have reached the end of the time frame and no alerts were captured in the last query.
            return alerts_dict
        else:
            arcpy.AddError('Internal Server Error: The Dataminr server experienced an error. | Code: {}.'.format(response_object.status_code))
            return alerts_dict

        # Convert response object to json so we process alerts.
        EventsList = json.loads(response_object.text)
        arcpy.AddMessage("{} alerts were captured.".format(len(EventsList)))

        alerts_dict.update(extract_dataminr_content(EventsList, listID, listName))

    return alerts_dict


def convert_time_value(time_value, units):
    '''
    Convert user specified time interger to minutes 
    '''
    if units == 'Minutes':
        return time_value
    if units == 'Hours':
        return time_value * 60
    if units == 'Days':
        return time_value * 1440


def create_empty_fc(input_field_info, fc_name, path):
    '''
    Create an empty feature class with from a list of input fields.

    Required input:
        List of field information - [Field Name, Field Type, Field Length] *Must be in this order
        Name of table to be created
        Path of workspace where table will be saved to.
    '''
    sr = arcpy.SpatialReference(4326) # WGS 84 coordinate system
    tmp_fc = os.path.join('in_memory', 'in_mem_fc')
    arcpy.CreateFeatureclass_management('in_memory', 'in_mem_fc', 'POINT', spatial_reference=sr)
    for field in input_field_info:
        arcpy.AddField_management(tmp_fc, field[0], field_type=field[1], field_length=field[2])
    
    # Create the actual output feature class.
    try:
        arcpy.CreateFeatureclass_management(path, fc_name, template=tmp_fc, spatial_reference=sr)
    except:
        arcpy.AddError(("Unable to create the feature class since it already exists at '{}'. "
                "Please close out of the ArcGIS Pro session that may be accessing the table, '{}' "
                "and re-run the script").format(path, fc_name))
        arcpy.Delete_management(tmp_fc)
        sys.exit()
    arcpy.Delete_management(tmp_fc)


def create_fl(LayerName, FCPath, expression=''):
    '''
    Create a Feature layer from a feature class. Optionally, an expression clause can be passed 
    in to filter out a subset of data.
    '''
    if arcpy.Exists(LayerName):
        desc = arcpy.Describe(LayerName)
        if desc.dataType is "FeatureLayer":
            arcpy.Delete_management(LayerName)
    try:
        if expression:
            return arcpy.MakeFeatureLayer_management(FCPath, LayerName, expression, "")
        else:
            return arcpy.MakeFeatureLayer_management(FCPath, LayerName, "", "")
    except:
        return arcpy.AddError(arcpy.GetMessages(2))


def default_dataminr_attributes():
    '''
    Set default values for each dictionary keys.  The purpose of this function is to account for the differences
    of the Dataminr data structure. Not all alerts will have the attributes we are calling upon available.  We need
    to account for this by setting defaults.

    Any dataminr attributes that we want to ingest should be maintained here and the process_alerts_obj function.

    Returned dictionary with default values for each attribute.
    '''

    base_dict = dict()
    base_dict.setdefault('alert_id', None)
    base_dict.setdefault('list_id', None)
    base_dict.setdefault('list_name', None)
    base_dict.setdefault('list_color', None)
    base_dict.setdefault('event_time', None)
    base_dict.setdefault('alert_type', None)
    base_dict.setdefault('alert_type_color', None)
    base_dict.setdefault('latitude', None)
    base_dict.setdefault('longitude', None)
    base_dict.setdefault('place', None)
    base_dict.setdefault('source', None)
    base_dict.setdefault('main_category', None)
    base_dict.setdefault('sub_category', None)
    base_dict.setdefault('caption', None)
    base_dict.setdefault('post_text', None)
    base_dict.setdefault('publisher_category', None)
    base_dict.setdefault('publisher_category_color', None)
    base_dict.setdefault('expanded_alert_url', None)
    base_dict.setdefault('related_terms', None)
    base_dict.setdefault('related_terms_query_url', None)
    return base_dict


def export_dictionary_to_fc(fc_name, path_to_table, field_info, input_dict):
    '''
    This function will export a dictionary to a feature class.
    '''
    #Create empty table with the schema 
    create_empty_fc(field_info, fc_name, path_to_table)

    #Append data to empty table.
    output_path = os.path.join(path_to_table, fc_name)
    truncate_and_append_to_fc(input_dict, fc_name, output_path)


def extract_dataminr_content(response_obj, list_id='', list_name=''):
    '''
    Process items from the response object of the Dataminr API Request.

    The paths to each key from the response object is hardcoded. If the Dataminr schema changes, this is where
    modications need to be made.

    Items are captured in a dictionary which is returned.
    '''
    # Extract items and hold in a dictionary
    response_dict_holder = dict()
    for alertObj in response_obj:
        rec_id = alertObj['alertId']
        response_dict_holder[rec_id] = default_dataminr_attributes()
        response_dict_holder[rec_id]['alert_id'] = rec_id

        # If user selects topic list over custom list, default to the type of topic list selected
        try:response_dict_holder[rec_id]['list_id'] = alertObj['watchlistsMatchedByType'][0]['id']
        except: response_dict_holder[rec_id]['list_id'] = list_id    

        # If user selects topic list over custom list, default to the type of topic list selected
        try:response_dict_holder[rec_id]['list_name'] = alertObj['watchlistsMatchedByType'][0]['name']
        except: response_dict_holder[rec_id]['list_name'] = list_name      

        # Default color is gray for custom lists.
        try: response_dict_holder[rec_id]['list_color'] = alertObj['watchlistsMatchedByType'][0]['userProperties']['watchlistColor']
        except: response_dict_holder[rec_id]['list_color'] = 'LightGray'

        # Value must be returned in UTC as eventTime is timezone aware (Need to confirm but seems true based on testing)
        # The datetime value in the hosted feature class is automatically translated from UTC to the timezone of the client.
        try: response_dict_holder[rec_id]['event_time'] = datetime.utcfromtimestamp(alertObj['eventTime']/1000)
        except: response_dict_holder[rec_id]['event_time'] = '1900-01-01 12:00:00 PM' # If eventTime is null, need to return default value as date fields need a some kind of date value

        try:response_dict_holder[rec_id]['alert_type'] = alertObj['alertType']['name']
        except: response_dict_holder[rec_id]['alert_type'] = 'Alert'

        # These hex values represent the colors for the different alert levels.  This is added to the final dictionary for
        # HTML color code references in the pop-up and dashboard list. 
        Color_HEX_Vals = {
            'Alert': 'FFFF00',
            'Urgent': 'E69800',
            'Flash': 'E60000'
        }

        try: response_dict_holder[rec_id]['alert_type_color'] = Color_HEX_Vals[alertObj['alertType']['name']]
        except: response_dict_holder[rec_id]['alert_type_color'] = 'FFFF00'

        try: response_dict_holder[rec_id]['latitude'] = alertObj['eventLocation']['coordinates'][0]
        except: pass

        try: response_dict_holder[rec_id]['longitude'] = alertObj['eventLocation']['coordinates'][1]
        except: pass

        try: response_dict_holder[rec_id]['place'] = alertObj['eventLocation']['name']
        except: pass

        try: response_dict_holder[rec_id]['source'] = alertObj['post']['link']
        except: pass
        
        try: response_dict_holder[rec_id]['main_category'] = alertObj['categories'][0]['name']
        except: pass

        try: response_dict_holder[rec_id]['sub_category'] = alertObj['categories'][1]['name']
        except: pass
        
        try: response_dict_holder[rec_id]['caption'] = alertObj['caption']
        except: pass

        try: response_dict_holder[rec_id]['post_text'] = alertObj['post']['text']
        except: pass

        try: response_dict_holder[rec_id]['publisher_category'] = alertObj['publisherCategory']['name']
        except: pass

        try: response_dict_holder[rec_id]['publisher_category_color'] = alertObj['publisherCategory']['color']
        except: pass
        
        # Needs to be formatted this way for keyword queries
        # Should look into adding this URL to the top of the script if it ever changes
        try: response_dict_holder[rec_id]['expanded_alert_url'] = '{}{}'.format(r'https://app.dataminr.com/#alertDetail/5/', rec_id)
        except: pass

        #Extract related terms for the alert
        try:
            terms = list()
            for k in alertObj['relatedTerms']:
                terms.append(k['text'])
            response_dict_holder[rec_id]['related_terms'] = ", ".join(str(t) for t in terms)
        except:
            pass

        try: response_dict_holder[rec_id]['related_terms_query_url'] = alertObj['relatedTermsQueryURL']
        except: pass
    
    return response_dict_holder


def extract_field_name(fc):
    '''
    Return a list of fields name from a FC.
    '''
    fields = [f.name for f in arcpy.ListFields(fc)]
    return fields


def get_authentication():
    config = configparser.ConfigParser()
    bin_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    config.read(os.path.join(bin_directory,'config.ini'))
    credential_section = config['Dataminr API Credentials']
    credential_dict = {str(k):v for k,v in credential_section.items()}
    return credential_dict


def get_columns(input_dict):
    '''
    Retrieve the keys of the first item of the dictionary.
    The outer dictionary will represent a unique record ID and the inner dictionary represents all the attributes
    stored attributes for the record.

    A list is returned that represents all the attributes names.
    '''
    first_alert = list(input_dict.keys())[0]
    column_names = [c_name for c_name in input_dict[first_alert].keys()]
    return column_names


def get_dataminr_token(client_id, client_secret):
    '''
    Authenticate and retrieve dataminr token using client id and client secret
    '''
    myUrl = 'https://gateway.dataminr.com/auth/2/token'
    grant_type = 'api_key'
    payload = {'grant_type': grant_type, 'client_id': client_id, 'client_secret': client_secret}
    r = requests.post(myUrl, data=payload)
    tokenDetails = json.loads(r.text)
    #TODO: Add better error handeling to account for:
    # - invalid credentials
    # - no internet
    # If dataminr site API is down
    # If dataminr changes name of token attribute of dmaToken
    token = tokenDetails["dmaToken"]

    return token


def input_check(Input_Layer):
    '''
    Check if there is a filepath from the input layers. If not, pre-pend the path. Also extract the Layer names.
    return is a list [Layer Path, Layer Name]
    '''
    if arcpy.Exists(Input_Layer):
        InputPath = arcpy.Describe(Input_Layer).catalogPath #join(arcpy.Describe(Input_Layer).catalogPath,arcpy.Describe(Input_Layer).name)
        InputName = arcpy.Describe(Input_Layer).name
    else:
        arcpy.AddError("{} Does not exist".format(Input_Layer))
        sys.exit()
    return InputPath, InputName


def is_empty(fc):
    '''
    Check to see if a feature class is empty.  Return True if it is.
    '''
    count = str(arcpy.GetCount_management(fc))
    if count == "0":
        return True
    else:
        return False


def make_dataminr_request(payload, start_time='', end_time='', list_id='', keyword=''):
    '''
    Function to query alerts using either lists or query parameters.

    Start time represents alerts triggered after specified time.
    End time represents alerts triggered before specified time.
    '''
    # Should look into adding this URL to the top of the script if it ever changes
    list_url = 'https://gateway.dataminr.com/alerts/2/get_alert?alertversion=14'
    if keyword:
        if start_time and end_time:
            response_obj = requests.get(list_url, params={'query': keyword, 'pagesize': '100', 'start_time': start_time, 'end_time': end_time}, headers=payload)
        else:
            response_obj = requests.get(list_url, params={'query': keyword, 'pagesize': '100'}, headers=payload)
    
    if list_id:
        if start_time and end_time:
            response_obj = requests.get(list_url, params={'lists': list_id, 'pagesize': '100', 'start_time': start_time, 'end_time': end_time}, headers=payload)
        else:
            response_obj = requests.get(list_url, params={'lists': list_id, 'pagesize': '100'}, headers=payload)
    return response_obj


def prep_dataminr_time_request(payload, start_time, end_time, lookback_interval, end_time_frame, listID='', keyword=''):
    '''
    Function to retrieve alerts from based on user input time constraints.  This function is recursive in order to dynamically adjust time frame between the start and end time parameters. 
    This ensures we are under our max return alert size.
    '''
    # Request alerts
    response_object = make_dataminr_request(payload, start_time, end_time, listID, keyword)
    
    # Check status of response object. Make sure it's ok before moving on.
    # If not, modify start/end time frame and recall prep_dataminr_time_request function.
    if response_object.status_code == 200:
        pass
    elif response_object.status_code == 400:
        if start_time <= end_time_frame: # Only look back to user specified end time frame.
            arcpy.AddMessage('No content was found from {} to {}. This is the end of the user specified time frame.'.format(
                datetime.fromtimestamp(int(start_time/1000)).strftime('%Y-%m-%d %I:%M:%S %p'),
                datetime.fromtimestamp(int(end_time/1000)).strftime('%Y-%m-%d %I:%M:%S %p')
            ))
            # Returned object will be error code 400 and will exit in the logic gate in the process alerts function. 
            return response_object, lookback_interval, start_time, end_time
        arcpy.AddMessage('No content was found from {} to {}. Skipping to next time frame and trying again'.format(
            datetime.fromtimestamp(int(start_time/1000)).strftime('%Y-%m-%d %I:%M:%S %p'),
            datetime.fromtimestamp(int(end_time/1000)).strftime('%Y-%m-%d %I:%M:%S %p')
            ))
        end_time = start_time
        start_time -= lookback_interval * 60 * 1000
        return prep_dataminr_time_request(payload, start_time, end_time, lookback_interval, end_time_frame, listID, keyword)
    # TODO May need to account for times when the API rate limit is exceeded.  In order work around this, will need to get a new token
    # and then recall this function. Will need to look into make the client id and client secrete global variables.
    # elif response_content.status_code is 429:
    #     arcpy.AddMessage('Exceeded rate limit, getting a new token')
    #     token = get_dataminr_token(client_id, client_secret)
    #     payload = {'Authorization': 'dmauth {}'.format(token)}
    #     return prep_dataminr_time_request(payload, start_time, end_time, lookback_interval, end_time_frame, listID, keyword)
    else:
        arcpy.AddError('Internal Server Error: The Dataminr server experienced an error. | Code: {}.'.format(response_object.status_code))
        sys.exit()

    # If response was ok, check the following cases
    json_object = json.loads(response_object.text) # Convert response to json object so we can count the number of alerts.

    if 10 < len(json_object) < 100: # enough alerts.
        return response_object, lookback_interval, start_time, end_time
    elif len(json_object) == 100: # Too many alerts
        if lookback_interval <= 1: # Unable to retrieve alerts when the start and end times are 1 minute or less.
            arcpy.AddWarning("CAUTION: Reached minimum time range between start and end time parameters. Some alert's may be missing")
            return response_object, lookback_interval, start_time, end_time
        else:
            lookback_interval = int(lookback_interval/2)
        start_time = (end_time - (lookback_interval * 60 * 1000))
        arcpy.AddMessage('Maximum Alerts returned. Cutting time interval in half. Checking new timeframe: from {} to {}.'.format(
            datetime.fromtimestamp(int(start_time/1000)).strftime('%Y-%m-%d %I:%M:%S %p'),
            datetime.fromtimestamp(int(end_time/1000)).strftime('%Y-%m-%d %I:%M:%S %p')
            ))
        return prep_dataminr_time_request(payload, start_time, end_time, lookback_interval, end_time_frame, listID, keyword)
    elif len(json_object) <= 10: # Too few alerts
        lookback_interval = int(lookback_interval * 2)
        start_time = (end_time - (lookback_interval * 60 * 1000))
        if start_time <= end_time_frame: # Only look back to user specified time frame.
            return response_object, lookback_interval, start_time, end_time
        arcpy.AddMessage('Returned too few alerts. Doubling time interval. Checking new timeframe: from {} to {}.'.format(
            datetime.fromtimestamp(int(start_time/1000)).strftime('%Y-%m-%d %I:%M:%S %p'),
            datetime.fromtimestamp(int(end_time/1000)).strftime('%Y-%m-%d %I:%M:%S %p')
            ))
        return prep_dataminr_time_request(payload, start_time, end_time, lookback_interval, end_time_frame, listID, keyword)


# def process_alerts_obj(response_obj, list_id='', list_name=''):
#     '''
#     This function will take a dataminr response object that's converted to JSON and loop through
#     each item in the response
#     '''
#     #Collection dictionary to store alerts for the current list
    
#     obj_dict_holder = dict()
#     print(response_obj)
#     for item in response_obj:
#         rec_id = item['alertId']
#         obj_dict_holder[rec_id] = default_dataminr_attributes()
#         #if list_id and list_name:
#         obj_dict_holder[rec_id] = extract_dataminr_content(item)
#         obj_dict_holder[rec_id]['alert_id'] = rec_id
#         obj_dict_holder[rec_id]['list_id'] = list_id
#         obj_dict_holder[rec_id]['list_name'] = list_name
#     return obj_dict_holder


def set_data_type(fields):
    '''
    Return a list of pertinent field information for a list of fields.  This function sets the default schema for variables of interest.
    The return list contains:
        - Field Name
        - Field Type
        - Field Length

    Required Inputs:
        List of input fields from Dataminr

    *NOTE*
    The default field datatype is a String with a length of 1000 characters for all fields except:
        - latitude (numeric)
        - longitude (numeric)
        - caption (string 8000)
        - post_text (string 8000)
    '''
    numeric_fields = ['latitude', 'longitude']
    description_fields = ['caption', 'post_text']
    field_info = []
    for field in fields:
        temp = []
        if field is 'event_time':
            temp.append(field)
            temp.append('Date')
            temp.append('')
        if field in description_fields:
            temp.append(field)
            temp.append('String')
            temp.append('8000')
        if field not in numeric_fields:
            temp.append(field)
            temp.append('String')
            temp.append('1000')
        else:
            temp.append(field)
            temp.append('Double')
            temp.append('')
        field_info.append(temp)
    return field_info


def truncate_and_append_to_fc(input_dict, fc_name, fc_path):
    '''
    Add data from a dictionary to an ArcGIS Feature Class.

    Required input:
        Dictionary
        Name of Feature Class
        Path to Feature Class

    *Note*
    Feature Class should be empty before adding data to it. If the feature class
    already exists, the truncate function will delete existing features.
    '''
    input_fields = extract_field_name(fc_path)
    input_fields.append('SHAPE@XY')
    input_fields.remove('Shape')
    input_fields.remove('OBJECTID')
    cursor = arcpy.da.InsertCursor(fc_path, input_fields)

    if not is_empty(fc_path):
        arcpy.AddMessage("Deleting Features in  {}".format(fc_name))
        arcpy.TruncateTable_management(fc_path)
    
    counter=0
    arcpy.AddMessage("Adding features to {}".format(fc_name))
    for val in input_dict.values():
        tmp_list = list()
        for k, v in val.items():
            if not v:
                # Account for instances where values are empty i.e. Null/False.
                # Insert cursor will not be able to add invalid values.  
                v = "NULL"
            tmp_list.append(v)
        tmp_list.append((val['longitude'], val['latitude'])) # Combine lat/long values to create shape field
        row_tuple= tuple(tmp_list)
        try:
            cursor.insertRow(row_tuple)
            counter+=1
        except:
            #TODO add logging to capture instances when record cannot be added
            arcpy.AddWarning("Unable to insert the following row: {}".format(row_tuple))
    
    del cursor
    arcpy.AddMessage("Added a total of {} features to the feature class, {}".format(counter, fc_name))


def unique_values(fc, field):
    '''
    Return a list of unique values from a user specified field in a feature class or table.
    '''
    with arcpy.da.SearchCursor(fc, [field]) as cur:
        return sorted({row[0] for row in cur})
