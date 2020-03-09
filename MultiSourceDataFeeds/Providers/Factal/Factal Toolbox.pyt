import arcpy
import os
import requests
import factal
import re
import pandas as pd

from configparser import ConfigParser


def make_topic_list(response):
    '''
    Convert topic response for selection list
    '''
    # keys need to build selection list
    schema = [
        'id',
        'name',
        'category',
        'latest_item_date'
    ]   

    df = pd.DataFrame(response)
    df['latest_item_date'] = pd.to_datetime(df['latest_item_date'], utc=True).dt.strftime("%m/%d/%Y %H:%M:%S")
    cols = [col for col in df.columns if col in schema]
    topics = df[cols].sort_values('latest_item_date', ascending=False).to_dict('records')
    topic_param = list()
    for topic in topics:
        if topic['category'] == 'Incident':
            topic_param.append('{} | {} | {}'.format(topic['latest_item_date'], topic['name'], topic['id']))
    return topic_param


def apply_symbology():
    '''
    Apply symbology to the output layer
    '''
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    symbology_layer_path = os.path.join(this_dir, 'Factal_Symbology.lyrx')
    return symbology_layer_path


def _get_config_params():
    '''
    Function pull parameters from the config.ini file
    '''
    # Get Current Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]

    # Read Configuration File
    config = ConfigParser()
    config.read(os.path.join(this_dir, 'config.ini'))

    # Factal Parameters
    factal_token = config.get('Factal', 'token')

    return factal_token


def replace_invalid_chars(input_string):
    '''
    Function to replace all unsupported characters
    http://desktop.arcgis.com/en/arcmap/10.3/map/publish-map-services/00057-layer-name-contains-invalid-characters.htm
    
    Also referenced this post for the reg-expression: https://stackoverflow.com/a/52182412
    '''
    clean_string = re.sub(r'[^a-zA-Z0-9 ]', r'', input_string).replace(' ','_')
    return clean_string



class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [ActiveIncidents]


class ActiveIncidents(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Explore Active Incidents"
        self.description = "GP Tool to quickly pull active incidents from the Factal API"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
            
        param0 = arcpy.Parameter(
            displayName='Select a Topic',
            name="SelectTopic",
            datatype='GPString',
            parameterType="Required",
            direction="Input",
            multiValue=False
        )

        param1 = arcpy.Parameter(
            displayName='Layer Name',
            name="layer_name",
            datatype='DEFeatureClass',
            parameterType="Required",
            direction="Output",
            multiValue=False
        )

        # Extract Token and make request to factal and return response
        e = factal.Extractor(_get_config_params())
        topic_request = e.fetch_items(limit=100, endpoint='topic')
        topic_list = make_topic_list(topic_request)

        # Assigning values to parameters
        param0.filter.list = topic_list
        params = [param0, param1]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        if parameters[0].altered:
            name = parameters[0].valueAsText
            param_name = replace_invalid_chars(name.split(' | ')[1].strip())
            parameters[1].value = f'Incident_{param_name}'
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        try:

            topic_id = parameters[0].valueAsText.split('| ')[2]

            # Extract Token and make request to factal and return response
            factal_token = _get_config_params()
            e = factal.Extractor(factal_token)
            itms, topics = e.parse_items(e.fetch_items(limit=600, endpoint='item', topics=topic_id))
            itm_df = e.convert_item_to_df(itms)
            itm_df['severity'] = itm_df['severity'].astype(str)
            itm_df['updated_date'] = itm_df['updated_date'].dt.ceil(freq='s')
            itm_df['created_date'] = itm_df['created_date'].dt.ceil(freq='s')
            itm_df['date'] = itm_df['date'].dt.ceil(freq='s')
            
            itm_df.spatial.to_featureclass(
                location=parameters[1].valueAsText)

            # apply symbology to output layer
            parameters[1].symbology = apply_symbology()


        except Exception as e:
            arcpy.AddError(str(e))