import arcpy
import re
import os
import tempfile
import traceback
import shutil
from arcgis.features import GeoAccessor
from extractor import Extractor
from functools import wraps


def file_datestamp(url):
    """
    Return the timestamp from GDELT URL string
    """
    # TODO: Find out how to set re to search text after the nth charcater
    # date_from_url = re.search(r'/(.*?).export', url).group(1)
    try:
        date_from_url = re.search(r'gdeltv2/(.*?).export', url).group(1)
    except:
        date_from_url = re.search(r'events/(.*?).export', url).group(1)
    return date_from_url


def gdelt_database_selection():
    """ Dropdown selection list for different GDELT Databases"""
    e = Extractor()
    gdelt_params = {
        'GDELT Event 1.0 (Latest Daily Updates)': {'Version': '1', 'Source': e.fetch_last_v1_url()},
        'GDELT Event 2.0 (Latest 15 Minutes)': {'Version': '2', 'Source': e.fetch_last_v2_url()}
    }

    return gdelt_params


def temp_handler(func):
    """
    Wrapper function that appends a temporary file directory value that's passed into
    the build_fc function. The directory path is used to temporarily store the .csv
    downloaded for processing. After processing has finished, file contents and directory
    are removed.
    """

    @wraps(func)
    def wrap(*args, **kwargs):

        temp_dir = tempfile.mkdtemp()

        args = list(args)
        args.insert(1, temp_dir)

        try:
            func(*args, **kwargs)
        except:
            print(traceback.format_exc())
        finally:
            shutil.rmtree(temp_dir)
            print(f'Removed Temp Directory: {temp_dir}')

    return wrap


@temp_handler
def build_fc(output_path, temp_dir, version, flatten_flag=True, article_flag=True):
    """
    Build function to extract, process and push events from GDELT 2.0 into a feature class.
    """

    e = Extractor()
    e.articles = article_flag
    e.flatten = flatten_flag

    # Create spatial data frame based on selected version.
    if version == '1':
        csv_file, csv_name = e.collect_v1_csv(temp_dir)
        df = e.get_v1_sdf(csv_file, csv_name)
    if version == '2':
        csv_file, csv_name = e.collect_v2_csv(temp_dir)
        df = e.get_v2_sdf(csv_file, csv_name)

    # Publish as Hosted Feature Layer
    df.spatial.to_featureclass(output_path)


def replace_invalid_chars(input_string):
    '''
    Function to replace all unsupported characters
    http://desktop.arcgis.com/en/arcmap/10.3/map/publish-map-services/00057-layer-name-contains-invalid-characters.htm
    
    Also referenced this post for the reg-expression: https://stackoverflow.com/a/52182412
    '''
    clean_string = re.sub(r'[^a-zA-Z0-9 ]', r'',
                          input_string).replace(' ', '_')
    return clean_string


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [ImportData]


class ImportData(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Import GDELT Event Data"
        self.description = "GP Tool to import the latest GDELT events from the GDELT Project"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""

        param0 = arcpy.Parameter(
            displayName='Select GDELT Event Database to Pull Event Data',
            name="GdeltEventDatabase",
            datatype='GPString',
            parameterType="Required",
            direction="Input",
            multiValue=False
        )

        param1 = arcpy.Parameter(
            displayName='Flatten Records',
            name="FlattenFlag",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input",
            multiValue=False
        )

        param2 = arcpy.Parameter(
            displayName='Enrich with Article Content',
            name="ArticleFlag",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input",
            multiValue=False
        )

        param3 = arcpy.Parameter(
            displayName='Layer Name',
            name="layer_name",
            datatype='DEFeatureClass',
            parameterType="Required",
            direction="Output",
            multiValue=False
        )
        
        param0.filter.type = "ValueList"
        param0.filter.list = list(gdelt_database_selection().keys())

        # Setting default value for FlattenFlag/ArticleFlag boolean
        param1.value = False
        param2.value = False

        params = [param0, param1, param2, param3]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""

        gdelt_params = gdelt_database_selection()

        parameters[3].enabled = bool(parameters[0].value)
        if parameters[0].altered:
            url_string = gdelt_params[parameters[0].value]['Source']
            timestamp = file_datestamp(url_string)
            parameters[3].value = f'Event_{timestamp}'

        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        gdelt_params = gdelt_database_selection()
        dataset = gdelt_params[parameters[0].value]['Version']
        layerpath = parameters[3].valueAsText

        build_fc(layerpath,
                dataset,
                flatten_flag=bool(parameters[1].value),
                article_flag=bool(parameters[2].value)
                )

        return
