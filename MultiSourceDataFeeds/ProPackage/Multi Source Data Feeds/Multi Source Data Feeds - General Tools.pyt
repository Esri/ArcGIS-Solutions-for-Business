import arcpy
import os
import subprocess
import zipfile
import xml.etree.cElementTree as et
from configparser import SafeConfigParser, RawConfigParser
from shutil import copyfile


def get_config_path(provider):
    '''
    Return path of config file
    '''
    this_dir = os.path.dirname(__file__)
    root_dir = os.path.abspath(os.path.join(this_dir, '..'))

    providers = {
        'Dataminr': {'path': os.path.join(root_dir, 'Providers', 'Dataminr', 'scripts'), 'file': 'config.ini'},
        'Factal': {'path': os.path.join(root_dir, 'Providers', 'Factal'), 'file': 'config.ini'},
        'GDELT': {'path': os.path.join(root_dir, 'Providers', 'GDELT'), 'file': 'config.ini'}
        }

    config_path = os.path.join(providers[provider]['path'], providers[provider]['file'])

    return config_path


def read_config_params(provider):
    '''
    Function pull parameters from the config.ini file and return config object
    '''
    # Read Configuration File
    config = SafeConfigParser()
    config.read(get_config_path(provider))
    return config


def write_config_param(provider, changes):
    '''
    Function to save changes to config file
    '''
    parser = RawConfigParser()
    parser.read(get_config_path(provider))

    updated_keys = [k for k in changes.keys()]

    #Loop through each of the section changes and set the new values
    for group in parser.sections():
        for key, val in parser.items(group):
            if key in updated_keys:
                parser.set(group, key, changes[key])

    #Save changes to config file
    with open(get_config_path(provider), 'w') as configfile:
        parser.write(EqualsSpaceRemover(configfile))


class EqualsSpaceRemover:
    '''
    The write function from the configparser library adds a space in the front/end of the = sign.
    The extra space causes issues with the batch file reading in the config parameters from the .ini file.
    This class replaces " = " with "=" in all lines of the config file.
    
    See this stackexchange question for more info: https://stackoverflow.com/questions/14021135/how-to-remove-spaces-while-writing-in-ini-file-python/25084055#25084055
    '''
    output_file = None
    def __init__( self, new_output_file ):
        self.output_file = new_output_file

    def write( self, what ):
        self.output_file.write( what.replace( " = ", "=", 1 ) )


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [UnpackZip, ConfigureParameters, CreateScheduledTasks]


class UnpackZip(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "1: Unpack Provider Scripts"
        self.description = "Geo-processing Tool to extract providers package"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        params = None
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        this_dir = os.path.dirname(__file__)
        root_dir = os.path.abspath(os.path.join(this_dir, '..'))
        provider_package = os.path.join(root_dir, 'commondata', 'userdata', 'Providers.zip')

        # if not os.path.exists(os.path.join(root_dir, 'Providers')):
        #     with zipfile.ZipFile(provider_package, mode='r') as zip_ref:
        #         zip_ref.extractall(root_dir)
        # else:
        #     arcpy.AddWarning('The MSDF providers package has already been unpacked.')
        # return

        subprocess.call(os.path.join(root_dir, 'Providers', 'Tasks', 'deploy_python_env.bat'))      


class CreateScheduledTasks(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "3: Create Windows Scheduler Task"
        self.description = "Geo-processing tool to deploy a Windows Task Scheduler"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        param0 = arcpy.Parameter(
            displayName="Select a Windows Task Schedule to Deploy",
            name="CreateTask",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            )

        param0.filter.type = "ValueList"
        param0.filter.list = [
            'Dataminr - Pull Alerts',
            'Factal - Pull Alerts',
            'GDELT - Pull Events V1',
            'GDELT - Pull Events V2'
        ]

        params = [param0]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        # Get key paths
        this_dir = os.path.dirname(__file__)
        root_dir = os.path.abspath(os.path.join(this_dir, '..'))

        # Dictionary of the batch file paths for each data provider.
        task_list = {
            'Dataminr - Pull Alerts': {
                'bat': {
                    'path': os.path.join(root_dir, 'Providers', 'Dataminr', 'scripts'),
                    'name': 'DataminrTaskScheduler.bat'
                },
                'xml': {
                    'path': os.path.join(root_dir, 'Providers', 'Tasks'),
                    'name': 'Data_Pull.xml'
                }
            },
            'Factal - Pull Alerts': {
                'bat': {
                    'path': os.path.join(root_dir, 'Providers', 'Factal'),
                    'name': 'runner.bat'
                },
                'xml': {
                    'path': os.path.join(root_dir, 'Providers', 'Tasks'),
                    'name': 'Data_Pull.xml'
                }
            },
            'GDELT - Pull Events V1': {
                'bat': {
                    'path': os.path.join(root_dir, 'Providers', 'GDELT'),
                    'name': 'run_v1.bat'
                },
                'xml': {
                    'path': os.path.join(root_dir, 'Providers', 'Tasks'),
                    'name': 'GDELT_V1.xml'
                }
            },
            'GDELT - Pull Events V2': {
                'bat': {
                    'path': os.path.join(root_dir, 'Providers', 'GDELT'),
                    'name': 'run_v2.bat'
                },
                'xml': {
                    'path': os.path.join(root_dir, 'Providers', 'Tasks'),
                    'name': 'GDELT_V2.xml'
                }
            }
        }

        # Get user parameters
        selected_task = parameters[0].valueAsText

        #Copy XML to root folder
        task_xml = os.path.join(task_list[selected_task]['xml']['path'], task_list[selected_task]['xml']['name'])
        copied_xml = os.path.join(root_dir, os.path.basename(task_xml))
        copyfile(task_xml, copied_xml)

        #Prep XML object for modification
        tree = et.ElementTree(file=task_xml)
        root = tree.getroot()

        # Write changes to XML file
        # Add path to batch file for working directory tag
        for elem in root.iter('{http://schemas.microsoft.com/windows/2004/02/mit/task}WorkingDirectory'):
            elem.text = task_list[selected_task]['bat']['path']
            tree.write(copied_xml)

        # Add name of batch file to command tag
        for elem in root.iter('{http://schemas.microsoft.com/windows/2004/02/mit/task}Command'):
            elem.text = '"{}"'.format(task_list[selected_task]['bat']['name'])
            tree.write(copied_xml)

        # Run command to run schtasks.exe and import modified xml 
        # /F command will overwrite task if it already exists.
        cmd_line_call = 'schtasks /create /TN "{}" /XML "{}" /F'.format(selected_task, copied_xml)
        subprocess.call(cmd_line_call)

        # Delete Tmp XML File
        os.remove(copied_xml)

        return


class ConfigureParameters(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "2: Modify Configuration File Parameters"
        self.description = "Geo-processing tool to allow user to modify configuration files"
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""

        param0 = arcpy.Parameter(
            displayName="Select a Data Provider",
            name="CreateTask",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            )

        param1 = arcpy.Parameter(
            displayName="Configuration File Parameters",
            name="Configfile",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            multiValue=True
            )

        param0.filter.type = "ValueList"
        param0.filter.list = [
            'Dataminr',
            'Factal',
            'GDELT'
            ]

        # Assigning values to parameters
        param1.columns = [['GPString', 'Parameter'], ['GPString', 'Value']]
        param1.filters[1].type = 'ValueList'
        params = [param0, param1]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        # has been changed."""
        parameters[1].enabled = bool(parameters[0].value)
        if parameters[0].altered and not parameters[0].hasBeenValidated:
            # Extracting parameters from the config file
            # Loop through each section and extract each key/value pair.
            config_paramameters = read_config_params(parameters[0].valueAsText)
            param_list = list()
            for group in config_paramameters.sections():
                for k, v in config_paramameters.items(group):
                    tmp = [k,v]
                    param_list.append(tmp)
            parameters[1].values = param_list

        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        provider = parameters[0].valueAsText
        params = dict(parameters[1].values)  
        write_config_param(provider, params)
        return
