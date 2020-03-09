import arcpy
import configparser
import os
import sys
from helper import *
from datetime import datetime
arcpy.env.overwriteOutput = True

startTime = datetime.now()
arcpy.AddMessage(startTime)

#...........................................................
# Geo-processing Inputs
#...........................................................

query_type = arcpy.GetParameterAsText(0)
list_selection = arcpy.GetParameterAsText(1)
keyword_query = arcpy.GetParameterAsText(2)
output_fc_name = arcpy.arcpy.GetParameterAsText(3)
output_fgdb = arcpy.GetParameterAsText(4)

#...........................................................
# SETTING UP ENVIRONMENT DEFAULTS
#...........................................................

# Setting the default paths for folder locations
BASE_SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))

# Path to Alert Symbology
symbology_layer = os.path.join(BASE_SCRIPT_PATH, 'Alert Symbology.lyrx')

#Create a dictionary to store data that will then be passed into a GIS object.
Accumulated_Alerts = dict()

#..........................................................................
# MAIN SCRIPT
#..........................................................................

# Get Token value
credentials = get_authentication()
client_id = credentials['client_id']
client_secret = credentials['client_secret']
token = get_dataminr_token(client_id, client_secret)

if query_type == 'List':
    # Run the following if the query type is set to List
    if list_selection == '':
        arcpy.AddError("Please select at least one list.")
        sys.exit()
        
    list_items = {lists.split(" : ")[0]:lists.split(" : ")[1] for lists in list_selection.replace("'",'').split(";")}
    arcpy.AddMessage("Selected Lists: {}\n\n".format(list_items))
    for list_name, list_id in list_items.items():
        arcpy.AddMessage("List Name - {} | List ID {}".format(list_name, list_id))

    # Loop through each list and append alerts
    for list_name, list_id in list_items.items():
        alert_dict = call_dataminr(token, client_id, client_secret, '', list_id, list_name, '')
        Accumulated_Alerts.update(alert_dict)
else:
    if keyword_query == '':
        arcpy.AddError("Please enter a Keyword phrase to query.")
        sys.exit()

    arcpy.AddMessage("Keyword Query: {}\n\n".format(keyword_query))
    alert_dict = call_dataminr(token, client_id, client_secret, '', '', '', keyword_query)
    Accumulated_Alerts.update(alert_dict)

# Check if any results were returned.
if not Accumulated_Alerts:
    arcpy.AddWarning("No alerts were pulled. Exiting......")
    sys.exit()

# Set Schema for fields
column_schema = set_data_type(get_columns(Accumulated_Alerts))
export_dictionary_to_fc(output_fc_name, output_fgdb, column_schema, Accumulated_Alerts)

#Update the map view with the output layer
arcpy.SetParameter(5, os.path.join(output_fgdb, output_fc_name))
arcpy.management.ApplySymbologyFromLayer(os.path.join(output_fgdb, output_fc_name), symbology_layer, "VALUE_FIELD alert_type alert_type", "DEFAULT")

arcpy.AddMessage("Script Runtime: " + str(datetime.now()-startTime))