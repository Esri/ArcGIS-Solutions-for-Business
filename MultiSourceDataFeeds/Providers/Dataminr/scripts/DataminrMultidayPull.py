import arcpy
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
lookback_time_value = arcpy.GetParameterAsText(3)
lookback_time_units = arcpy.arcpy.GetParameterAsText(4)
output_fc_name = arcpy.arcpy.GetParameterAsText(5)
output_fgdb = arcpy.GetParameterAsText(6)

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

# Logic check to confirm that input time value is a whole number
try:
    lookback_time_value = int(lookback_time_value)
except:
    arcpy.AddError("Please enter a whole number for the 'Specify Time' parameter")
    sys.exit()

# Convert time into correct units
time_in_minutes = convert_time_value(lookback_time_value, lookback_time_units)

if query_type == 'List':
    # Run the following if the query type is set to List
    if list_selection == '':
        arcpy.AddWarning("Please select at least one list.")
        sys.exit()
        
    list_items = {x.split(" : ")[0]:x.split(" : ")[1] for x in list_selection.replace("'",'').split(";")}
    arcpy.AddMessage("Selected Lists: {}\n\n".format(list_items))
    for k,v in list_items.items():
        arcpy.AddMessage("List Name - {} | List ID {}".format(k,v))

    # Loop through each list and append alerts
    for list_name, list_id in list_items.items():
        alert_dict = call_dataminr(token, client_id, client_secret, time_in_minutes, list_id, list_name, '')
        Accumulated_Alerts.update(alert_dict)
else:
    if keyword_query == '':
        arcpy.AddWarning("Please enter a Keyword phrase to query.")
        sys.exit()

    arcpy.AddMessage("Keyword Query: {}\n\n".format(keyword_query))
    alert_dict = call_dataminr(token, client_id, client_secret, time_in_minutes, '', '', keyword_query)
    Accumulated_Alerts.update(alert_dict)

# Check if any results were returned.
if not Accumulated_Alerts:
    arcpy.AddWarning("No alerts were pulled. Exiting......")
    sys.exit()

# Set Schema for fields
column_names = get_columns(Accumulated_Alerts)
column_schema = set_data_type(column_names)
export_dictionary_to_fc(output_fc_name, output_fgdb, column_schema, Accumulated_Alerts)

#Update the map view with the output layer
arcpy.SetParameter(7, os.path.join(output_fgdb, output_fc_name))
arcpy.management.ApplySymbologyFromLayer(os.path.join(output_fgdb, output_fc_name), symbology_layer, "VALUE_FIELD alert_type alert_type", "DEFAULT")

print("Script Runtime: ", datetime.now()-startTime)
arcpy.AddMessage("Script Runtime: " + str(datetime.now()-startTime))