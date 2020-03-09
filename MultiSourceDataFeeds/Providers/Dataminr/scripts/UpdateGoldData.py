import arcpy
import os
from arcgis.gis import GIS
from arcgis.features import GeoAccessor
from helper import *
from datetime import datetime
arcpy.env.overwriteOutput = True

startTime = datetime.now()
arcpy.AddMessage(startTime)


def create_sql_clause(field_name, values):
    SQL_Clause = "{} IN ('{}')".format(
        field_name,
        "','".join(str(id) for id in values)
    )
    return SQL_Clause


#.........................................................................
# User Input data
#.........................................................................

New_Data_Layer = arcpy.GetParameterAsText(0)
Web_Gold_layer_id = arcpy.GetParameterAsText(1)
org_url = arcpy.GetParameterAsText(2)
username = arcpy.GetParameterAsText(3)
password = arcpy.GetParameterAsText(4)

#...........................................................
# SETTING UP ENVIRONMENT DEFAULTS
#...........................................................

# Setting the default paths for folder locations
SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))

# Extract FC name and path path from source/target layer
New_Data_Path, New_Data_Name= input_check(New_Data_Layer)

# Field that's associated with the Alert ID
Alert_Field_Name = "alert_id"

# Using the ArcGIS Python API, Create GIS Object with user credentials
# and extract service URL from the provided item ID
gis = GIS(org_url, username, password)
web_gold_service = gis.content.get(Web_Gold_layer_id)
web_gold_lyr = web_gold_service.layers[0]

# Sign into portal for duration of this active python session
# Extract token for the signed in user and append to web service URL
Web_Gold_Path = '{}?token={}'.format(web_gold_lyr.url, gis._con.token)

#.........................................................................
# Main Program
#.........................................................................

# Get alert ID's from the source layer
source_alertIDs = unique_values(New_Data_Path, Alert_Field_Name)

# Create a SQL clause and query the web layer based on the alert id's of the from the new data layer.
SQL_Clause = create_sql_clause(Alert_Field_Name, source_alertIDs)
web_gold_query = web_gold_lyr.query(where=SQL_Clause)

# Get a list of alert id's that were matched with the SQL clause
target_alertIDs = list()
for item in web_gold_query:
    target_alertIDs.append(item.attributes[Alert_Field_Name])

# Get the differences between the Alert ID's.
# The set operator will return the values in source that's not in target
new_alertIDs = list(set(source_alertIDs) - set(target_alertIDs))
arcpy.AddMessage("Number of New alerts to add: {}".format(len(new_alertIDs)))

# Convert feature class to spatial dataframe and get alert id's
sdf = GeoAccessor.from_featureclass(New_Data_Path)

# Append results to target layer
# Append results to target layer
arcpy.AddMessage("Appending results to the hosted feature layer, {}".format(
    web_gold_service.title))
add_features = sdf[sdf[Alert_Field_Name].isin(new_alertIDs)]
if len(new_alertIDs) > 0:
    res = web_gold_lyr.edit_features(
        adds=add_features.spatial.to_featureset())['addResults']
else:
    arcpy.AddMessage('No new results to add')

print("Script Runtime: ", datetime.now()-startTime)
arcpy.AddMessage("Script Runtime: " + str(datetime.now()-startTime))
