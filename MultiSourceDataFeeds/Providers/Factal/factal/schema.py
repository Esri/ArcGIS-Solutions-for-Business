# TODO - Validate Schema

# These are the attributes that exist in the Factal API. These are the items that we are intersted in that we are populating every time. 
# Item Fields Not Manipulated Before Passed to Data Frame
item_fields = [
    'resource_uri',
    'permalink',
    'slug',
    'url_domain',
    'url',
    'type',
    'content',
    'source',
    'date',
    'created_date',
    'updated_date',
    'severity',
    'status',
    'submitter',
    'pushed',
    'pushed_major',
    'pushed_emerging',
    'tweeted'   
]

# # Arc Fields Not Manipulated Before Passed to Data Frame
# arc_fields = [
#     'name',
#     'permalink',
#     'latest_item_date',
#     'item_count'
# ]

# # Tag Fields Not Manipulated Before Passed to Data Frame
# tag_fields = [
#     'name',
#     'permalink'
# ]

# Fields to be Converted to Datetime During Data Frame Creation
item_times = ['updated_date', 'created_date', 'date']
topic_times = ['latest_item_date']#, 'created_on']
# arc_times = ['latest_item_date']

# IDs for Joining Items & Arcs
itm_id = 'id'
topic_id = 'item_id'
# arc_id = 'item_id'

# Time Field for Update & Delete Operations
itm_time_check = 'updated_date'
topic_time_check = 'latest_item_date'#'latest_item_date'
# arc_time_check = 'latest_item_date'

# Used for Sorting Location Categories & Selecting Lowest Value
loc_pref = {
    'POI': 1,
    'Aiport': 2,
    'Town': 3,
    'Suburb': 4,
    'County': 5,
    'State': 6,
    'Township': 7,
    'Country': 8,
    'Natural Feature': 9,
    'Colloquial': 10
}

# List of the topic kinds that each item can have
topic_kinds = [
    'arc',
    'location',
    'region',
    'tag',
    'vertical'
]

# These attributes are inside the topic key.
topic_fields = [
    'id',
    'resource_uri',
    'permalink',
    'items_resource_uri',
    'related_topics_uri',
    'slug',
    'active',
    'visible',
    'moderation_status',
    'name',
    'created_on',
    'kind',
    'category',
    'placetype',
    'latitude',
    'longitude',
    'point',
    'description',
    'wikipedia_url',
    'latest_item_date',
    'symbol'
]

# These attributes are outside the topic key
topic_fields_other = [
    'relevance'
]

# List that stores Hex-codes for resolution values. This is used to drive the values of display in Operations Dashboard for ArcGIS. 
resolution_hex_color_codes = {
    'POI':  '#92D050',
    'Airport':  '#92D050',
    'Town':  '#92D050',
    'Suburb':  '#FFFF00',
    'County':  '#FFFF00',
    'State': '#FF0000',
    'NaturalFeature': '#FF0000',
    'Country': '#FF0000',
    'Township': '#FF0000',
    'Colloquial': '#FF0000'
}
# List that stores Hex-codes for resolution values. This is used to drive the values of display in Operations Dashboard for ArcGIS. 
severity_hex_color_codes = {
    '1':  '#FBE741',
    '2':  '#FBA234',
    '3':  '#E85025',
    '4':  '#F40725',
    '5':  '#9A0113'
}
