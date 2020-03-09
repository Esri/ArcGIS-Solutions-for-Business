v2_header = [
    'globaleventid',
    'sqldate',
    'monthyear',
    'year',
    'fractiondate',
    'actor1code',
    'actor1name',
    'actor1countrycode',
    'actor1knowngroupcode',
    'actor1ethniccode',
    'actor1religion1code',
    'actor1religion2code',
    'actor1type1code',
    'actor1type2code',
    'actor1type3code',
    'actor2code',
    'actor2name',
    'actor2countrycode',
    'actor2knowngroupcode',
    'actor2ethniccode',
    'actor2religion1code',
    'actor2religion2code',
    'actor2type1code',
    'actor2type2code',
    'actor2type3code',
    'isrootevent',
    'eventcode',
    'eventbasecode',
    'eventrootcode',
    'quadclass',
    'goldsteinscale',
    'nummentions',
    'numsources',
    'numarticles',
    'avgtone',
    'actor1geo_type',
    'actor1geo_fullname',
    'actor1geo_countrycode',
    'actor1geo_adm1code',
    'actor1geo_adm2code',
    'actor1geo_lat',
    'actor1geo_long',
    'actor1geo_featureid',
    'actor2geo_type',
    'actor2geo_fullname',
    'actor2geo_countrycode',
    'actor2geo_adm1code',
    'actor2geo_adm2code',
    'actor2geo_lat',
    'actor2geo_long',
    'actor2geo_featureid',
    'actiongeo_type',
    'actiongeo_fullname',
    'actiongeo_countrycode',
    'actiongeo_adm1code',
    'actiongeo_adm2code',
    'actiongeo_lat',
    'actiongeo_long',
    'actiongeo_featureid',
    'dateadded',
    'sourceurl'
]

v1_header = [
    'globaleventid',
    'sqldate',
    'monthyear',
    'year',
    'fractiondate',
    'actor1code',
    'actor1name',
    'actor1countrycode',
    'actor1knowngroupcode',
    'actor1ethniccode',
    'actor1religion1code',
    'actor1religion2code',
    'actor1type1code',
    'actor1type2code',
    'actor1type3code',
    'actor2code',
    'actor2name',
    'actor2countrycode',
    'actor2knowngroupcode',
    'actor2ethniccode',
    'actor2religion1code',
    'actor2religion2code',
    'actor2type1code',
    'actor2type2code',
    'actor2type3code',
    'isrootevent',
    'eventcode',
    'eventbasecode',
    'eventrootcode',
    'quadclass',
    'goldsteinscale',
    'nummentions',
    'numsources',
    'numarticles',
    'avgtone',
    'actor1geo_type',
    'actor1geo_fullname',
    'actor1geo_countrycode',
    'actor1geo_adm1code',
    'actor1geo_lat',
    'actor1geo_long',
    'actor1geo_featureid',
    'actor2geo_type',
    'actor2geo_fullname',
    'actor2geo_countrycode',
    'actor2geo_adm1code',
    'actor2geo_lat',
    'actor2geo_long',
    'actor2geo_featureid',
    'actiongeo_type',
    'actiongeo_fullname',
    'actiongeo_countrycode',
    'actiongeo_adm1code',
    'actiongeo_lat',
    'actiongeo_long',
    'actiongeo_featureid',
    'dateadded',
    'sourceurl'
]

article_columns = [
    'sourceurl',
    'title',
    'site',
    'summary',
    'keywords',
    'meta'
]

stat_names = {
    'globaleventid': 'records'
}

aggregates = {
    'goldsteinscale': 'mean',
    'goldsteinscale_max': 'max',
    'goldsteinscale_min': 'min',
    'numarticles': 'mean',
    'numsources': 'mean',
    'nummentions': 'mean',
    'avgtone': 'mean'
}

group_by_columns = [
    'actor1code',
    'actor2code',
    'actor1name',
    'actor2name',
    'actor1countrycode',
    'actor2countrycode',
    'actor1knowngroupcode',
    'actor2knowngroupcode',
    'actor1ethniccode',
    'actor2ethniccode',
    'actor1religion1code',
    'actor2religion1code',
    'actor1religion2code',
    'actor2religion2code',
    'actor1type1code',
    'actor2type1code',
    'actor1type2code',
    'actor2type2code',
    'actor1type3code',
    'actor2type3code',
    'eventcode',
    'eventbasecode',
    'eventrootcode'
]

# This dictionary specifies the geographic resolution of the event.
"""
Is applied to Actor1, Actor2 and Action prefixes
1=COUNTRY (match was at the country level), 
2=USSTATE (match was to a US state), 
3=USCITY (match was to a US city or landmark),
4=WORLDCITY (match was to a city or landmark outside the US),
5=WORLDSTATE (match was to an Administrative Division 1 outside the US – roughly equivalent to a US state).
Note that matches with codes 1 (COUNTRY), 2 (USSTATE), and 5 (WORLDSTATE) will still provide a latitude/longitude pair, which will be the
centroid of that country or state, but the FeatureID field below will be blank
"""

Geo_Type = {
    '1': 'Country',
    '2': 'US State',
    '3': 'US City',
    '4': 'World City',
    '5': 'World State'
}


# Primary classification  for CAMEO event taxonomy.
"""
This field specifies this primary classification for the event type, allowing analysis at the highest level of aggregation.
1=Verbal Cooperation, 2=Material Cooperation, 3=Verbal Conflict, 4=Material Conflict.
"""

quad_class_domains = {
    '1': 'Verbal Cooperation',
    '2': 'Material Cooperation',
    '3': 'Verbal Conflict',
    '4': 'Material Conflict'
}

# Manually set datatype of attributes.
# Event codes needs to be read in as strings
dtype_map = {
    'isrootevent': str,
    'eventcode': str,
    'eventbasecode': str,
    'eventrootcode': str,
    'globaleventid': str,
    'dateadded': str,
    'sqldate': int,
    'monthyear': int,
    'year': int,
    'fractiondate': float,
    'actiongeo type': int,
    'actor1code': str,
    'actor2code': str,
    'actor1name': str,
    'actor2name': str,
    'actor1countrycode': str,
    'actor2countrycode': str,
    'actor1knowngroupcode': str,
    'actor2knowngroupcode': str,
    'actor1ethniccode': str,
    'actor2ethniccode': str,
    'actor1religion1code': str,
    'actor2religion1code': str,
    'actor1religion2code': str,
    'actor2religion2code': str,
    'actor1type1code': str,
    'actor2type1code': str,
    'actor1type2code': str,
    'actor2type2code': str,
    'actor1type3code': str,
    'actor2type3code': str,
    'quadclass': str,
    'goldsteinscale': float,
    'nummentions': int,
    'numsources': int,
    'numarticles': int,
    'avgtone': float,
    'actiongeo_fullname': str,
    'actiongeo_lat': float,
    'actiongeo_long': float,
    'actiongeo_featureid': str,
    'actor1geo_fullname': str,
    'actor1geo_countrycode': str,
    'actor1geo_adm1code': str,
    'actor1geo_lat': float,
    'actor1geo_long': float,
    'actor1geo_featureid': str,
    'actor2geo_fullname': str,
    'actor2geo_countrycode': str,
    'actor2geo_adm1code': str,
    'actor2geo_lat': float,
    'actor2geo_long': float,
    'actor2geo_featureid': str
}
