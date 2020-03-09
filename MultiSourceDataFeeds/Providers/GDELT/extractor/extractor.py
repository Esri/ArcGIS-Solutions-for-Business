from .schema import v2_header, v1_header, article_columns, stat_names, aggregates, dtype_map, quad_class_domains, group_by_columns

from arcgis.features import GeoAccessor
from arcgis.gis import GIS

from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from newspaper import Article
from itertools import chain
from functools import wraps
import pandas as pd
import numpy as np
import traceback
import requests
import tempfile
import zipfile
import shutil
import pytz
import time
import os
import re

import warnings
warnings.filterwarnings("ignore")


class Extractor(object):

    def __init__(self):

        self.scratch = os.path.split(os.path.realpath(__file__))[0]

        self.v2_urls = self.get_v2_urls()
        self.v1_urls = self.get_v1_urls()

        self.look_dir     = os.path.join(self.scratch, 'lookups')
        self.cameo        = self.get_lookup(os.path.join(self.look_dir, 'cameo.txt'))
        self.country      = self.get_lookup(os.path.join(self.look_dir, 'country.txt'))
        self.country_fips = self.get_lookup(os.path.join(self.look_dir, 'country_fips.txt'))
        self.ethnic       = self.get_lookup(os.path.join(self.look_dir, 'ethnic.txt'))
        self.groups       = self.get_lookup(os.path.join(self.look_dir, 'groups.txt'))
        self.religion     = self.get_lookup(os.path.join(self.look_dir, 'religion.txt'))
        self.types        = self.get_lookup(os.path.join(self.look_dir, 'types.txt'))

        self.articles  = True
        self.max_age   = 24
        self.gis       = None
        self.delimiter = ';'
        self.flatten   = True

    @staticmethod
    def get_v2_urls():

        return {
            'last_update': 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
        }

    @staticmethod
    def get_v1_urls():

        return {
            'events': 'http://data.gdeltproject.org/events'
        }


    @staticmethod
    def batch_it(l, n):

        for i in range(0, len(l), n):
            yield l[i:i + n]

    @staticmethod
    def get_lookup(table_path):

        df = pd.read_csv(table_path, sep='\t', dtype={'CODE': str, 'LABEL': str})

        return dict(zip(df.CODE, df.LABEL))

    @staticmethod
    def extract_csv(csv_url, temp_dir):
        """
        Extract csv from the GDELT event package. Content is extracted and
        stored in specified directory. See temp_handler function for more details.

        This is used to extract content from GDELT 1.0 and 2.0.
        """

        response = requests.get(csv_url, stream=True)

        zip_name = csv_url.split('/')[-1]
        zip_path = os.path.join(temp_dir, zip_name)

        with open(zip_path, 'wb') as file: file.write(response.content)
        with zipfile.ZipFile(zip_path, 'r') as the_zip: the_zip.extractall(temp_dir)

        txt_name = zip_name.strip('export.CSV.zip')
        txt_name += '.txt'
        txt_path = os.path.join(temp_dir, txt_name)

        os.rename(zip_path.strip('.zip'), txt_path)

        return txt_path

    @staticmethod
    def run_df_stats(df, extracted_date):
        """
        Run summary statistics on dataframe. Events are grouped by country and event category
        and summarized by:
            - Avg. Tone (Goldstein Scale)
            - Number of Arcticles
            - Number of unique unique events

        This is used to summarize events from GDELT 1.0 and 2.0.
        """

        stat_df = df.groupby(['actor1countrycode', 'category']) \
            .aggregate({'AvgTone': 'mean', 'NumArticles': 'sum'}) \
            .reset_index()

        stat_df['extracted_date'] = pd.to_datetime(extracted_date).replace(tzinfo=pytz.UTC)

        stat_df.rename(columns=stat_names, inplace=True)

        return stat_df

    @staticmethod
    def get_gis_item(item_id, gis):

        item = gis.content.get(item_id)

        if not item:
            raise Exception(f'Input Item ID Not Found in GIS: {item_id}')
        else:
            return item

    @staticmethod
    def delete(lyr, df, create_field, oid_field, max_date):
        """
        Delete features from a hosted feature service that's beyond a max specified age.
        """

        del_oids = df[df[create_field] < max_date][oid_field].to_list()

        if del_oids:
            del_list = ','.join([str(i) for i in del_oids])
            res = lyr.delete_features(del_list)['deleteResults']
            print(f"Deleted {len([i for i in res if i['success']])} rows")
        else:
            print('No Records Found for Deletion')

    @staticmethod
    def batch_process_articles(article_list):
        """
        Enrichment function to parse and article metadata and extend into GDELT event data.
        """

        print(f"Subprocess Handling {len(article_list)} Articles")

        processed_data = []

        for event_article in article_list:

            try:
                # Parse GDELT Source
                article = Article(event_article)
                article.download()
                article.parse()
                article.nlp()

                # Unpack Article Properties & Replace Special Characters
                title     = article.title.replace("'", '')
                site      = urlparse(article.source_url).netloc
                summary   = '{} . . . '.format(article.summary.replace("'", '')[:500])
                keywords = '; '.join(sorted([re.sub('[^a-zA-Z0-9 \n]', '', key) for key in article.keywords]))
                meta_keys = '; '.join(sorted([re.sub('[^a-zA-Z0-9 \n]', '', key) for key in article.meta_keywords]))

                processed_data.append([event_article, title, site, summary, keywords, meta_keys])

            except:
                processed_data.append([event_article, None, None, None, None, None])

        return processed_data

    def handle_updates(self, all_lyr, all_sdf, new_sdf, id_field):

        if not len(all_sdf):
            res = all_lyr.edit_features(adds=new_sdf.spatial.to_featureset(), rollback_on_failure=False)['addResults']
            print(f"Added {len([i for i in res if i['success']])} rows")

        else:
            merged = all_sdf.merge(new_sdf, on=id_field, how='outer', indicator=True)

            adds = merged[merged['_merge'] == 'right_only']
            upds = merged[merged['_merge'] == 'both']

            # TODO - Keep _merge and Move The Following to a Single Line Right After the Merge
            #      - I.E. We Absolutely Don't Have to do This Twice
            adds = adds[[c for c in adds.columns if not c.endswith('_x')]]
            adds.columns = adds.columns.str.replace('_y', '')
            adds.drop(columns=['_merge'], inplace=True)
            upds = upds[[c for c in upds.columns if not c.endswith('_x')]]
            upds.columns = upds.columns.str.replace('_y', '')
            upds.drop(columns=['_merge'], inplace=True)

            if len(adds):
                self.process_edits(all_lyr, adds, 'add')

            if len(upds):
                self.process_edits(all_lyr, upds, 'update')

    def process_edits(self, feature_layer, data_frame, operation):
        """
        Push edits from SDF to hosted feature layer.
        """

        print(f"Running {operation.upper()} on Hosted Feature Layer")

        # Chunk edits into lists of 500 items. Python API can only push so many updates; item sized based on bytes.
        update_sets = list(self.batch_it(data_frame.spatial.to_featureset().features, 500))

        for edits in update_sets:
            if operation == 'update':
                res = feature_layer.edit_features(updates=edits, rollback_on_failure=False)['updateResults']
                print(f"Updated {len([i for i in res if i['success']])} rows of {len(edits)}")
            else:
                res = feature_layer.edit_features(adds=edits, rollback_on_failure=False)['addResults']
                print(f"Added {len([i for i in res if i['success']])} rows of {len(edits)}")

    def temp_handler(func):
        """
        Wrapper function that appends a temporary file directory value that's passed into
        the get_v2_sdf function. The directory path is used to temporarily store the .csv
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

    def connect(self, esri_url, username, password):

        self.gis = GIS(esri_url, username, password)

    def article_enrichment(self, article_list):
        """
        Multi-processing function that handles the article enrichment of GDELT events.
        """

        batches = list(self.batch_it(article_list, int(len(article_list) / cpu_count() - 1)))

        # Create Pool & Run Records
        pool = Pool(processes=cpu_count() - 1)
        data = pool.map(self.batch_process_articles, batches)
        pool.close()
        pool.join()

        return list(chain(*data))


    def collect_geometry(self, all_df):

        """
        Takes in All of the GDELT Records, Determines the Most Frequently Occurring Coordinate for Each
        sourceurl, and Return a Data Frame with 1 Unique sourceurl and Coordinate
        """

        # Tally Coordinates for Each sourceurl
        geo_df = all_df.groupby(['sourceurl', 'actiongeo_lat', 'actiongeo_long'])\
                 .aggregate({'globaleventid': 'count'})\
                 .reset_index()

        # Order Data Frame & Take the First Record - I.E. Most Frequently Occuring
        geo_df.sort_values('globaleventid', inplace=True, ascending=False)
        geo_df.drop(columns=['globaleventid'], inplace=True)
        geo_df.drop_duplicates('sourceurl', inplace=True)

        return geo_df


    def groupby_return_top_value(self, groupby_column, input_column, index_column, input_df):
        """
        Create a dataframe of the most frequently occurring value within a column based on a user specified groupby column. The processed
        dataframe is then sorted to select the most frequently occurring value and used to update in the input_df.

        NOTE: 
            - "NaN" values should not appear input_column. 
            - Group-by column is a list of columns. At a minimum, input and index columns should be in the list. Count of values based on the grouping.
            - Input column is placed into the aggregation function to get a count of unique values.
            - To return the most frequently occurring value based on the grouping, the index_column parameter is applied to the drop duplicates 
              and update dataframe functions.   
        """
        
         # Add Counter Column and set to 0.  This column will be used to store count value and sort.
        input_df[f'{input_column}_Count'] = 0

        # Tally Coordinates for Each sourceurl
        df_gb = input_df.groupby(groupby_column)\
                 .aggregate({f'{input_column}_Count': 'count'})\
                 .reset_index()

        # Order Data Frame & Take the First Record - I.E. Most Frequently Occuring
        df_gb.sort_values(f'{input_column}_Count', inplace=True, ascending=False)
        df_gb.drop(columns=[f'{input_column}_Count'], inplace=True) # Remove Counter Column before merge
        input_df.drop(columns=[f'{input_column}_Count'], inplace=True) # Remove counter from
        df_gb.drop_duplicates(index_column, inplace=True) # Remove duplicates leaving the most frequently occurring value 

        # Update the input_column in the input_df with top occuring values in df_gb 
        [input_df.drop(columns=[col], inplace=True) for col in groupby_column if col != index_column]
        input_df = input_df.merge(df_gb, on=index_column)

        return input_df


    def process_groupby(self, groupby_column, input_column, input_df, merge_override=''):
        """
        Create a dataframe of unique values within a column based on a user specified groupby column. The processed
        dataframe is then added back to the input_df and returned.

        NOTE: 
            - "NaN" values should not appear input_column. 
            - Input column represents a single column value.
            - Group-by column can be a single value or a list of values. Note this is used in 
            - Pass in a column name to override the default merge settings which uses the groupby_column
        """

        df_gb = input_df.groupby(groupby_column)[input_column].apply(
            lambda x: f'{self.delimiter} '.join([i for i in set(x) if i if i != 'nan'])).reset_index()
        input_df.drop(columns=[input_column], inplace=True)
        if not merge_override:
            input_df = input_df.merge(df_gb, on=groupby_column)
        else:
            input_df = input_df.merge(df_gb, on=merge_override)
        return input_df


    def process_df(self, df, extracted_date):
        """
        This function performs additional processing of the SDF.  Modifications to the dataframe
        include setting default values, hardsetting datatypes, and enriching with additional content.

        This is used to process content from GDELT 1.0 and 2.0. By default, event links are
        are enriched using the newspaper3k library.

        NOTE: any events that do not have coordinates are dropped.

        Process Methadology:
        1. Drop all records that don't have lat/long coordinates 
        2. Swap key/value pairs with lookup dictionary/tables
        3. Build out Group-by dataframes: Flattening rows based on source URL and semi-colon list of unique values for select attributes
        4. Get statistics on select attributes with integer values
        5. Get geometry based on the most common occuring lat/long set per article
        6. Merge processed dataframes back into main dataframe.
        """

        print(f'Received {len(df)} GDELT Records')
        # Put Timestamp for Deleting & Identifying Gaps in Later Runs
        df['extracted_date'] = pd.to_datetime(extracted_date).replace(tzinfo=pytz.UTC)

        # Discard Anything Without Coordinates or with Country Resolution
        df.dropna(subset=['actiongeo_long', 'actiongeo_lat'], inplace=True)
        df = df[df['actiongeo_type'] > 1]

        # Replace all nan in group_by_columns list; See schema.py for more info
        # Ensure "nan" Does Not Appear in Aggregate Output Fields
        [df[col].replace(np.nan, '', regex=True, inplace=True) for col in group_by_columns]

        # swap key/value pairs with lookup dictionary/tables
        df.replace({'eventcode': self.cameo}, inplace=True)
        df.replace({'eventbasecode': self.cameo}, inplace=True)
        df.replace({'eventrootcode': self.cameo}, inplace=True)
        df.replace({'quadclass': quad_class_domains}, inplace=True)
        df.replace({'actor1countrycode': self.country}, inplace=True)
        df.replace({'actor2countrycode': self.country}, inplace=True)
        df.replace({'actor1geo_countrycode': self.country_fips}, inplace=True)
        df.replace({'actor2geo_countrycode': self.country_fips}, inplace=True)
        df.replace({'actiongeo_countrycode': self.country_fips}, inplace=True)

        # Flatten GDELT records If Specified
        if self.flatten:

            # Identify the most frequently occuring values for Quadclass and Coordinates.  
            df = self.groupby_return_top_value(['sourceurl', 'quadclass'], 'quadclass', 'sourceurl', df)
            df = self.groupby_return_top_value(['sourceurl', 'actiongeo_lat', 'actiongeo_long'], 'actiongeo_lat', 'sourceurl', df)
            # self.collect_geometry(df)

            # Group By selected attributes and return unique values for each source URL
            for col in group_by_columns:
                df = self.process_groupby('sourceurl', col, df)

            # Add GoldsteinScale Max/Min columns
            df['goldsteinscale_max'] = df['goldsteinscale']
            df['goldsteinscale_min'] = df['goldsteinscale']

            # Aggregate numeric values based on statistics maintined in aggregates dictionary. See schema.py for more info
            num_gb = df.groupby('sourceurl').aggregate(aggregates).reset_index()
            for k, v in aggregates.items():
                if v == 'mean':
                    num_gb[k] = round(num_gb[k], 1)

            # Keep Only 1 Unique Row Based sourceurl - We Only Need the Coordinate Attributes Now
            df.drop_duplicates('sourceurl', inplace=True)
            print(f'Processing {len(df)} articles')

            # Drop Columns Before Adding Aggregated Versions of the Same Attribute
            df.drop(columns=[k for k in aggregates.keys()], inplace=True)

            # Bring Grouped Data Back to Data Frame
            df = df.merge(num_gb, on='sourceurl')

        # Process and Append Article Information If Specified
        if self.articles:
            article_data = self.article_enrichment(df['sourceurl'].values.tolist())
            a_df = pd.DataFrame(article_data, columns=article_columns)
            df = df.merge(a_df, on='sourceurl')

        # Build Geometry
        df = df.spatial.from_xy(df, 'actiongeo_long', 'actiongeo_lat')

        print(f'Returned {len(df)} GDELT Records')

        return df

    def fetch_last_v2_url(self):
        """
        Grab the V2 export .csv from the latest update URL. The url contains a list of three
        packages that can be downloaded.  This function will return the export package in
        the list.  This represents the newest events in the 15 minute dump.
        """
        response = requests.get(self.v2_urls.get('last_update'))
        last_url = [r for r in response.text.split('\n')[0].split(' ') if 'export' in r][0]

        return last_url

    def fetch_last_v1_url(self):
        """
        Grab the V1 export .csv from the events index URL. The url contains a list of daily
        packages that can be downloaded, dating back to 2013-04-01. This function will return
        the latest package in the list.  This represents the newest events in the 24 hour dump.
        """

        response = requests.get(f"{self.v1_urls.get('events')}/index.html")
        the_soup = BeautifulSoup(response.content[:2000], features='lxml')
        last_csv = the_soup.find_all('a')[3]['href']
        last_url = f"{self.v1_urls.get('events')}/{last_csv}"

        return last_url

    def collect_v1_csv(self, temp_dir):

        """
        Collects Latest V1 CSV & Returns Path to CSV & CSV Name (Extraction Date)
        """

        last_url = self.fetch_last_v1_url()

        csv_file = self.extract_csv(last_url, temp_dir)

        # CSV File Name Will be Converted to Date & Stored in "Extracted_Date" Column
        csv_name = os.path.basename(csv_file).split('.')[0]

        return csv_file, csv_name

    def collect_v2_csv(self, temp_dir):

        """
        Collects Latest V2 CSV & Returns Path to CSV & CSV Name (Extraction Date)
        """

        last_url = self.fetch_last_v2_url()

        csv_file = self.extract_csv(last_url, temp_dir)

        # CSV File Name Will be Converted to Date & Stored in "Extracted_Date" Column
        csv_name = os.path.basename(csv_file).split('.')[0]

        return csv_file, csv_name

    def get_v2_sdf(self, csv_file, csv_name):
        """
        Process GDELT 2.0 event data in a .csv format and convert into a spatial data frame.
        """

        try:
            # Convert csv into a pandas dataframe. See schema.py for columns processed from GDELT 2.0
            df = pd.read_csv(csv_file, sep='\t', names=v2_header, dtype=dtype_map)

            return self.process_df(df, csv_name)

        except Exception as gen_exc:
            print(f'Error Building SDF: {gen_exc}')

    def get_v1_sdf(self, csv_file, csv_name):
        """
        Process GDELT 1.0 event data in a .csv format and convert into a spatial data frame.
        """

        try:
            # Convert csv into a pandas dataframe. See schema.py for columns processed from GDELT 2.0
            df = pd.read_csv(csv_file, sep='\t', names=v1_header, dtype=dtype_map)

            return self.process_df(df, csv_name)

        except Exception as gen_exc:
            print(f'Error Building SDF: {gen_exc}')

    @temp_handler
    def build_v2(self, temp_dir, folder=None):
        """
        Build function to extract, process and push events from GDELT 2.0 into a a new hosted feature layer and table.

        NOTE: It's recommend to run this function before running run_v2 function as this crates the base layer with the
        correct schema to push new events into.
        """

        # Process Started
        start = time.time()

        # Get Data Frame with SHAPE Attributes
        csv_file, csv_name = self.collect_v2_csv(temp_dir)
        df = self.get_v2_sdf(csv_file, csv_name)

        # Publish as Hosted Feature Layer
        full_hfl = df.spatial.to_featurelayer(f'V2_{csv_name}', gis=self.gis)

        if folder:

            self.gis.content.create_folder(folder)

            for item in [full_hfl]:
                item.move(folder)

        print(f'Created Baseline: {round((time.time() - start) / 60, 2)}')

    @temp_handler
    def run_v2(self, temp_dir, hfl_id):
        """
        Runner function to extract, process and push events from GDELT 2.0 into an existing hosted feature layer and table.

        NOTE: If Hosted feature layer and table do not exist, it's recommend to run the build_V2 function to create layer
        with the necessary schema to load data into.
        """

        start = time.time()

        try:
            # Flag for Summary Table Deletion
            past_date = (datetime.utcnow() - timedelta(hours=self.max_age))

            # Process Latest 15 Minute Hosted Feature Layer
            all_itm = self.get_gis_item(hfl_id, self.gis)
            all_lyr = all_itm.layers[0]
            all_sdf = all_lyr.query(out_fields='extracted_date', return_geometry=False).sdf

            # Collect & Unpack Latest 15 Minute CSV Dump
            csv_file, csv_name = self.collect_v2_csv(temp_dir)
            csv_date = pd.to_datetime(csv_name).replace(tzinfo=pytz.UTC)

            # Skip Anything Already Processed
            if len(all_sdf) > 0 and np.datetime64(csv_date) in all_sdf['extracted_date'].unique():
                print(f'Data Already Extracted for Current Date: {csv_date}')
                return

            # Convert Current 15 Minute GDELT Data to Spatial Data Frame
            new_df = self.get_v2_sdf(csv_file, csv_name)

            # Remove Data Older Than Max Age from GDELT 2.0 hosted feature layer table.
            # Return If Date Already Processed
            if len(all_sdf):
                self.delete(all_lyr, all_sdf, 'extracted_date', all_lyr.properties.objectIdField, past_date)

            # Push New Data
            self.process_edits(all_lyr, new_df, 'add')

        finally:
            print(f'Ran V2 Solution: {round((time.time() - start) / 60, 2)}')

    @temp_handler
    def run_v1(self, temp_dir, hft_id, gdb_path):
        """
        Runner function to extract, process and push events from GDELT 1.0 into an existing hosted feature layer and table.

        NOTE: If Hosted feature layer and table do not exist, it's recommend to run the build_v1 function to create layer
        with the necessary schema to load data into.
        """

        # Process Started
        start = time.time()

        try:
            # Collect & Unpack Latest Daily CSV Dump
            csv_file, csv_name = self.collect_v1_csv(temp_dir)

            # Get Data Frame with SHAPE Attributes
            df = self.get_v1_sdf(csv_file, csv_name)

            # Create Local Feature Class
            fc = df.spatial.to_featureclass(os.path.join(gdb_path, f'V1_{csv_name}'), overwrite=True)
            print(f"Created Local Feature Class: {fc}")

        finally:
            print(f'Ran V1 Solution: {round((time.time() - start) / 60, 2)}')
