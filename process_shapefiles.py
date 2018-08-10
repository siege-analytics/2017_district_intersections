# http://newcoder.io/scrape/part-3/
# https://geoalchemy-2.readthedocs.io/en/latest/orm_tutorial.html
# https://gis.stackexchange.com/questions/239198/geopandas-dataframe-to-postgis-table-help/239231
# https://stackoverflow.com/questions/34383000/pandas-to-sql-all-columns-as-nvarchar

# Python STDLib

import glob
import sys

# Python installed libs

from sqlalchemy import *
from sqlalchemy import types
from sqlalchemy.engine.url import URL
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely import wkt

from geoalchemy2 import Geometry, WKTElement, types as gatypes

# Handwritten code

def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine('sqlite:///local_spatial.db')


def associate_column_names_and_sqlalchemy_types(dataframe):

    """
    This creates a dict of column names and sqlalchemy types for use in Pandas to_sql

    :returns dict
    """

    dtypedict = {}

    for i,j in zip(dataframe.columns, dataframe.dtypes):

        if "geom" in str(i):
            dtypedict.update({i: gatypes.Geometry(geometry_type="POINT", srid=4326)})

        else:
            dtypedict.update({i: types.TEXT()})

    return dtypedict


def load_all_states(target_path='./', db_engine=None):
    """
    Loads all the TXT files in the target directory

    :return True
    """

    glob_path = target_path + "*.TXT"

    try:

        for voter_file in glob.glob(glob_path):
            new_table_name = voter_file[:-4].lower().replace('.', '_').replace('precinctgis', 'pdi_file').split('/')[-1]

            print("Just created a new table name:  {}".format(new_table_name))

            table_data = pd.read_csv(voter_file, sep='\t', low_memory=False)

            print("Just loaded the dataset to pandas")

            # table_data['geometry'] = table_data.apply(lambda z: WKTElement(Point(z.Longitude, z.Latitude), srid=4326), axis=1)

            table_data['geometry'] = [Point(xy) for xy in zip(table_data.Longitude, table_data.Latitude)]

            # crs = {'init': 'epsg:4326'}

            # print("Just added a geometry column with value {0}".format(table_data['geometry']))

            geo_table_data = gpd.GeoDataFrame(table_data)
            # geo_table_data = geo_table_data.set_geometry('geometry')

            print("Just converted the pandas dataset to GeoPandas DataFrame")

            geo_table_data['geometry_alt'] = geo_table_data.apply(lambda z: WKTElement(Point(z.Longitude, z.Latitude), srid=4326), axis=1)
            # columns_and_data_types = associate_column_names_and_sqlalchemy_types(dataframe=geo_table_data)
            geo_table_data.drop(columns='geometry')

            # print(columns_and_data_types)

            # print(geo_table_data.geometry)

            geo_table_data.to_sql(new_table_name,
                                  db_engine,
                                  if_exists='replace',
                                  # index=True,
                                  # dtype=columns_and_data_types
                                  dtype={'geometry_alt': Geometry('POINT', srid=4326)}
                                  )

            print("Just loaded {} to PSQL".format(new_table_name))


    except Exception as ex:

        print("Error: {0}".format(ex))
        sys.exit(0)


if __name__ == '__main__':

    # Connect to DB

    try:

        engine = db_connect()
        print("Successfully connected to SQLite with SQLAlchemy")

    except Exception as ex:

        print("Error : {0}".format(ex))
        sys.exit(0)

