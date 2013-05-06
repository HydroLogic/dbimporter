#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This script will mosaic and georeference GIO tiles from a single product.
'''

import re
import os
import datetime as dt
from fabric.api import local, settings, hide

# TODO - replace the placeholder code with a real implementation
def process_timeslot(product, timeslot_string, db_name, db_user, db_pass):
    product_patterns = {
        'LST' : re.compile(r''),
    }
    prod_patt = product_patterns.get(product)
    if prod_patt is not None:
        working_dir_path = dt.datetime.utcnow().strftime('/tmp/working_dir_' \
                                                         '%Y%m%d%H%M%S')
        local('mkdir --parents %s' % working_dir_path)
        timeslot = dt.datetime.strptime(timeslot_string, '%Y%m%d%H%M')
        tiles = _get_tiles(prod_patt, timeslot, working_dir_path,
                           archive_host, archive_path)
        georefs = _georeference_tiles(working_dir_path, *tiles)
        global_datasets = _merge_tiles(working_dir_path, *georefs)
        _import_into_database(db_name, db_user, db_pass, *global_datasets)
        local('rm -rf %s' % working_dir_path)

def _get_tiles(product_re, timeslot, destination_directory,
               archive_host, archive_path):
    '''
    Retreive the correct product tiles from the archive and bunzip them.

    The tiles get fetched to the working_directory and are bunzipped in
    place.
    '''

    tiles = []
    return tiles

def _merge_tiles(destination_directory, *tiles):
    '''
    Take the already georeferenced tiles and merge them into a global mosaic.
    '''

    mosaics = []
    # is it necessary to build overview files?
    return mosaics

def _import_into_database(db_name, db_user, db_pass, *global_datasets):
    '''
    Take the global mosaic and import it into the PostGIS database.
    '''

    pass

#TODO - the bounds are not being calculated correctly
def _georeference_tiles(*tiles):
    '''
    Take the product HDF5 tiles and georeference them.
    '''

    for tile in tiles:
        general_md = _get_general_metadata(tile)
        for ds_path in general_md.get('subdatasets', []):
            dataset_md = _get_dataset_metadata(ds_path)
            ulx, uly, lrx, lry = _calculate_bounds(general_md['first_lon'],
                                                   general_md['first_lat'],
                                                   dataset_md['n_cols'],
                                                   dataset_md['n_lines'],
                                                   general_md['pixel_size'])
            path, extension = os.path.splitext(tile)
            output_path = '%s_%s.tif' % path, dataset_md['name'])
            output_crs = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'
            local('gdal_translate -of GTiff -a_srs "%s" -a_ullr %6.3f ' \
                  '%6.3f %6.3f %6.3f -a_nodata %s %s %s' % (output_crs, ulx,
                  uly, lrx, lry, dataset_md['missing_value'], ds_path,
                  output_path))

def _get_general_metadata(h5_path):
    '''
    retrieve subdatasets, first lat and lon, no data, missing value, pixel size, etc.
    '''

    the_info = _run_gdalinfo(h5_path)
    metadata = dict()
    metadata['subdatasets'] = []
    lat_re = re.compile(r'FIRST_LAT=(-?\d+)$')
    lon_re = re.compile(r'FIRST_LON=(-?\d+)$')
    timeslot_re = re.compile(r'IMAGE_ACQUISITION_TIME=(\d+)$')
    pixelsize_re = re.compile(r'PIXEL_SIZE=(\d+\.?\d*)$')
    product_re = re.compile(r'^PRODUCT=(\w+)$')
    subdatasets_re = re.compile(r'SUBDATASET_\d_NAME=(HDF5:.*)$')
    for line in the_info.split():
        lat_obj = lat_re.search(line)
        lon_obj = lon_re.search(line)
        timeslot_obj = timeslot_re.search(line)
        pixelsize_obj = pixelsize_re.search(line)
        product_obj = product_re.search(line)
        subdatasets_obj = subdatasets_re.search(line)
        if lat_obj is not None:
            metadata['first_lat'] = float(lat_obj.group(1))
        elif lon_obj is not None:
            metadata['first_lon'] = float(lon_obj.group(1))
        elif timeslot_obj is not None:
            metadata['timeslot'] = dt.datetime.strptime(timeslot_obj.group(1),
                                                        '%Y%m%d%H%M')
        elif pixelsize_obj is not None:
            metadata['pixel_size'] = float(pixelsize_obj.group(1))
        elif product_obj is not None:
            metadata['product'] = product_obj.group(1)
        elif subdatasets_obj is not None:
            metadata['subdatasets'].append(subdatasets_obj.group(1))
    if len(metadata['subdatasets']) == 0:
        del metadata['subdatasets']
    return metadata

def _get_dataset_metadata(file_path):
    metadata = dict()
    the_info = _run_gdalinfo(file_path)
    missing_value_re = re.compile(r'MISSING_VALUE=(-?\d+)$')
    n_cols_re = re.compile(r'N_COLS=(\d+)$')
    n_lines_re = re.compile(r'N_LINES=(\d+)$')
    scaling_factor_re = re.compile(r'SCALING_FACTOR=(\d+)$')
    dataset_name_re = re.compile(r'_PRODUCT=(\w+)$')
    for line in the_info.split():
        missing_value_obj = missing_value_re.search(line)
        n_cols_obj = n_cols_re.search(line)
        n_lines_obj = n_lines_re.search(line)
        scaling_factor_obj = scaling_factor_re.search(line)
        dataset_name_obj = dataset_name_re.search(line)
        if missing_value_obj is not None:
            metadata['missing_value'] = int(missing_value_obj.group(1))
        elif n_cols_obj is not None:
            metadata['n_cols'] = int(n_cols_obj.group(1))
        elif n_lines_obj is not None:
            metadata['n_lines'] = int(n_lines_obj.group(1))
        elif scaling_factor_obj is not None:
            metadata['scaling_factor'] = int(scaling_factor_obj.group(1))
        elif dataset_name_obj is not None:
            metadata['name'] = dataset_name_obj.group(1)
    return metadata

def _run_gdalinfo(file_path):
    with settings(hide('stdout')):
        result = local('gdalinfo %s' % file_path, capture=True)
    return result

def _calculate_bounds(first_lon, first_lat, n_cols, n_lines, pixel_size):
    upper_left_lon = first_lon - pixel_size / 2.0
    upper_left_lat = first_lat + pixel_size / 2.0
    lower_right_lon = upper_left_lon * n_cols
    lower_right_lat = -upper_left_lat * n_lines
    return upper_left_lon, upper_left_lat, lower_right_lon, lower_right_lat
