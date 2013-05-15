#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This script will mosaic and georeference GIO tiles from a single product.

Strategy to adopt:

    - Add georeferencing information to each tile from a single product. Use
      gdal_translate to extract metadata for each tile and add the relevant CRS
      definitions. Add also the correct noData value to each file;
    - Gather all the tiles into global mosaics, one for each subdataset. Use
      gdal_merge;
    - Build a vrt file that combines the global georeferenced subdatasets into
      a single multiband file. The VRT file can be updated with the relevant
      scalingFactor for each band using the Scale tag;
    - import the vrt into a Postgis database as a raster. Use raster2pgsql
      with a tilesize of 200x200.

NOTES:

    - When creating the Postgis database, remember to alter the permissions on
      the tables after creating the postgis extension
'''

import re
import os
import datetime as dt

from lxml import etree
from fabric.api import local, settings, hide

def process_timeslot(working_dir, product, timeslot,
                     db_name=None, db_user=None, db_pass=None):
    patt = r'^g2_BIOPAR_%s_%s_[A-Z]+_GEO_v1$' % (product, timeslot)
    tiles = _get_tiles(working_dir, patt)
    georefs = _georeference_tiles(*tiles)
    global_datasets = _merge_tiles(*georefs)
    vrt_path = _build_vrt(working_dir, *global_datasets)
    #_import_into_database(db_name, db_user, db_pass, vrt_path)

def _get_tiles(working_dir, file_name_pattern):
    '''
    Return a list with the full paths to the tiles to process.

    Inputs:

        working_dir - The directory where the files are located.

        file_name_pattern - A string with a regex pattern for searching
            against the file names.
    '''

    tiles = []
    for entry in os.listdir(working_dir):
        if re.search(file_name_pattern, entry) is not None:
            tiles.append(os.path.realpath(os.path.join(working_dir, entry)))
    return tiles

def _merge_tiles(*tiles):
    '''
    Take the already georeferenced tiles and merge them into a global mosaic.
    '''

    mosaics = []
    datasets = dict()
    product = re.search(r'g2_BIOPAR_(\w+)_\d{12}', tiles[0]).group(1)
    for path in tiles:
        dataset = re.search('v1_(\w+).tif$', path).group(1)
        if dataset in datasets.keys():
            datasets[dataset].append(path)
        else:
            datasets[dataset] = [path]
    for dataset, tiles in datasets.iteritems():
        print('file: %s' % tiles[0])
        md = _extract_metadata(tiles[0])

        output_dir = os.path.dirname(tiles[0])
        ts = md['IMAGE_ACQUISITION_TIME'].strftime('%Y%m%d%H%M')
        output_path = '%s/global_%s_%s_%s.tif' % (output_dir, product,
                                                  ts, dataset)
        local('gdal_merge.py -o %s -a_nodata %s %s' % (output_path,
              md['MISSING_VALUE'], ' '.join(tiles)))
        mosaics.append(output_path)
    return mosaics

def _build_vrt(working_dir, *global_files):
    '''
    Build a VRT file with each of the already created global files as a band.
    '''

    x_size = '7200'
    y_size = '2800'
    dtype = 'Int16'
    root = etree.Element('VRTDataset', rasterXSize=x_size, rasterYSize=y_size)
    srs = etree.SubElement(root, 'srs')
    srs.text = 'EPSG:4326'
    geo_transform = etree.SubElement(root, 'GeoTransform')
    geo_transform.text = '-170.025, 0.05, 0, 80.025, 0, -0.05'
    for i, file_path in enumerate(global_files):
        global_meta = _get_global_meta(file_path)
        band = etree.SubElement(root, 'VRTRasterBand', {'dataType' : dtype,
                                'band': '%i' % (i+1)})
        description = etree.SubElement(band, 'Description')
        description.text = global_meta.get('subdataset')
        no_data = etree.SubElement(band, 'NoDataValue')
        no_data.text = '%s' % global_meta.get('MISSING_VALUE')
        scale = etree.SubElement(band, 'Scale')
        scale.text = '%s' % global_meta.get('SCALING_FACTOR')
        source = etree.SubElement(band, 'SimpleSource')
        source_path = etree.SubElement(source, 'SourceFileName', 
                                     {'relativeToVrt' : '0'})
        source_path.text = file_path
        source_band = etree.SubElement(source, 'SourceBand')
        source_band.text = '1'
        source_properties = etree.SubElement(source, 'SourceProperties',
                                             {'RasterXSize' : x_size,
                                              'RasterYSize' : y_size,
                                              'DataType' : dtype,
                                              'BlockXSize' : x_size,
                                              'BlockYSize' : '1'})
        src_rect = etree.SubElement(source, 'SrcRect', {'xOff' : '0',
                                                        'yOff' : '0',
                                                        'xSize' : x_size,
                                                        'ySize' : y_size})
        dst_rect = etree.SubElement(source, 'DstRect', {'xOff' : '0',
                                                        'yOff' : '0',
                                                        'xSize' : x_size,
                                                        'ySize' : y_size})
    tree = etree.ElementTree(root)
    tree.write(os.path.join(working_dir, 'out.vrt'), pretty_print=True)

def _get_global_meta(global_product):
    ds = re.search(r'_\d{12}_([\w_]+)\.tif$', global_product).group(1)
    global_meta = {'subdataset' : ds}
    working_dir = os.path.dirname(global_product)
    all_files = os.listdir(working_dir)
    found_one = False
    count = 0
    while not found_one:
        full_path = os.path.join(working_dir, all_files[count])
        if re.search(r'v1_%s\.tif' % ds, full_path) is not None:
            meta = _extract_metadata(full_path)
            global_meta['SCALING_FACTOR'] = meta['SCALING_FACTOR']
            global_meta['MISSING_VALUE'] = meta['MISSING_VALUE']
            global_meta['size_x'] = meta['size_x']
            global_meta['size_y'] = meta['size_y']
            found_one = True
        count +=1
    return global_meta

def _import_into_database_multiple_bands(db_name, db_user, db_pass,
                                         *global_datasets):
    '''
    Take the global mosaic and import it into the PostGIS database.

    1 - Will use the raster2pgsql utility to import the vrt file into the database
    2 - insert additional data in the respective rows (timeslot, ...)
    '''

    pass

def _import_into_database(db_name, db_user, db_pass, vrt file):
    pass

def _georeference_tiles(*tiles):
    '''
    Take the product HDF5 tiles and georeference them.

    Note that GDAL uses top left corner of pixel as a way to anchor its
    coordinates.
    '''

    georefs = []
    for tile in tiles:
        working_dir = os.path.dirname(tile)

        file_meta = _extract_metadata(tile)
        for ds_name, ds_meta in file_meta['subdatasets'].iteritems():
            ulx, uly, lrx, lry = _calculate_bounds(file_meta['FIRST_LON'],
                                                   file_meta['FIRST_LAT'],
                                                   ds_meta['N_COLS'],
                                                   ds_meta['N_LINES'],
                                                   file_meta['PIXEL_SIZE'])
            path, extension = os.path.splitext(tile)
            output_path = '%s_%s.tif' % (path, ds_name)
            output_crs = 'EPSG:4326'
            gdal_cmd = 'gdal_translate -q -of GTiff -a_srs "%s" -a_ullr ' \
                       '%6.3f %6.3f %6.3f %6.3f -a_nodata %s' % \
                       (output_crs, ulx, uly, lrx, lry,
                        ds_meta['MISSING_VALUE'])
            for meta_name, meta_val in ds_meta.iteritems():
                gdal_cmd += ' -mo "%s=%s"' % (meta_name, meta_val)
            gdal_cmd += ' %s %s' % (ds_meta['path'], output_path)
            with settings(hide('stdout')):
                local(gdal_cmd)
            georefs.append(output_path)
    return georefs

def _calculate_bounds(first_lon, first_lat, n_cols, n_lines, pixel_size):
    upper_left_lon = first_lon - pixel_size / 2.0
    upper_left_lat = first_lat + pixel_size / 2.0
    lower_right_lon = upper_left_lon + n_cols * pixel_size
    lower_right_lat = upper_left_lat - n_lines * pixel_size
    return upper_left_lon, upper_left_lat, lower_right_lon, lower_right_lat
#
#def _get_general_metadata(h5_path):
#    '''
#    retrieve subdatasets, first lat and lon, no data, missing value, pixel size, etc.
#    '''
#
#    the_info = _run_gdalinfo(h5_path)
#    metadata = dict()
#    metadata['subdatasets'] = _extract_subdataset_paths(the_info).values()
#    if len(metadata['subdatasets']) == 0:
#        del metadata['subdatasets']
#    lat_re = re.compile(r'FIRST_LAT=(-?\d+)')
#    lon_re = re.compile(r'FIRST_LON=(-?\d+)')
#    timeslot_re = re.compile(r'IMAGE_ACQUISITION_TIME=(\d+)')
#    pixelsize_re = re.compile(r'PIXEL_SIZE=(\d+\.?\d*)')
#    product_re = re.compile(r'^PRODUCT=(\w+)')
#    for line in the_info.split('\n'):
#        lat_obj = lat_re.search(line)
#        lon_obj = lon_re.search(line)
#        timeslot_obj = timeslot_re.search(line)
#        pixelsize_obj = pixelsize_re.search(line)
#        product_obj = product_re.search(line)
#        if lat_obj is not None:
#            metadata['first_lat'] = float(lat_obj.group(1))
#        elif lon_obj is not None:
#            metadata['first_lon'] = float(lon_obj.group(1))
#        elif timeslot_obj is not None:
#            metadata['timeslot'] = dt.datetime.strptime(timeslot_obj.group(1),
#                                                        '%Y%m%d%H%M')
#        elif pixelsize_obj is not None:
#            metadata['pixel_size'] = float(pixelsize_obj.group(1))
#        elif product_obj is not None:
#            metadata['product'] = product_obj.group(1)
#    return metadata
#
#def _get_dataset_metadata(file_path):
#    metadata = dict()
#    the_info = _run_gdalinfo(file_path)
#    missing_value_re = re.compile(r'MISSING_VALUE=(-?\d+)')
#    n_cols_re = re.compile(r'N_COLS=(\d+)')
#    n_lines_re = re.compile(r'N_LINES=(\d+)')
#    scaling_factor_re = re.compile(r'SCALING_FACTOR=(\d+)')
#    dataset_name_re = re.compile(r'_PRODUCT=(\w+)')
#    for line in the_info.split('\n'):
#        missing_value_obj = missing_value_re.search(line)
#        n_cols_obj = n_cols_re.search(line)
#        n_lines_obj = n_lines_re.search(line)
#        scaling_factor_obj = scaling_factor_re.search(line)
#        dataset_name_obj = dataset_name_re.search(line)
#        if missing_value_obj is not None:
#            metadata['missing_value'] = int(missing_value_obj.group(1))
#        elif n_cols_obj is not None:
#            metadata['n_cols'] = int(n_cols_obj.group(1))
#        elif n_lines_obj is not None:
#            metadata['n_lines'] = int(n_lines_obj.group(1))
#        elif scaling_factor_obj is not None:
#            metadata['scaling_factor'] = int(scaling_factor_obj.group(1))
#        elif dataset_name_obj is not None:
#            metadata['name'] = dataset_name_obj.group(1)
#    return metadata
#
#def _get_geotiff_metadata(file_path):
#    metadata = dict()
#    the_info = _run_gdalinfo(file_path)
#    missing_value_re = re.compile(r'NoData Value=(-?\d+)')
#    product_re = re.compile(r'(?<!_)PRODUCT=(\w+)')
#    dataset_re = re.compile(r'_PRODUCT=(\w+)')
#    timeslot_re = re.compile(r'IMAGE_ACQUISITION_TIME=(\d{12})')
#    for line in the_info.split('\n'):
#        mv_obj = missing_value_re.search(line)
#        p_obj = product_re.search(line)
#        d_obj = dataset_re.search(line)
#        t_obj = timeslot_re.search(line)
#        if mv_obj is not None:
#            metadata['missing_value'] = int(mv_obj.group(1))
#        elif p_obj is not None:
#            metadata['product'] = p_obj.group(1)
#        elif d_obj is not None:
#            metadata['dataset'] = d_obj.group(1)
#        elif t_obj is not None:
#            metadata['timeslot'] = t_obj.group(1)
#    return metadata
#
def _run_gdalinfo(file_path):
    with settings(hide('stdout')):
        result = local('gdalinfo %s' % file_path, capture=True)
    return result

def _extract_metadata(file_path):
    gdalinfo = _run_gdalinfo(file_path)
    subdatasets = _extract_subdataset_paths(gdalinfo)
    bands = _extract_band_properties(gdalinfo)
    start_md_line = False
    metadata = dict()
    if len(subdatasets) > 0:
        metadata['subdatasets'] = dict()
        for sds, path in subdatasets.iteritems():
            metadata['subdatasets'][sds] = {'path' : path}
    if len(bands) > 0:
        metadata['bands'] = bands
    metadata['size_x'], metadata['size_y'] = _extract_size(gdalinfo)
    for line in gdalinfo.split('\n'):
        if start_md_line:
            line_obj = re.search(r'^\s+(?P<key>\w+)=(?P<value>.*)$', line)
            if line_obj is not None:
                group = line_obj.groupdict()
                try:
                    is_subdataset_metadata = False
                    for sds in metadata['subdatasets'].keys():
                        found_sds_obj = re.search(r'^%s' % sds, group['key'])
                        if found_sds_obj is not None:
                            the_key = re.sub('%s_' % sds, '', group['key'])
                            metadata['subdatasets'][sds][the_key] = group['value']
                            is_subdataset_metadata = True
                    if not is_subdataset_metadata:
                        metadata[group['key']] = group['value']
                except KeyError:
                    metadata[group['key']] = group['value']
            else:
                start_md_line = False
        else:
            if re.search('^\s*Metadata:\s*$', line) is not None:
                start_md_line = True
    _parse_metadata(metadata)
    return metadata

def _extract_subdataset_paths(gdalinfo_stdout):
    subdatasets = dict()
    for line in gdalinfo_stdout.split('\n'):
        line_obj = re.search(r'^\s+SUBDATASET_\d+_NAME=(.*)', line)
        if line_obj is not None:
            ds_path = line_obj.group(1)
            ds_name = re.search(r'//(\w+)', ds_path).group(1)
            subdatasets[ds_name] = ds_path
    return subdatasets

def _extract_band_properties(gdalinfo_stdout):
    bands = dict()
    for line in gdalinfo_stdout.split('\n'):
        line_obj = re.search(r'^Band (\d) Block=(\d+)x(\d+) Type=(\w+),',
                             line)
        if line_obj is not None:
            g = line_obj.group
            bands[g(1)] = {'block_x' : g(2), 'block_y' : g(3), 'dtype' : g(4)}
    return bands

def _extract_size(gdalinfo_stdout):
    size_x = 0
    size_y = 0
    for line in gdalinfo_stdout.split('\n'):
        line_obj = re.search(r'^Size is (\d+), (\d+)', line)
        if line_obj is not None:
            size_x = line_obj.group(1)
            size_y = line_obj.group(2)
    return size_x, size_y

def _parse_metadata(metadata):

    # first deal with simple metadata fields, that just need direct conversion
    # to normal python types
    int_fields = ['NL', 'NC', 'N_COLS', 'N_LINES', 'MISSING_VALUE',
                  'SCALING_FACTOR', 'FIRST_LAT', 'FIRST_LON', 'size_x',
                  'size_y']
    for f in int_fields:
        if metadata.get(f) is not None:
            metadata[f] = int(metadata[f])
        elif metadata.get('subdatasets') is not None:
            for sds, metas in metadata['subdatasets'].iteritems():
                if metas.get(f) is not None:
                    metas[f] = int(metas[f])
        elif metadata.get('bands') is not None:
            for band_num, metas in metadata['bands'].iteritems():
                if metas.get(f) is not None:
                    metas[f] = int(metas[f])
    # now deal with more complex metadata fields
    pixel_size = metadata.get('PIXEL_SIZE')
    if pixel_size is not None:
        ps = float(re.search(r'(\d+\.?\d*)', pixel_size).group())
        metadata['PIXEL_SIZE'] = ps
    acq_time = metadata.get('IMAGE_ACQUISITION_TIME')
    if acq_time is not None:
        metadata['IMAGE_ACQUISITION_TIME'] = dt.datetime.strptime(acq_time,
                                                                  '%Y%m%d%H%M')
