import glob
import os
import datetime
import shutil
import zipfile

import pandas
import ephem
import numpy
from tqdm import tqdm

tqdm.pandas()  # Register tqdm instance with Pandas for progress meter on 'progress_apply' func


def calculate_central_angle(row):
    # Great circle central angle formula
    lat_rad = numpy.radians(row['lat'])
    lon_rad = numpy.radians(row['lon'])
    subsolar_lat_rad = numpy.radians(row['subsolar lat'])
    subsolar_lon_rad = numpy.radians(row['subsolar lon'])
    _1 = numpy.sin(lat_rad) * numpy.sin(subsolar_lat_rad)
    _2 = numpy.cos(lat_rad) * numpy.cos(subsolar_lat_rad)
    _3 = _1 + (_2 * numpy.cos(numpy.fabs(lon_rad - subsolar_lon_rad)))
    row['GCA'] = numpy.round(numpy.degrees(numpy.arccos(_3)), 1)
    return row


def calculate_subsolar_point(row):
    # Source: https://stackoverflow.com/questions/17262428/computing-sub-solar-point
    greenwich = ephem.Observer()
    greenwich.lat = 0
    greenwich.lon = 0
    greenwich.date = row['time']
    sun = ephem.Sun(greenwich)
    sun.compute(greenwich.date)
    sun_lon = numpy.degrees(sun.ra - greenwich.sidereal_time())
    if sun_lon < -180.0:
        sun_lon = 360.0 + sun_lon
    elif sun_lon > 180.0:
        sun_lon = sun_lon - 360.0
    sun_lat = numpy.degrees(sun.dec)
    row['subsolar lat'] = sun_lat
    row['subsolar lon'] = sun_lon
    return row


def calculate_gca_for_files_and_zip(temp_directory: str, output_directory):
    csv_glob = os.path.join(temp_directory, '*.csv')
    temp_output_directory = os.path.join(temp_directory, 'new')
    os.makedirs(temp_output_directory)
    data_files = glob.glob(csv_glob)

    six_minutes = datetime.timedelta(minutes=6)
    one_day = datetime.timedelta(days=1)
    data_midnight_offset = datetime.timedelta(minutes=8, seconds=26)

    for data_file in data_files:
        data_file_basename = os.path.basename(data_file)
        print("Processing data for %s..." % data_file_basename)
        data = pandas.read_csv(data_file)
        data = pandas.DataFrame(data)
        data['time'] = pandas.to_datetime(data['year'], format='%Y', utc=True)
        data['time'] += (data['kernel'] - 1) * six_minutes
        data['time'] += (data['day'] - 1) * one_day
        data['time'] += data_midnight_offset

        print("Calculating subsolar points...")
        data = data.progress_apply(calculate_subsolar_point, axis=1)
        print("Calculating central angles...")
        data = data.progress_apply(calculate_central_angle, axis=1)
        print("Done with {}!".format(data_file))
        final_filename = os.path.join(temp_directory, 'new', data_file_basename)
        data = data.rename(columns={'kernel': 'granule'})
        data.to_csv(final_filename,
                    columns=['year', 'day', 'granule', 'lat', 'lon', 'hdf_filename', 'GCA'], index=False)

        print("Finished GCA calculations, zipping CSVs and moving to output directory...")

        final_basename = os.path.basename(final_filename)
        zipped_name = final_basename + '.zip'  # Use .csv.zip extension
        original_directory = os.getcwd()
        os.chdir(os.path.join(temp_directory, 'new'))
        with zipfile.ZipFile(zipped_name, 'w', compression=zipfile.ZIP_DEFLATED) as zipped:
            zipped.write(final_basename)
        os.chdir(original_directory)

    new_dir_files = os.path.join(temp_directory, 'new', '*.csv.zip')
    files_to_move = glob.glob(new_dir_files)
    for file in files_to_move:
        file_basename = os.path.basename(file)
        moved_name = os.path.join(output_directory, file_basename)
        os.rename(file, moved_name)

    print('Remove directory where this program was run?')
    delete = input('Delete {}, y/n: '.format(temp_directory))
    if delete == 'y':
        shutil.rmtree(temp_directory)
    print("Finished processing.")
