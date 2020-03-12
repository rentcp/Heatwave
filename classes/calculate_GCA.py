import os
import datetime
import pandas
import ephem
import numpy


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


base_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'data'))

data_files = [
    os.path.join(base_dir, 'aqua_positions_%s.csv.zip' % year) for year in range(2002, 2002 + 1)
]

six_minutes = datetime.timedelta(minutes=6)
one_day = datetime.timedelta(days=1)
data_midnight_offset = datetime.timedelta(minutes=8, seconds=26)

for year in range(2002, 2016 + 1):
    print("Processing data for %s..." % year)
    data_file = os.path.join(base_dir, 'aqua_positions_%s.csv.zip' % year)
    data = pandas.read_csv(data_file)
    data = pandas.DataFrame(data)
    data['time'] = pandas.to_datetime(data['year'], format='%Y', utc=True)
    data['time'] += (data['granule'] - 1) * six_minutes
    data['time'] += (data['day'] - 1) * one_day
    data['time'] += data_midnight_offset

    print("Calculating subsolar points...")
    data = data.apply(calculate_subsolar_point, axis=1)
    print("Calculating central angles...")
    data = data.apply(calculate_central_angle, axis=1)
    print("Done with %s!" % year)
    data.to_csv('new/aqua_positions_%s.csv' % year,
                columns=['year', 'day', 'granule', 'lat', 'lon', 'hdf_filename', 'GCA'], index=False)

print("Finished processing.")
