import os
from datetime import datetime

import pandas

BASE_URL = 'https://airsl2.gesdisc.eosdis.nasa.gov/data/Aqua_AIRS_Level2/AIRS2CCF.006/'


def calculate_lat_lon_filter_condition(data, min_lat, max_lat, min_lon, max_lon, include_prime_meridian,
                                       is_search_area):
    def normalize_latitude_arithmetic(latitude):
        if latitude < -90:
            return -90
        if latitude > 90:
            return 90

        return latitude

    def normalize_longitude_arithmetic(longitude):
        if longitude < -180:
            return longitude % 180
        if longitude > 180:
            return -180 + (longitude % 180)

        return longitude

    # Handle special logic for expanded search area
    if is_search_area:
        search_min_lat = normalize_latitude_arithmetic(min_lat - 10)
        search_max_lat = normalize_latitude_arithmetic(max_lat + 10)
        latitude_condition = (data.lat >= search_min_lat) & (data.lat < search_max_lat)
    else:
        latitude_condition = (data.lat >= min_lat) & (data.lat < max_lat)

    # include longitudes within the specified range considering whether or not the prime meridian is included
    lon_naively_contains_zero = (min_lon <= 0 <= max_lon)
    longitude_condition = (data.lon >= min_lon) & (data.lon < max_lon)

    # special logic for meridian setting
    special_meridian_logic = False
    if not ((lon_naively_contains_zero and include_prime_meridian) or
            (not lon_naively_contains_zero and not include_prime_meridian)):
        # take from the complement of the usual longitude slice
        longitude_condition = (data.lon < min_lon) | (data.lon >= max_lon)
        special_meridian_logic = True

    # Expand search area longitude further near the poles
    if is_search_area:
        # 1st tier, +/- 10 degrees at absolute latitude < 60
        if special_meridian_logic:
            longitude_condition |= (
                ((data.lon < normalize_longitude_arithmetic(min_lon + 10))
                 |
                 (data.lon >= normalize_longitude_arithmetic(max_lon - 10)))
                &
                ((data.lat > -60) & (data.lat < 60))
            )
        else:
            longitude_condition |= (
                ((data.lon >= normalize_longitude_arithmetic(min_lon - 10))
                 &
                 (data.lon < normalize_longitude_arithmetic(max_lon + 10)))
                &
                ((data.lat > -60) & (data.lat < 60))
            )
        # 2nd tier, +/- 25 degrees at 60-70 absolute latitude
        if special_meridian_logic:
            longitude_condition |= (
                ((data.lon < normalize_longitude_arithmetic(min_lon + 25))
                 |
                 (data.lon >= normalize_longitude_arithmetic(max_lon - 25)))
                &
                ((data.lat <= -60) | (data.lat >= 60))
            )
        else:
            longitude_condition |= (
                ((data.lon >= normalize_longitude_arithmetic(min_lon - 25))
                 &
                 (data.lon < normalize_longitude_arithmetic(max_lon + 25)))
                &
                ((data.lat <= -60) | (data.lat >= 60))
            )
        # 3rd tier, +/- 45 degrees at 70-80 absolute latitude
        if special_meridian_logic:
            longitude_condition |= (
                ((data.lon < normalize_longitude_arithmetic(min_lon + 45))
                 |
                 (data.lon >= normalize_longitude_arithmetic(max_lon - 45)))
                &
                ((data.lat <= -70) | (data.lat >= 70))
            )
        else:
            longitude_condition |= (
                ((data.lon >= normalize_longitude_arithmetic(min_lon - 45))
                 &
                 (data.lon < normalize_longitude_arithmetic(max_lon + 45)))
                &
                ((data.lat <= -70) | (data.lat >= 70))
            )
        # 4th tier, all longitudes at absolute latitude > 80
        longitude_condition |= (
            ((data.lat <= -80) | (data.lat >= 80))
        )

    geo_condition = latitude_condition & longitude_condition

    return geo_condition


class AquaPositions(object):

    def get_hdf_urls(self, start_granule, end_granule, min_latitude, min_longitude, max_latitude, max_longitude,
                     include_prime_meridian, min_gca, test_hdf_output):
        min_year, max_year = start_granule.year, end_granule.year
        base_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'data'))

        data_files = [
            os.path.join(base_dir, 'aqua_positions_%s.csv.zip' % year) for year in range(min_year, max_year + 1)
        ]

        data = pandas.concat(
            (pandas.read_csv(filename) for filename in data_files), sort=True
        )

        condition = calculate_lat_lon_filter_condition(data, min_latitude, max_latitude, min_longitude,
                                                       max_longitude, include_prime_meridian,
                                                       is_search_area=True)

        # granule times must be within the specified range - this can be calculated against the granule numbers
        if end_granule.granule_number >= start_granule.granule_number:
            condition &= (
                    (data.granule >= start_granule.granule_number) & (data.granule <= end_granule.granule_number)
            )
        else:
            condition &= (
                ((data.granule >= start_granule.granule_number) | (data.granule <= end_granule.granule_number))
            )

        # check if granule was captured within minimum specified solar GCA
        condition &= (data.GCA >= min_gca)

        # granule dates must be within the specified range
        data['date_value'] = data.year * 1000 + data.day  # same value as in Granule
        condition &= ((data.date_value >= start_granule.date_value) & (data.date_value <= end_granule.date_value))

        data = data[condition]

        if test_hdf_output:
            actual_granule_numbers = [int(filename.split('.')[4]) for filename in data.hdf_filename]
            correct_granule_numbers = [121, 122, 123, 124, 125, 138, 139, 140, 154, 155]
            print(actual_granule_numbers)
            num_correct = 0
            num_extra = 0
            for granule in actual_granule_numbers:
                if granule in correct_granule_numbers:
                    num_correct += 1
                else:
                    num_extra += 1

            if num_correct >= len(correct_granule_numbers) and num_extra == 0:
                print("Correct granules found, code is correct!")
            elif num_correct >= len(correct_granule_numbers) and num_extra > 0:
                print("All correct granules found, but also {} incorrect.".format(num_extra))
            else:
                print("{} correct granules found with {} incorrect granules.".format(num_correct, num_extra))

            return []

        return (self.get_url(filename) for filename in data.hdf_filename)

    @staticmethod
    def get_url(filename):
        # AIRS.2002.08.30.225.L2.CC.v6.0.7.0.G13201091521.hdf
        year, month, day, granule_number = map(int, filename.split('.')[1:5])

        day_of_year = datetime.strftime(datetime(year, month, day), '%j')
        # add leading zeros
        if len(day_of_year) == 2:
            day_of_year = '0' + day_of_year
        elif len(day_of_year) == 1:
            day_of_year = '00' + day_of_year
        return '%s/%s/%s/%s' % (BASE_URL, year, day_of_year, filename)
