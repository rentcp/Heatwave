import os
from datetime import datetime

import pandas

BASE_URL = 'https://airsl2.gesdisc.eosdis.nasa.gov/data/Aqua_AIRS_Level2/AIRS2CCF.006/'


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


def includes_intl_date_line(min_lon, max_lon, include_prime_meridian) -> bool:
    if min_lon == -180 or max_lon == 180:
        return True
    lon_naively_contains_zero = (min_lon <= 0 <= max_lon)
    if ((lon_naively_contains_zero and include_prime_meridian) or
            (not lon_naively_contains_zero and not include_prime_meridian)):
        return False
    else:
        return True


def calculate_longitude_angle_in_degrees(min_lon, max_lon, include_prime_meridian) -> int:
    if min_lon > max_lon:
        # Swap min and max to simplify math
        _min_lon, max_lon = max_lon, min_lon

    if (min_lon <= 0 <= max_lon) and include_prime_meridian:
        return abs(min_lon) + max_lon

    antimeridian = includes_intl_date_line(min_lon, max_lon, include_prime_meridian)

    if antimeridian and (min_lon <= 0 <= max_lon) or include_prime_meridian:
        # Special meridian logic
        return (180 - max_lon) + abs(-180 - min_lon)
    else:
        # Regular meridian logic
        return abs(max_lon - min_lon)


def expand_longitude_slice_by_degrees(min_lon, max_lon, include_prime_meridian, degrees) \
        -> (int, int, bool, int):
    """
    Returns a tuple of expanded min and max longitude, whether the expanded area includes the prime meridian, and
    the final span of the expanded angle in degrees.
    """
    if min_lon > max_lon:
        # Swap min and max to simplify math
        min_lon, max_lon = max_lon, min_lon
    original_span = calculate_longitude_angle_in_degrees(min_lon, max_lon, include_prime_meridian)

    # Handle trivial case where expanded angle is the whole globe
    if original_span + (degrees * 2) >= 360:
        return -180, 180, True, 360

    expanded_min_lon = normalize_longitude_arithmetic(min_lon - degrees)
    expanded_max_lon = normalize_longitude_arithmetic(max_lon + degrees)

    if expanded_min_lon > expanded_max_lon:
        # Swap min and max to simplify math
        expanded_min_lon, expanded_max_lon = expanded_max_lon, expanded_min_lon

    if not include_prime_meridian:
        # Does this expanded area now include the prime meridian?
        if 0 < min_lon <= degrees:
            include_prime_meridian = True
        elif max_lon < 0 and (degrees + max_lon >= 0):
            include_prime_meridian = True

    expanded_span = calculate_longitude_angle_in_degrees(expanded_min_lon, expanded_max_lon,
                                                         include_prime_meridian)
    if not (expanded_span >= original_span):
        raise ValueError('Expanded span is smaller than original span. {} < {}'.format(expanded_span,
                                                                                       original_span))
    expected_span = original_span + (degrees * 2)
    if expected_span > 360:
        expected_span = 360
    if expected_span != expanded_span and expanded_span < 360:
        raise ValueError(
            'Expanded span is smaller than expected. Original {}, expanded to {}, expected {} (expansion angle {})'
            .format(original_span, expanded_span, expected_span, degrees))

    return expanded_min_lon, expanded_max_lon, include_prime_meridian, expanded_span


def calculate_lat_lon_filter_condition(data, min_lat, max_lat, min_lon, max_lon, include_prime_meridian,
                                       is_search_area):

    # Handle special logic for expanded search area
    if is_search_area:
        search_min_lat = normalize_latitude_arithmetic(min_lat - 10)
        search_max_lat = normalize_latitude_arithmetic(max_lat + 10)
        latitude_condition = (data.lat >= search_min_lat) & (data.lat <= search_max_lat)
    else:
        latitude_condition = (data.lat >= min_lat) & (data.lat <= max_lat)

    # include longitudes within the specified range considering whether or not the prime meridian is included
    antimeridian = includes_intl_date_line(min_lon, max_lon, include_prime_meridian)
    if not antimeridian and not include_prime_meridian:
        longitude_condition = (data.lon >= min_lon) & (data.lon <= max_lon)
    else:
        longitude_condition = (data.lon <= min_lon) | (data.lon >= max_lon)

    # Expand search area longitude further near the poles
    if is_search_area:
        # 1st tier, +/- 10 degrees at absolute latitude < 60
        expanded_min_lon, expanded_max_lon, includes_prime, span = \
            expand_longitude_slice_by_degrees(min_lon, max_lon, include_prime_meridian, 10)
        antimeridian = includes_intl_date_line(min_lon, max_lon, include_prime_meridian)
        if antimeridian and not include_prime_meridian:
            longitude_condition |= (
                ((data.lon <= expanded_min_lon)
                 |
                 (data.lon >= expanded_max_lon))
                &
                ((data.lat > -60) & (data.lat < 60))
            )
        else:
            longitude_condition |= (
                ((data.lon >= expanded_min_lon)
                 &
                 (data.lon <= expanded_max_lon))
                &
                ((data.lat > -60) & (data.lat < 60))
            )
        # 2nd tier, +/- 25 degrees at 60-70 absolute latitude
        expanded_min_lon, expanded_max_lon, includes_prime, span = \
            expand_longitude_slice_by_degrees(min_lon, max_lon, include_prime_meridian, 25)
        antimeridian = includes_intl_date_line(min_lon, max_lon, include_prime_meridian)
        if antimeridian and not include_prime_meridian:
            longitude_condition |= (
                ((data.lon <= expanded_min_lon)
                 |
                 (data.lon >= expanded_max_lon))
                &
                ((data.lat <= -60) | (data.lat >= 60))
            )
        else:
            longitude_condition |= (
                ((data.lon >= expanded_min_lon)
                 &
                 (data.lon <= expanded_max_lon))
                &
                ((data.lat <= -60) | (data.lat >= 60))
            )
        # 3rd tier, +/- 45 degrees at 70-80 absolute latitude
        expanded_min_lon, expanded_max_lon, includes_prime, span = \
            expand_longitude_slice_by_degrees(min_lon, max_lon, include_prime_meridian, 45)
        antimeridian = includes_intl_date_line(min_lon, max_lon, include_prime_meridian)
        if antimeridian and not include_prime_meridian:
            longitude_condition |= (
                ((data.lon <= expanded_min_lon)
                 |
                 (data.lon >= expanded_max_lon))
                &
                ((data.lat <= -70) | (data.lat >= 70))
            )
        else:
            longitude_condition |= (
                ((data.lon >= expanded_min_lon)
                 &
                 (data.lon <= expanded_max_lon))
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
            return data

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
