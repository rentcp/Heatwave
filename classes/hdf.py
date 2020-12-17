import os
from multiprocessing import Pool

import numpy as np
import pandas as pd
import pyhdf
import requests
from pyhdf.SD import SD
from pyhdf.error import HDF4Error

from classes.constants import CHANNELS_TO_WAVELENGTHS
from ._http import SessionWithHeaderRedirection
from .aqua_positions import calculate_lat_lon_filter_condition


def print_stats(filter_stats):
    if not filter_stats:
        return
    try:
        # print some stats about the filtering
        print("-- FILTER INFO --")
        if filter_stats['total'] == 0:
            print("None of selected data was within the specified lat/lon area.")
            return

        print("- Pre-filter:\t{:,}".format(filter_stats['total']).expandtabs(16))

        print("- Land:\t{0:,}\t({1:.3g}%)".format(filter_stats['land_frac'], filter_stats['land_frac']
                                                  / filter_stats['total'] * 100).expandtabs(16))

        print("- TotCld_4:\t{0:,}\t({1:.3g}%)".format(filter_stats['cloud_cover'], filter_stats['cloud_cover']
                                                      / filter_stats['total'] * 100).expandtabs(16))

        print("- all_spots:\t{0:,}\t({1:.3g}%)".format(filter_stats['all_spots'], filter_stats['all_spots']
                                                       / filter_stats['total'] * 100).expandtabs(16))

        print("- Noise:\t{0:,}\t({1:.3g}%)".format(filter_stats['noise'], filter_stats['noise'] / filter_stats['total']
                                                   * 100).expandtabs(16))

        print("- Scanang:\t{0:,}\t({1:.3g}%)".format(filter_stats['scanang'], filter_stats['scanang']
                                                     / filter_stats['total'] * 100).expandtabs(16))

        print("- Dust flags:\t{0:,}\t({1:.3g}%)".format(filter_stats['dust'], filter_stats['dust']
                                                        / filter_stats['total'] * 100).expandtabs(16))

        print("- Solar zenith:\t{0:,}\t({1:.3g}%)".format(filter_stats['solzen'], filter_stats['solzen']
                                                          / filter_stats['total'] * 100).expandtabs(16))

        print("- Quality:\t{0:,}\t({1:.3g}%)".format(filter_stats['quality'], filter_stats['quality']
                                                     / filter_stats['total'] * 100).expandtabs(16))

        print("- Total after:\t{0:,}\t({1:.3g}% filtered)".format(filter_stats['total']
                                                                  - filter_stats['total_filtered'],
                                                                  filter_stats['total_filtered']
                                                                  / filter_stats['total'] * 100).expandtabs(16))
    except ZeroDivisionError:
        # there was no data to filter
        pass


def perform_download(url, output_dir, username, password):
    with SessionWithHeaderRedirection(username, password) as http_client:
        try:
            response = http_client.get(url, stream=True, timeout=10)
            response.raise_for_status()  # raise an exception in case of http errors

            filename = os.path.join(output_dir, url.split('/')[-1])
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)

        except requests.exceptions.HTTPError:
            raise
        except Exception as e:
            raise Exception('Unhandled exception: %s' % e)


class HDFFilter(object):
    """
    so a 'curve' is the monthly average of all radiances observed, grouped by wavelength.
    the filtering on 'radiance at wavelength' will pick the series that have values within
    the specified range at the specified wavelength.
    """

    def __init__(self, use_radiance_filters, radiance, radiance_range, channel, data_quality_best, data_quality_enough,
                 data_quality_worst, landfrac_threshold, landfrac_threshold_is_max, cloud_cover_threshold,
                 cloud_cover_threshold_is_max, all_spots_avg_threshold, all_spots_avg_threshold_is_max, noise_amp,
                 dust_flag_no_dust, dust_flag_single_fov, dust_flag_detected, examine_wavenumber_mode,
                 selected_wavenumber, scanang, inside_scanang, solzen_threshold, solzen_is_max, min_lat, max_lat,
                 min_lon, max_lon, include_prime_meridian, delete_unreadable):
        self.use_radiance_filters = use_radiance_filters
        self.radiance = radiance
        self.radiance_range = radiance_range
        self.channel = channel
        self.landfrac_threshold = landfrac_threshold
        self.landfrac_threshold_is_max = landfrac_threshold_is_max
        self.cloud_cover_threshold = cloud_cover_threshold
        self.cloud_cover_threshold_is_max = cloud_cover_threshold_is_max
        self.all_spots_avg_threshold = all_spots_avg_threshold
        self.all_spots_avg_threshold_is_max = all_spots_avg_threshold_is_max
        self.noise_amp = noise_amp
        self.dust_flag_no_dust = dust_flag_no_dust
        self.dust_flag_single_fov = dust_flag_single_fov
        self.dust_flag_detected = dust_flag_detected
        self.data_quality_best = data_quality_best
        self.data_quality_enough = data_quality_enough
        self.data_quality_worst = data_quality_worst
        self.examine_wavenumber_mode = examine_wavenumber_mode
        self.selected_wavenumber = selected_wavenumber
        self.scanang = scanang
        self.inside_scanang = inside_scanang
        self.solzen_threshold = solzen_threshold
        self.solzen_is_max = solzen_is_max
        self.min_lat = min_lat
        self.max_lat = max_lat
        self.min_lon = min_lon
        self.max_lon = max_lon
        self.include_prime_meridian = include_prime_meridian
        self.delete_unreadable = delete_unreadable


class HDFStorage(object):
    def __init__(self, storage_path, username, password):
        self._username = username
        self._password = password
        self._storage_directory = storage_path

    def download_files(self, urls, count_callback=None):
        """Downloads all necessary files that are not yet stored on the disk using multiple processes.
        download_finished_callback and error_callback cannot be bound or unbound methods in Windows, so pass
        functions instead.
        """
        urls = list(self.filter_files(urls, self._storage_directory))

        if count_callback is not None:
            count_callback(len(urls))

        process_args = [(url, self._storage_directory, self._username, self._password) for url in urls]

        with Pool(processes=5) as pool:
            pool.starmap_async(perform_download, process_args, callback=lambda x: None, error_callback=lambda x: None)
            pool.close()
            pool.join()

        # check if any files failed to download, and return false if so
        urls = list(self.filter_files(urls, self._storage_directory))
        return len(urls) == 0

    @staticmethod
    def filter_files(urls, directory):
        requested = set(map(lambda x: x.split('/')[-1], urls))
        existing = set([f for f in os.listdir(directory) if f.endswith('.hdf')])
        missing = requested.difference(existing)
        return (url for url in urls if any(map(lambda x: x in url, missing)))


class HDFDataAggregator(object):
    def process(self, granules, hdf_filter):
        process_args = [(granule, hdf_filter) for granule in granules]

        with Pool(processes=10) as pool:
            async_results = pool.starmap_async(extract_granule_dataset, process_args)
            pool.close()
            pool.join()

        # (granule, radiances_sum, radiances_count, radiances, filter_stats)
        results = async_results.get()

        # curve_data, filter_stats, count_data, wavenumber_details
        return calculate_averages_and_filter(results, hdf_filter)


def calculate_averages_and_filter(results, hdf_filter):
    columns = ['period', 'wavenumber']
    if results:
        for bucket in results[0][2].keys():
            columns.append(bucket + '_sum')
            columns.append(bucket + '_count')
    wavelengths = list(CHANNELS_TO_WAVELENGTHS.values())

    data = []
    wavenumber_data = None
    if hdf_filter.examine_wavenumber_mode and results:
        wavenumber_data = pd.DataFrame(columns=['timestamp', 'lat', 'lon', 'radiances'])
        data = {}
        data = data.fromkeys(results[0][2], [])

    filter_stats = {'total': 0, 'total_filtered': 0, 'land_frac': 0, 'cloud_cover': 0, 'all_spots': 0, 'noise': 0,
                    'dust': 0, 'quality': 0, 'scanang': 0, 'solzen': 0}

    most_cloud_free_granule = (None, 1)

    for result in results:
        if result[1] is not None:
            if not hdf_filter.examine_wavenumber_mode:
                granule, radiances_by_latitude_sum, radiances_by_latitude_count, radiances_by_latitude, stats, \
                cloud_info, wavenumber_details = result

                if cloud_info[1] < most_cloud_free_granule[1]:
                    most_cloud_free_granule = cloud_info

                periods = [granule.month_period] * len(wavelengths)
                data += list(zip(periods, wavelengths,
                                 radiances_by_latitude_sum['-90to-80'].tolist(),
                                 radiances_by_latitude_count['-90to-80'].tolist(),
                                 radiances_by_latitude_sum['-80to-70'].tolist(),
                                 radiances_by_latitude_count['-80to-70'].tolist(),
                                 radiances_by_latitude_sum['-70to-60'].tolist(),
                                 radiances_by_latitude_count['-70to-60'].tolist(),
                                 radiances_by_latitude_sum['-60to-50'].tolist(),
                                 radiances_by_latitude_count['-60to-50'].tolist(),
                                 radiances_by_latitude_sum['-50to-40'].tolist(),
                                 radiances_by_latitude_count['-50to-40'].tolist(),
                                 radiances_by_latitude_sum['-40to-30'].tolist(),
                                 radiances_by_latitude_count['-40to-30'].tolist(),
                                 radiances_by_latitude_sum['-30to-20'].tolist(),
                                 radiances_by_latitude_count['-30to-20'].tolist(),
                                 radiances_by_latitude_sum['-20to-10'].tolist(),
                                 radiances_by_latitude_count['-20to-10'].tolist(),
                                 radiances_by_latitude_sum['-10to0'].tolist(),
                                 radiances_by_latitude_count['-10to0'].tolist(),
                                 radiances_by_latitude_sum['0to10'].tolist(),
                                 radiances_by_latitude_count['0to10'].tolist(),
                                 radiances_by_latitude_sum['10to20'].tolist(),
                                 radiances_by_latitude_count['10to20'].tolist(),
                                 radiances_by_latitude_sum['20to30'].tolist(),
                                 radiances_by_latitude_count['20to30'].tolist(),
                                 radiances_by_latitude_sum['30to40'].tolist(),
                                 radiances_by_latitude_count['30to40'].tolist(),
                                 radiances_by_latitude_sum['40to50'].tolist(),
                                 radiances_by_latitude_count['40to50'].tolist(),
                                 radiances_by_latitude_sum['50to60'].tolist(),
                                 radiances_by_latitude_count['50to60'].tolist(),
                                 radiances_by_latitude_sum['60to70'].tolist(),
                                 radiances_by_latitude_count['60to70'].tolist(),
                                 radiances_by_latitude_sum['70to80'].tolist(),
                                 radiances_by_latitude_count['70to80'].tolist(),
                                 radiances_by_latitude_sum['80to90'].tolist(),
                                 radiances_by_latitude_count['80to90'].tolist(),
                                 ))
            else:
                granule, radiances_by_latitude_sum, radiances_by_latitude_count, radiances_by_latitude, stats, \
                cloud_info, wavenumber_details = result

                if cloud_info[1] < most_cloud_free_granule[1]:
                    most_cloud_free_granule = cloud_info

                try:
                    if wavenumber_details is not None:
                        wavenumber_data = pd.concat([wavenumber_data, wavenumber_details])

                except Exception as e:
                    print('Error concatenating wavenumber details data. Operands:')
                    print(wavenumber_data.head())
                    print(wavenumber_details.head())

                for k, v in radiances_by_latitude.items():
                    if not data[k]:
                        data[k] = v
                    else:
                        data[k] += v

            num_data_points, num_filtered_total, num_filtered_landfrac, num_filtered_cloud_cover, \
            num_filtered_all_spots, num_filtered_noise_amp, num_filtered_dust, num_filtered_quality, \
            num_filtered_scanang, num_filtered_solzen = stats

            # statistics for filter percentage readout
            filter_stats['total'] += num_data_points
            filter_stats['total_filtered'] += num_filtered_total
            filter_stats['land_frac'] += num_filtered_landfrac
            filter_stats['cloud_cover'] += num_filtered_cloud_cover
            filter_stats['all_spots'] += num_filtered_all_spots
            filter_stats['noise'] += num_filtered_noise_amp
            filter_stats['dust'] += num_filtered_dust
            filter_stats['quality'] += num_filtered_quality
            filter_stats['scanang'] += num_filtered_scanang
            filter_stats['solzen'] += num_filtered_solzen

    print_stats(filter_stats)

    if most_cloud_free_granule[0] is None:
        print('\nNo granule was the most cloud-free.')
    else:
        print('\nMost cloud-free granule: {0} ({1:.3g}% cloud-free)'.format(most_cloud_free_granule[0],
                                                                            (1 - most_cloud_free_granule[1]) * 100))

    if hdf_filter.examine_wavenumber_mode:
        curve_data = pd.DataFrame.from_dict(data, orient='index').T
    else:
        curve_data = pd.DataFrame(data, columns=columns)

    if hdf_filter.examine_wavenumber_mode:
        # curves_data, filter_stats, count_data, wavenumber_details
        return curve_data, None, None, wavenumber_data

    # convert 'period' column to datetime for sorting
    curve_data['period'] = pd.to_datetime(curve_data.period)

    # sort rows by date
    curve_data.sort_values(by=['period'])

    if curve_data.empty:
        print("No data to filter!")
        # curves_data, filter_stats, count_data, wavenumber_details
        return curve_data, None, None, None

    curve_data = curve_data.groupby(['period', 'wavenumber']).sum().reset_index()
    count_data = curve_data.copy()
    drop_columns = []
    count_drop_columns = []
    if results:
        for bucket in results[0][2].keys():
            sum_bucket = bucket + '_sum'
            count_bucket = bucket + '_count'
            drop_columns.append(sum_bucket)
            drop_columns.append(count_bucket)
            count_drop_columns.append(sum_bucket)
            curve_data[bucket] = curve_data[sum_bucket] / curve_data[count_bucket]

    # drop unnecessary columns
    curve_data.drop(columns=drop_columns,
                    inplace=True)
    count_data.drop(columns=count_drop_columns,
                    inplace=True)

    # curves_data, filter_stats, count_data, wavenumber_details
    return curve_data, filter_stats, count_data, None


def extract_granule_dataset(granule, hdf_filter: HDFFilter):
    try:
        data = SD(granule.local_file_name)
    except (HDF4Error, ValueError):
        print("WARNING: Granule could not be read: " + granule.local_file_name)
        if hdf_filter.delete_unreadable:
            try:
                os.remove(granule.local_file_name)
                print('Deleted unreadable granule.')
            except Exception as e:
                print(e)
                print('Could not delete unreadable granule.')
        else:
            print(
                'Enable "delete_unreadable_granules" flag to delete and re-download automatically in a subsequent run.'
            )
        return granule, None, None, None

    # relevant datasets are dust_flag (2D), landFrac (2D), CCfinal_Noise_Amp (2D), radiances_QC(3D), radiances (3D)
    try:
        dust_flag = pd.DataFrame(data.select('dust_flag').get())
        land_frac = pd.DataFrame(data.select('landFrac').get())
        cloud_cover = pd.DataFrame(data.select('TotCld_4_CCfinal').get())
        all_spots = pd.DataFrame(data.select('all_spots_avg').get())
        final_noise_amp = pd.DataFrame(data.select('CCfinal_Noise_Amp').get())
        latitude = pd.DataFrame(data.select('Latitude').get())
        longitude = pd.DataFrame(data.select('Longitude').get())
        scanang = pd.DataFrame(data.select('scanang').get())
        solzen = pd.DataFrame(data.select('solzen').get())
        timestamp = pd.DataFrame(data.select('Time').get())
    except pyhdf.error.HDF4Error:
        print("A dataset is missing in granule: {}".format(granule.local_file_name))
        return granule, None, None, None

    multi_index = pd.MultiIndex.from_product([np.arange(45), np.arange(30)])
    try:
        radiances = pd.DataFrame(data.select('radiances').get().reshape(1350, 2378), index=multi_index)
    except ValueError:
        print("WARNING: could not reshape data: " + granule.local_file_name)
        return granule, None, None, None
    radiances.rename(columns={x: 'radiance_channel_%s' % x for x in range(2378)}, inplace=True)

    radiances_qc = pd.DataFrame(data.select('radiances_QC').get().reshape(1350, 2378), index=multi_index)

    # use same columns as radiances for proper filtering
    radiances_qc.rename(columns={x: 'radiance_channel_%s' % x for x in range(2378)}, inplace=True)

    datasets = {
        'dust_flag': dust_flag,
        'land_frac': land_frac,
        'final_noise_amp': final_noise_amp,
        'cloud_cover': cloud_cover,
        'all_spots': all_spots,
        'latitude': latitude,
        'longitude': longitude,
        'scanang': scanang,
        'solzen': solzen,
        'timestamp': timestamp
    }
    dataset = pd.concat([df.stack() for df in datasets.values()], axis=1, keys=datasets.keys())

    granule_cloud_cover_avg = cloud_cover.mean().mean()
    cloud_info = (granule.local_file_name, granule_cloud_cover_avg)

    radiances_by_latitude_sum, radiances_by_latitude_count, selected_radiances, stats, wavenumber_details = \
        filter_dataset(
            dataset,
            radiances,
            radiances_qc,
            hdf_filter)

    return granule, radiances_by_latitude_sum, radiances_by_latitude_count, selected_radiances, stats, cloud_info, wavenumber_details


def filter_dataset(df: pd.DataFrame, radiances: pd.DataFrame, radiances_quality: pd.DataFrame, hdf_filter: HDFFilter):
    # Pre-filter data to only include data points within lat/lon specification
    df = df.rename(columns={'latitude': 'lat', 'longitude': 'lon'})
    prefilter_geo_condition = calculate_lat_lon_filter_condition(df, hdf_filter.min_lat, hdf_filter.max_lat,
                                                                 hdf_filter.min_lon, hdf_filter.max_lon,
                                                                 hdf_filter.include_prime_meridian,
                                                                 is_search_area=False)
    radiances = radiances[prefilter_geo_condition]
    df = df[prefilter_geo_condition]
    radiances_quality = radiances_quality[prefilter_geo_condition]

    # start counting amount of data points removed by filters
    num_data_points = radiances.count().sum()
    num_filtered_total = 0
    selected_channel = -1
    if hdf_filter.examine_wavenumber_mode:
        wavelengths_to_channels = {v: k for k, v in CHANNELS_TO_WAVELENGTHS.items()}
        try:
            selected_channel = int(wavelengths_to_channels[str(hdf_filter.selected_wavenumber)])
        except KeyError:
            print("ERROR: Invalid wavenumber selected: " + str(hdf_filter.selected_wavenumber))

    radiances_by_latitude_mask = {
        '-90to-80': (df.lat >= -90) & (df.lat < -80),
        '-80to-70': (df.lat >= -80) & (df.lat < -70),
        '-70to-60': (df.lat >= -70) & (df.lat < -60),
        '-60to-50': (df.lat >= -60) & (df.lat < -50),
        '-50to-40': (df.lat >= -50) & (df.lat < -40),
        '-40to-30': (df.lat >= -40) & (df.lat < -30),
        '-30to-20': (df.lat >= -30) & (df.lat < -20),
        '-20to-10': (df.lat >= -20) & (df.lat < -10),
        '-10to0': (df.lat >= -10) & (df.lat < 0),
        '0to10': (df.lat >= 0) & (df.lat <= 10),
        '10to20': (df.lat > 10) & (df.lat <= 20),
        '20to30': (df.lat > 20) & (df.lat <= 30),
        '30to40': (df.lat > 30) & (df.lat <= 40),
        '40to50': (df.lat > 40) & (df.lat <= 50),
        '50to60': (df.lat > 50) & (df.lat <= 60),
        '60to70': (df.lat > 60) & (df.lat <= 70),
        '70to80': (df.lat > 70) & (df.lat <= 80),
        '80to90': (df.lat > 80) & (df.lat <= 90)
    }

    # max landFrac
    if hdf_filter.landfrac_threshold_is_max:
        condition = df.land_frac <= hdf_filter.landfrac_threshold
    else:
        condition = df.land_frac >= hdf_filter.landfrac_threshold

    num_filtered_landfrac = num_data_points - radiances[condition].count().sum() - num_filtered_total
    num_filtered_total += num_filtered_landfrac

    # max cloud cover
    if hdf_filter.cloud_cover_threshold_is_max:
        condition &= df.cloud_cover <= hdf_filter.cloud_cover_threshold
    else:
        condition &= df.cloud_cover >= hdf_filter.cloud_cover_threshold

    num_filtered_cloud_cover = num_data_points - radiances[condition].count().sum() - num_filtered_total
    num_filtered_total += num_filtered_cloud_cover

    # max all_spots_avg
    if hdf_filter.all_spots_avg_threshold_is_max:
        condition &= df.all_spots <= hdf_filter.all_spots_avg_threshold
    else:
        condition &= df.all_spots >= hdf_filter.all_spots_avg_threshold

    num_filtered_all_spots = num_data_points - radiances[condition].count().sum() - num_filtered_total
    num_filtered_total += num_filtered_all_spots

    # consider noise amplification
    num_filtered_noise_amp = 0
    if hdf_filter.noise_amp:
        condition &= (
                (df.final_noise_amp > 0.3333) & (df.final_noise_amp < 0.3334)
        )
        num_filtered_noise_amp = num_data_points - radiances[condition].count().sum() - num_filtered_total
        num_filtered_total += num_filtered_noise_amp

    # scanang
    inverse_scanang = hdf_filter.scanang * -1
    if hdf_filter.inside_scanang:
        condition &= (
                (df.scanang > inverse_scanang) & (df.scanang < hdf_filter.scanang)
        )
    else:
        condition &= (
                (df.scanang <= inverse_scanang) & (df.scanang >= hdf_filter.scanang)
        )
    num_filtered_scanang = num_data_points - radiances[condition].count().sum() - num_filtered_total
    num_filtered_total += num_filtered_scanang

    # dust filters/flags
    dust_filters = [hdf_filter.dust_flag_no_dust, hdf_filter.dust_flag_single_fov, hdf_filter.dust_flag_detected]
    dust_values = [index - 1 for index, selected in enumerate(dust_filters) if selected]
    condition &= (df.dust_flag.isin(dust_values))
    num_filtered_dust = num_data_points - radiances[condition].count().sum() - num_filtered_total
    num_filtered_total += num_filtered_dust

    # solar zenith
    if hdf_filter.solzen_is_max:
        condition &= (df.solzen <= hdf_filter.solzen_threshold)
    else:
        condition &= (df.solzen >= hdf_filter.solzen_threshold)
    num_filtered_solzen = num_data_points - radiances[condition].count().sum() - num_filtered_total
    num_filtered_total += num_filtered_solzen

    filtered_wavenumber_data = None

    if hdf_filter.examine_wavenumber_mode:
        # Create dataset for examine wavenumber mode: Timestamp, Lat, Lon, Radiance of each value that passed filters
        filtered_wavenumber_data = df[condition]
        filtered_wavenumber_data = filtered_wavenumber_data[['timestamp', 'lat', 'lon']]

        # Convert 'seconds since 01-01-1993' timestamp to Unix timestamp
        filtered_wavenumber_data['timestamp'] = filtered_wavenumber_data['timestamp'].astype(int) + 725846400

        filtered_wavenumber_data['timestamp'] = pd.to_datetime(filtered_wavenumber_data.timestamp, unit='s')
        filtered_wavenumber_data['radiances'] = list(radiances[condition].iloc[:, selected_channel - 1])
        if len(filtered_wavenumber_data) == 0:
            filtered_wavenumber_data = None

    for bucket, data in radiances_by_latitude_mask.items():
        radiances_by_latitude_mask[bucket] &= condition

    radiances_by_latitude = dict.fromkeys(radiances_by_latitude_mask)
    selected_radiances_by_latitude = radiances_by_latitude.copy()
    for bucket, data in radiances_by_latitude.items():
        lat_condition = radiances_by_latitude_mask[bucket]
        radiances_by_latitude[bucket] = radiances[lat_condition]

    radiances_quality_by_latitude = dict.fromkeys(radiances_by_latitude)
    for bucket, data in radiances_quality_by_latitude.items():
        lat_condition = radiances_by_latitude_mask[bucket]
        radiances_quality_by_latitude[bucket] = radiances_quality[lat_condition]

    radiance_qc_filters = [hdf_filter.data_quality_best, hdf_filter.data_quality_enough, hdf_filter.data_quality_worst]
    radiance_qc_values = [index for index, selected in enumerate(radiance_qc_filters) if selected]
    radiances_quality_mask_by_latitude = dict.fromkeys(radiances_by_latitude)
    for bucket, data in radiances_quality_mask_by_latitude.items():
        radiances_quality_mask_by_latitude[bucket] = radiances_quality_by_latitude[bucket].isin(radiance_qc_values)

    num_filtered_quality = 0
    for bucket, radiances_ in radiances_by_latitude.items():
        radiances_mask_ = radiances_quality_mask_by_latitude[bucket]
        num_filtered_quality += radiances_.count().sum() - radiances_[radiances_mask_].count().sum()

    num_filtered_total += num_filtered_quality

    filter_stats = (num_data_points, num_filtered_total, num_filtered_landfrac, num_filtered_cloud_cover,
                    num_filtered_all_spots, num_filtered_noise_amp, num_filtered_dust, num_filtered_quality,
                    num_filtered_scanang, num_filtered_solzen)

    # sum radiance values only where the quality matches the selected ones
    for bucket, radiances_ in radiances_by_latitude.items():
        radiances_mask_ = radiances_quality_mask_by_latitude[bucket]
        radiances_by_latitude[bucket] = radiances_[radiances_mask_]

    if hdf_filter.examine_wavenumber_mode:
        for bucket, radiances_ in radiances_by_latitude.items():
            selected_radiances_by_latitude[bucket] = list(radiances_.iloc[:, selected_channel - 1])

    # bin radiances by latitude
    radiances_by_latitude_sum = dict.fromkeys(radiances_by_latitude)
    radiances_by_latitude_count = dict.fromkeys(radiances_by_latitude)
    for bucket, radiances_ in radiances_by_latitude.items():
        radiances_by_latitude_sum[bucket] = radiances_.sum()
        radiances_by_latitude_count[bucket] = radiances_.count()

    # radiances_by_latitude_count = dict.fromkeys(radiances_by_latitude)
    for bucket, radiances_mask_ in radiances_quality_mask_by_latitude.items():
        pass
        # radiances_by_latitude_count[bucket] = radiances_mask_.sum()

    # return the sum of all applicable radiances and their count per channel
    return radiances_by_latitude_sum, radiances_by_latitude_count, selected_radiances_by_latitude, filter_stats, filtered_wavenumber_data
