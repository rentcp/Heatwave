from datetime import datetime
import os
import pathlib

from classes.aqua_positions import AquaPositions
from classes.constants import CHANNELS_TO_WAVELENGTHS, COLORS
from classes.granule import Granule
from classes.hdf import HDFFilter, HDFStorage, HDFDataAggregator


class MainController(object):
    def __init__(self, status_callback=None):
        self._status_callback = status_callback

    def signal_status_update(self, message, done=False, data=None):
        if self._status_callback is not None:
            self._status_callback(message, done, data)

    def process(self, data: dict):
        self.signal_status_update('>>> Calculating...')

        granules = self.get_granules(data)
        urls = self.get_urls_for_granules(data, *granules)

        self.download_files(data, urls)

        # comment out the following two lines to re-enable data processing after download:
        # self.signal_status_update('>>> Bypassing granule processing step. See main_controller.py, line 27.')
        # return None

        # granules that map to the downloaded files
        self.signal_status_update('>>> Processing HDF data...')
        all_granules = self.build_granules_for_aggregation(data, urls)

        hdf_filter = self.build_hdf_filter(data)

        curves_data, filter_stats, count_data = self.aggregate_hdf_data(all_granules, hdf_filter)

        self.signal_status_update('>>> Writing output...')

        data_copy = data.copy()
        data_copy['output_directory'] = os.path.join(data['output_directory'], 'stats')
        self.write_output_files(data, curves_data)
        if not hdf_filter.examine_wavenumber_mode:
            self.write_output_files(data_copy, count_data)

        self.signal_status_update('>>> Process finished', done=True)

        return filter_stats

    @staticmethod
    def delete_empty_hdfs(data_directory):
        hdf_file_list = pathlib.Path(data_directory).glob('*.hdf')
        # Filter out files less than 1MB
        hdf_file_list = [f for f in hdf_file_list if os.path.getsize(f) < 1000000]
        if hdf_file_list is not []:
            print('Deleting empty HDFs...')
        for file in hdf_file_list:
            print('Deleting file ' + str(file) + '... (' + str(os.path.getsize(file)) + ' bytes)')
            os.remove(file)

    @staticmethod
    def write_output_files(data, curves_data):
        output_dir = data['output_directory']
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        if not data['examine_wavenumber_mode']:
            base_filename = os.path.join(
                output_dir,
                'radiance_wavelength_month_' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            )
        else:
            date = data['date_range_start']
            year = str(date.year)
            month = date.strftime('%m')
            base_filename = os.path.join(
                output_dir,
                'radiance_wavelength_month_' + year + '-' + month
            )

        # write CSV of data and format dates as MM-YYYY
        curves_data.to_csv(base_filename + '.csv', index=False, date_format='%m-%Y')

        return  # No longer need to show plot

        # if this is at the module level, it breaks multiprocessing
        import matplotlib.pyplot as plt

        # plot
        grouped = curves_data.groupby('period')
        fig, ax = plt.subplots()
        keys = []
        color_code = 0
        for key, group in grouped:
            # format date nicely for plot
            key = key.strftime("%B %Y")

            group.plot(ax=ax, kind='line', x='wavenumber', y='radiance', label=key, color=COLORS[color_code], lw=.5)
            keys.append(key)
            color_code += 1

        title = (
                    'Average Radiances for Similarly Bright Curves at \n'
                    '%s mW/m$^2$/sr/cm$^{-1}$ $\pm$%s mW at %s cm$^{-1}$'
                ) % (data['radiance'], data['radiance_range'], CHANNELS_TO_WAVELENGTHS[data['channel']])

        ax.set(
            xlabel=r'wavelength (cm$^{-1}$)',
            ylabel='Luminosity (mW/m$^2$/sr/cm$^{-1}$)',
            title=title
        )
        ax.grid()

        fig.savefig(base_filename + '.png')

        plt.show(block=True)
        plt.gcf().clear()
        plt.close()

    @staticmethod
    def aggregate_hdf_data(granules, hdf_filter):
        aggregator = HDFDataAggregator()
        return aggregator.process(granules, hdf_filter)

    @staticmethod
    def build_granules_for_aggregation(data, urls):
        result = []
        data_directory = data['data_directory']

        for url in urls:
            # AIRS.2002.08.30.225.L2.CC.v6.0.7.0.G13201091521.hdf
            filename = url.split('/')[-1]
            year, month, day, granule_number = map(int, filename.split('.')[1:5])

            day_of_year = int(datetime.strftime(datetime(year, month, day), '%j'))

            result.append(Granule(year, day_of_year, granule_number, os.path.join(data_directory, filename)))

        return result

    def download_files(self, data, urls):
        data_directory = data['data_directory']
        username = data['username']
        password = data['password']

        if not os.path.isdir(data_directory):
            os.makedirs(data_directory)

        def count_callback(count):
            self.signal_status_update('>>> Downloading %s granules...' % count)

        storage = HDFStorage(data_directory, username, password)

        finished = False
        max_retries = 50
        num_retries = 0
        while not finished:
            success = storage.download_files(urls, count_callback)
            if success:
                finished = True
            elif num_retries == max_retries:
                print("Max retries reached. Some granules failed to download!")
                finished = True
            else:
                num_retries += 1
                print("Retrying failed granule downloads. Retry {} of {}...".format(num_retries, max_retries))
                self.delete_empty_hdfs(data_directory)

    @staticmethod
    def get_granules(data):
        date_range_start = data['date_range_start']  # datetime.date
        date_range_end = data['date_range_end']  # datetime.date
        time_range_start = data['time_range_start']  # datetime.time
        time_range_end = data['time_range_end']  # datetime.time

        start_granule, end_granule = Granule.get_granule_bounds_for_time_range(
            datetime(date_range_start.year, date_range_start.month, date_range_start.day,
                     time_range_start.hour, time_range_start.minute, time_range_start.second),
            datetime(date_range_end.year, date_range_end.month, date_range_end.day,
                     time_range_end.hour, time_range_end.minute, time_range_end.second),
        )

        return start_granule, end_granule

    @staticmethod
    def get_urls_for_granules(data, start_granule, end_granule):
        min_latitude = float(data['min_latitude'])
        max_latitude = float(data['max_latitude'])
        min_longitude = float(data['min_longitude'])
        max_longitude = float(data['max_longitude'])
        min_gca = float(data['minimum_gca'])
        include_prime_meridian = data['include_prime_meridian']
        if 'test_hdf_output' in data:
            test_hdf_output = data['test_hdf_output']
        else:
            test_hdf_output = False

        aqua_positions = AquaPositions()

        return list(aqua_positions.get_hdf_urls(
            start_granule, end_granule, min_latitude, min_longitude, max_latitude, max_longitude,
            include_prime_meridian, min_gca, test_hdf_output))

    @staticmethod
    def build_hdf_filter(data):
        channel = None
        use_radiance_filters = None
        data_quality_best = data['data_quality_best']  # bool
        data_quality_enough = data['data_quality_enough']  # bool
        data_quality_worst = data['data_quality_worst']  # bool
        dust_flag_no_dust = data['dust_flag_no_dust']  # bool
        dust_flag_single_fov = data['dust_flag_single_fov']  # bool
        dust_flag_detected = data['dust_flag_detected']  # bool
        landfrac_threshold = float(data['landfrac_threshold'])
        landfrac_threshold_is_max = data['landfrac_threshold_is_max']  # bool
        cloud_cover_threshold = data['TotCld_4_CCfinal_threshold']
        cloud_cover_threshold_is_max = data['TotCld_4_CCfinal_threshold_is_max']
        all_spots_avg_threshold = data['all_spots_avg_threshold']
        all_spots_avg_threshold_is_max = data['all_spots_avg_threshold_is_max']
        examine_wavenumber_mode = data['examine_wavenumber_mode']
        selected_wavenumber = data['selected_wavenumber']
        scanang = float(data['scanang_limit'])
        inside_scanang = data['inside_scanang']  # bool
        solzen_threshold = float(data['solzen_threshold'])
        solzen_is_max = data['solzen_is_max']  # bool

        noise_amp = data['noise_amp']  # bool
        radiance = None
        radiance_range = None

        return HDFFilter(
            use_radiance_filters, radiance, radiance_range, channel, data_quality_best, data_quality_enough,
            data_quality_worst, landfrac_threshold, landfrac_threshold_is_max, cloud_cover_threshold,
            cloud_cover_threshold_is_max, all_spots_avg_threshold, all_spots_avg_threshold_is_max, noise_amp,
            dust_flag_no_dust, dust_flag_single_fov, dust_flag_detected, examine_wavenumber_mode, selected_wavenumber,
            scanang, inside_scanang, solzen_threshold, solzen_is_max
        )
