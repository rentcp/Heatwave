from datetime import date, time
from getpass import getpass

from classes.interface.main_controller import MainController


def status_callback(message, done, data):
    print(message, '-- data:', data)
    if done:
        print('Process indicates it is done.')


username = input('EarthData Login username: ')
password = getpass()

data = {
    'data_directory': '/tmp/data_tests/',
    'output_directory': '/tmp/output_dir',
    'username': username,
    'password': password,
    'before_date_range_start': date(2002, 9, 1),
    'before_date_range_end': date(2002, 10, 31),
    'after_date_range_start': date(2016, 4, 1),
    'after_date_range_end': date(2016, 5, 31),
    'time_range_start': time(16, 0),
    'time_range_end': time(20, 0),
    'min_latitude': '-60',
    'max_latitude': '60',
    'min_longitude': '-146',
    'max_longitude': '130',
    'center_scans_only': True,
    'channel': 281,  # 730.2 cm-1
    'data_quality_best': True,
    'data_quality_enough': True,
    'data_quality_worst': False,
    'dust_flag_no_dust': True,
    'dust_flag_single_fov': True,
    'dust_flag_detected': False,
    'max_landfrac': '1',
    'noise_amp': False,
    'use_radiance_filters': False,
    'radiance': '80',
    'radiance_range': '50000'
}

controller = MainController(status_callback)
controller.process(data)
