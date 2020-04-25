"""CLI interface to process the HDF files. Takes the parameters from a file-path argument passed to the
script's initialization."""

import os
import sys


def main():
    if len(sys.argv) != 2:
        print('''
    Pass in the path of a JSON file with the necessary parameters for processing the data:
        python cli.py <path to params file>.json
    
    The possible/necessary parameters are listed in the example JSON file contents below (REMEMBER TO REMOVE
    THE COMMENTS):
    
        {
            "data_directory": "C:\\TEMP\\DATA",  # where HDF files will be stored
            "output_directory": "C:\\TEMP\\OUTPUT",  # where the CSV and PNG output files will be stored
            
            "date_range_start": "12/31/2002",  # must follow mm/dd/yyyy
            "date_range_end": "12/31/2002",  # must follow mm/dd/yyyy
            
            "time_range_start": "16:00",  # format must be hh:mm
            "time_range_end": "20:00",  # format must be hh:mm
            
            "min_latitude": -60, (Latitude lower limit is INCLUSIVE, upper limit is EXCLUSIVE. min <= lat < max)
            "max_latitude": 60,
            
            ## A NOTE ABOUT LONGITUDE LIMITS ##
            In most cases, longitude lower limit is INCLUSIVE, upper limit EXCLUSIVE. min <= lon < max. However, in the
            case that the longitude slice is defined by 'all longitude lower than min and greater than max', these
            limits are inverted, e.g. (lon < min) || (lon >= max). This way, inverting include_prime_meridian will 
            always select a perfectly complementary slice of the globe.
            
            "min_longitude": -146,
            "max_longitude": 130,
            "include_prime_meridian": true,  # whether the desired longitude section includes the prime meridian
            
            "data_quality_best": true,  # whether to use each type of data quality
            "data_quality_enough": true,
            "data_quality_worst": false,
            
            "dust_flag_no_dust": true,  # set the dust filters that should be considered/ignored
            "dust_flag_single_fov": true,
            "dust_flag_detected": false,
            
            "landfrac_threshold": 1,  # value between 0 and 1 (Threshold is INCLUSIVE regardless of max or min setting)
            "landfrac_threshold_is_max": true, # whether the landfrac threshold is a minimum (false) or maximum (true)
            "noise_amp": false,  # use noise amplification?
            
            "TotCld_4_CCfinal_threshold": 0,  # Cloud cover threshold, between 0 and 1 (Threshold is INCLUSIVE regardless of max or min setting)
            "TotCld_4_CCfinal_threshold_is_max": true,  # whether the above threshold is a minimum (false) or maximum (true)
            "all_spots_avg_threshold": 0,  # Alternate cloud cover threshold, between 0 and 1 (Threshold is INCLUSIVE regardless of max or min setting)
            "all_spots_avg_threshold_is_max": true,  # whether the above threshold is a minimum (false) or maximum (true)
            
            "minimum_gca": 139.4,  # minimum central angle between AQUA AIRS and Earth's subsolar point, in degrees. (Threshold is INCLUSIVE)
            "solzen_threshold": 180,  # value between 0 and 180, min or max solar zenith (Threshold is INCLUSIVE)
            "solzen_is_max": true,  # whether the solzen threshold is a minimum (false) or maximum (true)
            
            "show_plot": false,  # whether or not to render a plot at the end of processing
            "num_batches": 0  # number of batches to break up processing into. 0 is interpreted as "by year".
            
            "examine_wavenumber_mode": false,  # whether to activate mode for examining the components of an average
            "selected_wavenumber": 649.6,  # wavelength to examine
            
            "scanang_limit": 30,  # max inside/outside scan angle (Threshold is EXCLUSIVE if 'inside', INCLUSIVE otherwise)
            "inside_scanang": true  # whether or not scans must be inside or outside the above angle
            "delete_unreadable_granules": true  # If true, delete granules that are unreadable so they can be re-downloaded.
            
        }         
            
        ''')
    else:
        from datetime import time, datetime, timedelta, date
        from getpass import getpass
        import json

        from classes.constants import CHANNELS_TO_WAVELENGTHS
        from classes.interface.main_controller import MainController

        def parse_channel(wavelength):
            for channel, wave in CHANNELS_TO_WAVELENGTHS.items():
                if wave == wavelength:
                    return channel

            raise ValueError('Wavelength %s could not be converted to an AIRS channel.' % wavelength)

        def status_callback(message, done, _):
            print(message)
            if done:
                print('Process indicates it is done.')

        with open(sys.argv[1], 'r') as f:
            global_data = json.load(f)

        _username = input('EarthData Login username: ')
        _password = getpass()

        def main_single_file_loop(data, global_username, global_pass, input_file_name):
            if 'test_hdf_output' not in data or not data['test_hdf_output']:
                username = global_username
                password = global_pass
            else:
                username = 'test'
                password = 'test'

            data['username'] = username
            data['password'] = password

            data['date_range_start'] = datetime.strptime(data['date_range_start'], '%m/%d/%Y').date()
            data['date_range_end'] = datetime.strptime(data['date_range_end'], '%m/%d/%Y').date()
            data['time_range_start'] = time(*list(map(int, data['time_range_start'].split(':'))))
            data['time_range_end'] = time(*list(map(int, data['time_range_end'].split(':'))))

            if data.get('wavelength') is not None:
                data['channel'] = parse_channel(data['wavelength'])
                del data['wavelength']

            data_list = []

            if data['examine_wavenumber_mode']:
                temp_folder_name = 'wv_' + str(data['selected_wavenumber']) + '_info_' \
                                   + datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            else:
                temp_folder_name = 'temp_' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

            num_total_days: int = (data['date_range_end'] - data['date_range_start']).days + 1

            if data['num_batches'] == 0:  # split data by months
                end_year = data['date_range_end'].year
                end_month = data['date_range_end'].month
                start_year = data['date_range_start'].year
                start_month = data['date_range_start'].month
                num_years = end_year - start_year + 1
                num_batches = 0
                if num_years == 1:
                    num_batches = end_month - start_month + 1
                else:
                    num_months_first_year = 12 - start_month + 1
                    num_months_final_year = end_month
                    num_inner_months = 0
                    if num_years > 2:
                        num_inner_months = (num_years - 2) * 12

                    num_batches = num_months_first_year + num_months_final_year + num_inner_months

                for i in range(num_batches):
                    batch = data.copy()
                    batch['output_directory'] = os.path.join(data['output_directory'], temp_folder_name)

                    current_month = (start_month + i) % 12
                    if current_month == 0:
                        current_month = 12
                    years_finished = (start_month - 1 + i) // 12
                    current_year = start_year + years_finished
                    next_month = (current_month + 1) % 12
                    if next_month == 0:
                        next_month = 12

                    if i != 0:  # Leave the start date intact for the first batch
                        # This batch will start on the first day of the i'th month
                        batch['date_range_start'] = date(current_year, current_month, 1)

                    if i is not (num_batches - 1):  # Leave the end date intact for the last batch
                        # This batch will end on the last day of the i'th month
                        if next_month < current_month:
                            current_year += 1
                        batch['date_range_end'] = date(current_year, next_month, 1) - timedelta(days=1)

                    data_list.append(batch)

            else:
                num_batches: int = data['num_batches']

                days_per_batch: int = num_total_days // num_batches
                if days_per_batch == 0:
                    days_per_batch = 1
                days_accounted_for: int = 0

                while days_accounted_for < num_total_days:
                    batch = data.copy()
                    batch['output_directory'] = data['output_directory'] + temp_folder_name

                    batch['date_range_start'] = data['date_range_start'] + timedelta(days=days_accounted_for)
                    days_to_process: int = (num_total_days - days_accounted_for) % days_per_batch
                    if days_to_process == 0:  # "one batch" and other edge cases
                        days_to_process = days_per_batch
                    batch['date_range_end'] = batch['date_range_start'] + timedelta(days=days_to_process - 1)
                    days_accounted_for += days_to_process

                    data_list.append(batch)

            controller = MainController(status_callback)

            filter_stats = {}

            days_processed: int = 0

            time_started = datetime.now()

            for data_item in data_list:
                min_lon = data_item['min_longitude']
                max_lon = data_item['max_longitude']
                if min_lon > max_lon:
                    print('Error: Min lon is greater than max lon. Switch min and max values.')
                    exit(0)

                if min_lon == 0 or max_lon == 0:
                    from classes.aqua_positions import calculate_longitude_angle_in_degrees
                    western_min_lon = min_lon + 0.001
                    eastern_min_lon = min_lon - 0.001
                    western_max_lon = max_lon + 0.001
                    eastern_max_lon = max_lon - 0.001
                    eastern_angle_pm = calculate_longitude_angle_in_degrees(eastern_min_lon, eastern_max_lon, True)
                    western_angle_pm = calculate_longitude_angle_in_degrees(western_min_lon, western_max_lon, True)
                    eastern_angle_no_pm = calculate_longitude_angle_in_degrees(eastern_min_lon, eastern_max_lon, False)
                    western_angle_no_pm = calculate_longitude_angle_in_degrees(western_min_lon, western_max_lon, False)

                    print(
                        '\nError: Hemisphere selection is ambiguous. Max or min longitude CANNOT be zero. '
                        '\n'
                        '\nUse 0.001 or -0.001 instead.'
                        '\n'
                        '\nDouble check that you use the correct include_prime_meridian setting after making this change.'
                        '\n'
                        '\n 0.001 and include_prime_meridian = false: {} degree slice'
                        '\n-0.001 and include_prime_meridian = false: {} degree slice'
                        '\n 0.001 and include_prime_meridian = true:  {} degree slice'
                        '\n-0.001 and include_prime_meridian = true:  {} degree slice'
                            .format(western_angle_no_pm, eastern_angle_no_pm, western_angle_pm, eastern_angle_pm)
                    )
                    exit(0)

                time_batch_started = datetime.now()
                print("Processing data for dates: {} through {}".format(
                    (data_item['date_range_start']), data_item['date_range_end']))
                controller.delete_empty_hdfs(data_item['data_directory'])
                data_stats = controller.process(data_item)

                if 'test_hdf_output' in data_item and data_item['test_hdf_output']:
                    return data_stats

                #  Collect and sum stats from output
                if filter_stats:
                    filter_stats = \
                        {k: filter_stats.get(k, 0) + data_stats.get(k, 0) for k in set(filter_stats) | set(data_stats)}
                    days_processed += (data_item['date_range_end'] - data_item['date_range_start']).days + 1
                    print("\nProcessed {0:,} of {1:,} days ({2:.3g}%)".format(
                        days_processed, num_total_days, days_processed / num_total_days * 100))
                elapsed_seconds = (datetime.now() - time_batch_started).total_seconds()
                elapsed_minutes = int(elapsed_seconds // 60) % 60
                elapsed_hours = int(elapsed_seconds // 3600)
                batch_remainder_seconds = int(elapsed_seconds % 60)
                print('Batch completed in {}H {}m {}s'.format(elapsed_hours, elapsed_minutes, batch_remainder_seconds))

            from classes.hdf import print_stats
            print("-- FINAL STATS --")
            print_stats(filter_stats)

            # Finally, concatenate all CSVs and remove the temp folder

            if not data['examine_wavenumber_mode'] and ('test_hdf_output' not in data or not data['test_hdf_output']):
                from pandas import concat, read_csv

                path = os.path.join(data['output_directory'], temp_folder_name)
                stats_path = os.path.join(path, 'stats')
                filenames = os.listdir(path)
                stats_filenames = os.listdir(stats_path)
                filenames = [os.path.join(path, f) for f in filenames]
                stats_filenames = [os.path.join(stats_path, f) for f in stats_filenames]
                filenames = [f for f in filenames if f.endswith('.csv')]
                stats_filenames = [f for f in stats_filenames if f.endswith('.csv')]

                if len(filenames) > 1:
                    combined_csv = concat([read_csv(f) for f in filenames])
                else:
                    combined_csv = read_csv(filenames[0])

                if len(stats_filenames) > 1:
                    combined_stats_csv = concat([read_csv(f) for f in stats_filenames])
                else:
                    combined_stats_csv = read_csv(stats_filenames[0])

                combined_csv = combined_csv.sort_values(by=['period', 'wavenumber'])
                combined_stats_csv = combined_stats_csv.sort_values(by=['period', 'wavenumber'])
                combined_stats_csv = combined_stats_csv.round(decimals=3)
                # round to 2 decimal places
                combined_csv = combined_csv.round(decimals=3)

                base_filename = os.path.join(
                    data['output_directory'],
                    input_file_name.split('.')[0] + '-' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
                )

                combined_csv.to_csv(base_filename + '_concatenated' + '.csv', index=False, date_format='%Y-%m')
                combined_stats_csv.to_csv(base_filename + '_stats' + '.csv', index=False, date_format='%Y-%m')

                # Delete the temp folder and all its contents
                import shutil
                try:
                    shutil.rmtree(os.path.join(data['output_directory'], temp_folder_name))
                except Exception as e:
                    print("Unable to delete temporary folder. Caught {}".format(e))

            total_elapsed_seconds = (datetime.now() - time_started).total_seconds()
            total_elapsed_minutes = int(total_elapsed_seconds // 60) % 60
            total_elapsed_hours = int(total_elapsed_seconds // 3600)
            remainder_seconds = int(total_elapsed_seconds % 60)
            print('Processing completed in {}H {}m {}s'.format(total_elapsed_hours, total_elapsed_minutes,
                                                               remainder_seconds))

        if 'batch_directory' in global_data:
            # This is a batch run!
            print('Running batch of files...')
            return_values = []
            files = os.listdir(global_data['batch_directory'])
            for file in files:
                path = os.path.join(global_data['batch_directory'], file)
                with open(path) as f:
                    batch_data = json.load(f)
                    print('\nRunning file {}...\n'.format(file))
                    filename = os.path.basename(file)
                    return_values.append(main_single_file_loop(batch_data, _username, _password, filename))
                    print('\nFinished processing file {}.\n'.format(file))

            return return_values

        else:
            # Regular run!
            return main_single_file_loop(global_data, _username, _password, sys.argv[1])


if __name__ == '__main__':
    main()
