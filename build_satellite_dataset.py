"""
Quick and dirty multithreaded script to download location and file information for each of the available kernels
in GESDISC. Still has a long ways to go in terms of code, functionality, error handling (including ability to
resume and cache already retrieved information), and memory management. Though it gets the job done.

Some errors may happen - those missing kernels need to be worked out from the processing logs - the script itself
could have a better method of handling those errors. Most common problem is taht GESDISC will report the IP is
using more than the allowed 15 simultaneous connections.

2007-2016 took about 3h to finish on an m5.large machine.

The functions in here are quite testable - so some coverage wouldn't hurt. Functionality to check that
all available days and kernels have been parsed would also be interesting.

One doesn't need to use this script unless rebuilding the data/ files from scratch, or in the event that additional
dates/kernels are made available.
"""
import datetime
import re
import os
from getpass import getpass
from time import sleep
from tqdm import tqdm
from multiprocessing.pool import ThreadPool as Pool

from urllib3.exceptions import NewConnectionError, MaxRetryError
from requests.exceptions import ConnectionError
from classes._http import SessionWithHeaderRedirection
from classes.calculate_GCA import calculate_gca_for_files_and_zip


THREADS = 13

DAY_PATTERN = re.compile(r'href=["\'](\d{3})/["\']', re.MULTILINE)
XML_FILENAME_PATTERN = re.compile(
    r'<a href="(AIRS.\d{4}.\d{2}.\d{2}.\d{3}.L2.CC_IR.v6.\d+.\d+.\d+.G\d+.hdf.xml)">', re.MULTILINE)

XML_LON = re.compile(r'LonGranuleCen</PSAName>\n\s*<PSAValue>(.+?)<', re.MULTILINE)
XML_LAT = re.compile(r'LatGranuleCen</PSAName>\n\s*<PSAValue>(.+?)<', re.MULTILINE)


def get_html(http_client, url, count=0, echo=False):
    try:
        if count <= 10:
            page_text = http_client.get(url).text
            if echo:
                print(page_text)
            return page_text
        else:
            print('ERROR: Max retries exceeded for url %s' % url)
            return None
    except NewConnectionError as e:
        print(e)
        sleep(.5)
        count += 1
    except ConnectionError as e:
        print(e)
        sleep(.5)
        count += 1
    except MaxRetryError as e:
        print(e)
        sleep(.5)
        count += 1
    except TimeoutError as e:
        sleep(2)
        print('Timeout error: %s' % e)
        count += 1

    return get_html(http_client, url, count)


def get_days_from_html(_year):
    with SessionWithHeaderRedirection(user_name, password) as http_client:
        url = "%s/%s/" % (BASE_URL, _year)
        html = get_html(http_client, url)
        days_list = sorted(list(set(re.findall(DAY_PATTERN, html))))
        print("    - Finished fetching available days for", _year)
    return _year, days_list


def get_xml_links_for_day(_year, _day):
    with SessionWithHeaderRedirection(user_name, password) as http_client:
        url = "%s/%s/%s" % (BASE_URL, _year, _day)
        html = get_html(http_client, url)
        files = list(set(re.findall(XML_FILENAME_PATTERN, html)))
        print('    - XML links for day %s/%s listed (%s total)' % (_year, _day, len(files)))
    return (_year, _day), files


def get_information_from_xml(period, files):
    _year, _day = period
    _positions = []
    with SessionWithHeaderRedirection(user_name, password) as http_client:

        failed_kernel_files = []
        finished = False
        max_retries = 500
        num_retries = 0
        while not finished:
            desc = 'day {}'.format(_day)
            if num_retries != 0:
                desc = 'day {} (retry #{})'.format(_day, num_retries)
            for filename in tqdm(files, desc='day {}'.format(_day)):
                url = "%s/%s/%s/%s" % (BASE_URL, _year, _day, filename)
                xml = get_html(http_client, url)
                _kernel = filename.split('.')[4]
                try:
                    # west, north, east, south = map(float, re.findall(COORDINATES_PATTERN, xml)[0])
                    # _latitude = (north + south) / 2
                    # _longitude = (east + west) / 2
                    _longitude = int(re.findall(XML_LON, xml)[0])
                    _latitude = int(re.findall(XML_LAT, xml)[0])

                    _positions.append((_year, _day, _kernel, _latitude, _longitude, filename.replace('.xml', '')))
                    # print('    - Read %s/%s/%s' % (_year, _day, _kernel))
                except IndexError as e:
                    print("Error encountered, will retry.")
                    failed_kernel_files.append(filename)
                    # print('[get_information_from_xml] INDEX ERROR FOR %s/%s/%s: %s' % (_year, _day, filename, e))
                    # with open(os.path.join(output_dir, '%s-%s-%s.err' % (_year, _day, _kernel)), 'w') as err_file:
                    #     err_file.write(xml)
                except Exception as e:
                    print('[get_information_from_xml] GENERIC ERROR FOR %s/%s/%s: %s' % (_year, _day, filename, e))
                    print('Error encountered, will retry.')
                    failed_kernel_files.append(filename)

            if not failed_kernel_files:
                finished = True
            elif num_retries == max_retries:
                print("Max retries reached. Returning incomplete data!")
                finished = True

                # Write empty file with year name as flag to operator
                with open(os.path.join(output_dir, '%s.err' % _year), 'w') as err_file:
                    err_file.write("")
            else:
                num_retries += 1
                files = failed_kernel_files
                failed_kernel_files = []

    return _positions


if __name__ == '__main__':
    user_name = input('Username for GESDISC/Earthdata Login: ')
    password = getpass()
    STARTING_YEAR = int(input('Start year: '))
    current_year = datetime.datetime.now().year
    if not 2002 <= STARTING_YEAR <= current_year:
        print('Start year must be between 2002 and the present.')
        exit(1)
    ENDING_YEAR = int(input('End year: '))
    if not 2002 <= ENDING_YEAR <= current_year:
        print('End year must be between 2002 and the present.')
        exit(1)
    urls = ["https://airsl2.gesdisc.eosdis.nasa.gov/data/Aqua_AIRS_Level2/AIRS2CCF.006/",
            "https://airsl2.gesdisc.eosdis.nasa.gov/data/Aqua_AIRS_Level2/AIRI2CCF.006/",
            "https://airsl1.gesdisc.eosdis.nasa.gov/data/Aqua_AIRS_Level1/AIRIBRAD.005/"
            ]
    print('From which URL do you want to get AIRS data? Choose one, or input a custom URL.')
    for i in range(len(urls)):
        print('{}: {}'.format(i+1, urls[i]))
    chosen_url = input('URL or option: ')
    try:
        BASE_URL = urls[int(chosen_url)-1]
    except ValueError:
        BASE_URL = chosen_url
    print('Chosen URL: {}'.format(BASE_URL))
    output_dir = ''
    while output_dir == '' or not os.path.exists(output_dir):
        output_dir = input('Output path: ')
        if not os.path.exists(output_dir):
            print('This does not seem to be a valid path. Use an absolute or relative path to an existing directory:')

    years_range = list(range(STARTING_YEAR, ENDING_YEAR + 1))

    # in each year page, get list of available days
    print('Getting list of available days per year...')

    # Get these XML files with fewer threads so as to ensure that NONE of this data is dropped
    with Pool(processes=7) as pool:
        async_results = pool.map_async(get_days_from_html, years_range)
        pool.close()
        pool.join()

        days_mapping = async_results.get()

    print('\n')
    sleep(3)

    print('Getting links for the relevant XML files...')

    current_datetime = datetime.datetime.now()
    date = str(current_datetime.date())
    timestamp = str(current_datetime.timestamp())
    temp_directory_name = 'temp_{}'.format(date + timestamp)
    os.makedirs(temp_directory_name)
    xml_mapping = dict()
    for year in years_range:
        # Get these XML files with fewer threads so as to ensure that NONE of this data is dropped
        with Pool(processes=7) as pool:
            year_days = list(filter(lambda x: x[0] == year, days_mapping))[0][1]
            mapping = [(year, d) for d in year_days]
            day_results = pool.starmap_async(get_xml_links_for_day, mapping)
            pool.close()
            pool.join()
            print('Finished async process pool...')

            # (kernel_xmls[(_year, _day)] = list(set(re.findall(XML_FILENAME_PATTERN, html)))
            # ((_year, _day, kernel_xmls), (_year, _day, kernel_xmls))
            xml_mapping = day_results.get()

        print('Finished getting links for the relevant XML files for', year)

        print('\n')
        sleep(3)

        print('Extracting data from XML content for {} ({} days to process, using {} threads)...'.format(year,
                                                                                                         len(mapping),
                                                                                                         THREADS))
        # grab each xml file and extract coordinates, BoundingRectangle, AIRSRunTag
        dropped_data = False

        with Pool(processes=THREADS) as pool:
            mapping = filter(lambda x: x[0][0] == year, xml_mapping)
            position_results = pool.starmap_async(get_information_from_xml, mapping)
            pool.close()
            pool.join()

            # [[_year, _day, _kernel, _latitude, _longitude, hdf_filename], ...]
            positions = position_results.get()
            with open(os.path.join(temp_directory_name, 'aqua_positions_%s.csv' % year), 'w') as output_file:
                output_file.write('year,day,kernel,lat,lon,hdf_filename\n')
                for resulting_groups in positions:
                    for position in tqdm(resulting_groups, desc='Writing data'):
                        if position is not None:
                            output_file.write('%s,%s,%s,%s,%s,%s\n' % position)
                            # print('    - Wrote %s/%s/%s' % position[:3])
                        else:
                            dropped_data = True
                            print('MISSING DATA FOR WRITE')

        print('%s finished' % year)
        if not dropped_data:
            print('✔️  No data dropped')
        else:
            print('❌  Some data was dropped!')
            positive_responses = ['y', 'Y']
            negative_responses = ['n', 'N']
            cancel = ''
            while cancel not in positive_responses and cancel not in negative_responses:
                cancel = input('Cancel process? Y/N: ')
            if cancel in positive_responses:
                exit(0)
        sleep(5)

    print('\n---------- Process finished ----------\n')

    # Begin doing GCA calculation
    calculate_gca_for_files_and_zip(temp_directory_name, output_dir)
