import pytest
from cli import main
import sys
import pandas as pd
import glob


# Collect test JSON and CSV files
json_files = glob.glob('tests/geo_expansion_test_data/*.json')
test_data = []
for json_file in json_files:
    csv_file = json_file.split('.')[0] + '.csv'
    test_data.append({'json': json_file, 'csv': csv_file})


# Run each JSON file through the program
@pytest.mark.parametrize('test_run', test_data)
def test_geo_expansion(test_run):
    sys.argv = ['', test_run['json']]
    selected_granules = main()

    # Test that all expected granules exist in selected
    expected_granules = pd.read_csv(test_run['csv'])

    correctly_found_condition = selected_granules['hdf_filename'].isin(expected_granules['hdf_filename'])
    correctly_found_granules = selected_granules[correctly_found_condition]
    incorrectly_found_granules = selected_granules[~correctly_found_condition]
    missed_granules_condition = ~(expected_granules['hdf_filename'].isin(selected_granules['hdf_filename']))
    missed_granules = expected_granules[missed_granules_condition]

    num_correct = len(correctly_found_granules)
    num_expected = len(expected_granules)
    num_extra = len(incorrectly_found_granules)
    num_missed = len(missed_granules)

    # Report results
    if num_extra == 0 and num_missed == 0:
        print('Selected {}/{} expected granules and no extras.'.format(num_correct, num_expected))
    else:
        print('Selected {}/{} expected granules, {} unexpected, {} missed.'.format(num_correct, num_expected,
                                                                                   num_extra, num_missed))

        if num_extra > 0:
            print('Unexpected granules:')
            print(incorrectly_found_granules)

        if num_missed > 0:
            print('Missed granules:')
            print(missed_granules)

    assert(num_correct == num_expected)
    assert(num_extra == 0)
    assert(num_missed == 0)
