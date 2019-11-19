import pytest
from datetime import datetime, time

from classes.hdf import Earthdata


class TestEarthdata(object):

    @pytest.mark.parametrize('time_of_day, is_end_of_range, expected', [
        (time(23, 53, 22), False, (239, False)),  # 6 min tick, boundary end
        (time(23, 53, 22), True, (240, False)),  # 6 min tick, boundary start
        # 6 min tick, boundary end, kernel is max; kernel in the same day
        (time(23, 59, 22), False, (240, False)),
        # 6 min tick, boundary start, kernel crosses max; kernel is in the next day
        (time(23, 59, 22), True, (1, True)),
        (time(0, 5, 21), False, (1, False)),  # 1s before tick, range end, kernel is min
        (time(0, 5, 21), True, (1, False)),  # 1s before tick, range start, kernel is min
        (time(0, 5, 23), False, (2, False)),  # 1s after tick, range end, kernel is between min and max
        (time(0, 5, 23), True, (2, False)),  # 1s after tick, range start, kernel is between min and max
        (time(0, 6, 22), False, (2, False)),  # 60s after tick, range end, kernel is between min and max
        (time(0, 6, 22), True, (2, False)),  # 60s after tick, range start, kernel is between min and max
        (time(23, 54), False, (240, False)),   # between ticks, range end, no seconds
        (time(23, 58), True, (240, False)),  # between ticks, range start, no seconds
    ])
    def test_get_time_kernel(self, time_of_day, is_end_of_range, expected):
        instance = Earthdata('', '', '')
        assert instance.get_time_kernel(time_of_day, is_end_of_range) == expected

    @pytest.mark.parametrize('start_datetime, end_datetime, expected', [
        (
             # different dates, same kernel
             '01:01:2017 23:59:22', '01:02:2017 00:05:22', {
                 'total_days': 1, 'first_kernel': 1, 'last_kernel': 1, 'total_kernels': 1
             }
        ),
        (
             # different dates, first kernel in start + 1
             '01:01:2017 23:59:23', '01:02:2017 23:50:00', {
                 'total_days': 1, 'first_kernel': 1, 'last_kernel': 239, 'total_kernels': 239
             }
        ),
        (
             # same dates, same kernel
             '01:02:2017 15:59:22', '01:02:2017 16:05:22', {
                 'total_days': 1, 'first_kernel': 161, 'last_kernel': 161, 'total_kernels': 1
             }
        ),
        (
             # same dates, last kernel in end + 1
             '01:01:2017 00:00:00', '01:01:2017 23:59:59', {
                 'total_days': 2, 'first_kernel': 1, 'last_kernel': 1, 'total_kernels': 241
             }
        ),
        (
            # different dates, different kernels, small test
            '01:01:2012 18:00:00', '01:04:2012 09:52:00', {
                'total_days': 4, 'first_kernel': 181, 'last_kernel': 99, 'total_kernels': 639
            }
        ),
        (
            # different dates, different kernels, large test
            '01:01:2012 18:00:00', '04:05:2016 19:45:00', {
                'total_days': 1557, 'first_kernel': 181, 'last_kernel': 198, 'total_kernels': 373458
            }
        ),

    ])
    def test_get_kernels_for_time_range(self, start_datetime, end_datetime, expected):
        instance = Earthdata('', '', '')
        result = instance.get_kernels_for_time_range(start_datetime, end_datetime)

        assert len(result.keys()) == expected['total_days']
        assert min(result[min(result.keys())]) == expected['first_kernel']
        assert max(result[max(result.keys())]) == expected['last_kernel']
        assert sum(map(len, result.values())) == expected['total_kernels']

