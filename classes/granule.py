from calendar import isleap, month_name
from datetime import datetime, timedelta
from functools import total_ordering


@total_ordering
class Granule(object):
    def __init__(self, year, day, granule_number, local_file_name=None):
        self._validate_init_parameters(year, day, granule_number)

        self.year = year
        self.day = day
        self.granule_number = granule_number
        self.local_file_name = local_file_name

    def __str__(self):
        return 'Granule<%s, %s, %s>' % (self.year, str(self.day).zfill(3), str(self.granule_number).zfill(3))

    def __repr__(self):
        return 'Granule<%s, %s, %s>' % (self.year, str(self.day).zfill(3), str(self.granule_number).zfill(3))

    def __int__(self):
        return self.year * 10 ** 6 + self.day * 10 ** 3 + self.granule_number

    def __add__(self, granule_count: int):
        days, granule_number = divmod(self.granule_number + granule_count, 240)
        final_date = datetime.strptime('%s%s' % (self.year, str(self.day).zfill(3)), '%Y%j') + timedelta(days=days)
        return self.__class__(final_date.year, int(final_date.strftime('%j')), granule_number)

    def __eq__(self, other):
        return int(self) == int(other)

    def __gt__(self, other):
        return int(self) > int(other)

    @property
    def date_value(self):
        """Returns an integer representing year and day of year"""
        return self.year * 1000 + self.day

    @property
    def month_period(self):
        """Returns a string representing the period of the year (year and month)"""
        month = datetime.strptime('%s%s' % (self.year, str(self.day).zfill(3)), '%Y%j').month
        return '%s %s' % (month_name[month], self.year)

    @staticmethod
    def _validate_init_parameters(year, day, granule_number):
        messages = []

        if not 2002 <= year <= 2019:
            messages.append('Year must be between 2002 and 2019 [%s passed].' % year)
        if not 1 <= day <= 365 + isleap(year):
            messages.append('Day must be between 1 and %s for %s [%s passed].' % (365 + isleap(year), year, day))
        if not 1 <= granule_number <= 240:
            messages.append('Granule number must be between 1 and 240 [%s passed].' % granule_number)

        if len(messages):
            raise ValueError(' '.join(messages))

    @classmethod
    def datetime_to_granule(cls, d: datetime, start_of_range=False):
        granule_number, offset = cls.get_granule_number_for_time(d, start_of_range)
        granule_date = d + timedelta(days=offset)
        return cls(granule_date.year, granule_date.timetuple().tm_yday, granule_number)

    @classmethod
    def get_granule_bounds_for_time_range(cls, start: datetime, end: datetime):
        """
        Returns a tuple with min and max granules that cover the passed time range. Assumes the timestamps
        to be UTC - no TZ conversion is performed.
        """
        start_granule_number, start_offset = cls.get_granule_number_for_time(start.time(), start_of_range=True)
        end_granule_number, end_offset = cls.get_granule_number_for_time(end.time())

        start_timestamp = start + timedelta(days=int(start_offset))
        end_timestamp = end + timedelta(days=int(end_offset))

        return (
            cls(start_timestamp.year, int(start_timestamp.strftime('%j')), start_granule_number),
            cls(end_timestamp.year, int(end_timestamp.strftime('%j')), end_granule_number)
        )

    @classmethod
    def get_granule_number_for_time(cls, time_of_day, start_of_range=False):
        """Returns the numeric granule representing the measurement that covers the passed hour of day. The measurements
        are recorded every 6 minutes, and are offset -38 seconds from midnight. As an example, below is an excerpt of
        an HDF's XML description file:

            <RangeDateTime>
                <RangeEndingDate>2016-01-01</RangeEndingDate>
                <RangeEndingTime>00:05:22.000000Z</RangeEndingTime>
                <RangeBeginningDate>2015-12-31</RangeBeginningDate>
                <RangeBeginningTime>23:59:22.000000Z</RangeBeginningTime>
            </RangeDateTime>

        The next measurement will then be from 23:59:22 on day D to 00:05:22 on day D+1. Accepts time strings
        with and without seconds. Some of the granules in the GESDISC files have a different offset (-34s was observed)
        - as long as the requested date does not use seconds, this code should be accurate enough.

        Also returns a boolean indicating whether the granule falls on D+1.
        """
        # the number of seconds since midnight for that time of day (hour * 3600 + minutes * 60 + seconds),
        # plus the opposite of the offset
        seconds_since_midnight = time_of_day.hour * 3600 + time_of_day.minute * 60 + time_of_day.second + 38

        # the base granule value is the integer quotient of the division of the offset-corrected seconds elapsed since
        # midnight by the period of each tick (360 secs).
        g, r = divmod(seconds_since_midnight, 360)

        # add 1 to the kernel if the seconds since midnight is a full minute (after offsetting) AND we're looking at
        # a time that represents the start of a time range or if the seconds since midnight is not a full minute
        # (outside of the 6 min ticks).
        g += int((start_of_range and r == 0) or r > 0)

        # return 1 if it exceeds the maximum value for the kernel
        return g ** (g <= 240), g > 240
