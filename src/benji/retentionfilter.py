#
# This is loosely based on timegaps/timefilter.py and timegaps/main.py
# which are part of the timegaps module. Please see: https://github.com/jgehrcke/timegaps.
#
# The original copyright and license are:
#
# Copyright 2014 Jan-Philip Gehrcke (http://gehrcke.de)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import datetime
import re
from collections import OrderedDict
from collections import defaultdict
from typing import List, Dict, Sequence, Union, Tuple, Set

import dateutil

from benji.database import Version
from benji.exception import UsageError
from benji.logging import logger
from benji.repr import ReprMixIn


class RetentionFilter(ReprMixIn):

    _valid_categories = ('latest', 'hours', 'days', 'weeks', 'months', 'years')

    # This method is taken from timegaps/main.py, its original name is parse_rules_from_cmdline.
    @classmethod
    def _parse_rules(cls, rules_spec: str) -> OrderedDict:
        tokens = rules_spec.split(',')
        rules_dict: Dict[str, int] = {}
        for token in tokens:
            if not token:
                raise ValueError('Empty retention policy element.')
            match = re.search(r'^([a-z]+)([0-9]+)$', token)
            if match:
                category = match.group(1)
                timecount = int(match.group(2))
                if category not in cls._valid_categories:
                    raise ValueError('Time category {} in retention policy is invalid.'.format(category))
                if category in rules_dict:
                    raise ValueError('Time category {} listed more than once in retention policy.'.format(category))
                if timecount <= 0:
                    raise UsageError('Count of time category {} must be a positive integer.'.format(category))
                rules_dict[category] = timecount
                continue
            raise ValueError('Invalid retention policy element {}.'.format(token))

        rules: OrderedDict[str, int] = OrderedDict()
        for category in cls._valid_categories:
            if category in rules_dict:
                rules[category] = rules_dict[category]

        return rules

    def __init__(self, rules_spec: str, reference_time: datetime.datetime = None, tz: datetime.tzinfo = None) -> None:
        self.tz = tz if tz is not None else dateutil.tz.tzlocal()
        if reference_time is None:
            self.reference_time = datetime.datetime.now(tz=self.tz)
        else:
            if reference_time.tzinfo is None:
                # Assume it is in UTC
                self.reference_time = reference_time.replace(tzinfo=datetime.timezone.utc)
            else:
                self.reference_time = reference_time

        self.rules = self._parse_rules(rules_spec)

        logger.debug('Retention filter set up with reference time {} and rules {}.'.format(
            self.reference_time.isoformat(timespec='seconds'), self.rules))

    def filter(self, versions: Union[Sequence[Version], Set[Version]]) -> List[Version]:
        return self._filter(versions)[0]

    def _filter(self, versions: Union[Sequence[Version], Set[Version]]
               ) -> Tuple[List[Version], Dict[str, Dict[int, List[Version]]]]:
        # Category labels without latest
        categories = [category for category in self.rules.keys() if category != 'latest']

        versions_by_category: Dict[str, Dict[int, List[Version]]] = {}
        versions_by_category_remaining: Dict[str, Dict[int, List[Version]]] = {}
        for category in categories:
            versions_by_category[category] = defaultdict(list)
            versions_by_category_remaining[category] = {}

        # Make our own copy
        versions = list(versions)
        # Sort from youngest to oldest
        versions.sort(key=lambda version: version.date, reverse=True)

        # Remove latest versions from consideration if configured
        if 'latest' in self.rules:
            logger.debug('Keeping {} latest versions.'.format(self.rules['latest']))
            versions_by_category_remaining['latest'] = {0: versions[:self.rules['latest']]}
            del versions[:self.rules['latest']]

        dismissed_versions = []
        for version in versions:
            try:
                # version.date is naive and in UTC, attach time zone to make it time zone aware.
                td = _Timedelta(version.date.replace(tzinfo=datetime.timezone.utc), self.reference_time, tz=self.tz)
            except ValueError as exception:
                # Err on the safe side, ignore this versions (i.e. it won't be dismissed)
                logger.warning('Version {}: {}.'.format(version.uid.v_string, exception))
                continue

            logger.debug('Time and time delta for version {} are {} and {}.'.format(
                version.uid.v_string, version.date.isoformat(timespec='seconds'), td))

            for category in categories:
                timecount = getattr(td, category)
                if timecount <= self.rules[category]:
                    logger.debug('Found matching category {}, timecount {}.'.format(category, timecount))
                    versions_by_category[category][timecount].append(version)
                    break
            else:
                # For loop did not break: The item doesn't fit into any category, it's too old.
                dismissed_versions.append(version)
                logger.debug('Dismissing version, it doesn\'t fit into any category.')

        for category in categories:
            for timecount in versions_by_category[category]:
                # Keep the oldest of each category, reject the rest
                dismissed_versions.extend(versions_by_category[category][timecount][:-1])
                versions_by_category_remaining[category][timecount] = versions_by_category[category][timecount][-1:]

        return dismissed_versions, versions_by_category_remaining


class _Timedelta(ReprMixIn):

    @staticmethod
    def _round_down(t: datetime.datetime, *, start_of: str):
        if start_of == 'hour':
            return t.replace(minute=0, second=0, microsecond=0)
        elif start_of == 'day':
            return t.replace(hour=0, minute=0, second=0, microsecond=0)
        elif start_of == 'week':
            # This will round down to the last Monday at 00:00.0 before t.
            return t + dateutil.relativedelta.relativedelta(
                weekday=dateutil.relativedelta.MO(-1), hour=0, minute=0, second=0, microsecond=0)
        elif start_of == 'month':
            return t.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        elif start_of == 'year':
            return t.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            raise ValueError('Start of {} is unknown.'.format(start_of))

    def __init__(self, t: datetime.datetime, reference_time: datetime.datetime, tz: datetime.tzinfo) -> None:
        if t.tzinfo is None:
            raise ValueError('Time is not time zone aware.')
        if reference_time.tzinfo is None:
            raise ValueError('Reference time is not time zone aware.')
        if reference_time - t < datetime.timedelta(0):
            raise ValueError('{} isn\'t earlier than the reference time {} (difference = {}).'.format(
                t.isoformat(timespec='seconds'), reference_time.isoformat(timespec='seconds'),
                (reference_time - t).total_seconds()))

        t = t.astimezone(tz=tz)
        reference_time = reference_time.astimezone(tz=tz)

        # Hours
        delta = self._round_down(reference_time, start_of='hour') - self._round_down(t, start_of='hour')
        self.hours = int(delta.total_seconds() // 3600)

        # Days
        delta = self._round_down(reference_time, start_of='day') - self._round_down(t, start_of='day')
        self.days = int(delta.days)

        # Weeks
        delta = self._round_down(reference_time, start_of='week') - self._round_down(t, start_of='week')
        self.weeks = int(delta // datetime.timedelta(weeks=1))

        # Months
        delta = dateutil.relativedelta.relativedelta(self._round_down(reference_time, start_of='month'),
                                                     self._round_down(t, start_of='month')).normalized()
        self.months = delta.years * 12 + delta.months

        # Years
        delta = dateutil.relativedelta.relativedelta(self._round_down(reference_time, start_of='year'),
                                                     self._round_down(t, start_of='year')).normalized()
        self.years = delta.years
