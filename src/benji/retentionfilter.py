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

import re
import time
from collections import OrderedDict
from collections import defaultdict
from typing import List, Dict, Sequence

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

    def __init__(self, rules_spec: str, reference_time: float = None) -> None:
        self.reference_time = time.time() if reference_time is None else reference_time
        self.rules = self._parse_rules(rules_spec)
        logger.debug('Retention filter set up with reference time {} and rules {}'.format(
            self.reference_time, self.rules))

    def filter(self, versions: Sequence[Version]) -> List[Version]:
        # Category labels without latest
        categories = [category for category in self.rules.keys() if category != 'latest']

        versions_by_category = {}
        for category in categories:
            versions_by_category[category] = defaultdict(list)

        # Make our own copy
        versions = list(versions)
        # Sort from youngest to oldest
        versions.sort(key=lambda version: version.date_timestamp, reverse=True)

        # Remove latest versions from consideration if configured
        if 'latest' in self.rules:
            logger.debug('Keeping {} latest versions.'.format(self.rules['latest']))
            del versions[:self.rules['latest']]

        dismissed_versions = []
        for version in versions:
            try:
                td = _Timedelta(version.date_timestamp, self.reference_time)
            except _TimedeltaError as exception:
                # Err on the safe side, ignore this versions (i.e. it won't be dismissed)
                logger.warning('Version {}: {}'.format(version.uid.v_string, exception))
                continue

            logger.debug('Time and time delta for version {} are {} and {}.'.format(version.uid.v_string, version.date,
                                                                                    td))

            for category in categories:
                timecount = getattr(td, category)
                if timecount <= self.rules[category]:
                    logger.debug('Found matching category {}, timecount {}.'.format(category, timecount))
                    versions_by_category[category][timecount].append(version)
                    break
            else:
                # For loop did not break: The item doesn't fit into any category,
                # it's too old
                dismissed_versions.append(version)
                logger.debug('Dismissing version, it doesn\'t fit into any category.')

        for category in categories:
            for timecount in versions_by_category[category]:
                # Keep the oldest of each category, reject the rest
                dismissed_versions.extend(versions_by_category[category][timecount][:-1])

        return dismissed_versions


class _TimedeltaError(RuntimeError):
    pass


class _Timedelta(ReprMixIn):
    """
    Represent how many years, months, weeks, days, hours time `t` (float, seconds) is earlier than reference time
    `reference_time`. Represent these metrics with integer attributes. Both time values are converted to the respective
    unit by integer division before calculating the difference.
    There is no implicit summation, each of the numbers is to be considered independently. Time units are considered
    strictly linear: months are 30 days, years are 365 days, weeks are 7 days, one day is 24 hours.
    """

    def __init__(self, t: float, reference_time: float) -> None:
        # Expect two numeric values. Might raise TypeError for other types.
        if reference_time - t < 0:
            raise _TimedeltaError('{} isn\'t earlier than the reference time {}.'.format(t, reference_time))
        self.hours = reference_time // 3600 - t // 3600
        self.days = reference_time // 86400 - t // 86400
        self.weeks = reference_time // 604800 - t // 604800
        self.months = reference_time // 2592000 - t // 2592000
        self.years = reference_time // 31536000 - t // 31536000
