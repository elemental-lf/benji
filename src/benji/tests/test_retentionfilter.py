import datetime
from itertools import count, tee
from typing import Sequence, Set, Union
from unittest import TestCase
from unittest.mock import Mock, MagicMock

import dateutil
from parameterized import parameterized

from benji.database import VersionUid, Version
from benji.retentionfilter import RetentionFilter


# From https://docs.python.org/3/library/itertools.html#itertools-recipes
def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


class RetentionFilterTestCase(TestCase):

    REF_TIME = datetime.datetime(2019, 5, 9, 0, 0, 0, 0, tzinfo=None)

    versions: Set[Version]

    @staticmethod
    def _make_version(uid: str, date: datetime.datetime) -> Version:
        version = MagicMock(spec=('uid', 'date', '__repr__'))
        version.uid = VersionUid(uid)
        version.date = date
        version.__repr__ = Mock()
        version.__repr__.return_value = '{} - {}'.format(version.uid, version.date.isoformat(timespec='seconds'))
        return version

    @classmethod
    def setUpClass(cls) -> None:
        timestamps = (cls.REF_TIME - dateutil.relativedelta.relativedelta(minutes=n * 15) for n in range(0, 8640 + 1))
        cls.versions = set([cls._make_version(f'v{c}', t) for c, t in zip(count(start=1), timestamps)])

    @parameterized.expand([
        ('latest3', 3, 15 * 60),
        ('latest10', 10, 15 * 60),
        ('hours12', 13, 60 * 60),
        ('hours25', 26, 60 * 60),
        ('days15', 16, 24 * 60 * 60),
        ('days31', 32, 24 * 60 * 60),
        ('weeks4', 5, 7 * 24 * 60 * 60),
        ('months1', 2, 31 * 24 * 60 * 60),
        ('months2', 3, 31 * 24 * 60 * 60),
        ('years1', 1, 366 * 31 * 24 * 60 * 60),
    ])
    def test_single(self, spec: str, expected_length: int, base_unit: float) -> None:
        filter = RetentionFilter(rules_spec=spec, reference_time=self.REF_TIME, tz=datetime.timezone.utc)
        dismissed_versions = set(filter.filter(self.versions))
        remaining_versions = self.versions - dismissed_versions

        self.assertEqual(expected_length, len(remaining_versions))

        for version in remaining_versions:
            # No version must be older than the cutoff
            cutoff = base_unit * expected_length
            self.assertLessEqual((self.REF_TIME - version.date).total_seconds(), cutoff)

        sorted_versions = sorted(remaining_versions, key=lambda version: version.date)
        for older, younger in pairwise(sorted_versions):
            # Difference between adjacent versions must be less than or at most equal to the base unit
            self.assertLessEqual((younger.date - older.date).total_seconds(), base_unit)

        # Filter remaining versions a second time, no further versions must be filtered
        dismissed_versions_2 = filter.filter(remaining_versions)
        self.assertEqual(0, len(dismissed_versions_2))

    @parameterized.expand([
        ('latest3,months2', 6, 3 * 30 * 24 * 60 * 60),
        ('latest3,hours24,days3', 29, 4 * 24 * 60 * 60),
        ('latest3,hours48,days3', 52, 4 * 24 * 60 * 60),
        ('latest3,hours48,days3,months2', 55, 3 * 30 * 24 * 60 * 60),
        ('latest3,hours48,days30,months2', 81, 3 * 30 * 24 * 60 * 60),
    ])
    def test_multiple(self, spec: str, expected_length: int, cutoff: float) -> None:
        filter = RetentionFilter(rules_spec=spec, reference_time=self.REF_TIME, tz=datetime.timezone.utc)
        dismissed_versions = set(filter.filter(self.versions))
        remaining_versions = self.versions - set(dismissed_versions)

        self.assertEqual(expected_length, len(remaining_versions))

        for version in remaining_versions:
            # No version must be older than the cutoff
            self.assertLessEqual((self.REF_TIME - version.date).total_seconds(), cutoff)

        # Filter remaining versions a second time, no further versions must be filtered
        dismissed_versions_2 = filter.filter(remaining_versions)
        self.assertEqual(0, len(dismissed_versions_2))

    def test_moving_single(self) -> None:
        current_time = self.REF_TIME
        filter = RetentionFilter(rules_spec='hours30', reference_time=current_time, tz=datetime.timezone.utc)
        dismissed_versions = set(filter.filter(self.versions))
        remaining_versions = self.versions - set(dismissed_versions)

        self.assertEqual(31, len(remaining_versions))

        # Move in steps of 1 hour into the future
        for hour in range(1, 31):
            current_time = self.REF_TIME + dateutil.relativedelta.relativedelta(hours=hour)
            filter = RetentionFilter(rules_spec='hours30', reference_time=current_time, tz=datetime.timezone.utc)
            dismissed_versions = set(filter.filter(remaining_versions))
            # We're moving in steps of one hour, so each time one version is dismissed
            self.assertEqual(len(dismissed_versions), 1)
            # The dismissed version must be older than 31 hours
            self.assertGreaterEqual((current_time - list(dismissed_versions)[0].date).total_seconds(), 31 * 60 * 60)
            remaining_versions = remaining_versions - dismissed_versions

        self.assertEqual(1, len(remaining_versions))

    @parameterized.expand([
        ('hours30', ['hours'], [range(0, 31)], [1]),
        ('latest10', ['latest'], [range(0, 1)], [10]),
        ('latest8,hours48,days7,weeks4,months2,years1', ['latest', 'hours', 'days', 'weeks', 'months', 'years'],
         [range(0, 1), range(2, 49), range(2, 8),
          range(1, 5), range(1, 3), range(0, 1)], [8, 1, 1, 1, 1, 1]),
    ])
    def test_classification(self, spec: str, expected_categories: Sequence[str],
                            expected_timecounts: Sequence[Sequence[int]], expected_in_each: Sequence[int]) -> None:
        remaining_versions = set(self.versions)
        previous_versions_by_category_remaining = None
        # From self.REF_TIME to roughly 6 months into the future
        for hour in range(0, 4464):
            current_time = self.REF_TIME + dateutil.relativedelta.relativedelta(hours=hour)
            filter = RetentionFilter(rules_spec=spec, reference_time=current_time, tz=datetime.timezone.utc)
            dismissed_versions: Union[Sequence, Set]
            dismissed_versions, versions_by_category_remaining = filter._filter(remaining_versions)
            dismissed_versions = set(dismissed_versions)
            remaining_versions = remaining_versions - dismissed_versions

            try:
                self.assertSetEqual(set(versions_by_category_remaining.keys()), set(expected_categories))

                for i, category in enumerate(expected_categories):
                    actual_timecounts_set = set(versions_by_category_remaining[category].keys())
                    expected_timecounts_set = set(expected_timecounts[i])

                    # Due to the fact that we align on the start of day, week, etc. the first (youngest) expected
                    # timecount category can be missing.
                    if len(actual_timecounts_set) != len(expected_timecounts_set):
                        expected_timecounts_set = set(list(expected_timecounts[i])[1:])

                    self.assertSetEqual(actual_timecounts_set, expected_timecounts_set, category)

                    for timecount in versions_by_category_remaining[category].keys():
                        self.assertEqual(len(versions_by_category_remaining[category][timecount]), expected_in_each[i])
                        self.assertTrue(versions_by_category_remaining[category][timecount][0] in remaining_versions)
                        self.assertTrue(versions_by_category_remaining[category][timecount][0] not in dismissed_versions)
            except AssertionError:
                print('Time at failed assertion: {}.'.format(current_time.isoformat(timespec='seconds')))
                if previous_versions_by_category_remaining:
                    print(previous_versions_by_category_remaining)
                print(versions_by_category_remaining)
                raise

            # Add four new versions
            for minute in (15, 30, 45, 60):
                remaining_versions.add(
                    self._make_version('v{}'.format(1000000 + hour * 60 + minute),
                                       current_time + dateutil.relativedelta.relativedelta(minutes=minute)))

            previous_versions_by_category_remaining = versions_by_category_remaining
