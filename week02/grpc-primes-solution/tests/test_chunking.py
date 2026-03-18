from grpc_common import iter_ranges, split_into_slices


def test_iter_ranges_even():
    assert iter_ranges(0, 10, 5) == [(0, 5), (5, 10)]


def test_iter_ranges_uneven():
    assert iter_ranges(0, 11, 5) == [(0, 5), (5, 10), (10, 11)]


def test_split_into_slices():
    assert split_into_slices(0, 10, 3) == [(0, 4), (4, 7), (7, 10)]
