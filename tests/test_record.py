import pytest

from aiochsa.record import Record


@pytest.fixture
def record():
    return Record(['a', 'b'], [1, 'v'])


def test_len(record):
    assert len(record) == 2


def test_getitem_name(record):
    assert record['a'] == 1
    assert record['b'] == 'v'
    with pytest.raises(KeyError):
        record['c']


def test_getitem_index(record):
    assert record[0] == record[-2] == 1
    assert record[1] == record[-1] == 'v'
    with pytest.raises(IndexError):
        record[2]
    with pytest.raises(IndexError):
        record[-3]


def test_slice(record):
    assert record[:] == record[:2] == [1, 'v']
    assert record[2:] == record[:-2] == []
    assert record[1:] == record[1:2] == ['v']
    assert record[:1] == record[:-1] == [1]


def test_get(record):
    assert record.get('a') == record.get(0) == 1
    assert record.get('b') == record.get(1) == 'v'
    assert record.get('c') is record.get(2) is None
    default = object()
    assert record.get('c', default) is record.get(2, default) is default


def test_to_sequence(record):
    assert tuple(record) == (1, 'v')
    assert list(record) == [1, 'v']
    assert list(iter(record)) == [1, 'v']
    assert [*record] == [1, 'v']


def test_to_dict(record):
    assert dict(record) == {'a': 1, 'b': 'v'}
    assert {**record} == {'a': 1, 'b': 'v'}


def test_eq_dict(record):
    assert record == {'a': 1, 'b': 'v'}
    assert record == {'b': 'v', 'a': 1}
    assert record != {'a': 2, 'b': 'v'}
    assert record != {'a': 1, 'c': 'v'}
    assert record != {'a': 1}
    assert record != {'a': 1, 'b': 'v', 'c': 2}


@pytest.mark.parametrize('t', [tuple, list, iter])
def test_eq_sequence(record, t):
    assert record == t([1, 'v'])
    assert record != t(['v', 1])
    assert record != t([1])
    assert record != t([1, 'v', 2])


def test_mapping_interface(record):
    assert list(record.keys()) == ['a', 'b']
    assert list(record.values()) == [1, 'v']
    assert list(record.items()) == [('a', 1), ('b', 'v')]


def test_repr(record):
    assert repr(record) == "<Record a=1 b='v'>"
