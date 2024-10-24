"""Test the postgres/tabular backend."""
from sheerwater_benchmarking.utils import cacheable

@cacheable(data_type='basic', cache_args=['number'])
def num(number=5):
    """Test function for tabular data."""

    return number


@cacheable(data_type='basic', cache_args=['el'])
def ls(el):
    ret = ['a']
    ret.append(el)

    return ret


def test_tab():
    """Test the tabular function."""
    num(10)
    ls('test')
