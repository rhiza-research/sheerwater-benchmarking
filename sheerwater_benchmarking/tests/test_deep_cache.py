"""Test the caching functions in the cacheable decorator."""
import numpy as np
from sheerwater_benchmarking.utils import cacheable


def test_deep_cache():
    """Test the deep cache recompute functionality."""
    @cacheable(data_type='basic',
               cache_args=[])
    def deep_cached_func():  # noqa: ARG001
        return np.random.randint(1000)

    @cacheable(data_type='basic',
               cache_args=[])
    def deep_cached_func2():  # noqa: ARG001
        return deep_cached_func() + np.random.randint(1000)

    @cacheable(data_type='basic',
               cache_args=[])
    def deep_cached_func3():  # noqa: ARG001
        return deep_cached_func2()

    print("should match")
    first = deep_cached_func3()
    second = deep_cached_func3()
    assert first == second

    # now verify the recomputing all the way back works
    fourth = deep_cached_func3(recompute=['deep_cached_func', 'deep_cached_func2'], force_overwrite=True)
    assert first != fourth

    # now verify that just recompute the second one works
    init = deep_cached_func()
    second = fourth - init
    final = deep_cached_func3(recompute='deep_cached_func2', force_overwrite=True)
    dsecond = final - init
    assert second != dsecond
    init2 = deep_cached_func()
    assert init == init2

    first = deep_cached_func3()
    second = deep_cached_func3(force_overwrite=True, recompute='all')
    assert first != second


if __name__ == "__main__":
    test_deep_cache()
