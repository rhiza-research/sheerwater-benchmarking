"""Test the caching functions in the cacheable decorator."""

import numpy as np
from sheerwater_benchmarking.utils import cacheable, dask_remote


def test_deep_cache():
    """Test the deep cache recompute functionality."""

    @cacheable(data_type="basic", cache_args=[])
    def deep_cached_func():  # noqa: ARG001
        return np.random.randint(1000000)

    @cacheable(data_type="basic", cache_args=[])
    def deep_cached_func2():  # noqa: ARG001
        return deep_cached_func() + np.random.randint(1000000)

    @cacheable(data_type="basic", cache_args=[])
    def deep_cached_func3():  # noqa: ARG001
        return deep_cached_func2()

    first = deep_cached_func3()
    second = deep_cached_func3()
    assert first == second

    # now verify the recomputing all the way back works
    fourth = deep_cached_func3(recompute=["deep_cached_func", "deep_cached_func2"], force_overwrite=True)
    assert first != fourth

    # now verify that just recompute the second one works
    init = deep_cached_func()
    second = fourth - init
    final = deep_cached_func3(recompute="deep_cached_func2", force_overwrite=True)
    dsecond = final - init
    assert second != dsecond
    init2 = deep_cached_func()
    assert init == init2

    # verify that recompute="_all" recomputes nested functions.
    first = deep_cached_func3()
    second = deep_cached_func3(recompute="_all", force_overwrite=True)
    assert first != second

    # verify that recompute=[f,g] recomputes both f and g.
    first = deep_cached_func3()
    second = deep_cached_func3(recompute=["deep_cached_func3", "deep_cached_func2"], force_overwrite=True)
    assert first != second

    # verify that recompute=[f,g] doesn't recompute anything but f and g.
    first = deep_cached_func3()
    second = deep_cached_func3(recompute=["deep_cached_func3", "deep_cached_func1"], force_overwrite=True)
    assert first == second


@dask_remote
def test_remote_deep_cache():
    """Identical to test_deep_cache(), but decorated with @dask_remote."""
    test_deep_cache()


if __name__ == "__main__":
    test_deep_cache()
    test_remote_deep_cache(remote=False)
