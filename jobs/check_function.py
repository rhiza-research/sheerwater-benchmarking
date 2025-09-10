"""Generate land-sea masks for all grids and bases."""
from itertools import product
# from importlib import import_module
from sheerwater_benchmarking.climatology import climatology_2015

# import argparse

# # Create an ArgumentParser object
# parser = argparse.ArgumentParser(description='This script checks the validity of pipeline functions.')

# # Add arguments
# parser.add_argument('function', type=str, help='The function to test')
# parser.add_argument('start_time', type=str, help='The first timestamp to check.')
# parser.add_argument('end_time', type=str, help='The last timestamp to check')
# parser.add_argument('iter', type=str, help='The set of parameters to iterate through, as a comma-separated list',
#                     default="vars,grids,aggs,prob_types")
# parser.add_argument('kwargs', type=str, help='A keyword argument dict to pass to the function')

# # Parse the arguments
# args = parser.parse_args()
# start_time = args.start_time
# end_time = args.end_time
# kwargs = dict(args.kwargs)
# iters = args.iters.split(',')

# # Get the function
# func = args.function
# mod = import_module("sheerwater_benchmarking.baselines")
# fn = getattr(mod, func)

# Define a set of parameters to iterate through
start_time = "1979-01-01"
end_time = "2024-01-01"
vars = ["tmp2m", "precip"]
grids = ["global0_25", "global1_5"]
prob_types = ["determ", "prob"]
leads = ["week1", "weeks12"]
product_list = [vars, leads, prob_types, grids]
param_list = ["variable", "lead", "prob_type", "grid"]
# product_list = []
# for iter in iters:
#     if iter == 'vars':
#         product_list.append(vars)
#     elif iter == 'grids':
#         product_list.append(grids)
#     elif iter == 'aggs':
#         product_list.append(aggs)
#     elif iter == 'prob_types':
#         product_list.append(prob_types)

# Parameters for function
clim_years = 30
first_year = 1985
last_year = 2014

for param in product(*product_list):
    func_params = dict(zip(param_list, param))
    # print(f"Running function with parameters: {func_params}")
    # print(f"and keyword arguments: {kwargs}")
    # ds = func(start_time, end_time, *func_params, **kwargs)

    ds = climatology_2015(start_time, end_time, **func_params, mask=None, region='global',
                          remote=True,
                          remote_config={'name': 'genevieve2',
                                         'n_workers': 10,
                                         'idle_timeout': '120 minutes'})

    has_invalid_date = (ds.isnull().mean(dim=['lat', 'lon']) > 0.95).any().compute()
    # Convert to a scalar
    has_invalid_date = has_invalid_date[func_params['variable']].values
    if has_invalid_date:
        print(f"Dataset contains invalid dates: {func_params}")
    assert not has_invalid_date, "Dataset contains invalid dates."
