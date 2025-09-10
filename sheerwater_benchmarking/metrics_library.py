"""Library of metrics implementations for verification."""
# flake8: noqa: D102
from abc import ABC, abstractmethod
from inspect import signature

import xarray as xr
import numpy as np
import pandas as pd

from sheerwater_benchmarking.statistics_library import statistic_factory
from sheerwater_benchmarking.utils import (get_datasource_fn, get_lead_info,
                                           get_admin_level, get_time_level,
                                           get_mask)
from sheerwater_benchmarking.climatology import climatology_2020, seeps_wet_threshold, seeps_dry_fraction
from sheerwater_benchmarking.masks import region_labels


# Global metric registry dictionary
SHEERWATER_METRIC_REGISTRY = {}


def get_bins(bin_str: str) -> np.ndarray:
    """Get the categorical bin string of the form 'edge-edge...'.

    For example,
        '5' returns [-np.inf, 5, np.inf]
        '5-10' returns [-np.inf, 5, 10, np.inf]
    """
    if bin_str == 'none':
        return np.array([])
    try:
        bins = [float(x) for x in bin_str.split('-')]
    except ValueError:
        raise ValueError(f'Invalid bins for metric {bin_str}. Bins must be integers.')
    bins = [-np.inf] + bins + [np.inf]
    return np.array(bins)


def mean_or_sum(ds, agg_fn, dims=['lat', 'lon']):
    """A light wrapper around standard groupby aggregation functions."""
    # Note, for some reason:
    # ds.groupby('region').mean(['lat', 'lon'], skipna=True).compute()
    # raises:
    # *** AttributeError: 'bool' object has no attribute 'blockwise'
    # or
    # *** TypeError: reindex_intermediates() missing 1 required positional argument: 'array_type'
    # So we have to do it via apply
    if agg_fn == 'mean':
        return ds.mean(dims, skipna=True)
    else:
        return ds.sum(dims, skipna=True)


def groupby_time(ds, time_grouping, agg_fn='mean'):
    """Aggregate a statistic over time."""
    if time_grouping is not None:
        if time_grouping == 'month_of_year':
            coords = [f'M{x:02d}' for x in ds.time.dt.month.values]
        elif time_grouping == 'year':
            coords = [f'Y{x:04d}' for x in ds.time.dt.year.values]
        elif time_grouping == 'quarter_of_year':
            coords = [f'Q{x:02d}' for x in ds.time.dt.quarter.values]
        elif time_grouping == 'day_of_year':
            coords = [f'D{x:03d}' for x in ds.time.dt.dayofyear.values]
        elif time_grouping == 'month':
            coords = [f'{pd.to_datetime(x).year:04d}-{pd.to_datetime(x).month:02d}-01' for x in ds.time.values]
        else:
            raise ValueError("Invalid time grouping")
        ds = ds.assign_coords(group=("time", coords))
        ds = ds.groupby("group").apply(mean_or_sum, agg_fn=agg_fn, dims='time')
        # Rename the group coordinate to time
        ds = ds.rename({"group": "time"})
    else:
        # Average in time
        if agg_fn == 'mean':
            ds = ds.mean(dim="time")
        elif agg_fn == 'sum':
            ds = ds.sum(dim="time")
        else:
            raise ValueError(f"Invalid aggregation function {agg_fn}")
    # TODO: we can convert this to a groupby_time call when we're ready
    # ds = groupby_time(ds, grouping=time_grouping, agg_fn=xr.DataArray.mean, dim='time')
    return ds


def latitude_weights(ds, lat_dim='lat', lon_dim='lon'):
    """Return latitude weights as an xarray DataArray.

    This function weights each latitude band by the actual cell area,
    which accounts for the fact that grid cells near the poles are smaller
    in area than those near the equator.
    """
    # Calculate latitude cell bounds
    lat_rad = np.deg2rad(ds[lat_dim].values)
    # Get the centerpoint of each latitude band
    pi_over_2 = np.array([np.pi / 2], dtype=lat_rad.dtype)
    bounds = np.concatenate([-pi_over_2, (lat_rad[:-1] + lat_rad[1:]) / 2, pi_over_2])
    # Calculate the area of each latitude band
    # Calculate cell areas from latitude bounds
    upper = bounds[1:]
    lower = bounds[:-1]
    # normalized cell area: integral from lower to upper of cos(latitude)
    weights = np.sin(upper) - np.sin(lower)

    # Normalize weights
    weights /= np.mean(weights)
    # Return an xarray DataArray with dimensions lat
    weights = xr.DataArray(weights, coords=[ds[lat_dim]], dims=[lat_dim])
    weights = weights.expand_dims({lon_dim: ds[lon_dim]})
    return weights


class Metric(ABC):
    """Abstract base class for metrics.

    Based on the implementation in WeatherBenchX, a metric is defined
    in terms of statistics and final computation.

    Metrics defined here are automatically registered with the metric registry, and can be used by the grouped_metric
    function in the metrics.py file by setting metric equal to the name of the metric class in lower camel case.
    For example, by defining a metric class MAE here, the grouped_metric function in the metrics.py file can be
    called with metric = 'mae'.

    If you want to sum instead of mean the statistics, you can set self.statistics = [('mae', 'sum')].

    If a metric depends on a non-linear calculation involving multiple statistics, simply define those statistics
    in a list, e.g,.
        self.statistics = ['squared_fcst_anom', 'squared_obs_anom', 'anom_covariance']
    Again, this assumes each of the statistics is implemented by the global_statistic function. The metric
    will be provided with the mean value of each statistic in each grouping at runtime to operate on and return
    one metric value per grouping.
    """
    def __init_subclass__(cls, **kwargs):
        """Automatically register derived Metrics classes with the metric registry."""
        super().__init_subclass__(**kwargs)
        # Register this metric class with the registry
        cls.name = cls.__name__.lower()
        SHEERWATER_METRIC_REGISTRY[cls.name] = cls

    def __init__(self, start_time, end_time, variable, lead, forecast, truth, bin_str,
                 time_grouping=None, spatial=False, grid="global1_5",
                 mask='lsm', region='global'):
        """Initialize the metric."""
        # Save the variables needed for spatial grouping
        self.variable = variable
        self.region = region
        self.spatial = spatial
        self.forecast = forecast
        self.time_grouping = time_grouping
        self.mask = mask
        self.grid = grid
        self.bin_str = bin_str
        self.bins = get_bins(bin_str)

        # Save the kwargs needed for calling the statistics
        self.statistic_kwargs = {
            'start_time': start_time,
            'end_time': end_time,
            'variable': variable,
            'lead': lead,
            'forecast': forecast,
            'truth': truth,
            'bin_str': self.bin_str,
            'grid': grid,
        }

        # Prepare the forecasting, observation, and auxiliary data for the metric
        self.fcst, \
            self.obs, \
            self.aux_data = self.prepare_data(
                start_time, end_time, variable, lead, forecast, truth, grid)

        # Convert bin string to numpy bin thresholds
        self.aux_data['bins'] = self.bins

    def prepare_data(self, start_time, end_time, variable, lead, forecast, truth, grid):
        """Prepare the data for the metric."""
        # Auxiliary data for metric calculation
        aux_data = {}

        # Get the forecast and check validity
        fcst_fn = get_datasource_fn(forecast)

        # Decide if this is a forecast with a lead or direct datasource with just an agg
        # Enables the same code to be used for both forecasts and truth sources
        sparse = False  # A variable used to indicate whether the statistic is expected to be sparse
        if 'lead' in signature(fcst_fn).parameters:
            # TODO: this is no longer clearly true, since we can pass in daily, weekly to forecasters
            # TODO: do we still want this check
            # if lead_or_agg(lead) == 'agg':
            #     raise ValueError("Evaluating the function {forecast} must be called with a lead, not an aggregation")
            fcst = fcst_fn(start_time, end_time, variable, lead=lead,
                           prob_type=self.prob_type, grid=grid, mask=None, region='global')
            # Check to see the prob type attribute
            enhanced_prob_type = fcst.attrs['prob_type']
        else:
            # TODO: do we still want this check?
            # if lead_or_agg(lead) == 'lead':
            #     raise "Evaluating the function {forecast} must be called with an aggregation, but not at a lead."
            fcst = fcst_fn(start_time, end_time, variable, agg_days=get_lead_info(lead)['agg_days'],
                           grid=grid, mask=None, region='global')
            # Prob type is always deterministic for truth sources
            enhanced_prob_type = "deterministic"

        # Make sure the prob type is consistent
        if enhanced_prob_type == 'deterministic' and self.prob_type == 'probabilistic':
            raise ValueError("Cannot run probabilistic metric on deterministic forecasts.")
        elif (enhanced_prob_type == 'ensemble' or enhanced_prob_type == 'quantile') \
                and self.prob_type == 'deterministic':
            raise ValueError("Cannot run deterministic metric on probabilistic forecasts.")
        aux_data['prob_type'] = enhanced_prob_type

        # If the metric is sparse, save a copy of the original forecast for validity checking
        if self.sparse:
            self.fcst_orig = fcst.copy()

        # Assign sparsity if it exists
        if 'sparse' in fcst.attrs:
            sparse = fcst.attrs['sparse']

        # Get the truth to compare against
        truth_fn = get_datasource_fn(truth)
        obs = truth_fn(start_time, end_time, variable, agg_days=get_lead_info(lead)['agg_days'],
                       grid=grid, mask=None, region='global')

        # We need a lead specific obs, so we know which times are valid for the forecast
        lead_labels = get_lead_info(lead)['labels']
        obs = obs.expand_dims({'lead_time': lead_labels})

        # Assign sparsity if it exists
        if 'sparse' in obs.attrs:
            sparse |= obs.attrs['sparse']

        # Pack the final data for the statistics library
        aux_data['sparse'] = sparse

        # Drop all times not in fcst
        valid_times = set(obs.time.values).intersection(set(fcst.time.values))
        valid_times = list(valid_times)
        valid_times.sort()
        obs = obs.sel(time=valid_times)
        fcst = fcst.sel(time=valid_times)

        # Ensure a matching null pattern
        # If the observations are sparse, the forecaster and the obs must be the same length
        # for metrics like ACC to work
        no_null = obs.notnull() & fcst.notnull()
        if 'member' in no_null.dims:
            # TODO: This will probably break with sparse forecaster and dense observations
            # Drop all vars except lat, lon, time, and lead_time from no_null
            # Squeeze the member dimension. A note, things like ACC won't work well across members
            no_null = no_null.isel(member=0).drop('member')
            # Drop all other coords except lat, lon, time, and lead_time
            no_null = no_null.drop_vars([var for var in no_null.coords if var not in [
                                        'lat', 'lon', 'time', 'lead_time']], errors='ignore')
        fcst = fcst.where(no_null, np.nan, drop=False)
        obs = obs.where(no_null, np.nan, drop=False)

        # For the case where obs and forecast are datetime objects, do a special conversion to seconds since epoch
        # TODO: This is a hack to get around the fact that the metrics library doesn't support datetime objects
        if np.issubdtype(obs[variable].dtype, np.datetime64) or (obs[variable].dtype == np.dtype('<M8[ns]')):
            # Forecast must be datetime64
            assert np.issubdtype(fcst[variable].dtype, np.datetime64) or (fcst[variable].dtype == np.dtype('<M8[ns]'))
            obs = obs.astype('int64') / 1e9
            fcst = fcst.astype('int64') / 1e9
            # NaT get's converted to -9.22337204e+09, so filter that to a proper nan
            obs = obs.where(obs > -1e9, np.nan)
            fcst = fcst.where(fcst > -1e9, np.nan)

        if self.name == 'acc':
            # Get the appropriate climatology dataframe for metric calculation
            clim_ds = climatology_2020(start_time, end_time, variable, lead=lead, prob_type='deterministic',
                                       grid=grid, mask=None, region='global')
            clim_ds = clim_ds.sel(time=valid_times)
            clim_ds = clim_ds.where(no_null, np.nan, drop=False)
            aux_data['climatology'] = clim_ds.copy()
        elif self.name == 'seeps':
            aux_data['wet_threshold'] = seeps_wet_threshold(
                first_year=1991, last_year=2020, agg_days=get_lead_info(lead)['agg_days'], grid=grid)
            aux_data['dry_fraction'] = seeps_dry_fraction(
                first_year=1991, last_year=2020, agg_days=get_lead_info(lead)['agg_days'], grid=grid)

        return fcst, obs, aux_data

    @property
    @abstractmethod
    def sparse(self) -> bool:
        """Whether the metric induces NaNs."""
        pass

    @property
    @abstractmethod
    def prob_type(self) -> str:
        """Either 'deterministic' or 'probabilistic'."""
        pass

    @property
    @abstractmethod
    def valid_variables(self) -> list[str]:
        """What variables is the metric valid for?"""
        pass

    @property
    @abstractmethod
    def categorical(self) -> bool:
        """Is the metric categorical?"""
        pass

    @property
    def statistics(self) -> list[str]:
        """List of statistics that the metric is computed from."""
        pass

    def gather_statistics(self) -> dict[str, xr.DataArray]:
        """Gather the statistics by the metric's configuration.

        By default, returns the statistic values as is.
        Subclasses can override this for more complex groupings.
        """
        self.statistic_values = {}
        for statistic in self.statistics:
            # Statistics can be a tuple of (statistic, agg_fn), or just a statistic with default mean agg
            if isinstance(statistic, tuple):
                statistic, agg_fn = statistic
            else:
                agg_fn = 'mean'

            if 'n_obs_bin' in statistic or 'n_fcst_bin' in statistic:
                # Remove the category from the statistic name
                category = int(statistic.split('_')[3])
                statistic_name = statistic.replace(f'_{category}', '')
                self.aux_data['category'] = category
            else:
                statistic_name = statistic

            stat_fn = statistic_factory(statistic_name)
            ds = stat_fn(self.fcst, self.obs, self.aux_data, **self.statistic_kwargs)
            if ds is None:
                # If any statistic is None, return None
                self.statistic_values = None
                return
            self.statistic_values[statistic] = (ds.copy(), agg_fn)

    def group_statistics(self) -> dict[str, xr.DataArray]:
        """Group the statistics by the metric's configuration.

        By default, returns the statistic values as is.
        Subclasses can override this for more complex groupings.
        """
        self.grouped_statistics = {}
        # Get admin level for grouping
        admin_level, is_admin = get_admin_level(self.region)
        if is_admin and self.spatial:
            raise ValueError(f"Cannot compute spatial metrics for admin level '{self.region}'. " +
                             "Pass in a specific region instead.")
        region_ds = region_labels(grid=self.grid, admin_level=admin_level)
        mask_ds = get_mask(self.mask, self.grid)

        # Get time level for grouping
        time_level = get_time_level(self.time_grouping)

        # Iterate through the statistics and compute them
        for statistic in self.statistics:
            ############################################################
            # Aggregate and and check validity of the statistic
            ############################################################
            ds, agg_fn = self.statistic_values[statistic]
            data_sparse = ds.attrs['sparse']  # Whether the input data to the statistic is expected to be sparse

            # Drop any extra random coordinates that shouldn't be there
            for coord in ds.coords:
                if coord not in ['time', 'lead_time', 'lat', 'lon']:
                    ds = ds.reset_coords(coord, drop=True)

            # Prepare the check_ds for validity checking, considering sparsity
            if data_sparse or self.sparse:
                print("Metric is sparse, need to check the underlying forecast validity directly.")
                check_ds = self.fcst_orig.copy()
            else:
                check_ds = ds.copy()
            # Create a non_null indicator and add it to the statistic
            ds['non_null'] = check_ds[self.variable].notnull().astype(float)
            ############################################################
            # Statistic aggregation
            ############################################################
            # Group by time
            ds = groupby_time(ds, time_level, agg_fn=agg_fn)

            # For any lat / lon / lead where there is at least one non-null value, reset to one for space validation
            ds['non_null'] = (ds['non_null'] > 0.0).astype(float)
            # Create an indicator variable that is 1 for all dimensions
            ds['indicator'] = xr.ones_like(ds['non_null'])

            # Add the region coordinate to the statistic
            ds = ds.assign_coords(region=region_ds.region)

            # Aggregate in space
            if not self.spatial:
                if agg_fn == 'mean':
                    # Group by region and average in space, while applying weighting for mask
                    # and latitudes
                    weights = latitude_weights(ds, lat_dim='lat', lon_dim='lon')
                    ds['weights'] = weights * mask_ds.mask
                else:
                    ds['weights'] = mask_ds.mask

                ds[self.variable] = ds[self.variable] * ds['weights']
                ds['non_null'] = ds['non_null'] * mask_ds.mask
                ds['indicator'] = ds['indicator'] * mask_ds.mask

                ds = ds.groupby('region').apply(mean_or_sum, agg_fn='sum', dims='stacked_lat_lon')
                if agg_fn == 'mean':
                    # Normalize by weights
                    ds[self.variable] = ds[self.variable] / ds['weights']
                ds = ds.drop_vars(['weights'])
            else:
                # If returning a spatial metric, mask and drop
                # Mask and drop the region coordinate
                ds = ds.where(mask_ds.mask, np.nan, drop=False)
                ds = ds.where((ds.region == self.region).compute(), drop=True)
                ds = ds.drop_vars('region')

            # Check if the statistic is valid per grouping
            is_valid = (ds['non_null'] / ds['indicator'] > 0.98)
            ds = ds.where(is_valid, np.nan, drop=False)
            ds = ds.drop_vars(['indicator', 'non_null'])

            # Assign the final statistic value
            self.grouped_statistics[statistic] = ds.copy()

    def compute_metric(self) -> xr.DataArray:
        """Compute the metric from the statistics.

        By default, returns the single statistic value.
        Subclasses can override this for more complex computations.
        """
        assert len(self.statistics) == 1, "Metric must have exactly one statistic to use default compute."
        return self.grouped_statistics[self.statistics[0]]

    def compute(self) -> xr.DataArray:
        # Check that the variable is valid for the metric
        if self.valid_variables and self.variable not in self.valid_variables:
            raise ValueError(f"Variable {self.variable} is not valid for metric {self.name}")

        # Gather the statistics
        self.gather_statistics()
        # Group and mean the statistics
        self.group_statistics()
        # Apply nonlinearly and return the metric
        return self.compute_metric()


class MAE(Metric):
    """Mean Absolute Error metric."""
    sparse = False
    prob_type = 'deterministic'
    valid_variables = None  # all variables are valid
    categorical = False
    statistics = ['mae']


class MSE(Metric):
    """Mean Squared Error metric."""
    sparse = False
    prob_type = 'deterministic'
    valid_variables = None  # all variables are valid
    categorical = False
    statistics = ['mse']


class RMSE(Metric):
    """Root Mean Squared Error metric."""
    sparse = False
    prob_type = 'deterministic'
    valid_variables = None  # all variables are valid
    categorical = False
    statistics = ['mse']

    def compute_metric(self):
        return self.grouped_statistics['mse'] ** 0.5


class Bias(Metric):
    """Bias metric."""
    sparse = False
    prob_type = 'deterministic'
    valid_variables = None  # all variables are valid
    categorical = False
    statistics = ['bias']


class CRPS(Metric):
    """Continuous Ranked Probability Score metric."""
    sparse = False
    prob_type = 'probabilistic'
    valid_variables = None  # all variables are valid
    categorical = False
    statistics = ['crps']


class Brier(Metric):
    """Brier score metric."""
    sparse = False
    prob_type = 'probabilistic'
    valid_variables = ['precip']
    categorical = True
    statistics = ['brier']


class SMAPE(Metric):
    """Symmetric Mean Absolute Percentage Error metric."""
    sparse = False
    prob_type = 'deterministic'
    valid_variables = ['precip']
    categorical = False
    statistics = ['smape']


class MAPE(Metric):
    """Mean Absolute Percentage Error metric."""
    sparse = False
    prob_type = 'deterministic'
    valid_variables = ['precip']
    categorical = False
    statistics = ['mape']


class SEEPS(Metric):
    """Spatial Error in Ensemble Prediction Scale metric."""
    sparse = True
    prob_type = 'deterministic'
    valid_variables = ['precip']
    categorical = False
    statistics = ['seeps']


class ACC(Metric):
    """ACC (Anomaly Correlation Coefficient) metric."""
    sparse = False
    prob_type = 'deterministic'
    valid_variables = None
    categorical = False
    statistics = ['squared_fcst_anom', 'squared_obs_anom', 'anom_covariance']

    def compute_metric(self):
        gs = self.grouped_statistics
        fcst_norm = np.sqrt(gs['squared_fcst_anom'])
        gt_norm = np.sqrt(gs['squared_obs_anom'])
        dot = gs['anom_covariance']
        ds = (dot / (fcst_norm * gt_norm))
        return ds


class Pearson(Metric):
    """Pearson's correlation coefficient metric.

    Implemented with a rewrite of the standard formula to enable grouping first.

    The standard formula is:
    r = sum((x - E(x)) * (y - E(y))) / sqrt(sum(x - E(x))^2 * sum(y - E(y))^2)

    This can be rewritten as:
    r = (covariance - fcst_mean * obs_mean) / (sqrt(squared_fcst - fcst_mean^2) * sqrt(squared_obs - obs_mean^2))
    """
    sparse = False
    prob_type = 'deterministic'
    valid_variables = ['precip']
    categorical = False
    statistics = ['fcst', 'obs', 'squared_fcst', 'squared_obs', 'covariance']

    def compute_metric(self):
        gs = self.grouped_statistics
        numerator = gs['covariance'] - gs['fcst'] * gs['obs']
        denominator = (gs['squared_fcst'] - gs['fcst']**2) ** 0.5 * \
            (gs['squared_obs'] - gs['obs']**2) ** 0.5
        return numerator / denominator


class Heidke(Metric):
    """Heidke Skill Score metric for streaming data."""
    sparse = False
    prob_type = 'deterministic'
    valid_variables = ['precip']
    categorical = True

    @property
    def statistics(self):
        stats = ['n_correct', 'n_valid']
        stats += [f'n_fcst_bin_{i}' for i in range(1, len(self.bins))]
        stats += [f'n_obs_bin_{i}' for i in range(1, len(self.bins))]
        return stats

    def compute_metric(self):
        gs = self.grouped_statistics
        prop_correct = gs['n_correct'] / gs['n_valid']
        n2 = gs['n_valid']**2
        right_by_chance = (gs['n_fcst_bin_1'] * gs['n_obs_bin_1']) / n2
        for i in range(2, len(self.bins)):
            right_by_chance += (gs[f'n_fcst_bin_{i}'] * gs[f'n_obs_bin_{i}']) / n2

        return (prop_correct - right_by_chance) / (1 - right_by_chance)


class POD(Metric):
    """Probability of Detection metric."""
    sparse = True
    prob_type = 'deterministic'
    valid_variables = ['precip']
    categorical = True
    statistics = ['true_positives', 'false_negatives']

    def compute_metric(self):
        tp = self.grouped_statistics['true_positives']
        fn = self.grouped_statistics['false_negatives']
        return tp / (tp + fn)


class FAR(Metric):
    """False Alarm Rate metric."""
    sparse = True
    prob_type = 'deterministic'
    valid_variables = ['precip']
    categorical = True
    statistics = ['false_positives', 'true_negatives']

    def compute_metric(self):
        fp = self.grouped_statistics['false_positives']
        tn = self.grouped_statistics['true_negatives']
        return fp / (fp + tn)


class ETS(Metric):
    """Equitable Threat Score metric."""
    sparse = True
    prob_type = 'deterministic'
    valid_variables = ['precip']
    categorical = True
    statistics = ['true_positives', 'false_positives', 'false_negatives', 'true_negatives']

    def compute_metric(self):
        gs = self.grouped_statistics
        tp = gs['true_positives']
        fp = gs['false_positives']
        fn = gs['false_negatives']
        tn = gs['true_negatives']
        chance = ((tp + fp) * (tp + fn)) / (tp + fp + fn + tn)
        return (tp - chance) / (tp + fp + fn - chance)


class CSI(Metric):
    """Critical Success Index metric."""
    sparse = True
    prob_type = 'deterministic'
    valid_variables = ['precip']
    categorical = True
    statistics = ['true_positives', 'false_positives', 'false_negatives']

    def compute_metric(self):
        tp = self.grouped_statistics['true_positives']
        fp = self.grouped_statistics['false_positives']
        fn = self.grouped_statistics['false_negatives']
        return tp / (tp + fp + fn)


class FrequencyBias(Metric):
    """Frequency Bias metric."""
    sparse = True
    prob_type = 'deterministic'
    valid_variables = ['precip']
    categorical = True
    statistics = ['true_positives', 'false_positives', 'false_negatives']

    def compute_metric(self):
        tp = self.grouped_statistics['true_positives']
        fp = self.grouped_statistics['false_positives']
        fn = self.grouped_statistics['false_negatives']
        return (tp + fp) / (tp + fn)


def metric_factory(metric_name: str, **init_kwargs) -> Metric:
    """Get a metric class by name from the registry."""
    try:
        # Convert
        if '-' in metric_name:
            mn = metric_name.split('-')[0]  # support for metric names of the form 'metric-edge-edge...'
            bin_str = metric_name[metric_name.find('-')+1:]
        else:
            mn = metric_name
            bin_str = 'none'
        metric = SHEERWATER_METRIC_REGISTRY[mn.lower()]
        # Add runtime metric configuration to the metric class
        return metric(bin_str=bin_str, **init_kwargs)

    except KeyError:
        raise ValueError(f"Unknown metric: {metric_name}. Available metrics: {list_metrics()}")


def list_metrics():
    """List all available metrics in the registry."""
    return list(SHEERWATER_METRIC_REGISTRY.keys())
