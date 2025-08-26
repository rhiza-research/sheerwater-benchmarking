"""Library of metrics implementations for verification."""
# flake8: noqa: D102
from abc import ABC
import xarray as xr
import numpy as np

import weatherbench2
import xskillscore

# Global metric registry dictionary
SHEERWATER_METRIC_REGISTRY = {}


def get_stat_name(stat_name, metric_info):
    """Get the statistic name from the statistic-edge-edge... format for categorical statistics.

    Returns:
        name: the statistic name
        bin_str: the bins for the categorical statistic, e.g., '5' or '5-10'
    """
    if metric_info.categorical:
        return stat_name.split('-')[0], stat_name[stat_name.find('-')+1:]
    return stat_name, ''  # name, bin_str


def groupby_time(ds, time_grouping, agg_fn='mean'):
    """Aggregate a statistic over time."""
    if time_grouping is not None:
        if time_grouping == 'month_of_year':
            ds.coords["time"] = ds.time.dt.month
        elif time_grouping == 'year':
            ds.coords["time"] = ds.time.dt.year
        elif time_grouping == 'quarter_of_year':
            ds.coords["time"] = ds.time.dt.quarter
        else:
            raise ValueError("Invalid time grouping")
        if agg_fn == 'mean':
            ds = ds.groupby("time").mean()
        elif agg_fn == 'sum':
            ds = ds.groupby("time").sum()
        else:
            raise ValueError(f"Invalid aggregation function {agg_fn}")
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


def latitude_weighted_spatial_average(ds, lat_dim='lat', lon_dim='lon', agg_fn='mean'):
    """Compute latitude-weighted spatial average of a dataset.

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

    # Create weights array
    weights = ds[lat_dim].copy(data=weights)
    if agg_fn == 'mean':
        weighted = ds.weighted(weights).mean([lat_dim, lon_dim], skipna=True)
    else:
        weighted = ds.weighted(weights).sum([lat_dim, lon_dim], skipna=True)
    return weighted


def get_bins(metric_name):
    """Get the categorical bins for a metric name of the form 'metric-edge-edge...'.

    For example,
        'pod-5' returns [-np.inf, 5, np.inf]
        'pod-5-10' returns [-np.inf, 5, 10, np.inf]
    """
    bins = [int(x) for x in metric_name.split('-')[1:]]
    bins = [-np.inf] + bins + [np.inf]
    bins = np.array(bins)
    return bins


def compute_statistic(stat_data):
    """Compute a statistic from a dictionary of precompiled data.

    Stat data is populated by the global_statistic function and will contain:

    stat_data = {
        'statistic': the statistic name
        'prob_type': the probability type
        'obs': the observations
        'fcst': the forecast
        'fcst_digitized': the forecast digitized
        'obs_digitized': the observations digitized
        'fcst_anom': the forecast anomaly
        'obs_anom': the observations anomaly
        'statistic_kwargs': statistic kwargs needed for some statistics, e.g., SEEPS
    }
    """
    stat_name = stat_data['statistic']
    prob_type = stat_data['prob_type']

    # Get the appropriate climatology dataframe for metric calculation
    if stat_name == 'target':
        m_ds = stat_data['obs']
    elif stat_name == 'pred':
        m_ds = stat_data['fcst']
    elif stat_name == 'brier' and prob_type == 'ensemble':
        fcst_event_prob = (stat_data['fcst_digitized'] == 2).mean(dim='member')
        obs_event_prob = (stat_data['obs_digitized'] == 2)
        m_ds = (fcst_event_prob - obs_event_prob)**2
        # TODO implement brier for quantile forecasts
    elif stat_name == 'seeps':
        m_ds = weatherbench2.metrics.SpatialSEEPS(**stat_data['statistic_kwargs']) \
                            .compute(forecast=stat_data['fcst'], truth=stat_data['obs'],
                                     avg_time=False, skipna=True)
        m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    elif stat_name == 'squared_pred_anom':
        m_ds = stat_data['fcst_anom']**2
    elif stat_name == 'squared_target_anom':
        m_ds = stat_data['obs_anom']**2
    elif stat_name == 'anom_covariance':
        m_ds = stat_data['fcst_anom'] * stat_data['obs_anom']
    elif stat_name == 'false_positives':
        m_ds = (stat_data['obs_digitized'] == 1) & (stat_data['fcst_digitized'] == 2)
    elif stat_name == 'false_negatives':
        m_ds = (stat_data['obs_digitized'] == 2) & (stat_data['fcst_digitized'] == 1)
    elif stat_name == 'true_positives':
        m_ds = (stat_data['obs_digitized'] == 2) & (stat_data['fcst_digitized'] == 2)
    elif stat_name == 'true_negatives':
        m_ds = (stat_data['obs_digitized'] == 1) & (stat_data['fcst_digitized'] == 1)
    elif stat_name == 'digitized_obs':
        m_ds = stat_data['obs_digitized']
    elif stat_name == 'digitized_fcst':
        m_ds = stat_data['fcst_digitized']
    elif stat_name == 'squared_pred':
        m_ds = stat_data['fcst']**2
    elif stat_name == 'squared_target':
        m_ds = stat_data['obs']**2
    elif stat_name == 'pred_mean':
        m_ds = stat_data['fcst']
    elif stat_name == 'target_mean':
        m_ds = stat_data['obs']
    elif stat_name == 'covariance':
        m_ds = stat_data['fcst'] * stat_data['obs']
    elif stat_name == 'crps' and prob_type == 'ensemble':
        fcst = stat_data['fcst'].chunk(member=-1, time=1, lat=250, lon=250)  # member must be -1 to succeed
        m_ds = xskillscore.crps_ensemble(observations=stat_data['obs'], forecasts=fcst, mean=False, dim='time')
    elif stat_name == 'crps' and prob_type == 'quantile':
        m_ds = weatherbench2.metrics.SpatialQuantileCRPS(quantile_dim='member') \
                            .compute(forecast=stat_data['fcst'], truth=stat_data['obs'],
                                     avg_time=False, skipna=True)
        m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    elif stat_name == 'mape':
        m_ds = abs(stat_data['fcst'] - stat_data['obs']) / np.maximum(abs(stat_data['obs']), 1e-10)
    elif stat_name == 'smape':
        m_ds = abs(stat_data['fcst'] - stat_data['obs']) / (abs(stat_data['fcst']) + abs(stat_data['obs']))
    elif stat_name == 'mae':
        m_ds = abs(stat_data['fcst'] - stat_data['obs'])
    elif stat_name == 'mse':
        m_ds = (stat_data['fcst'] - stat_data['obs'])**2
    elif stat_name == 'bias':
        m_ds = stat_data['fcst'] - stat_data['obs']
    elif stat_name == 'n_valid':
        m_ds = xr.ones_like(stat_data['fcst'])
    else:
        raise ValueError(f"Statistic {stat_name} not implemented")
    return m_ds


def metric_factory(metric_name):
    """Get a metric class by name from the registry."""
    try:
        mn = metric_name.split('-')[0]  # support for metric names of the form 'metric-edge-edge...'
        metric = SHEERWATER_METRIC_REGISTRY[mn.lower()]
        # Add runtime metric configuration to the metric class
        if metric.categorical:
            metric.config_dict['bins'] = get_bins(metric_name)
        return metric

    except KeyError:
        raise ValueError(f"Unknown metric: {metric_name}. Available metrics: {list_metrics()}")


def list_metrics():
    """List all available metrics in the registry."""
    return list(SHEERWATER_METRIC_REGISTRY.keys())


class Metric(ABC):
    """Abstract base class for metrics.

    Based on the implementation in WeatherBenchX, a metric is defined
    in terms of statistics and final computation.

    All statistics are returned as means, unless otherwise specified in the metric.statistics property.
    Valid options are: 'mean', 'sum'
    """
    sparse = False  # does the metric induce NaNs
    prob_type = 'deterministic'  # is the forecast probabilistic?
    valid_variables = None  # what variables is the metric valid for?
    categorical = False  # is the metric categorical?
    coupled = False  # is the metric coupled?
    config_dict = {}  # a dictionary of configuration parameters for the metric

    def __init_subclass__(cls, **kwargs):
        """Automatically register derived Metrics classes with the metric registry."""
        super().__init_subclass__(**kwargs)
        # Register this metric class with the registry
        SHEERWATER_METRIC_REGISTRY[cls.__name__.lower()] = cls

    @property
    def statistics(self) -> list[str]:
        """List of statistics that the metric is computed from.

        By default, returns a list with the metric name converted to lowercase.
        Subclasses can override this to provide different statistics.
        """
        # Convert class name to lowercase metric name
        # e.g., MSE-> 'mse', Bias-> 'bias'
        metric_name = self.__class__.__name__.lower()
        return [metric_name]

    def compute(self, statistic_values: dict[str, xr.DataArray]) -> xr.DataArray:
        """Compute the metric from the statistics.

        By default, returns the single statistic value.
        Subclasses can override this for more complex computations.
        """
        # Get the default statistic name
        default_stat = self.statistics[0]
        return statistic_values[default_stat]


class MSE(Metric):
    """Mean Squared Error metric."""


class MAE(Metric):
    """Mean Absolute Error metric."""


class RMSE(Metric):
    """Root Mean Squared Error metric."""

    @property
    def statistics(self) -> list[str]:
        return ['mse']

    def compute(self, statistic_values):
        return statistic_values['mse'] ** 0.5


class Bias(Metric):
    """Bias metric."""


class CRPS(Metric):
    """Continuous Ranked Probability Score metric."""
    prob_type = 'probabilistic'


class Brier(Metric):
    """Brier score metric."""
    prob_type = 'probabilistic'
    valid_variables = ['precip']
    categorical = True


class SMAPE(Metric):
    """Symmetric Mean Absolute Percentage Error metric."""
    valid_variables = ['precip']


class MAPE(Metric):
    """Mean Absolute Percentage Error metric."""
    valid_variables = ['precip']


class SEEPS(Metric):
    """Spatial Error in Ensemble Prediction Scale metric."""
    sparse = True
    valid_variables = ['precip']


class ACC(Metric):
    """ACC (Anomaly Correlation Coefficient) metric."""

    @property
    def statistics(self):
        return ['squared_pred_anom', 'squared_target_anom', 'anom_covariance']

    def compute(self, statistic_values):
        fcst_norm = np.sqrt(statistic_values['squared_pred_anom'])
        gt_norm = np.sqrt(statistic_values['squared_target_anom'])
        dot = statistic_values['anom_covariance']
        ds = (dot / (fcst_norm * gt_norm))
        return ds


class Pearson(Metric):
    """Pearson's correlation coefficient metric.

    Implemented with a rewrite of the standard formula to enable just-in-time aggregation.

    The standard formula is:
    r = sum((x - E(x)) * (y - E(y))) / sqrt(sum(x - E(x))^2 * sum(y - E(y))^2)

    This can be rewritten as:
    r = (covariance - pred_mean * target_mean) / (sqrt(squared_pred - pred_mean^2) * sqrt(squared_target - target_mean^2))
    """
    @property
    def statistics(self):
        return ['pred_mean', 'target_mean', 'squared_pred', 'squared_target', 'covariance']

    def compute(self, statistic_values):
        numerator = statistic_values['covariance'] - statistic_values['pred_mean'] * statistic_values['target_mean']
        denominator = (statistic_values['squared_pred'] - statistic_values['pred_mean']**2) ** 0.5 * \
            (statistic_values['squared_target'] - statistic_values['target_mean']**2) ** 0.5
        return numerator / denominator


# class Pearson_old(Metric):
#     """Pearson's correlation coefficient metric, coupled implementation."""
#     coupled = True

#     @property
#     def statistics(self):
#         return ['target', 'pred']

#     def compute(self, statistic_values):
#         from xskillscore import pearson_r
#         fcst = statistic_values['pred']
#         obs = statistic_values['target']
#         spatial = statistic_values['spatial']
#         fcst = fcst.chunk(time=-1, lat=-1, lon=-1)  # all must be -1 to succeed
#         obs = obs.chunk(time=-1, lat=-1, lon=-1)  # all must be -1 to succeed
#         if spatial:
#             ds = pearson_r(a=obs, b=fcst, dim='time', skipna=True)
#         else:
#             ds = pearson_r(a=obs, b=fcst, skipna=True)
#         return ds


class Heidke(Metric):
    """Heidke Skill Score metric. TODO: considered an uncoupled implementation."""
    coupled = True
    valid_variables = ['precip']
    categorical = True

    @property
    def statistics(self):
        return ['target', 'pred']

    def compute(self, statistic_values):
        from xskillscore import Contingency
        fcst = statistic_values['pred']
        obs = statistic_values['target']
        spatial = statistic_values['spatial']
        bins = statistic_values['bins']
        dims = ['time'] if spatial else ['time', 'lat', 'lon']
        contingency_table = Contingency(obs, fcst, bins, bins, dim=dims)
        m_ds = contingency_table.heidke_score()
        return m_ds


class POD(Metric):
    """Probability of Detection metric."""
    valid_variables = ['precip']
    categorical = True

    @property
    def statistics(self):
        return ['true_positives', 'false_negatives']

    def compute(self, statistic_values):
        tp = statistic_values['true_positives']
        fn = statistic_values['false_negatives']
        return tp / (tp + fn)


class FAR(Metric):
    """False Alarm Rate metric."""
    valid_variables = ['precip']
    categorical = True

    @property
    def statistics(self):
        return ['false_positives', 'true_negatives']

    def compute(self, statistic_values):
        fp = statistic_values['false_positives']
        tn = statistic_values['true_negatives']
        return fp / (fp + tn)


class ETS(Metric):
    """Equitable Threat Score metric."""
    valid_variables = ['precip']
    categorical = True

    @property
    def statistics(self):
        return ['true_positives', 'false_positives', 'false_negatives', 'true_negatives']

    def compute(self, statistic_values):
        tp = statistic_values['true_positives']
        fp = statistic_values['false_positives']
        fn = statistic_values['false_negatives']
        tn = statistic_values['true_negatives']
        chance = ((tp + fp) * (tp + fn)) / (tp + fp + fn + tn)
        return (tp - chance) / (tp + fp + fn - chance)


class CSI(Metric):
    """Critical Success Index metric."""
    valid_variables = ['precip']
    categorical = True

    @property
    def statistics(self):
        return ['true_positives', 'false_positives', 'false_negatives']

    def compute(self, statistic_values):
        tp = statistic_values['true_positives']
        fp = statistic_values['false_positives']
        fn = statistic_values['false_negatives']
        return tp / (tp + fp + fn)


class FrequencyBias(Metric):
    """Frequency Bias metric."""
    valid_variables = ['precip']
    categorical = True

    @property
    def statistics(self):
        return ['true_positives', 'false_positives', 'false_negatives']

    def compute(self, statistic_values):
        tp = statistic_values['true_positives']
        fp = statistic_values['false_positives']
        fn = statistic_values['false_negatives']
        return (tp + fp) / (tp + fn)
