"""Library of metrics implementations for verification."""
# flake8: noqa: D102
from abc import ABC
import xarray as xr
import numpy as np

# Global metric registry dictionary
SHEERWATER_METRIC_REGISTRY = {}


def get_bins(metric_name: str) -> np.ndarray:
    """Get the categorical bins for a metric name of the form 'metric-edge-edge...'.

    For example,
        'pod-5' returns [-np.inf, 5, np.inf]
        'pod-5-10' returns [-np.inf, 5, 10, np.inf]
    """
    bins = [int(x) for x in metric_name.split('-')[1:]]
    bins = [-np.inf] + bins + [np.inf]
    bins = np.array(bins)
    return bins


class Metric(ABC):
    """Abstract base class for metrics.

    Based on the implementation in WeatherBenchX, a metric is defined
    in terms of statistics and final computation.

    Metrics defined here are automatically registered with the metric registry, and can be used by the grouped_metric
    function in the metrics.py file by setting metric equal to the name of the metric class in lower camel case.
    For example, by defining a metric class MAE here, the grouped_metric function in the metrics.py file can be
    called with metric = 'mae'.

    By default, each metric will return the mean value of the global statistic of the same name as the metric, 
    converted to lower camel case. So the metric MAE will, by default, return the mean of the global statistic 'mae',
    as defined by the global_statistic function. This can be configured explicitly by, and is the same as, 
    setting self.statistics = ['mae']. 

    If you want to sum instead of mean the statistics, you can set self.statistics = [('mae', 'sum')].

    If a metric depends on a non-linear calculation involving multiple statistics, simply define those statistics
    in a list, e.g,. 
        self.statistics = ['squared_fcst_anom', 'squared_obs_anom', 'anom_covariance']
    Again, this assumes each of the statistics is implemented by the global_statistic function. The metric
    will be provided with the mean value of each statistic in each grouping at runtime to operate on and return
    one metric value per grouping.
    """
    sparse = False  # does the metric induce NaNs
    prob_type = 'deterministic'  # is the forecast probabilistic?
    valid_variables = None  # what variables is the metric valid for?
    categorical = False  # is the metric categorical?
    config_dict = {}  # a dictionary of configuration parameters for the metric, e.g., bins

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
        return ['squared_fcst_anom', 'squared_obs_anom', 'anom_covariance']

    def compute(self, statistic_values):
        fcst_norm = np.sqrt(statistic_values['squared_fcst_anom'])
        gt_norm = np.sqrt(statistic_values['squared_obs_anom'])
        dot = statistic_values['anom_covariance']
        ds = (dot / (fcst_norm * gt_norm))
        return ds


class Pearson(Metric):
    """Pearson's correlation coefficient metric.

    Implemented with a rewrite of the standard formula to enable just-in-time aggregation.

    The standard formula is:
    r = sum((x - E(x)) * (y - E(y))) / sqrt(sum(x - E(x))^2 * sum(y - E(y))^2)

    This can be rewritten as:
    r = (covariance - fcst_mean * obs_mean) / (sqrt(squared_fcst - fcst_mean^2) * sqrt(squared_obs - obs_mean^2))
    """
    @property
    def statistics(self):
        return ['fcst_mean', 'obs_mean', 'squared_fcst', 'squared_obs', 'covariance']

    def compute(self, statistic_values):
        numerator = statistic_values['covariance'] - statistic_values['fcst_mean'] * statistic_values['obs_mean']
        denominator = (statistic_values['squared_fcst'] - statistic_values['fcst_mean']**2) ** 0.5 * \
            (statistic_values['squared_obs'] - statistic_values['obs_mean']**2) ** 0.5
        return numerator / denominator


# class Pearson_old(Metric):
#     """Pearson's correlation coefficient metric, coupled implementation."""
#     coupled = True

#     @property
#     def statistics(self):
#         return ['obs', 'fcst']

#     def compute(self, statistic_values):
#         from xskillscore import pearson_r
#         fcst = statistic_values['fcst']
#         obs = statistic_values['obs']
#         spatial = statistic_values['spatial']
#         fcst = fcst.chunk(time=-1, lat=-1, lon=-1)  # all must be -1 to succeed
#         obs = obs.chunk(time=-1, lat=-1, lon=-1)  # all must be -1 to succeed
#         if spatial:
#             ds = pearson_r(a=obs, b=fcst, dim='time', skipna=True)
#         else:
#             ds = pearson_r(a=obs, b=fcst, skipna=True)
#         return ds


class Heidke(Metric):
    """Heidke Skill Score metric for streaming data."""
    valid_variables = ['precip']
    categorical = True

    @property
    def statistics(self):
        stats = [('n_correct', 'sum'), ('n_valid', 'sum')]
        stats += [(f'n_fcst_bin_{i}', 'sum') for i in range(1, len(self.config_dict['bins']))]
        stats += [(f'n_obs_bin_{i}', 'sum') for i in range(1, len(self.config_dict['bins']))]
        return stats

    def compute(self, statistic_values):
        prop_correct = statistic_values['n_correct'] / statistic_values['n_valid']
        n2 = statistic_values['n_valid']**2
        right_by_chance = (statistic_values[f'n_fcst_bin_1'] * statistic_values[f'n_obs_bin_1']) / n2
        for i in range(2, len(self.config_dict['bins'])):
            right_by_chance += (statistic_values[f'n_fcst_bin_{i}'] * statistic_values[f'n_obs_bin_{i}']) / n2

        return (prop_correct - right_by_chance) / (1 - right_by_chance)


# class Heidke_old(Metric):
#     """Heidke Skill Score metric. TODO: considered an uncoupled implementation."""
#     coupled = True
#     valid_variables = ['precip']
#     categorical = True

#     @property
#     def statistics(self):
#         return ['obs', 'fcst']

#     def compute(self, statistic_values):
#         from xskillscore import Contingency
#         fcst = statistic_values['fcst']
#         obs = statistic_values['obs']
#         spatial = statistic_values['spatial']
#         bins = statistic_values['bins']
#         dims = ['time'] if spatial else ['time', 'lat', 'lon']
#         contingency_table = Contingency(obs, fcst, bins, bins, dim=dims)
#         m_ds = contingency_table.heidke_score()
#         return m_ds


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


def metric_factory(metric_name: str) -> Metric:
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
