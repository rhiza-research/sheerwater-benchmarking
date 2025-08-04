
from abc import ABC
import xarray as xr
import numpy as np

# Global metric registry dictionary
SHEERWATER_METRIC_REGISTRY = {}


def get_bins(metric_name):
    """Get the categorical bins for a metric name of the form 'metric-edge-edge...'

    For example,
        'pod-5' returns [-np.inf, 5, np.inf]
        'pod-5-10' returns [-np.inf, 5, 10, np.inf]
    """
    bins = [int(x) for x in metric_name.split('-')[1:]]
    bins = [-np.inf] + bins + [np.inf]
    bins = np.array(bins)
    return bins


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
    """
    sparse = False
    prob_type = 'deterministic'
    valid_variables = None
    categorical = False
    config_dict = {}

    def __init__(self, sparse=None, prob_type=None, valid_variables=None, config_dict=None):
        """Initialize the metric.

        Args:
            sparse (bool): Whether the metric is sparse.
            prob_type (str): The type of probability distribution of the forecast.
            valid_variables (list[str]): The variables that the metric is valid for.
                If None, the metric is valid for all variables.
            config_dict (dict): A dictionary of configuration parameters for the metric.
                For example, the bins for a contingency metric.

        """
        if sparse is not None:
            self.sparse = sparse
        if prob_type is not None:
            self.prob_type = prob_type
        if valid_variables is not None:
            self.valid_variables = valid_variables
        if config_dict is not None:
            self.config_dict = config_dict

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

    def compute(self, statistic_values: dict[str, xr.DataArray]) -> xr.DataArray:
        return statistic_values['mse'] ** 0.5


class Bias(Metric):
    """Bias metric."""


class CRPS(Metric):
    """Continuous Ranked Probability Score metric."""
    prob_type = 'probabilistic'


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
        # ACC = anom_covariance / sqrt(squared_pred_anom * squared_target_anom)
        # numerator = statistic_values['anom_covariance']
        # denominator = (statistic_values['squared_pred_anom'] * statistic_values['squared_target_anom']) ** 0.5
        # return numerator / denominator

        # fcst_norm = np.sqrt((forecast**2).mean(dim=avg_dim, skipna=skipna))
        # gt_norm = np.sqrt((truth**2).mean(dim=avg_dim, skipna=skipna))
        # dot = (forecast * truth).mean(dim=avg_dim, skipna=skipna)
        # ds = (dot / (fcst_norm * gt_norm))

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
    r = (n * sum(x * y) - sum(x) * sum(y)) / sqrt((n * sum(x^2) - sum(x)^2) * (n * sum(y^2) - sum(y)^2))
    """
    @property
    def statistics(self):
        return ['n_valid', 'pred_mean', 'target_mean', 'squared_pred', 'squared_target', 'covariance']

    def compute(self, statistic_values):
        # Pearson's r = covariance / sqrt(squared_pred * squared_target)
        numerator = statistic_values['n_valid'] * statistic_values['covariance'] - \
            statistic_values['pred_mean'] * statistic_values['target_mean']
        denominator = (statistic_values['n_valid'] * statistic_values['squared_pred'] - statistic_values['pred_mean']**2) ** 0.5 * \
            (statistic_values['n_valid'] * statistic_values['squared_target'] -
             statistic_values['target_mean']**2) ** 0.5
        return numerator / denominator


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


class Heidke(Metric):
    """Heidke Skill Score metric."""
    valid_variables = ['precip']
    categorical = True

    @property
    def statistics(self):
        return ['hits', 'misses']

    def compute(self, statistic_values):
        return statistic_values['hits'] / (statistic_values['hits'] + statistic_values['misses'])


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
