
from abc import ABC, abstractmethod
import xarray as xr

# Global metric registry dictionary
SHEERWATER_METRIC_REGISTRY = {}


def metric_factory(metric_name):
    """Get a metric class by name from the registry."""
    try:
        return SHEERWATER_METRIC_REGISTRY[metric_name.lower()]
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

    def __init__(self, sparse=False, prob_type='deterministic',
                 valid_variables=None, config_dict=None):
        """Initialize the metric.

        Args:
            sparse (bool): Whether the metric is sparse.
            prob_type (str): The type of probability distribution of the forecast.
            valid_variables (list[str]): The variables that the metric is valid for.
                If None, the metric is valid for all variables.
            config_dict (dict): A dictionary of configuration parameters for the metric.
                For example, the bins for a contingency metric.

        """
        self.sparse = sparse
        self.prob_type = prob_type
        self.valid_variables = valid_variables
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
        # e.g., MSEMetric -> 'mse', BiasMetric -> 'bias'
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
    prob_type = 'deterministic'


class MAE(Metric):
    """Mean Absolute Error metric."""
    prob_type = 'deterministic'


class RMSE(Metric):
    """Root Mean Squared Error metric."""
    prob_type = 'deterministic'

    @property
    def statistics(self) -> list[str]:
        return ['mse']

    def compute(self, statistic_values: dict[str, xr.DataArray]) -> xr.DataArray:
        return statistic_values['mse'] ** 0.5


class BiasMetric(Metric):
    """Bias metric."""
    prob_type = 'deterministic'


class CRPS(Metric):
    """Continuous Ranked Probability Score metric."""
    prob_type = 'probabilistic'


class SMAPE(Metric):
    """Symmetric Mean Absolute Percentage Error metric."""
    prob_type = 'deterministic'


class MAPE(Metric):
    """Mean Absolute Percentage Error metric."""
    prob_type = 'deterministic'


class ACC(Metric):
    """ACC (Anomaly Correlation Coefficient) metric."""
    prob_type = 'deterministic'

    @property
    def statistics(self):
        return ['squared_pred_anom', 'squared_target_anom', 'anom_covariance']

    def compute(self, statistic_values):
        # ACC = anom_covariance / sqrt(squared_pred_anom * squared_target_anom)
        numerator = statistic_values['anom_covariance']
        denominator = (statistic_values['squared_pred_anom'] * statistic_values['squared_target_anom']) ** 0.5
        return numerator / denominator


class Pearson(Metric):
    """Pearson's correlation coefficient metric.

    Implemented with a rewrite of the standard formula to enable just-in-time aggregation.

    The standard formula is:
    r = sum((x - E(x)) * (y - E(y))) / sqrt(sum(x - E(x))^2 * sum(y - E(y))^2)

    This can be rewritten as:
    r = (n * sum(x * y) - sum(x) * sum(y)) / sqrt((n * sum(x^2) - sum(x)^2) * (n * sum(y^2) - sum(y)^2))
    """
    prob_type = 'deterministic'

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
