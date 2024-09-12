"""Data utility functions for all parts of the data pipeline."""
import numpy as np


def apply_mask(ds, mask, var, val, rename_dict={}):
    """Apply a mask to a dataset.

    Args:
        ds (xr.Dataset): Dataset to apply mask to.
        mask (xr.Dataset): Mask to apply.
        var (str): Variable to mask.
        val (int): Value to mask.
        rename_dict (dict): Dictionary to rename variables.
    """
    # Apply mask
    if mask is not None:
        # This will mask and include any location where there is any land
        ds = ds[var].where(mask >= val, drop=False)
        ds = ds.rename(rename_dict)
    return ds


def roll_and_agg(ds, agg, agg_col, agg_fn="mean"):
    """Rolling aggregation of the dataset.

    Args:
        ds (xr.Dataset): Dataset to aggregate.
        variable (str): Variable to aggregate.
        agg (int): Aggregation period in days.
        agg_col (str): Column to aggregate over.
        agg_fn (str): Aggregation function. One of:
            - mean
            - sum 
    """
    agg_kwargs = {
        f"{agg_col}": agg,
        "min_periods": agg,
        "center": False
    }
    # Apply n-day rolling aggregation
    if agg_fn == "mean":
        ds_agg = ds.rolling(**agg_kwargs).mean()
    elif agg_fn == "sum":
        ds_agg = ds.rolling(**agg_kwargs).sum()
    else:
        raise NotImplementedError(f"Aggregation function {agg_fn} not implemented.")

    # Drop the nan values added by the rolling aggregration at the end
    ds_agg = ds_agg.dropna(agg_col, how="all")

    # Correct coords to left-align the aggregated forecast window
    # (default is right aligned)
    ds_agg = ds_agg.assign_coords(**{f"{agg_col}": ds_agg[agg_col]-np.timedelta64(agg-1, 'D')})
    return ds_agg
