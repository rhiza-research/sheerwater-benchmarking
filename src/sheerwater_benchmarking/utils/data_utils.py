"""Utility functions for data processing."""


def get_grid(region_id):
    """Get the longitudes, latitudes and grid size for a named region."""
    if region_id == "global1_5":
        longitudes = ["0", "358.5"]
        latitudes = ["-90.0", "90.0"]
        grid_size = "1.5"
    elif region_id == "global0_5":
        longitudes = ["0.25", "359.75"]
        latitudes = ["-89.75", "89.75"]
        grid_size = "0.5"
    elif region_id == "us1_0":
        longitudes = ["-125.0", "-67.0"]
        latitudes = ["25.0", "50.0"]
        grid_size = "1.0"
    elif region_id == "us1_5":
        longitudes = ["-123", "-67.5"]
        latitudes = ["25.5", "48"]
        grid_size = "1.5"
    else:
        raise NotImplementedError(
            "Only grids global1_5, us1_0 and us1_5 have been implemented.")
    return longitudes, latitudes, grid_size
