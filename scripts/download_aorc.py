#!/usr/bin/env python3
"""Download and subset NOAA AORC precipitation data from public AWS Zarr."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import dask  # noqa: F401  # required dependency and scheduler backend for xarray
import numpy as np
import pandas as pd
import s3fs
import xarray as xr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

AORC_ZARR_URI = os.getenv("AORC_ZARR_URI", "s3://noaa-nws-aorc-v1-1-1km")
OUTPUT_DIR = Path("outputs")
SUBSET_NETCDF = OUTPUT_DIR / "aorc_subset_apcp_2010_2020.nc"
AREA_MEAN_CSV = OUTPUT_DIR / "aorc_area_mean_apcp_hourly_2010_2020.csv"

TIME_START = "2010-01-01T00:00:00"
TIME_END = "2020-12-31T23:00:00"
LAT_MIN, LAT_MAX = 37.60, 37.85
LON_MIN, LON_MAX = -89.35, -89.05


def _get_slice(min_value: float, max_value: float, coordinate: xr.DataArray) -> slice:
    """Return an order-safe slice for ascending or descending coordinates."""
    ascending = bool(coordinate[0] <= coordinate[-1])
    return slice(min_value, max_value) if ascending else slice(max_value, min_value)


def open_aorc_dataset(uri: str) -> xr.Dataset:
    """Open remote AORC zarr dataset from public S3."""
    logging.info("Opening AORC dataset from %s", uri)
    fs = s3fs.S3FileSystem(anon=True)
    mapper = fs.get_mapper(uri)

    # Try consolidated metadata first, then fallback.
    try:
        ds = xr.open_zarr(mapper, consolidated=True, chunks={"time": 24})
    except Exception as exc:  # noqa: BLE001
        logging.warning("Consolidated open failed (%s); retrying without consolidation", exc)
        ds = xr.open_zarr(mapper, consolidated=False, chunks={"time": 24})

    if "apcp" not in ds:
        raise KeyError("Variable 'apcp' not found in dataset.")

    return ds


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    ds = open_aorc_dataset(AORC_ZARR_URI)
    apcp = ds["apcp"]

    lat_name = "latitude" if "latitude" in apcp.coords else "lat"
    lon_name = "longitude" if "longitude" in apcp.coords else "lon"

    if lat_name not in apcp.coords or lon_name not in apcp.coords:
        raise KeyError("Could not find latitude/longitude coordinates in dataset")

    logging.info("Subsetting data in time and space")
    subset = apcp.sel(
        time=slice(TIME_START, TIME_END),
        **{
            lat_name: _get_slice(LAT_MIN, LAT_MAX, apcp[lat_name]),
            lon_name: _get_slice(LON_MIN, LON_MAX, apcp[lon_name]),
        },
    )

    if subset.sizes.get("time", 0) == 0:
        raise ValueError("No time steps found in requested subset.")

    lat_radians = np.deg2rad(subset[lat_name])
    weights = np.cos(lat_radians)
    area_mean = subset.weighted(weights).mean(dim=(lat_name, lon_name))

    logging.info("Computing QA metrics")
    expected_index = pd.date_range(TIME_START, TIME_END, freq="h", tz="UTC")
    actual_index = pd.DatetimeIndex(pd.to_datetime(area_mean.time.values, utc=True))
    missing_hours = len(expected_index.difference(actual_index))

    area_mean_series = area_mean.compute().to_series()
    max_timestamp = area_mean_series.idxmax()
    max_value = float(area_mean_series.max())

    logging.info("Missing hours in requested period: %d", missing_hours)
    logging.info("Max hourly area-mean apcp: %.4f at %s", max_value, max_timestamp)

    logging.info("Writing NetCDF subset to %s", SUBSET_NETCDF)
    subset.to_dataset(name="apcp").to_netcdf(SUBSET_NETCDF)

    logging.info("Writing area-mean CSV to %s", AREA_MEAN_CSV)
    csv_df = area_mean_series.rename("apcp_area_mean").to_frame()
    csv_df.index.name = "time"
    csv_df.to_csv(AREA_MEAN_CSV)

    logging.info("Done")


if __name__ == "__main__":
    main()
